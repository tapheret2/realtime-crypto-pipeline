"""Hourly batch ETL: roll fact_price_tick + agg_price_minute up into agg_price_hourly.

Scheduled at minute 5 of every hour so the previous full hour's minute aggregates
have settled. Idempotent — re-running for the same logical hour replaces the
aggregate row via the UPSERT inside ``batch_transformer.py``.
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

DAG_ID = "crypto_batch_etl"
DEFAULT_ARGS = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    dag_id=DAG_ID,
    description="Hourly rollup: fact_price_tick + agg_price_minute -> agg_price_hourly",
    default_args=DEFAULT_ARGS,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="5 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["crypto", "etl", "hourly"],
) as dag:
    upsert_dim_asset_from_ticks = PostgresOperator(
        task_id="refresh_dim_asset",
        postgres_conn_id="crypto_pg",
        sql="""
            INSERT INTO dim_asset (asset_id, symbol, name, market_cap_rank, updated_at)
            SELECT DISTINCT ON (asset_id)
                   asset_id, symbol, name, market_cap_rank, NOW()
            FROM fact_price_tick
            WHERE event_time >= NOW() - INTERVAL '2 hours'
            ORDER BY asset_id, event_time DESC
            ON CONFLICT (asset_id) DO UPDATE
            SET symbol          = EXCLUDED.symbol,
                name            = EXCLUDED.name,
                market_cap_rank = EXCLUDED.market_cap_rank,
                updated_at      = NOW();
        """,
    )

    # The Spark batch transformer reads minute aggregates and upserts the hourly
    # rollup. We invoke it via spark-submit inside the spark-stream container so
    # we don't have to ship Java in the Airflow image's classpath.
    rollup_hourly = BashOperator(
        task_id="rollup_hourly",
        bash_command=(
            "docker exec crypto-spark-stream "
            "spark-submit "
            "--packages org.postgresql:postgresql:42.7.3 "
            "/opt/spark/jobs/batch_transformer.py hourly "
            "{{ data_interval_start.subtract(hours=1).isoformat() }} "
            "{{ data_interval_start.isoformat() }}"
        ),
    )

    record_run = PostgresOperator(
        task_id="record_run",
        postgres_conn_id="crypto_pg",
        sql="""
            INSERT INTO data_quality_results
                (check_name, check_target, status, metric_value, threshold, details)
            SELECT 'hourly_rollup_completed',
                   'agg_price_hourly',
                   'PASS',
                   COUNT(*),
                   1,
                   'rollup completed via Airflow run {{ run_id }}'
            FROM agg_price_hourly
            WHERE window_start = date_trunc('hour', NOW() - INTERVAL '1 hour');
        """,
    )

    upsert_dim_asset_from_ticks >> rollup_hourly >> record_run
