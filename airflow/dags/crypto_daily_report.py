"""Daily summary DAG.

Runs at 00:10 UTC every day. Builds the daily aggregate and emits a small
human-readable report. Slack/email delivery is stubbed (the report is logged
and stored in XCom) so the DAG works out of the box without extra credentials.
"""

from __future__ import annotations

from datetime import timedelta
from textwrap import dedent

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

DAG_ID = "crypto_daily_report"
DEFAULT_ARGS = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": False,
}


def _build_report(**context: object) -> str:
    pg = PostgresHook(postgres_conn_id="crypto_pg")
    rows = pg.get_records(
        """
        SELECT symbol,
               trade_date,
               open_usd,
               high_usd,
               low_usd,
               close_usd,
               price_change_pct,
               tick_count
        FROM agg_price_daily
        WHERE trade_date = (CURRENT_DATE - INTERVAL '1 day')::date
        ORDER BY price_change_pct DESC NULLS LAST;
        """
    )
    if not rows:
        report = "No daily data available for yesterday."
    else:
        header = (
            f"{'SYM':<6}{'DATE':<12}{'OPEN':>14}{'HIGH':>14}"
            f"{'LOW':>14}{'CLOSE':>14}{'CHG%':>9}{'TICKS':>8}"
        )
        body_lines = [
            (
                f"{symbol:<6}{str(trade_date):<12}{(open_usd or 0):>14.4f}"
                f"{(high_usd or 0):>14.4f}{(low_usd or 0):>14.4f}"
                f"{(close_usd or 0):>14.4f}{(pct or 0):>9.2f}{ticks:>8}"
            )
            for (symbol, trade_date, open_usd, high_usd, low_usd, close_usd, pct, ticks) in rows
        ]
        report = "\n".join([header, "-" * len(header), *body_lines])

    print(report)  # surfaces in Airflow task log
    context["ti"].xcom_push(key="report", value=report)  # type: ignore[union-attr]
    return report


with DAG(
    dag_id=DAG_ID,
    description="Daily aggregate + human-readable report.",
    default_args=DEFAULT_ARGS,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="10 0 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["crypto", "report", "daily"],
    doc_md=dedent(
        """
        ### crypto_daily_report

        Runs at **00:10 UTC** every day:

        1. Triggers the Spark batch transformer in ``daily`` mode.
        2. Builds a sorted leaderboard of yesterday's price change.
        3. Records a row in ``data_quality_results`` for observability.
        """
    ),
) as dag:
    rollup_daily = BashOperator(
        task_id="rollup_daily",
        bash_command=(
            "docker exec crypto-spark-stream "
            "spark-submit "
            "--packages org.postgresql:postgresql:42.7.3 "
            "/opt/spark/jobs/batch_transformer.py daily "
            "{{ data_interval_start.subtract(days=1).isoformat() }} "
            "{{ data_interval_start.isoformat() }}"
        ),
    )

    build_report = PythonOperator(
        task_id="build_report",
        python_callable=_build_report,
    )

    rollup_daily >> build_report
