"""Data quality DAG.

Runs every 15 minutes. Each check is a SQL statement that returns a metric and
a status; the results are appended to ``data_quality_results`` so they can be
trended on the dashboard. Failed checks turn the DAG run red but do not block
downstream pipelines — alerting belongs in a separate channel.
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

DAG_ID = "crypto_data_quality"
DEFAULT_ARGS = {
    "owner": "data-eng",
    "retries": 0,
    "email_on_failure": False,
}

CHECKS: list[dict[str, object]] = [
    {
        "name": "raw_freshness",
        "target": "fact_price_tick",
        "metric_sql": "SELECT EXTRACT(EPOCH FROM (NOW() - MAX(event_time))) FROM fact_price_tick",
        "threshold": 300,  # seconds
        "operator": "<=",
        "details": "Age of latest raw tick in seconds.",
    },
    {
        "name": "minute_freshness",
        "target": "agg_price_minute",
        "metric_sql": "SELECT EXTRACT(EPOCH FROM (NOW() - MAX(window_start))) FROM agg_price_minute",
        "threshold": 600,
        "operator": "<=",
        "details": "Age of latest minute aggregate in seconds.",
    },
    {
        "name": "null_price_rate_5m",
        "target": "fact_price_tick",
        "metric_sql": (
            "SELECT COALESCE(AVG(CASE WHEN price_usd IS NULL THEN 1 ELSE 0 END), 0) "
            "FROM fact_price_tick "
            "WHERE event_time >= NOW() - INTERVAL '5 minutes'"
        ),
        "threshold": 0.01,
        "operator": "<=",
        "details": "Fraction of NULL prices in the last 5 minutes.",
    },
    {
        "name": "asset_coverage_5m",
        "target": "fact_price_tick",
        "metric_sql": (
            "SELECT COUNT(DISTINCT asset_id) "
            "FROM fact_price_tick "
            "WHERE event_time >= NOW() - INTERVAL '5 minutes'"
        ),
        "threshold": 1,
        "operator": ">=",
        "details": "Distinct assets observed in the last 5 minutes.",
    },
]


def _evaluate(metric: float | None, threshold: float, op: str) -> str:
    if metric is None:
        return "WARN"
    if op == "<=":
        return "PASS" if metric <= threshold else "FAIL"
    if op == ">=":
        return "PASS" if metric >= threshold else "FAIL"
    return "WARN"


def run_checks(**_context: object) -> None:
    pg = PostgresHook(postgres_conn_id="crypto_pg")
    failures: list[str] = []
    for check in CHECKS:
        with pg.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(str(check["metric_sql"]))
                row = cur.fetchone()
                metric = float(row[0]) if row and row[0] is not None else None
                status = _evaluate(metric, float(check["threshold"]), str(check["operator"]))
                cur.execute(
                    """
                    INSERT INTO data_quality_results
                        (check_name, check_target, status, metric_value, threshold, details)
                    VALUES (%s, %s, %s, %s, %s, %s);
                    """,
                    (
                        check["name"],
                        check["target"],
                        status,
                        metric,
                        check["threshold"],
                        check["details"],
                    ),
                )
                conn.commit()
        if status == "FAIL":
            failures.append(f"{check['name']} metric={metric} threshold={check['threshold']}")

    if failures:
        raise AirflowFailException("DQ failures: " + "; ".join(failures))


with DAG(
    dag_id=DAG_ID,
    description="Periodic freshness, completeness, and validity checks.",
    default_args=DEFAULT_ARGS,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=timedelta(minutes=15),
    catchup=False,
    max_active_runs=1,
    tags=["crypto", "data-quality"],
) as dag:
    PythonOperator(
        task_id="run_checks",
        python_callable=run_checks,
    )
