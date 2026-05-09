"""Custom Airflow operator that wraps ``data_quality_results`` writes.

Most of the data quality DAG inserts metrics inline; this operator lets ad-hoc
checks elsewhere (notebook tasks, manual reruns) record into the same table
without copy-pasting SQL. Importable as
``from operators.crypto_quality_operator import RecordDqMetricOperator``.
"""

from __future__ import annotations

from typing import Any

from airflow.models.baseoperator import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.context import Context


class RecordDqMetricOperator(BaseOperator):
    template_fields = ("check_name", "check_target", "details")

    def __init__(
        self,
        *,
        check_name: str,
        check_target: str,
        status: str,
        metric_value: float | None = None,
        threshold: float | None = None,
        details: str | None = None,
        postgres_conn_id: str = "crypto_pg",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        if status not in {"PASS", "WARN", "FAIL"}:
            raise ValueError(f"invalid status {status!r}")
        self.check_name = check_name
        self.check_target = check_target
        self.status = status
        self.metric_value = metric_value
        self.threshold = threshold
        self.details = details
        self.postgres_conn_id = postgres_conn_id

    def execute(self, context: Context) -> None:
        pg = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        pg.run(
            """
            INSERT INTO data_quality_results
                (check_name, check_target, status, metric_value, threshold, details)
            VALUES (%s, %s, %s, %s, %s, %s);
            """,
            parameters=(
                self.check_name,
                self.check_target,
                self.status,
                self.metric_value,
                self.threshold,
                self.details,
            ),
        )
