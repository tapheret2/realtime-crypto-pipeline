"""Tests for the custom Airflow operator."""

from __future__ import annotations

import pytest

pytest.importorskip("airflow")

from operators.crypto_quality_operator import RecordDqMetricOperator


def test_invalid_status_rejected():
    with pytest.raises(ValueError, match="invalid status"):
        RecordDqMetricOperator(
            task_id="bad",
            check_name="x",
            check_target="y",
            status="UNKNOWN",
        )


def test_valid_construction():
    op = RecordDqMetricOperator(
        task_id="dq",
        check_name="row_count",
        check_target="fact_price_tick",
        status="PASS",
        metric_value=42,
        threshold=10,
        details="all good",
    )
    assert op.check_name == "row_count"
    assert op.status == "PASS"
    assert op.metric_value == 42
