"""Tests for the data quality DAG's evaluation logic."""

from __future__ import annotations

import pytest

from crypto_data_quality import _evaluate


@pytest.mark.parametrize(
    "metric,threshold,op,expected",
    [
        (10, 60, "<=", "PASS"),
        (61, 60, "<=", "FAIL"),
        (60, 60, "<=", "PASS"),
        (5, 1, ">=", "PASS"),
        (0, 1, ">=", "FAIL"),
        (None, 60, "<=", "WARN"),
        (10, 60, "??", "WARN"),
    ],
)
def test_evaluate_returns_expected_status(metric, threshold, op, expected):
    assert _evaluate(metric, threshold, op) == expected
