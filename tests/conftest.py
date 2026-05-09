"""Shared pytest fixtures.

We make the producer and spark source trees importable here rather than via a
package install, because the project layout is intentionally service-oriented
(each component ships its own Dockerfile + requirements).
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]

# Make the per-service modules importable from tests without installing.
for sub in ("producer", "spark/jobs", "airflow/dags", "airflow/plugins"):
    path = REPO_ROOT / sub
    if path.exists() and str(path) not in sys.path:
        sys.path.insert(0, str(path))


@pytest.fixture(scope="session")
def repo_root() -> Path:
    return REPO_ROOT


@pytest.fixture(scope="session")
def spark():
    """Lightweight local SparkSession for transformation tests."""
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.master("local[2]")
        .appName("crypto-tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()
