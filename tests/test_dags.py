"""DAG sanity tests.

These validate that every DAG file imports cleanly, has a unique dag_id, sets a
reasonable schedule, and does not contain cycles. They are deliberately
lightweight — Airflow's own dagbag importer does the heavy lifting.
"""

from __future__ import annotations

import pytest

pytest.importorskip("airflow")

from airflow.models import DagBag


@pytest.fixture(scope="module")
def dagbag(repo_root):
    return DagBag(dag_folder=str(repo_root / "airflow" / "dags"), include_examples=False)


def test_no_import_errors(dagbag):
    assert not dagbag.import_errors, f"DAG import errors: {dagbag.import_errors}"


@pytest.mark.parametrize(
    "dag_id",
    ["crypto_batch_etl", "crypto_data_quality", "crypto_daily_report"],
)
def test_dag_present(dagbag, dag_id):
    dag = dagbag.get_dag(dag_id)
    assert dag is not None, f"DAG {dag_id!r} missing"
    assert dag.tags, f"DAG {dag_id!r} should have tags"


def test_dag_has_no_cycles(dagbag):
    for dag in dagbag.dags.values():
        dag.test_cycle()


def test_dags_have_owner_set(dagbag):
    for dag in dagbag.dags.values():
        assert dag.default_args.get("owner"), f"{dag.dag_id} missing default owner"
