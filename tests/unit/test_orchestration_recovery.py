from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import pytest

from etl_framework.orchestration.pipeline import (
    ConflictingDeliveryError,
    acquire_delivery_lease,
    ensure_batch_running,
    ensure_pipeline_running,
    publish_pipeline_status,
    read_source_watermark,
    release_delivery_lease,
)


class Result:
    def __init__(self, *, scalar=None, row=None, rowcount=1):
        self._scalar = scalar
        self._row = row
        self.rowcount = rowcount

    def scalar_one_or_none(self):
        return self._scalar

    def mappings(self):
        return self

    def one_or_none(self):
        return self._row


class ScriptedConnection:
    def __init__(self, results):
        self.results = list(results)
        self.calls = []

    def execute(self, statement, parameters=None):
        self.calls.append((str(statement), parameters or {}))
        return self.results.pop(0)


class ScriptedEngine:
    def __init__(self, *results):
        self.connection = ScriptedConnection(results)

    @contextmanager
    def begin(self):
        yield self.connection

    @contextmanager
    def connect(self):
        yield self.connection


def test_pipeline_resume_is_idempotent_for_running_attempt() -> None:
    engine = ScriptedEngine(Result(scalar="RUNNING"), Result())
    ensure_pipeline_running(engine, uuid4())
    assert "FOR UPDATE" in engine.connection.calls[0][0]
    assert "status = 'RUNNING'" in engine.connection.calls[1][0]


def test_successful_pipeline_is_not_reset_during_replay() -> None:
    engine = ScriptedEngine(Result(scalar="SUCCEEDED"))
    ensure_pipeline_running(engine, uuid4())
    assert len(engine.connection.calls) == 1


def test_batch_resume_allows_incomplete_and_corrected_attempts() -> None:
    engine = ScriptedEngine(Result(rowcount=1))
    ensure_batch_running(engine, uuid4())
    sql = engine.connection.calls[0][0]
    assert "'PENDING', 'RUNNING', 'FAILED', 'QUARANTINED'" in sql
    assert "SUCCEEDED" not in sql


def test_conflicting_delivery_is_rejected_without_overwrite() -> None:
    engine = ScriptedEngine(Result(row=None))
    with pytest.raises(ConflictingDeliveryError, match="active run"):
        acquire_delivery_lease(
            engine,
            source_system="CRM_PG",
            delivery_key="window-1",
            owner_run_id=uuid4(),
        )


def test_same_owner_can_renew_or_expired_owner_can_reclaim_lease() -> None:
    run_id = uuid4()
    expires = datetime(2026, 11, 3, 12, tzinfo=timezone.utc)
    engine = ScriptedEngine(Result(row={
        "source_system": "STATUS_API",
        "delivery_key": "window-1",
        "owner_run_id": run_id,
        "expires_at": expires,
    }))
    lease = acquire_delivery_lease(
        engine,
        source_system="STATUS_API",
        delivery_key="window-1",
        owner_run_id=run_id,
    )
    assert lease.owner_run_id == run_id
    sql = engine.connection.calls[0][0]
    assert "owner_run_id = EXCLUDED.owner_run_id" in sql
    assert "expires_at <= CURRENT_TIMESTAMP" in sql


def test_only_owner_can_release_delivery_lease() -> None:
    engine = ScriptedEngine(Result(rowcount=0))
    released = release_delivery_lease(
        engine,
        source_system="OPS_CSV",
        delivery_key="file:checksum",
        owner_run_id=uuid4(),
    )
    assert released is False
    assert "owner_run_id = :owner_run_id" in engine.connection.calls[0][0]


def test_watermark_requires_valid_timezone() -> None:
    engine = ScriptedEngine(Result(scalar="2026-11-03T10:00:00"))
    with pytest.raises(Exception, match="timezone"):
        read_source_watermark(engine, "CRM_PG")


def test_final_status_does_not_overwrite_success() -> None:
    engine = ScriptedEngine(Result())
    publish_pipeline_status(engine, uuid4(), "FAILED", error_message="safe")
    sql, parameters = engine.connection.calls[0]
    assert "status <> 'SUCCEEDED'" in sql
    assert parameters["status"] == "FAILED"


def test_airflow_dag_contract_has_bounded_and_nonretryable_paths() -> None:
    dag = (
        Path(__file__).parents[2]
        / "airflow"
        / "dags"
        / "enterprise_etl_pipeline.py"
    ).read_text(encoding="utf-8")
    assert "TECHNICAL_RETRIES = 2" in dag
    assert "retry_exponential_backoff" in dag
    assert "max_active_runs=1" in dag
    assert "AirflowFailException" in dag
    assert "QUARANTINED outcome" in dag
    assert "trigger_rule=TriggerRule.ALL_DONE" in dag
    for source in ("CRM_PG", "BILLING_MSSQL", "OPS_CSV", "STATUS_API"):
        assert f'"{source}"' in dag
