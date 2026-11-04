from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
from uuid import uuid4

import pytest

from etl_framework.reporting import (
    ReportingRepository,
    refresh_recurring_issue_summaries,
    verify_report_totals,
)


class Result:
    def __init__(self, *, scalar=None, rows=(), rowcount=1):
        self._scalar = scalar
        self._rows = tuple(rows)
        self.rowcount = rowcount

    def scalar_one(self):
        return self._scalar

    def mappings(self):
        return self

    def __iter__(self):
        return iter(self._rows)

    def one_or_none(self):
        return self._rows[0] if self._rows else None


class Connection:
    def __init__(self, results):
        self.results = list(results)
        self.calls = []

    @contextmanager
    def begin(self):
        yield self

    def execute(self, statement, parameters=None):
        self.calls.append((str(statement), parameters or {}))
        return self.results.pop(0)


class Engine:
    def __init__(self, *results):
        self.connection = Connection(results)
        self.disposed = False

    @contextmanager
    def connect(self):
        yield self.connection

    @contextmanager
    def begin(self):
        yield self.connection

    def dispose(self):
        self.disposed = True


def test_pipeline_report_is_paginated_and_database_enforced_read_only() -> None:
    now = datetime(2026, 11, 4, tzinfo=timezone.utc)
    row = {
        "run_id": uuid4(),
        "airflow_run_id": "scheduled__2026-11-04",
        "status": "SUCCEEDED",
        "started_at": now,
        "finished_at": now,
        "created_at": now,
        "batch_count": 4,
        "total_rows": 10,
        "accepted_rows": 8,
        "quarantined_rows": 1,
        "duplicate_rows": 1,
        "totals_verified": True,
        "failure_count": 1,
        "reconciliation_failure_count": 0,
    }
    engine = Engine(Result(), Result(scalar=1), Result(rows=[row]))

    report = ReportingRepository(engine).list_pipeline_runs(
        status="succeeded", page=2, page_size=25
    )

    assert report.items == (row,)
    assert report.total == 1
    assert report.page == 2 and report.page_size == 25
    assert engine.connection.calls[0][0].strip() == "SET TRANSACTION READ ONLY"
    parameters = engine.connection.calls[2][1]
    assert parameters["status"] == "SUCCEEDED"
    assert parameters["limit"] == 25 and parameters["offset"] == 25


def test_report_totals_are_verified_against_stored_counts() -> None:
    assert verify_report_totals({
        "total_rows": 12,
        "accepted_rows": 9,
        "quarantined_rows": 2,
        "duplicate_rows": 1,
    })
    assert not verify_report_totals({
        "total_rows": 12,
        "accepted_rows": 9,
        "quarantined_rows": 1,
        "duplicate_rows": 1,
    })
    with pytest.raises(ValueError, match="invalid row counts"):
        verify_report_totals({"total_rows": "secret"})


def test_reporting_filters_reject_unknown_values_before_querying() -> None:
    engine = Engine()
    repository = ReportingRepository(engine)
    with pytest.raises(ValueError, match="Unsupported severity"):
        repository.list_validation_failures(severity="critical")
    assert engine.connection.calls == []


def test_recurring_issue_refresh_is_idempotent_and_marks_resolved() -> None:
    engine = Engine(Result(), Result())
    run_id = uuid4()

    refresh_recurring_issue_summaries(engine, run_id)

    first_sql, first_parameters = engine.connection.calls[0]
    second_sql, second_parameters = engine.connection.calls[1]
    assert "ON CONFLICT (source_system, rule_id, field_name) DO UPDATE" in first_sql
    assert "classification = 'RESOLVED'" in second_sql
    assert first_parameters["run_id"] == run_id
    assert second_parameters["run_id"] == run_id


def test_public_queries_exclude_sensitive_observed_values_and_details() -> None:
    from etl_framework.reporting.repository import LIST_FAILURES, LIST_RECONCILIATIONS

    failure_sql = str(LIST_FAILURES)
    reconciliation_sql = str(LIST_RECONCILIATIONS)
    assert "observed_value_masked" not in failure_sql
    assert "business_key" not in failure_sql
    assert "details" not in failure_sql
    assert "details" not in reconciliation_sql
