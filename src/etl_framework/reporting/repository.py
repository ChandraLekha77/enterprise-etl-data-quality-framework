"""Parameterized, read-only queries for pipeline and data-quality reports."""

from __future__ import annotations

from collections.abc import Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Iterator
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import SQLAlchemyError


ALLOWED_SOURCES = frozenset({"CRM_PG", "BILLING_MSSQL", "OPS_CSV", "STATUS_API"})
ALLOWED_RUN_STATUSES = frozenset(
    {"PENDING", "RUNNING", "QUARANTINED", "FAILED", "SUCCEEDED", "CANCELLED"}
)
ALLOWED_SEVERITIES = frozenset({"ERROR", "WARNING", "INFO"})
ALLOWED_RECONCILIATION_STATUSES = frozenset({"PASS", "FAIL"})
ALLOWED_ISSUE_CLASSIFICATIONS = frozenset(
    {"NEW", "RECURRING", "PERSISTENT", "RESOLVED"}
)


class ReportingError(RuntimeError):
    """Raised without database details when a report cannot be produced."""


@dataclass(frozen=True, slots=True)
class PageResult:
    items: tuple[dict[str, Any], ...]
    page: int
    page_size: int
    total: int


RUN_SUMMARY_COLUMNS = """
    run.run_id, run.airflow_run_id, run.status, run.started_at, run.finished_at,
    run.created_at,
    COALESCE(batch.batch_count, 0)::BIGINT AS batch_count,
    COALESCE(batch.total_rows, 0)::BIGINT AS total_rows,
    COALESCE(batch.accepted_rows, 0)::BIGINT AS accepted_rows,
    COALESCE(batch.quarantined_rows, 0)::BIGINT AS quarantined_rows,
    COALESCE(batch.duplicate_rows, 0)::BIGINT AS duplicate_rows,
    COALESCE(batch.totals_verified, TRUE) AS totals_verified,
    COALESCE(failure.failure_count, 0)::BIGINT AS failure_count,
    COALESCE(reconciliation.reconciliation_failure_count, 0)::BIGINT
        AS reconciliation_failure_count
"""

RUN_SUMMARY_JOINS = """
    LEFT JOIN LATERAL (
        SELECT COUNT(*) AS batch_count,
               COALESCE(SUM(source_row_count), 0) AS total_rows,
               COALESCE(SUM(accepted_row_count), 0) AS accepted_rows,
               COALESCE(SUM(quarantined_row_count), 0) AS quarantined_rows,
               COALESCE(SUM(duplicate_row_count), 0) AS duplicate_rows,
               BOOL_AND(
                   status NOT IN ('SUCCEEDED', 'QUARANTINED')
                   OR source_row_count = accepted_row_count
                      + quarantined_row_count + duplicate_row_count
               ) AS totals_verified
          FROM control.batch_run
         WHERE run_id = run.run_id
    ) batch ON TRUE
    LEFT JOIN LATERAL (
        SELECT COUNT(*) AS failure_count
          FROM quality.validation_result
         WHERE run_id = run.run_id
    ) failure ON TRUE
    LEFT JOIN LATERAL (
        SELECT COUNT(*) FILTER (WHERE status = 'FAIL')
                   AS reconciliation_failure_count
          FROM quality.reconciliation_result
         WHERE run_id = run.run_id
    ) reconciliation ON TRUE
"""

LIST_RUNS = text(
    f"""
    SELECT {RUN_SUMMARY_COLUMNS}
      FROM control.pipeline_run run
      {RUN_SUMMARY_JOINS}
     WHERE (:status IS NULL OR run.status = :status)
     ORDER BY run.created_at DESC, run.run_id DESC
     LIMIT :limit OFFSET :offset
    """
)
COUNT_RUNS = text(
    "SELECT COUNT(*) FROM control.pipeline_run WHERE (:status IS NULL OR status = :status)"
)
GET_RUN = text(
    f"""
    SELECT {RUN_SUMMARY_COLUMNS}
      FROM control.pipeline_run run
      {RUN_SUMMARY_JOINS}
     WHERE run.run_id = :run_id
    """
)

LIST_BATCHES = text(
    """
    SELECT batch.batch_run_id, batch.run_id, batch.batch_id,
           source.source_system, source.delivery_key, batch.status,
           COALESCE(batch.source_row_count, 0)::BIGINT AS total_rows,
           COALESCE(batch.accepted_row_count, 0)::BIGINT AS accepted_rows,
           COALESCE(batch.quarantined_row_count, 0)::BIGINT AS quarantined_rows,
           COALESCE(batch.duplicate_row_count, 0)::BIGINT AS duplicate_rows,
           CASE WHEN batch.status IN ('SUCCEEDED', 'QUARANTINED') THEN
               batch.source_row_count = batch.accepted_row_count
                   + batch.quarantined_row_count + batch.duplicate_row_count
           ELSE TRUE END AS totals_verified,
           batch.started_at, batch.finished_at, batch.created_at
      FROM control.batch_run batch
      JOIN control.source_batch source USING (batch_id)
     WHERE (:run_id IS NULL OR batch.run_id = :run_id)
       AND (:source_system IS NULL OR source.source_system = :source_system)
       AND (:status IS NULL OR batch.status = :status)
     ORDER BY batch.created_at DESC, batch.batch_run_id DESC
     LIMIT :limit OFFSET :offset
    """
)
COUNT_BATCHES = text(
    """
    SELECT COUNT(*)
      FROM control.batch_run batch
      JOIN control.source_batch source USING (batch_id)
     WHERE (:run_id IS NULL OR batch.run_id = :run_id)
       AND (:source_system IS NULL OR source.source_system = :source_system)
       AND (:status IS NULL OR batch.status = :status)
    """
)

LIST_FAILURES = text(
    """
    SELECT validation_result_id, run_id, batch_id, source_system, rule_id,
           severity, field_name, message, created_at
      FROM quality.validation_result
     WHERE (:run_id IS NULL OR run_id = :run_id)
       AND (:source_system IS NULL OR source_system = :source_system)
       AND (:rule_id IS NULL OR rule_id = :rule_id)
       AND (:severity IS NULL OR severity = :severity)
     ORDER BY created_at DESC, validation_result_id DESC
     LIMIT :limit OFFSET :offset
    """
)
COUNT_FAILURES = text(
    """
    SELECT COUNT(*) FROM quality.validation_result
     WHERE (:run_id IS NULL OR run_id = :run_id)
       AND (:source_system IS NULL OR source_system = :source_system)
       AND (:rule_id IS NULL OR rule_id = :rule_id)
       AND (:severity IS NULL OR severity = :severity)
    """
)
FAILURE_GROUPS = text(
    """
    SELECT source_system, rule_id, severity, COUNT(*)::BIGINT AS failure_count
      FROM quality.validation_result
     WHERE (:run_id IS NULL OR run_id = :run_id)
     GROUP BY source_system, rule_id, severity
     ORDER BY failure_count DESC, source_system, rule_id, severity
    """
)

LIST_RECONCILIATIONS = text(
    """
    SELECT reconciliation_result_id, run_id, batch_id, rule_id, metric_name,
           group_key, source_value, target_value, difference, tolerance,
           status, created_at
      FROM quality.reconciliation_result
     WHERE (:run_id IS NULL OR run_id = :run_id)
       AND (:rule_id IS NULL OR rule_id = :rule_id)
       AND (:status IS NULL OR status = :status)
       AND (:group_key IS NULL OR group_key = :group_key)
     ORDER BY created_at DESC, reconciliation_result_id DESC
     LIMIT :limit OFFSET :offset
    """
)
COUNT_RECONCILIATIONS = text(
    """
    SELECT COUNT(*) FROM quality.reconciliation_result
     WHERE (:run_id IS NULL OR run_id = :run_id)
       AND (:rule_id IS NULL OR rule_id = :rule_id)
       AND (:status IS NULL OR status = :status)
       AND (:group_key IS NULL OR group_key = :group_key)
    """
)
FINANCIAL_TOTALS = text(
    """
    SELECT batch.source_system, result.batch_id, result.rule_id,
           result.metric_name, result.group_key AS currency_code,
           result.source_value, result.target_value, result.difference,
           result.status
      FROM quality.reconciliation_result result
      JOIN control.source_batch batch USING (batch_id)
     WHERE result.run_id = :run_id
       AND result.rule_id IN ('RC-FIN-001', 'RC-FIN-002')
     ORDER BY result.group_key, result.metric_name, result.batch_id
    """
)

LIST_ISSUES = text(
    """
    SELECT source_system, rule_id, NULLIF(field_name, '') AS field_name,
           classification, first_seen_at, last_seen_at, affected_run_count,
           current_failure_count, updated_at
      FROM quality.issue_summary
     WHERE (:source_system IS NULL OR source_system = :source_system)
       AND (:rule_id IS NULL OR rule_id = :rule_id)
       AND (:classification IS NULL OR classification = :classification)
     ORDER BY last_seen_at DESC, source_system, rule_id, field_name
     LIMIT :limit OFFSET :offset
    """
)
COUNT_ISSUES = text(
    """
    SELECT COUNT(*) FROM quality.issue_summary
     WHERE (:source_system IS NULL OR source_system = :source_system)
       AND (:rule_id IS NULL OR rule_id = :rule_id)
       AND (:classification IS NULL OR classification = :classification)
    """
)


class ReportingRepository:
    """Execute reporting queries inside database-enforced read-only transactions."""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def health(self) -> bool:
        with self._read_connection() as connection:
            return connection.execute(text("SELECT 1")).scalar_one() == 1

    def list_pipeline_runs(
        self, *, status: str | None = None, page: int = 1, page_size: int = 50
    ) -> PageResult:
        status = _optional_choice(status, ALLOWED_RUN_STATUSES, "status")
        return self._page(
            LIST_RUNS, COUNT_RUNS, {"status": status}, page=page, page_size=page_size
        )

    def get_pipeline_run(self, run_id: UUID) -> dict[str, Any] | None:
        return self._one(GET_RUN, {"run_id": run_id})

    def list_source_batches(
        self,
        *,
        run_id: UUID | None = None,
        source_system: str | None = None,
        status: str | None = None,
        page: int = 1,
        page_size: int = 50,
    ) -> PageResult:
        source_system = _optional_choice(
            source_system, ALLOWED_SOURCES, "source_system"
        )
        status = _optional_choice(status, ALLOWED_RUN_STATUSES, "status")
        return self._page(
            LIST_BATCHES,
            COUNT_BATCHES,
            {"run_id": run_id, "source_system": source_system, "status": status},
            page=page,
            page_size=page_size,
        )

    def list_validation_failures(
        self,
        *,
        run_id: UUID | None = None,
        source_system: str | None = None,
        rule_id: str | None = None,
        severity: str | None = None,
        page: int = 1,
        page_size: int = 50,
    ) -> PageResult:
        source_system = _optional_choice(
            source_system, ALLOWED_SOURCES, "source_system"
        )
        severity = _optional_choice(severity, ALLOWED_SEVERITIES, "severity")
        rule_id = _optional_text(rule_id, "rule_id", maximum=30)
        return self._page(
            LIST_FAILURES,
            COUNT_FAILURES,
            {
                "run_id": run_id,
                "source_system": source_system,
                "rule_id": rule_id,
                "severity": severity,
            },
            page=page,
            page_size=page_size,
        )

    def failure_groups(self, *, run_id: UUID | None = None) -> tuple[dict[str, Any], ...]:
        return self._many(FAILURE_GROUPS, {"run_id": run_id})

    def list_reconciliations(
        self,
        *,
        run_id: UUID | None = None,
        rule_id: str | None = None,
        status: str | None = None,
        group_key: str | None = None,
        page: int = 1,
        page_size: int = 50,
    ) -> PageResult:
        rule_id = _optional_text(rule_id, "rule_id", maximum=30)
        status = _optional_choice(
            status, ALLOWED_RECONCILIATION_STATUSES, "status"
        )
        group_key = _optional_text(group_key, "group_key", maximum=50)
        return self._page(
            LIST_RECONCILIATIONS,
            COUNT_RECONCILIATIONS,
            {
                "run_id": run_id,
                "rule_id": rule_id,
                "status": status,
                "group_key": group_key,
            },
            page=page,
            page_size=page_size,
        )

    def financial_totals(self, run_id: UUID) -> tuple[dict[str, Any], ...]:
        return self._many(FINANCIAL_TOTALS, {"run_id": run_id})

    def list_recurring_issues(
        self,
        *,
        source_system: str | None = None,
        rule_id: str | None = None,
        classification: str | None = None,
        page: int = 1,
        page_size: int = 50,
    ) -> PageResult:
        source_system = _optional_choice(
            source_system, ALLOWED_SOURCES, "source_system"
        )
        rule_id = _optional_text(rule_id, "rule_id", maximum=30)
        classification = _optional_choice(
            classification, ALLOWED_ISSUE_CLASSIFICATIONS, "classification"
        )
        return self._page(
            LIST_ISSUES,
            COUNT_ISSUES,
            {
                "source_system": source_system,
                "rule_id": rule_id,
                "classification": classification,
            },
            page=page,
            page_size=page_size,
        )

    def close(self) -> None:
        self._engine.dispose()

    def _one(self, statement: Any, parameters: Mapping[str, Any]) -> dict[str, Any] | None:
        with self._read_connection() as connection:
            row = connection.execute(statement, dict(parameters)).mappings().one_or_none()
            return dict(row) if row is not None else None

    def _many(self, statement: Any, parameters: Mapping[str, Any]) -> tuple[dict[str, Any], ...]:
        with self._read_connection() as connection:
            rows = connection.execute(statement, dict(parameters)).mappings()
            return tuple(dict(row) for row in rows)

    def _page(
        self,
        statement: Any,
        count_statement: Any,
        parameters: Mapping[str, Any],
        *,
        page: int,
        page_size: int,
    ) -> PageResult:
        page, page_size = _pagination(page, page_size)
        query_parameters = {
            **parameters,
            "limit": page_size,
            "offset": (page - 1) * page_size,
        }
        with self._read_connection() as connection:
            total = int(connection.execute(count_statement, dict(parameters)).scalar_one())
            rows = connection.execute(statement, query_parameters).mappings()
            items = tuple(dict(row) for row in rows)
        return PageResult(items, page, page_size, total)

    @contextmanager
    def _read_connection(self) -> Iterator[Connection]:
        try:
            with self._engine.connect() as connection:
                with connection.begin():
                    connection.execute(text("SET TRANSACTION READ ONLY"))
                    yield connection
        except ReportingError:
            raise
        except SQLAlchemyError as exc:
            raise ReportingError("Reporting database operation failed") from exc


def verify_report_totals(summary: Mapping[str, Any]) -> bool:
    """Verify displayed row totals against the four stored count fields."""
    try:
        total = int(summary["total_rows"])
        accounted = sum(
            int(summary[name])
            for name in ("accepted_rows", "quarantined_rows", "duplicate_rows")
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError("Report summary contains invalid row counts") from exc
    return total == accounted


def _pagination(page: int, page_size: int) -> tuple[int, int]:
    if isinstance(page, bool) or not isinstance(page, int) or page < 1:
        raise ValueError("page must be at least 1")
    if (
        isinstance(page_size, bool)
        or not isinstance(page_size, int)
        or not 1 <= page_size <= 100
    ):
        raise ValueError("page_size must be between 1 and 100")
    return page, page_size


def _optional_choice(value: str | None, choices: frozenset[str], label: str) -> str | None:
    if value is None:
        return None
    normalized = value.strip().upper()
    if normalized not in choices:
        raise ValueError(f"Unsupported {label}")
    return normalized


def _optional_text(value: str | None, label: str, *, maximum: int) -> str | None:
    if value is None:
        return None
    normalized = value.strip()
    if not normalized or len(normalized) > maximum:
        raise ValueError(f"Invalid {label}")
    return normalized
