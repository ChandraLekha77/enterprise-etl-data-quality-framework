"""Idempotent recurring-issue summary maintenance after a pipeline run."""

from __future__ import annotations

from uuid import UUID

from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from etl_framework.reporting.repository import ReportingError


UPSERT_CURRENT_ISSUES = text(
    """
    WITH current_issues AS (
        SELECT source_system, rule_id, COALESCE(field_name, '') AS field_name,
               COUNT(*)::BIGINT AS current_failure_count
          FROM quality.validation_result
         WHERE run_id = :run_id
         GROUP BY source_system, rule_id, COALESCE(field_name, '')
    ), history AS (
        SELECT result.source_system, result.rule_id,
               COALESCE(result.field_name, '') AS field_name,
               MIN(result.created_at) AS first_seen_at,
               MAX(result.created_at) AS last_seen_at,
               COUNT(DISTINCT result.run_id)::INTEGER AS affected_run_count
          FROM quality.validation_result result
          JOIN current_issues current
            ON current.source_system = result.source_system
           AND current.rule_id = result.rule_id
           AND current.field_name = COALESCE(result.field_name, '')
         GROUP BY result.source_system, result.rule_id, COALESCE(result.field_name, '')
    )
    INSERT INTO quality.issue_summary (
        source_system, rule_id, field_name, classification,
        first_seen_at, last_seen_at, affected_run_count, current_failure_count
    )
    SELECT history.source_system, history.rule_id, history.field_name,
           CASE
               WHEN history.affected_run_count = 1 THEN 'NEW'
               WHEN history.affected_run_count >= 3 THEN 'PERSISTENT'
               ELSE 'RECURRING'
           END,
           history.first_seen_at, history.last_seen_at,
           history.affected_run_count, current.current_failure_count
      FROM history
      JOIN current_issues current USING (source_system, rule_id, field_name)
    ON CONFLICT (source_system, rule_id, field_name) DO UPDATE SET
        classification = EXCLUDED.classification,
        first_seen_at = EXCLUDED.first_seen_at,
        last_seen_at = EXCLUDED.last_seen_at,
        affected_run_count = EXCLUDED.affected_run_count,
        current_failure_count = EXCLUDED.current_failure_count,
        updated_at = CURRENT_TIMESTAMP
    """
)

MARK_RESOLVED_ISSUES = text(
    """
    UPDATE quality.issue_summary summary
       SET classification = 'RESOLVED',
           current_failure_count = 0,
           updated_at = CURRENT_TIMESTAMP
     WHERE classification <> 'RESOLVED'
       AND NOT EXISTS (
           SELECT 1 FROM quality.validation_result result
            WHERE result.run_id = :run_id
              AND result.source_system = summary.source_system
              AND result.rule_id = summary.rule_id
              AND COALESCE(result.field_name, '') = summary.field_name
       )
    """
)


def refresh_recurring_issue_summaries(engine: Engine, run_id: UUID) -> None:
    """Refresh current, recurring, persistent, and resolved issue states."""
    try:
        with engine.begin() as connection:
            connection.execute(UPSERT_CURRENT_ISSUES, {"run_id": run_id})
            connection.execute(MARK_RESOLVED_ISSUES, {"run_id": run_id})
    except SQLAlchemyError as exc:
        raise ReportingError("Recurring issue summary refresh failed") from exc
