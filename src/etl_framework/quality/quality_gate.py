"""Atomic batch quality gate and replay-safe promotion control."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import SQLAlchemyError

from etl_framework.quality.business_rules import BatchValidation
from etl_framework.quality.validation_results import UPSERT_VALIDATION_RESULT


class QualityGateError(RuntimeError):
    """Raised when a quality decision cannot be stored atomically."""


@dataclass(frozen=True, slots=True)
class QualityGateResult:
    promoted: bool
    status: str
    accepted: int
    quarantined: int
    duplicate: int


DELETE_ATTEMPT_RESULTS = text(
    "DELETE FROM quality.validation_result WHERE run_id = :run_id AND batch_id = :batch_id"
)
UPDATE_BATCH_OUTCOME = text(
    """
    UPDATE control.batch_run
    SET status = :status,
        source_row_count = :source,
        accepted_row_count = :accepted,
        quarantined_row_count = :quarantined,
        duplicate_row_count = :duplicate,
        started_at = COALESCE(started_at, CURRENT_TIMESTAMP),
        finished_at = CURRENT_TIMESTAMP,
        error_message = :error_message,
        updated_at = CURRENT_TIMESTAMP
    WHERE run_id = :run_id AND batch_id = :batch_id
    """
)


def apply_quality_gate(
    engine: Engine,
    validation: BatchValidation,
    *,
    promote: Callable[[Connection, tuple[dict[str, Any], ...]], None] | None = None,
) -> QualityGateResult:
    """Replace attempt results, save counts, and promote only an error-free batch."""
    counts = validation.counts
    status = "QUARANTINED" if validation.has_errors else "SUCCEEDED"
    try:
        with engine.begin() as connection:
            connection.execute(
                DELETE_ATTEMPT_RESULTS,
                {"run_id": validation.run_id, "batch_id": validation.batch_id},
            )
            for failure in validation.failures:
                result = connection.execute(UPSERT_VALIDATION_RESULT, failure.parameters())
                if result.rowcount == 0:
                    raise QualityGateError(
                        f"Rule {failure.rule_id} is missing, disabled, or has a severity mismatch"
                    )
            outcome = connection.execute(
                UPDATE_BATCH_OUTCOME,
                {
                    "status": status,
                    **counts.parameters(),
                    "error_message": (
                        "Mandatory data-quality errors blocked promotion"
                        if validation.has_errors else None
                    ),
                    "run_id": validation.run_id,
                    "batch_id": validation.batch_id,
                },
            )
            if outcome.rowcount != 1:
                raise QualityGateError("Registered batch attempt was not found")
            if not validation.has_errors and promote is not None:
                promote(connection, tuple(dict(row) for row in validation.accepted_records))
    except QualityGateError:
        raise
    except Exception as exc:
        raise QualityGateError("Quality gate failed") from exc
    return QualityGateResult(
        promoted=not validation.has_errors,
        status=status,
        accepted=counts.accepted,
        quarantined=counts.quarantined,
        duplicate=counts.duplicate,
    )
