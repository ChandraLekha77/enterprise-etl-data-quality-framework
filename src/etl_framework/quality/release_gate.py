"""Transactional promotion, reconciliation, release, and watermark advancement."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine

from etl_framework.quality.business_rules import BatchValidation
from etl_framework.quality.quality_gate import (
    DELETE_ATTEMPT_RESULTS,
    UPDATE_BATCH_OUTCOME,
    apply_quality_gate,
)
from etl_framework.quality.reconciliation import (
    ReconciliationResult,
    _persist_reconciliations,
    reconcile_financial_totals,
    reconcile_keys,
    reconcile_row_counts,
)
from etl_framework.quality.validation_results import UPSERT_VALIDATION_RESULT


class ReleaseGateError(RuntimeError):
    """Raised when promotion or release cannot complete safely."""


class _ReconciliationFailed(Exception):
    def __init__(self, results: tuple[ReconciliationResult, ...]) -> None:
        self.results = results


@dataclass(frozen=True, slots=True)
class ReleaseGateResult:
    released: bool
    status: str
    reconciliations: tuple[ReconciliationResult, ...]


DELETE_RECONCILIATIONS = text(
    "DELETE FROM quality.reconciliation_result WHERE run_id = :run_id AND batch_id = :batch_id"
)
ADVANCE_WATERMARK = text(
    """
    INSERT INTO control.source_watermark (
        source_system, watermark_value, successful_run_id, updated_at
    ) VALUES (
        :source_system, :watermark_value, :run_id, CURRENT_TIMESTAMP
    )
    ON CONFLICT (source_system) DO UPDATE SET
        watermark_value = EXCLUDED.watermark_value,
        successful_run_id = EXCLUDED.successful_run_id,
        updated_at = CURRENT_TIMESTAMP
    """
)


def apply_release_gate(
    engine: Engine,
    validation: BatchValidation,
    *,
    watermark_value: str,
    promote: Callable[[Connection, tuple[dict[str, Any], ...]], None],
    load_target_keys: Callable[[Connection], Iterable[str]],
    load_target_financial_rows: Callable[[Connection], Iterable[Mapping[str, Any]]] | None = None,
) -> ReleaseGateResult:
    """Promote, reconcile, and advance watermark as one successful unit of work."""
    if not watermark_value.strip():
        raise ValueError("watermark_value must not be blank")
    if validation.has_errors:
        apply_quality_gate(engine, validation)
        return ReleaseGateResult(False, "QUARANTINED", ())

    results: tuple[ReconciliationResult, ...] = ()
    try:
        with engine.begin() as connection:
            _replace_attempt_evidence(connection, validation)
            accepted_rows = tuple(dict(row) for row in validation.accepted_records)
            promote(connection, accepted_rows)
            results = _calculate_reconciliations(
                validation,
                target_keys=load_target_keys(connection),
                target_financial_rows=(
                    load_target_financial_rows(connection)
                    if load_target_financial_rows is not None else ()
                ),
            )
            if any(result.status == "FAIL" for result in results):
                raise _ReconciliationFailed(results)
            _persist_reconciliations(connection, results)
            _update_batch(connection, validation, "SUCCEEDED", None)
            connection.execute(
                ADVANCE_WATERMARK,
                {
                    "source_system": validation.source_system,
                    "watermark_value": watermark_value,
                    "run_id": validation.run_id,
                },
            )
    except _ReconciliationFailed as failure:
        _persist_failed_release(engine, validation, failure.results)
        return ReleaseGateResult(False, "QUARANTINED", failure.results)
    except Exception as exc:
        raise ReleaseGateError("Release gate failed") from exc
    return ReleaseGateResult(True, "SUCCEEDED", results)


def _calculate_reconciliations(
    validation: BatchValidation,
    *,
    target_keys: Iterable[str],
    target_financial_rows: Iterable[Mapping[str, Any]],
) -> tuple[ReconciliationResult, ...]:
    counts = validation.counts
    results = [reconcile_row_counts(
        run_id=validation.run_id,
        batch_id=validation.batch_id,
        source_rows=counts.source,
        accepted=counts.accepted,
        quarantined=counts.quarantined,
        duplicates=counts.duplicate,
    )]
    results.extend(reconcile_keys(
        run_id=validation.run_id,
        batch_id=validation.batch_id,
        accepted_source_keys=(record.business_key for record in validation.records if record.disposition.value == "accepted" and record.business_key is not None),
        target_keys=target_keys,
    ))
    if validation.source_system == "BILLING_MSSQL":
        results.extend(reconcile_financial_totals(
            run_id=validation.run_id,
            batch_id=validation.batch_id,
            accepted_source_rows=validation.accepted_records,
            target_rows=target_financial_rows,
        ))
    return tuple(results)


def _replace_attempt_evidence(connection: Connection, validation: BatchValidation) -> None:
    identifiers = {"run_id": validation.run_id, "batch_id": validation.batch_id}
    connection.execute(DELETE_ATTEMPT_RESULTS, identifiers)
    connection.execute(DELETE_RECONCILIATIONS, identifiers)
    for failure in validation.failures:
        result = connection.execute(UPSERT_VALIDATION_RESULT, failure.parameters())
        if result.rowcount == 0:
            raise ReleaseGateError(f"Rule {failure.rule_id} is missing, disabled, or mismatched")


def _update_batch(
    connection: Connection,
    validation: BatchValidation,
    status: str,
    error_message: str | None,
) -> None:
    outcome = connection.execute(
        UPDATE_BATCH_OUTCOME,
        {
            "status": status,
            **validation.counts.parameters(),
            "error_message": error_message,
            "run_id": validation.run_id,
            "batch_id": validation.batch_id,
        },
    )
    if outcome.rowcount != 1:
        raise ReleaseGateError("Registered batch attempt was not found")


def _persist_failed_release(
    engine: Engine,
    validation: BatchValidation,
    results: tuple[ReconciliationResult, ...],
) -> None:
    try:
        with engine.begin() as connection:
            _replace_attempt_evidence(connection, validation)
            _persist_reconciliations(connection, results)
            _update_batch(
                connection,
                validation,
                "QUARANTINED",
                "Reconciliation failure blocked release",
            )
    except Exception as exc:
        raise ReleaseGateError("Failed reconciliation evidence could not be stored") from exc
