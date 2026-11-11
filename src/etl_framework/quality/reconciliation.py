"""Exact row, key, and financial reconciliation with idempotent persistence."""

from __future__ import annotations

import json
from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Any
from uuid import UUID, uuid5

from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import SQLAlchemyError

from etl_framework.control.run_tracking import mask_audit_value


RECONCILIATION_NAMESPACE = UUID("928b942e-5853-470a-809b-a9ef5a3774b9")
ZERO = Decimal("0.0000")
CENT = Decimal("0.01")


class ReconciliationError(RuntimeError):
    """Raised when reconciliation input or persistence is invalid."""


@dataclass(frozen=True, slots=True)
class ReconciliationResult:
    run_id: UUID
    batch_id: UUID
    rule_id: str
    metric_name: str
    source_value: Decimal
    target_value: Decimal
    tolerance: Decimal = ZERO
    group_key: str | None = None
    details: Mapping[str, Any] = field(default_factory=dict, repr=False)

    def __post_init__(self) -> None:
        if not self.rule_id.startswith("RC-"):
            raise ReconciliationError("rule_id must be a documented reconciliation rule")
        if not self.metric_name.strip():
            raise ReconciliationError("metric_name must not be blank")
        if self.tolerance < 0:
            raise ReconciliationError("tolerance must not be negative")

    @property
    def difference(self) -> Decimal:
        return self.target_value - self.source_value

    @property
    def status(self) -> str:
        return "PASS" if abs(self.difference) <= self.tolerance else "FAIL"

    @property
    def result_id(self) -> UUID:
        identity = "|".join(
            (str(self.run_id), str(self.batch_id), self.rule_id, self.metric_name, self.group_key or "")
        )
        return uuid5(RECONCILIATION_NAMESPACE, identity)

    def parameters(self) -> dict[str, Any]:
        return {
            "reconciliation_result_id": self.result_id,
            "run_id": self.run_id,
            "batch_id": self.batch_id,
            "rule_id": self.rule_id,
            "metric_name": self.metric_name,
            "group_key": self.group_key,
            "source_value": self.source_value,
            "target_value": self.target_value,
            "tolerance": self.tolerance,
            "status": self.status,
            "details": json.dumps(mask_audit_value(dict(self.details)), sort_keys=True, default=str),
        }


UPSERT_RECONCILIATION = text(
    """
    INSERT INTO quality.reconciliation_result (
        reconciliation_result_id, run_id, batch_id, rule_id, metric_name,
        group_key, source_value, target_value, tolerance, status, details
    )
    SELECT
        CAST(:reconciliation_result_id AS UUID), CAST(:run_id AS UUID),
        CAST(:batch_id AS UUID), CAST(:rule_id AS VARCHAR(30)),
        CAST(:metric_name AS VARCHAR(100)), CAST(:group_key AS VARCHAR(100)),
        CAST(:source_value AS NUMERIC(20, 4)), CAST(:target_value AS NUMERIC(20, 4)),
        CAST(:tolerance AS NUMERIC(20, 4)), CAST(:status AS VARCHAR(10)),
        CAST(:details AS JSONB)
    FROM quality.rule_definition rule
    WHERE rule.rule_id = CAST(:rule_id AS VARCHAR(30))
      AND rule.rule_type = 'RECONCILIATION'
      AND rule.enabled
    ON CONFLICT (run_id, batch_id, rule_id, metric_name, group_key)
    DO UPDATE SET
        source_value = EXCLUDED.source_value,
        target_value = EXCLUDED.target_value,
        tolerance = EXCLUDED.tolerance,
        status = EXCLUDED.status,
        details = EXCLUDED.details
    """
)


def reconcile_row_counts(
    *, run_id: UUID, batch_id: UUID, source_rows: int,
    accepted: int, quarantined: int, duplicates: int,
) -> ReconciliationResult:
    for value in (source_rows, accepted, quarantined, duplicates):
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ReconciliationError("row counts must be non-negative integers")
    return ReconciliationResult(
        run_id, batch_id, "RC-ROW-001", "row_accounting",
        Decimal(source_rows), Decimal(accepted + quarantined + duplicates),
    )


def reconcile_keys(
    *, run_id: UUID, batch_id: UUID,
    accepted_source_keys: Iterable[str], target_keys: Iterable[str],
) -> tuple[ReconciliationResult, ...]:
    source = {str(key) for key in accepted_source_keys}
    target = {str(key) for key in target_keys}
    missing = sorted(source - target)
    unexpected = sorted(target - source)
    results = [
        ReconciliationResult(
            run_id, batch_id, "RC-KEY-001", "accepted_business_keys",
            Decimal(len(source)), Decimal(len(target)),
            details={"missing_count": len(missing), "unexpected_count": len(unexpected)},
        )
    ]
    if missing:
        results.append(ReconciliationResult(
            run_id, batch_id, "RC-KEY-001", "missing_target_keys", ZERO,
            Decimal(len(missing)), details={"keys": missing},
        ))
    if unexpected:
        results.append(ReconciliationResult(
            run_id, batch_id, "RC-KEY-001", "unexpected_target_keys", ZERO,
            Decimal(len(unexpected)), details={"keys": unexpected},
        ))
    return tuple(results)


def reconcile_financial_totals(
    *, run_id: UUID, batch_id: UUID,
    accepted_source_rows: Iterable[Mapping[str, Any]],
    target_rows: Iterable[Mapping[str, Any]],
) -> tuple[ReconciliationResult, ...]:
    source_totals = _financial_totals(accepted_source_rows)
    target_totals = _financial_totals(target_rows)
    results = []
    for status, rule_id, metric_name in (
        ("completed", "RC-FIN-001", "completed_amount"),
        ("refunded", "RC-FIN-002", "refunded_amount"),
    ):
        currencies = sorted(set(source_totals[status]) | set(target_totals[status]))
        for currency in currencies:
            results.append(ReconciliationResult(
                run_id, batch_id, rule_id, metric_name,
                source_totals[status].get(currency, Decimal("0.00")),
                target_totals[status].get(currency, Decimal("0.00")),
                Decimal("0.00"), currency,
            ))
    return tuple(results)


def persist_reconciliations(
    engine: Engine, results: Iterable[ReconciliationResult]
) -> int:
    rows = tuple(results)
    try:
        with engine.begin() as connection:
            _persist_reconciliations(connection, rows)
    except ReconciliationError:
        raise
    except SQLAlchemyError as exc:
        raise ReconciliationError("Reconciliation results could not be stored") from exc
    return len(rows)


def _persist_reconciliations(
    connection: Connection, results: Iterable[ReconciliationResult]
) -> int:
    count = 0
    for reconciliation in results:
        result = connection.execute(UPSERT_RECONCILIATION, reconciliation.parameters())
        if result.rowcount == 0:
            raise ReconciliationError(f"Rule {reconciliation.rule_id} is missing or disabled")
        count += 1
    return count


def _financial_totals(rows: Iterable[Mapping[str, Any]]) -> dict[str, dict[str, Decimal]]:
    totals: dict[str, dict[str, Decimal]] = {
        "completed": defaultdict(lambda: Decimal("0.00")),
        "refunded": defaultdict(lambda: Decimal("0.00")),
    }
    for row in rows:
        status = str(row.get("transaction_status", "")).strip().lower()
        if status not in totals:
            continue
        currency = str(row.get("currency_code", "")).strip().upper()
        if not currency:
            raise ReconciliationError("Eligible financial row has no currency")
        totals[status][currency] += _money(row.get("amount"))
    return totals


def _money(value: Any) -> Decimal:
    if isinstance(value, (float, bool)) or value is None:
        raise ReconciliationError("Financial values must use exact decimal representations")
    try:
        return Decimal(str(value)).quantize(CENT)
    except (InvalidOperation, ValueError) as exc:
        raise ReconciliationError("Financial value is invalid") from exc
