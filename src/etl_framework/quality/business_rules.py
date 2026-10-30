"""Business-rule validation, normalization, and record classification."""

from __future__ import annotations

import re
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import Any, Callable
from uuid import UUID

from etl_framework.control.run_tracking import BatchCounts
from etl_framework.quality.validation_results import Severity, ValidationFailure


EMAIL_PATTERN = re.compile(r"^[^@\s]+@[^@\s]+$")
COUNTRY_PATTERN = re.compile(r"^[A-Z]{2}$")
CURRENCY_PATTERN = re.compile(r"^[A-Z]{3}$")
CUSTOMER_STATUSES = frozenset({"active", "inactive", "suspended"})
TRANSACTION_STATUSES = frozenset({"pending", "completed", "failed", "refunded"})
EVENT_TYPES = frozenset({"service", "shipment", "support", "maintenance"})


class RecordDisposition(str, Enum):
    ACCEPTED = "accepted"
    QUARANTINED = "quarantined"
    DUPLICATE = "duplicate"


@dataclass(frozen=True, slots=True)
class RecordEvaluation:
    business_key: str | None
    normalized: Mapping[str, Any]
    disposition: RecordDisposition
    failures: tuple[ValidationFailure, ...] = ()


@dataclass(frozen=True, slots=True)
class BatchValidation:
    source_system: str
    run_id: UUID
    batch_id: UUID
    records: tuple[RecordEvaluation, ...]

    @property
    def failures(self) -> tuple[ValidationFailure, ...]:
        return tuple(failure for record in self.records for failure in record.failures)

    @property
    def counts(self) -> BatchCounts:
        return BatchCounts(
            source=len(self.records),
            accepted=sum(r.disposition is RecordDisposition.ACCEPTED for r in self.records),
            quarantined=sum(r.disposition is RecordDisposition.QUARANTINED for r in self.records),
            duplicate=sum(r.disposition is RecordDisposition.DUPLICATE for r in self.records),
        )

    @property
    def has_errors(self) -> bool:
        return any(f.severity is Severity.ERROR for f in self.failures)

    @property
    def accepted_records(self) -> tuple[Mapping[str, Any], ...]:
        return tuple(r.normalized for r in self.records if r.disposition is RecordDisposition.ACCEPTED)


def validate_customers(
    rows: Iterable[Mapping[str, Any]], *, run_id: UUID, batch_id: UUID
) -> BatchValidation:
    normalized = [_normalize_customer(row) for row in rows]

    def validate(row: Mapping[str, Any], key: str | None) -> list[ValidationFailure]:
        return _customer_failures(row, key, run_id=run_id, batch_id=batch_id)

    return _validate_deduplicated(
        normalized,
        source_system="CRM_PG",
        run_id=run_id,
        batch_id=batch_id,
        key_name="customer_id",
        order_name="updated_at",
        duplicate_rule="DQ-CUS-006",
        duplicate_field="customer_id",
        validator=validate,
    )


def validate_api_statuses(
    rows: Iterable[Mapping[str, Any]], *, run_id: UUID, batch_id: UUID
) -> BatchValidation:
    normalized = [
        {
            **dict(row),
            "customer_id": _text(row.get("customer_id")),
            "status": _lower(row.get("status")),
            "status_updated_at": _timestamp(row.get("status_updated_at")),
        }
        for row in rows
    ]

    def validate(row: Mapping[str, Any], key: str | None) -> list[ValidationFailure]:
        failures = []
        if key is None or row["status"] is None or row["status_updated_at"] is None:
            failures.append(_failure(run_id, batch_id, "STATUS_API", "DQ-CUS-001", Severity.ERROR, key, None, "Required API customer-status fields are missing"))
        if row["status"] is not None and row["status"] not in CUSTOMER_STATUSES:
            failures.append(_failure(run_id, batch_id, "STATUS_API", "DQ-CUS-004", Severity.ERROR, key, "status", "Customer status is not allowed", row["status"]))
        return failures

    return _validate_deduplicated(
        normalized, source_system="STATUS_API", run_id=run_id, batch_id=batch_id,
        key_name="customer_id", order_name="status_updated_at", duplicate_rule=None,
        duplicate_field=None, validator=validate,
    )


def validate_transactions(
    rows: Iterable[Mapping[str, Any]], *, customer_ids: set[str], run_id: UUID, batch_id: UUID
) -> BatchValidation:
    normalized = [_normalize_transaction(row) for row in rows]

    def validate(row: Mapping[str, Any], key: str | None) -> list[ValidationFailure]:
        failures = []
        required = (key, row["customer_id"], row["transaction_timestamp"], row["amount"], row["currency_code"], row["transaction_status"], row["updated_at"])
        if any(value is None for value in required):
            failures.append(_failure(run_id, batch_id, "BILLING_MSSQL", "DQ-TXN-001", Severity.ERROR, key, None, "Required transaction fields are missing or invalid"))
        if row["amount"] is not None and row["amount"] < 0:
            failures.append(_failure(run_id, batch_id, "BILLING_MSSQL", "DQ-TXN-002", Severity.ERROR, key, "amount", "Transaction amount must be nonnegative", row["amount"]))
        if row["currency_code"] is not None and not CURRENCY_PATTERN.fullmatch(row["currency_code"]):
            failures.append(_failure(run_id, batch_id, "BILLING_MSSQL", "DQ-TXN-003", Severity.ERROR, key, "currency_code", "Currency code must contain three ASCII letters", row["currency_code"]))
        if row["transaction_status"] is not None and row["transaction_status"] not in TRANSACTION_STATUSES:
            failures.append(_failure(run_id, batch_id, "BILLING_MSSQL", "DQ-TXN-004", Severity.ERROR, key, "transaction_status", "Transaction status is not allowed", row["transaction_status"]))
        if row["customer_id"] is not None and row["customer_id"] not in customer_ids:
            failures.append(_failure(run_id, batch_id, "BILLING_MSSQL", "DQ-TXN-005", Severity.ERROR, key, "customer_id", "Transaction customer does not resolve", row["customer_id"]))
        return failures

    return _validate_deduplicated(
        normalized, source_system="BILLING_MSSQL", run_id=run_id, batch_id=batch_id,
        key_name="transaction_id", order_name="updated_at", duplicate_rule="DQ-TXN-006",
        duplicate_field="transaction_id", validator=validate,
    )


def validate_operational_events(
    rows: Iterable[Mapping[str, Any]], *, customer_ids: set[str], run_id: UUID, batch_id: UUID
) -> BatchValidation:
    normalized = [_normalize_event(row) for row in rows]

    def validate(row: Mapping[str, Any], key: str | None) -> list[ValidationFailure]:
        failures = []
        if any(value is None for value in (key, row["customer_id"], row["event_type"], row["event_timestamp"])):
            failures.append(_failure(run_id, batch_id, "OPS_CSV", "DQ-OPS-001", Severity.ERROR, key, None, "Required operational event fields are missing or invalid"))
        if row["event_type"] is not None and row["event_type"] not in EVENT_TYPES:
            failures.append(_failure(run_id, batch_id, "OPS_CSV", "DQ-OPS-002", Severity.ERROR, key, "event_type", "Operational event type is not allowed", row["event_type"]))
        if row["customer_id"] is not None and row["customer_id"] not in customer_ids:
            failures.append(_failure(run_id, batch_id, "OPS_CSV", "DQ-OPS-003", Severity.ERROR, key, "customer_id", "Operational event customer does not resolve", row["customer_id"]))
        if row.get("location_code_invalid"):
            failures.append(_failure(run_id, batch_id, "OPS_CSV", "DQ-OPS-004", Severity.WARNING, key, "location_code", "Location code exceeded thirty characters and was cleared", row.get("location_code_original")))
        return failures

    return _validate_deduplicated(
        normalized, source_system="OPS_CSV", run_id=run_id, batch_id=batch_id,
        key_name="event_id", order_name="event_timestamp", duplicate_rule="DQ-OPS-006",
        duplicate_field="event_id", validator=validate,
    )


def _validate_deduplicated(
    rows: Sequence[Mapping[str, Any]], *, source_system: str, run_id: UUID, batch_id: UUID,
    key_name: str, order_name: str, duplicate_rule: str | None, duplicate_field: str | None,
    validator: Callable[[Mapping[str, Any], str | None], list[ValidationFailure]],
) -> BatchValidation:
    groups: dict[str, list[int]] = defaultdict(list)
    for index, row in enumerate(rows):
        key = row.get(key_name)
        if isinstance(key, str):
            groups[key].append(index)
    winners: dict[str, int] = {}
    for key, indexes in groups.items():
        winners[key] = max(indexes, key=lambda index: (_sort_time(rows[index].get(order_name)), index))
    records = []
    for index, row in enumerate(rows):
        key = row.get(key_name) if isinstance(row.get(key_name), str) else None
        if key is not None and winners.get(key) != index:
            records.append(RecordEvaluation(key, row, RecordDisposition.DUPLICATE))
            continue
        failures = validator(row, key)
        if duplicate_rule and key is not None and len(groups[key]) > 1:
            failures.append(_failure(run_id, batch_id, source_system, duplicate_rule, Severity.WARNING, key, duplicate_field, f"Duplicate {key_name} rows were resolved"))
        disposition = RecordDisposition.QUARANTINED if any(f.severity is Severity.ERROR for f in failures) else RecordDisposition.ACCEPTED
        records.append(RecordEvaluation(key, row, disposition, tuple(failures)))
    return BatchValidation(source_system, run_id, batch_id, tuple(records))


def _customer_failures(
    row: Mapping[str, Any], key: str | None, *, run_id: UUID, batch_id: UUID
) -> list[ValidationFailure]:
    failures = []
    required = (key, row["first_name"], row["last_name"], row["email"], row["country_code"], row["status"], row["created_at"], row["updated_at"])
    if any(value is None for value in required):
        failures.append(_failure(run_id, batch_id, "CRM_PG", "DQ-CUS-001", Severity.ERROR, key, None, "Required customer fields are missing or invalid"))
    if row["email"] is not None and not EMAIL_PATTERN.fullmatch(row["email"]):
        failures.append(_failure(run_id, batch_id, "CRM_PG", "DQ-CUS-002", Severity.ERROR, key, "email", "Email structure is invalid", row["email"]))
    if row["country_code"] is not None and not COUNTRY_PATTERN.fullmatch(row["country_code"]):
        failures.append(_failure(run_id, batch_id, "CRM_PG", "DQ-CUS-003", Severity.ERROR, key, "country_code", "Country code must contain two ASCII letters", row["country_code"]))
    if row["status"] is not None and row["status"] not in CUSTOMER_STATUSES:
        failures.append(_failure(run_id, batch_id, "CRM_PG", "DQ-CUS-004", Severity.ERROR, key, "status", "Customer status is not allowed", row["status"]))
    if row["created_at"] is not None and row["updated_at"] is not None and row["created_at"] > row["updated_at"]:
        failures.append(_failure(run_id, batch_id, "CRM_PG", "DQ-CUS-005", Severity.ERROR, key, "created_at", "Customer creation time exceeds update time", row["created_at"]))
    return failures


def _normalize_customer(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        **dict(row), "customer_id": _text(row.get("customer_id")),
        "first_name": _text(row.get("first_name")), "last_name": _text(row.get("last_name")),
        "email": _lower(row.get("email")), "phone": _text(row.get("phone")),
        "country_code": _upper(row.get("country_code")), "status": _lower(row.get("status")),
        "created_at": _timestamp(row.get("created_at")), "updated_at": _timestamp(row.get("updated_at")),
    }


def _normalize_transaction(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        **dict(row), "transaction_id": _text(row.get("transaction_id")),
        "customer_id": _text(row.get("customer_id")), "transaction_timestamp": _timestamp(row.get("transaction_timestamp")),
        "amount": _decimal(row.get("amount")), "currency_code": _upper(row.get("currency_code")),
        "transaction_status": _lower(row.get("transaction_status")), "payment_method": _lower(row.get("payment_method")),
        "updated_at": _timestamp(row.get("updated_at")),
    }


def _normalize_event(row: Mapping[str, Any]) -> dict[str, Any]:
    location = _text(row.get("location_code"))
    invalid = location is not None and len(location) > 30
    return {
        **dict(row), "event_id": _text(row.get("event_id")), "customer_id": _text(row.get("customer_id")),
        "event_type": _lower(row.get("event_type")), "event_timestamp": _timestamp(row.get("event_timestamp")),
        "location_code_original": location, "location_code_invalid": invalid,
        "location_code": None if invalid else location, "details": _text(row.get("details")),
    }


def _failure(run_id: UUID, batch_id: UUID, source: str, rule: str, severity: Severity, key: str | None, field: str | None, message: str, observed: Any = None) -> ValidationFailure:
    return ValidationFailure(run_id, batch_id, source, rule, severity, message, key, field, observed)


def _text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _lower(value: Any) -> str | None:
    text = _text(value)
    return text.lower() if text else None


def _upper(value: Any) -> str | None:
    text = _text(value)
    return text.upper() if text else None


def _timestamp(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc) if value.tzinfo else None
    text = _text(value)
    if text is None:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.astimezone(timezone.utc) if parsed.tzinfo else None


def _decimal(value: Any) -> Decimal | None:
    try:
        return Decimal(str(value)).quantize(Decimal("0.01")) if value is not None else None
    except (InvalidOperation, ValueError):
        return None


def _sort_time(value: Any) -> datetime:
    return value if isinstance(value, datetime) else datetime.min.replace(tzinfo=timezone.utc)
