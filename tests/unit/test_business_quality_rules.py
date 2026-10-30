from __future__ import annotations

import csv
import json
from collections import Counter
from pathlib import Path
from uuid import uuid4

from etl_framework.quality.business_rules import (
    RecordDisposition,
    validate_api_statuses,
    validate_customers,
    validate_operational_events,
    validate_transactions,
)


ROOT = Path(__file__).resolve().parents[2]


def read_csv(relative_path: str) -> list[dict[str, str]]:
    with (ROOT / relative_path).open(encoding="utf-8", newline="") as source:
        return list(csv.DictReader(source))


def customer_rows() -> list[dict[str, object]]:
    return [
        {"customer_id": "C000001", "first_name": "Ava", "last_name": "Patel", "email": "ava.patel@example.com", "phone": "+1-202-555-0101", "country_code": "US", "status": "active", "created_at": "2026-01-10T14:00:00Z", "updated_at": "2026-10-18T09:00:00Z"},
        {"customer_id": " C000002 ", "first_name": " Noah ", "last_name": "Kim", "email": "NOAH.KIM@EXAMPLE.COM ", "phone": None, "country_code": " ca ", "status": "ACTIVE", "created_at": "2026-02-11T15:30:00Z", "updated_at": "2026-10-17T10:00:00Z"},
        {"customer_id": "C000003", "first_name": "Mia", "last_name": "Garcia", "email": "mia.garcia@example.com", "phone": "+1-202-555-0103", "country_code": "GB", "status": "suspended", "created_at": "2026-03-12T08:15:00Z", "updated_at": "2026-10-20T12:00:00Z"},
        {"customer_id": "C000004", "first_name": "Liam", "last_name": "Brown", "email": "liam.brown.example.com", "country_code": "US", "status": "active", "created_at": "2026-04-13T09:45:00Z", "updated_at": "2026-10-19T11:00:00Z"},
        {"customer_id": "C000005", "first_name": "Emma", "last_name": "Wilson", "email": "emma.wilson@example.com", "country_code": "USA", "status": "active", "created_at": "2026-05-14T10:20:00Z", "updated_at": "2026-10-19T13:00:00Z"},
        {"customer_id": "C000006", "first_name": "Oliver", "last_name": "Davis", "email": "oliver.davis@example.com", "country_code": "US", "status": "inactive", "created_at": "2026-10-21T10:00:00Z", "updated_at": "2026-10-20T10:00:00Z"},
        {"customer_id": "C000002", "first_name": "Noah", "last_name": "Kim", "email": "noah.kim@example.com", "phone": "+1-202-555-0102", "country_code": "CA", "status": "active", "created_at": "2026-02-11T15:30:00Z", "updated_at": "2026-10-20T14:00:00Z"},
    ]


def transaction_rows() -> list[dict[str, object]]:
    base = [
        ("T000001", "C000001", "2026-10-20T10:00:00Z", "100.00", "USD", "completed", "card", "2026-10-20T10:01:00Z"),
        ("T000002", "C000002", "2026-10-20T11:00:00Z", "250.50", "usd", "COMPLETED", "bank_transfer", "2026-10-20T11:01:00Z"),
        ("T000003", "C000003", "2026-10-20T12:00:00Z", "40.00", "USD", "refunded", "card", "2026-10-20T12:01:00Z"),
        ("T000004", "C000001", "2026-10-20T13:00:00Z", "75.00", "EUR", "pending", None, "2026-10-20T13:01:00Z"),
        ("T000005", "C000002", "2026-10-20T14:00:00Z", "-10.00", "USD", "completed", "card", "2026-10-20T14:01:00Z"),
        ("T000006", "C999999", "2026-10-20T15:00:00Z", "25.00", "USD", "completed", "card", "2026-10-20T15:01:00Z"),
        ("T000007", "C000003", "2026-10-20T16:00:00Z", "15.00", "US", "completed", "cash", "2026-10-20T16:01:00Z"),
        ("T000008", "C000001", "2026-10-20T17:00:00Z", "120.00", "USD", "pending", "card", "2026-10-20T17:01:00Z"),
        ("T000008", "C000001", "2026-10-20T17:00:00Z", "125.00", "USD", "completed", "card", "2026-10-20T18:01:00Z"),
    ]
    return [
        {"transaction_id": transaction_id, "customer_id": customer_id, "transaction_timestamp": timestamp, "amount": amount, "currency_code": currency, "transaction_status": status, "payment_method": payment_method, "updated_at": updated_at}
        for transaction_id, customer_id, timestamp, amount, currency, status, payment_method, updated_at in base
    ]


def expected_failures() -> set[tuple[str, str, str, str, str]]:
    with (ROOT / "data/expected/validation_failures.csv").open(encoding="utf-8", newline="") as source:
        return {
            (row["source_system"], row["business_key"], row["rule_id"], row["severity"], row["field_name"])
            for row in csv.DictReader(source)
        }


def outcome(validation: object) -> tuple[int, int, int]:
    counts = validation.counts
    return counts.accepted, counts.quarantined, counts.duplicate


def test_all_business_validators_match_expected_failure_fixture_and_counts() -> None:
    run_id = uuid4()
    customer_validation = validate_customers(customer_rows(), run_id=run_id, batch_id=uuid4())
    customer_ids = {str(row["customer_id"]) for row in customer_validation.accepted_records}
    transaction_validation = validate_transactions(transaction_rows(), customer_ids=customer_ids, run_id=run_id, batch_id=uuid4())
    with (ROOT / "data/raw/csv/operational_events_20261020.csv").open(encoding="utf-8", newline="") as source:
        operation_validation = validate_operational_events(list(csv.DictReader(source)), customer_ids=customer_ids, run_id=run_id, batch_id=uuid4())
    status_rows = json.loads((ROOT / "data/raw/json/customer_status_20261020.json").read_text(encoding="utf-8"))["data"]
    api_validation = validate_api_statuses(status_rows, run_id=run_id, batch_id=uuid4())

    assert outcome(customer_validation) == (3, 3, 1)
    assert outcome(transaction_validation) == (5, 3, 1)
    assert outcome(operation_validation) == (4, 2, 1)
    assert outcome(api_validation) == (3, 1, 0)

    actual = {
        (failure.source_system, failure.business_key or "", failure.rule_id, failure.severity.value, failure.field_name or "")
        for validation in (customer_validation, transaction_validation, operation_validation, api_validation)
        for failure in validation.failures
    }
    assert actual == expected_failures()

    expected_transactions = {row["transaction_id"]: row for row in read_csv("data/expected/transactions.csv")}
    for row in transaction_validation.accepted_records:
        expected = expected_transactions[str(row["transaction_id"])]
        assert f"{row['amount']:.2f}" == expected["amount"]
        assert row["currency_code"] == expected["currency_code"]
        assert row["transaction_status"] == expected["transaction_status"]
        assert (row["payment_method"] or "") == expected["payment_method"]

    expected_operations = {row["event_id"]: row for row in read_csv("data/expected/operational_events.csv")}
    for row in operation_validation.accepted_records:
        expected = expected_operations[str(row["event_id"])]
        assert row["event_type"] == expected["event_type"]
        assert (row["location_code"] or "") == expected["location_code"]
        assert row["details"] == expected["details"]


def test_warning_only_records_are_accepted_and_normalized() -> None:
    validation = validate_operational_events(
        [{"event_id": "E1", "customer_id": "C1", "event_type": " SUPPORT ", "event_timestamp": "2026-10-20T12:00:00Z", "location_code": "X" * 31, "details": " note "}],
        customer_ids={"C1"}, run_id=uuid4(), batch_id=uuid4(),
    )
    record = validation.records[0]
    assert record.disposition is RecordDisposition.ACCEPTED
    assert record.normalized["event_type"] == "support"
    assert record.normalized["location_code"] is None
    assert [failure.rule_id for failure in record.failures] == ["DQ-OPS-004"]


def test_missing_and_invalid_required_values_are_quarantined() -> None:
    validation = validate_customers(
        [{"customer_id": " ", "first_name": None, "last_name": "User", "email": "bad", "country_code": "1", "status": "unknown", "created_at": "not-a-date", "updated_at": None}],
        run_id=uuid4(), batch_id=uuid4(),
    )
    assert validation.records[0].disposition is RecordDisposition.QUARANTINED
    assert {failure.rule_id for failure in validation.failures} >= {"DQ-CUS-001", "DQ-CUS-002", "DQ-CUS-003", "DQ-CUS-004"}
