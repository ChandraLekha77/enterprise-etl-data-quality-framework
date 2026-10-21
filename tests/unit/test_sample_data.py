from __future__ import annotations

import csv
import json
import re
from collections import Counter
from decimal import Decimal
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def read_csv(relative_path: str) -> list[dict[str, str]]:
    with (ROOT / relative_path).open(encoding="utf-8", newline="") as source:
        return list(csv.DictReader(source))


def test_source_and_expected_row_counts() -> None:
    operations = read_csv("data/raw/csv/operational_events_20261020.csv")
    customers = read_csv("data/expected/customers.csv")
    transactions = read_csv("data/expected/transactions.csv")
    expected_operations = read_csv("data/expected/operational_events.csv")
    status_payload = json.loads(
        (ROOT / "data/raw/json/customer_status_20261020.json").read_text(encoding="utf-8")
    )

    assert len(operations) == 7
    assert len(status_payload["data"]) == 4
    assert status_payload["next_page"] is None
    assert len(customers) == 3
    assert len(transactions) == 5
    assert len(expected_operations) == 4


def test_expected_keys_are_unique() -> None:
    datasets = {
        "customers": ("data/expected/customers.csv", "customer_id"),
        "transactions": ("data/expected/transactions.csv", "transaction_id"),
        "operations": ("data/expected/operational_events.csv", "event_id"),
    }
    for name, (path, key) in datasets.items():
        values = [row[key] for row in read_csv(path)]
        assert len(values) == len(set(values)), f"duplicate expected key in {name}"


def test_expected_financial_totals_use_decimal_values() -> None:
    rows = read_csv("data/expected/transactions.csv")
    completed_usd = sum(
        Decimal(row["amount"])
        for row in rows
        if row["transaction_status"] == "completed" and row["currency_code"] == "USD"
    )
    refunded_usd = sum(
        Decimal(row["amount"])
        for row in rows
        if row["transaction_status"] == "refunded" and row["currency_code"] == "USD"
    )

    assert completed_usd == Decimal("475.50")
    assert refunded_usd == Decimal("40.00")


def test_reconciliation_expectations_are_balanced() -> None:
    rows = read_csv("data/expected/reconciliation.csv")
    for row in rows:
        difference = Decimal(row["target_value"]) - Decimal(row["source_value"])
        assert abs(difference) <= Decimal(row["tolerance"])
        assert row["status"] == "PASS"


def test_expected_failure_rules_are_documented() -> None:
    rules = {
        row["rule_id"] for row in read_csv("data/expected/validation_failures.csv")
    }
    documentation = (ROOT / "docs/architecture/transformation_rules.md").read_text(
        encoding="utf-8"
    )

    assert all(rule in documentation for rule in rules)


def test_expected_failure_outcomes_by_source() -> None:
    rows = read_csv("data/expected/validation_failures.csv")
    outcomes = Counter((row["source_system"], row["expected_outcome"]) for row in rows)

    assert outcomes[("CRM_PG", "quarantine")] == 3
    assert outcomes[("BILLING_MSSQL", "quarantine")] == 3
    assert outcomes[("OPS_CSV", "quarantine")] == 2
    assert outcomes[("STATUS_API", "quarantine")] == 1


def test_database_seed_scripts_have_fixed_row_counts() -> None:
    crm_seed = (ROOT / "sql/postgresql/source/002_seed_customers.sql").read_text(
        encoding="utf-8"
    )
    billing_seed = (ROOT / "data/raw/sqlserver/002_seed_transactions.sql").read_text(
        encoding="utf-8"
    )

    assert len(re.findall(r"^\s*\(\d+,\s*'", crm_seed, flags=re.MULTILINE)) == 7
    assert len(re.findall(r"^\s*\(\d+,\s*N'T", billing_seed, flags=re.MULTILINE)) == 8
    assert "CAST(1 AS BIGINT), N'T000001'" in billing_seed
