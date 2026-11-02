from __future__ import annotations

import csv
from collections import Counter
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from etl_framework.quality.reconciliation import (
    ReconciliationError,
    persist_reconciliations,
    reconcile_financial_totals,
    reconcile_keys,
    reconcile_row_counts,
)


ROOT = Path(__file__).resolve().parents[2]


def test_row_accounting_passes_and_fails_the_documented_equation() -> None:
    passing = reconcile_row_counts(
        run_id=uuid4(), batch_id=uuid4(), source_rows=9,
        accepted=5, quarantined=3, duplicates=1,
    )
    failing = reconcile_row_counts(
        run_id=uuid4(), batch_id=uuid4(), source_rows=9,
        accepted=5, quarantined=2, duplicates=1,
    )
    assert passing.status == "PASS"
    assert passing.source_value == passing.target_value == Decimal("9")
    assert failing.status == "FAIL"
    assert failing.difference == Decimal("-1")


def test_key_reconciliation_identifies_missing_and_unexpected_keys() -> None:
    results = reconcile_keys(
        run_id=uuid4(), batch_id=uuid4(),
        accepted_source_keys={"A", "B"}, target_keys={"A", "C"},
    )
    by_metric = {result.metric_name: result for result in results}
    assert by_metric["accepted_business_keys"].status == "PASS"
    assert by_metric["missing_target_keys"].status == "FAIL"
    assert by_metric["missing_target_keys"].details == {"keys": ["B"]}
    assert by_metric["unexpected_target_keys"].status == "FAIL"
    assert by_metric["unexpected_target_keys"].details == {"keys": ["C"]}


def test_financial_reconciliation_uses_currency_status_and_exact_decimals() -> None:
    source = [
        {"amount": Decimal("100.10"), "currency_code": "USD", "transaction_status": "completed"},
        {"amount": Decimal("20.20"), "currency_code": "USD", "transaction_status": "completed"},
        {"amount": Decimal("5.00"), "currency_code": "EUR", "transaction_status": "refunded"},
        {"amount": Decimal("99.00"), "currency_code": "USD", "transaction_status": "pending"},
    ]
    target = [
        {"amount": Decimal("120.30"), "currency_code": "USD", "transaction_status": "completed"},
        {"amount": Decimal("4.99"), "currency_code": "EUR", "transaction_status": "refunded"},
    ]
    results = reconcile_financial_totals(
        run_id=uuid4(), batch_id=uuid4(),
        accepted_source_rows=source, target_rows=target,
    )
    by_key = {(result.rule_id, result.group_key): result for result in results}
    assert by_key[("RC-FIN-001", "USD")].status == "PASS"
    assert by_key[("RC-FIN-001", "USD")].source_value == Decimal("120.30")
    assert by_key[("RC-FIN-002", "EUR")].status == "FAIL"
    assert by_key[("RC-FIN-002", "EUR")].difference == Decimal("-0.01")
    assert all(result.tolerance == Decimal("0.00") for result in results)


def test_financial_reconciliation_rejects_binary_float_input() -> None:
    with pytest.raises(ReconciliationError, match="exact decimal"):
        reconcile_financial_totals(
            run_id=uuid4(), batch_id=uuid4(),
            accepted_source_rows=[{"amount": 0.1, "currency_code": "USD", "transaction_status": "completed"}],
            target_rows=[],
        )


def test_reconciliation_results_match_expected_fixture() -> None:
    run_id, batch_id = uuid4(), uuid4()
    generated = []
    for source_rows, accepted, quarantined, duplicates in ((7, 3, 3, 1), (4, 3, 1, 0), (9, 5, 3, 1), (7, 4, 2, 1)):
        generated.append(reconcile_row_counts(
            run_id=run_id, batch_id=batch_id, source_rows=source_rows,
            accepted=accepted, quarantined=quarantined, duplicates=duplicates,
        ))
        generated.extend(reconcile_keys(
            run_id=run_id, batch_id=batch_id,
            accepted_source_keys={f"K{i}" for i in range(accepted)},
            target_keys={f"K{i}" for i in range(accepted)},
        ))
    generated.extend(reconcile_financial_totals(
        run_id=run_id, batch_id=batch_id,
        accepted_source_rows=[
            {"amount": "475.50", "currency_code": "USD", "transaction_status": "completed"},
            {"amount": "40.00", "currency_code": "USD", "transaction_status": "refunded"},
        ],
        target_rows=[
            {"amount": "475.50", "currency_code": "USD", "transaction_status": "completed"},
            {"amount": "40.00", "currency_code": "USD", "transaction_status": "refunded"},
        ],
    ))
    with (ROOT / "data/expected/reconciliation.csv").open(encoding="utf-8", newline="") as source:
        expected = list(csv.DictReader(source))
    assert len(generated) == len(expected)
    assert all(result.status == "PASS" for result in generated)
    actual_values = Counter(
        (
            result.rule_id, result.metric_name, result.group_key or "",
            f"{result.source_value:.4f}", f"{result.target_value:.4f}",
            f"{result.tolerance:.4f}", result.status,
        )
        for result in generated
    )
    expected_values = Counter(
        (
            row["rule_id"], row["metric_name"], row["group_key"],
            row["source_value"], row["target_value"], row["tolerance"], row["status"],
        )
        for row in expected
    )
    assert actual_values == expected_values


def test_reconciliation_persistence_is_idempotent() -> None:
    engine = MagicMock()
    connection = engine.begin.return_value.__enter__.return_value
    connection.execute.return_value = MagicMock(rowcount=1)
    result = reconcile_row_counts(
        run_id=uuid4(), batch_id=uuid4(), source_rows=1,
        accepted=1, quarantined=0, duplicates=0,
    )
    assert persist_reconciliations(engine, [result, result]) == 2
    first = connection.execute.call_args_list[0].args[1]
    second = connection.execute.call_args_list[1].args[1]
    assert first["reconciliation_result_id"] == second["reconciliation_result_id"]
    assert "ON CONFLICT" in str(connection.execute.call_args_list[0].args[0])
