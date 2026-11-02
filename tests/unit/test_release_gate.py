from __future__ import annotations

import copy
from decimal import Decimal
from types import SimpleNamespace
from uuid import uuid4

from etl_framework.quality.business_rules import validate_transactions
from etl_framework.quality.release_gate import apply_release_gate


class FakeConnection:
    def __init__(self, state: dict[str, object]) -> None:
        self.state = state

    def execute(self, statement: object, parameters: dict[str, object]) -> SimpleNamespace:
        sql = str(statement)
        if "DELETE FROM quality.reconciliation_result" in sql:
            self.state["reconciliations"] = []
        elif "INSERT INTO quality.reconciliation_result" in sql:
            self.state["reconciliations"].append(dict(parameters))
        elif "UPDATE control.batch_run" in sql:
            self.state["batch_status"] = parameters["status"]
        elif "INSERT INTO control.source_watermark" in sql:
            self.state["watermark"] = parameters["watermark_value"]
        return SimpleNamespace(rowcount=1)


class FakeTransaction:
    def __init__(self, engine: "FakeEngine") -> None:
        self.engine = engine
        self.snapshot: dict[str, object] | None = None

    def __enter__(self) -> FakeConnection:
        self.snapshot = copy.deepcopy(self.engine.state)
        return FakeConnection(self.engine.state)

    def __exit__(self, exception_type: object, exception: object, traceback: object) -> bool:
        if exception_type is not None:
            self.engine.state = self.snapshot
        return False


class FakeEngine:
    def __init__(self) -> None:
        self.state: dict[str, object] = {
            "core_rows": [], "reconciliations": [], "watermark": None,
            "batch_status": "RUNNING",
        }
        self.transaction_count = 0

    def begin(self) -> FakeTransaction:
        self.transaction_count += 1
        return FakeTransaction(self)


def valid_billing_validation() -> object:
    return validate_transactions(
        [{
            "transaction_id": "T1", "customer_id": "C1",
            "transaction_timestamp": "2026-10-20T10:00:00Z",
            "amount": "100.10", "currency_code": "USD",
            "transaction_status": "completed", "payment_method": "card",
            "updated_at": "2026-10-20T10:01:00Z",
        }],
        customer_ids={"C1"}, run_id=uuid4(), batch_id=uuid4(),
    )


def test_successful_release_persists_results_and_advances_watermark_last() -> None:
    engine = FakeEngine()
    validation = valid_billing_validation()

    def promote(connection: FakeConnection, rows: tuple[dict[str, object], ...]) -> None:
        connection.state["core_rows"] = list(rows)

    result = apply_release_gate(
        engine, validation, watermark_value="2026-10-20T10:01:00Z",
        promote=promote,
        load_target_keys=lambda connection: [row["transaction_id"] for row in connection.state["core_rows"]],
        load_target_financial_rows=lambda connection: connection.state["core_rows"],
    )

    assert result.released
    assert result.status == "SUCCEEDED"
    assert all(item.status == "PASS" for item in result.reconciliations)
    assert engine.state["batch_status"] == "SUCCEEDED"
    assert engine.state["watermark"] == "2026-10-20T10:01:00Z"
    assert engine.state["core_rows"][0]["amount"] == Decimal("100.10")
    assert len(engine.state["reconciliations"]) == 3
    assert engine.transaction_count == 1


def test_failed_financial_reconciliation_rolls_back_promotion_and_blocks_watermark() -> None:
    engine = FakeEngine()
    validation = valid_billing_validation()

    def promote_wrong_amount(connection: FakeConnection, rows: tuple[dict[str, object], ...]) -> None:
        promoted = dict(rows[0])
        promoted["amount"] = Decimal("100.09")
        connection.state["core_rows"] = [promoted]

    result = apply_release_gate(
        engine, validation, watermark_value="2026-10-20T10:01:00Z",
        promote=promote_wrong_amount,
        load_target_keys=lambda connection: [row["transaction_id"] for row in connection.state["core_rows"]],
        load_target_financial_rows=lambda connection: connection.state["core_rows"],
    )

    assert not result.released
    assert result.status == "QUARANTINED"
    assert any(item.rule_id == "RC-FIN-001" and item.status == "FAIL" for item in result.reconciliations)
    assert engine.state["core_rows"] == []
    assert engine.state["watermark"] is None
    assert engine.state["batch_status"] == "QUARANTINED"
    assert any(item["status"] == "FAIL" for item in engine.state["reconciliations"])
    assert engine.transaction_count == 2


def test_key_mismatch_blocks_release_even_when_key_counts_match() -> None:
    engine = FakeEngine()
    validation = valid_billing_validation()

    result = apply_release_gate(
        engine, validation, watermark_value="next",
        promote=lambda connection, rows: connection.state.update(core_rows=list(rows)),
        load_target_keys=lambda connection: ["T-unexpected"],
        load_target_financial_rows=lambda connection: connection.state["core_rows"],
    )

    assert not result.released
    metrics = {item.metric_name: item.status for item in result.reconciliations}
    assert metrics["accepted_business_keys"] == "PASS"
    assert metrics["missing_target_keys"] == "FAIL"
    assert metrics["unexpected_target_keys"] == "FAIL"
    assert engine.state["watermark"] is None


def test_quality_error_blocks_promotion_before_reconciliation() -> None:
    engine = FakeEngine()
    validation = validate_transactions(
        [{
            "transaction_id": "T1", "customer_id": "C1",
            "transaction_timestamp": "2026-10-20T10:00:00Z",
            "amount": "-1.00", "currency_code": "USD",
            "transaction_status": "completed", "updated_at": "2026-10-20T10:01:00Z",
        }],
        customer_ids={"C1"}, run_id=uuid4(), batch_id=uuid4(),
    )
    promoted = []
    result = apply_release_gate(
        engine, validation, watermark_value="next",
        promote=lambda connection, rows: promoted.extend(rows),
        load_target_keys=lambda connection: [],
    )
    assert not result.released
    assert promoted == []
    assert result.reconciliations == ()
    assert engine.state["watermark"] is None
