from __future__ import annotations

from datetime import datetime, timedelta, timezone
from time import perf_counter
from uuid import uuid4

import pytest

from etl_framework.quality.business_rules import validate_transactions


@pytest.mark.performance
def test_ten_thousand_transaction_validation_is_bounded() -> None:
    started = perf_counter()
    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    rows = [{
        "transaction_id": f"T{i}", "customer_id": f"C{i % 1000}",
        "transaction_category": "sale", "amount": "12.34",
        "currency_code": "USD", "transaction_status": "completed",
        "transaction_timestamp": (base + timedelta(seconds=i)).isoformat(),
        "updated_at": (base + timedelta(seconds=i)).isoformat(),
    } for i in range(10_000)]
    result = validate_transactions(
        rows, customer_ids={f"C{i}" for i in range(1000)},
        run_id=uuid4(), batch_id=uuid4(),
    )
    assert result.counts.accepted == 10_000
    assert perf_counter() - started < 10.0
