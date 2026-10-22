from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import MagicMock

import pytest
from sqlalchemy.exc import OperationalError

from etl_framework.ingestion.database_sources import (
    ExtractionWindow,
    SourceExtractionError,
    extract_billing_transactions,
    extract_crm_customers,
)


UTC = timezone.utc


def configured_engine(rows: list[dict[str, object]]) -> tuple[MagicMock, MagicMock]:
    engine = MagicMock()
    connection = engine.connect.return_value
    connection.execution_options.return_value = connection
    mappings = connection.execute.return_value.mappings.return_value
    mappings.partitions.return_value = [rows[:1], rows[1:]]
    return engine, connection


def test_window_requires_timezone_aware_values() -> None:
    with pytest.raises(ValueError, match="upper_watermark"):
        ExtractionWindow(upper_watermark=datetime(2026, 10, 22))


def test_window_rejects_reversed_bounds() -> None:
    upper = datetime(2026, 10, 22, tzinfo=UTC)

    with pytest.raises(ValueError, match="lower_watermark"):
        ExtractionWindow(upper_watermark=upper, lower_watermark=upper + timedelta(seconds=1))


def test_window_builds_stable_utc_delivery_key() -> None:
    eastern = timezone(timedelta(hours=-4))
    window = ExtractionWindow(
        lower_watermark=datetime(2026, 10, 21, 8, 0, tzinfo=eastern),
        upper_watermark=datetime(2026, 10, 22, 8, 0, tzinfo=eastern),
    )

    assert window.delivery_key == (
        "2026-10-21T12:00:00.000000Z/2026-10-22T12:00:00.000000Z"
    )


def test_crm_extraction_is_incremental_chunked_and_closes_connection() -> None:
    engine, connection = configured_engine(
        [
            {"source_row_id": 1, "customer_id": "C000001"},
            {"source_row_id": 2, "customer_id": "C000002"},
        ]
    )
    window = ExtractionWindow(
        lower_watermark=datetime(2026, 10, 21, tzinfo=UTC),
        upper_watermark=datetime(2026, 10, 22, tzinfo=UTC),
    )

    rows = list(extract_crm_customers(engine, window, fetch_size=1))

    assert [row["customer_id"] for row in rows] == ["C000001", "C000002"]
    statement, parameters = connection.execute.call_args.args
    assert "updated_at > :lower_watermark" in str(statement)
    assert parameters == window.parameters
    connection.execution_options.assert_called_once_with(stream_results=True, yield_per=1)
    connection.close.assert_called_once_with()


def test_initial_extraction_omits_lower_watermark_parameter() -> None:
    engine, connection = configured_engine([])
    window = ExtractionWindow(upper_watermark=datetime(2026, 10, 22, tzinfo=UTC))

    assert list(extract_crm_customers(engine, window)) == []

    statement, parameters = connection.execute.call_args.args
    assert ":lower_watermark" not in str(statement)
    assert parameters == {"upper_watermark": window.upper_watermark}


def test_billing_extraction_preserves_decimal_values() -> None:
    engine, _ = configured_engine(
        [{"source_row_id": 1, "transaction_id": "T000001", "amount": Decimal("10.25")}]
    )
    window = ExtractionWindow(upper_watermark=datetime(2026, 10, 22, tzinfo=UTC))

    row = next(extract_billing_transactions(engine, window))

    assert row["amount"] == Decimal("10.25")
    assert isinstance(row["amount"], Decimal)


@pytest.mark.parametrize("fetch_size", [0, 100_001, True, 1.5, "1000"])
def test_invalid_fetch_size_is_rejected(fetch_size: object) -> None:
    engine = MagicMock()
    window = ExtractionWindow(upper_watermark=datetime(2026, 10, 22, tzinfo=UTC))

    with pytest.raises(ValueError, match="fetch_size"):
        list(extract_crm_customers(engine, window, fetch_size=fetch_size))

    engine.connect.assert_not_called()


def test_database_error_is_sanitized_and_connection_is_closed() -> None:
    engine, connection = configured_engine([])
    connection.execute.side_effect = OperationalError(
        "SELECT secret", {}, Exception("driver rejected super-secret-password")
    )
    window = ExtractionWindow(upper_watermark=datetime(2026, 10, 22, tzinfo=UTC))

    with pytest.raises(SourceExtractionError) as error:
        list(extract_billing_transactions(engine, window))

    assert str(error.value) == "Source extraction failed for BILLING_MSSQL"
    assert "super-secret-password" not in str(error.value)
    connection.close.assert_called_once_with()
