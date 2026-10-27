from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import MagicMock
from uuid import uuid4

import pytest
from sqlalchemy.exc import OperationalError

from etl_framework.ingestion.staging_loaders import (
    StagingLoadError,
    load_billing_transactions,
    load_crm_customers,
    load_operational_events,
)


UTC = timezone.utc


def configured_engine(*rowcounts: int) -> tuple[MagicMock, MagicMock]:
    engine = MagicMock()
    connection = engine.begin.return_value.__enter__.return_value
    results = []
    for rowcount in rowcounts:
        result = MagicMock()
        result.rowcount = rowcount
        results.append(result)
    connection.execute.side_effect = results
    return engine, connection


def customer(number: int) -> dict[str, object]:
    return {
        "source_row_id": number,
        "customer_id": f"C{number:06d}",
        "first_name": " Ada ",
        "last_name": "Lovelace",
        "email": "ADA@example.com",
        "phone": None,
        "country_code": "us",
        "status": "ACTIVE",
        "created_at": datetime(2026, 10, 1, tzinfo=UTC),
        "updated_at": datetime(2026, 10, number, tzinfo=UTC),
    }


def test_crm_load_maps_lineage_and_writes_bounded_chunks() -> None:
    engine, connection = configured_engine(0, 2, 1)
    run_id = uuid4()
    batch_id = uuid4()
    loaded_at = datetime(2026, 10, 27, 12, 0, tzinfo=UTC)

    result = load_crm_customers(
        engine,
        [customer(1), customer(2), customer(3)],
        run_id=run_id,
        batch_id=batch_id,
        batch_size=2,
        ingested_at=loaded_at,
    )

    assert result.rows_received == 3
    assert result.rows_inserted == 3
    assert connection.execute.call_count == 3
    delete_statement, delete_parameters = connection.execute.call_args_list[0].args
    assert "DELETE FROM stg.customer_raw" in str(delete_statement)
    assert delete_parameters == {"batch_id": batch_id}
    first_chunk = connection.execute.call_args_list[1].args[1]
    second_chunk = connection.execute.call_args_list[2].args[1]
    assert len(first_chunk) == 2
    assert len(second_chunk) == 1
    assert first_chunk[0]["run_id"] == run_id
    assert first_chunk[0]["batch_id"] == batch_id
    assert first_chunk[0]["source_system"] == "CRM_PG"
    assert first_chunk[0]["source_record_id"] == "C000001"
    assert first_chunk[0]["created_at_raw"] == "2026-10-01T00:00:00+00:00"
    assert first_chunk[0]["source_updated_at"] == datetime(2026, 10, 1, tzinfo=UTC)
    assert first_chunk[0]["ingested_at"] == loaded_at
    assert len(first_chunk[0]["record_checksum"]) == 64
    assert "ON CONFLICT (batch_id, record_checksum) DO NOTHING" in str(
        connection.execute.call_args_list[1].args[0]
    )


def test_billing_load_preserves_exact_decimal_representation() -> None:
    engine, connection = configured_engine(0, 1)

    result = load_billing_transactions(
        engine,
        [
            {
                "source_row_id": 1,
                "transaction_id": "T000001",
                "customer_id": "C000001",
                "transaction_timestamp": datetime(2026, 10, 20, tzinfo=UTC),
                "amount": Decimal("100.50"),
                "currency_code": "USD",
                "transaction_status": "completed",
                "payment_method": "card",
                "updated_at": datetime(2026, 10, 21, tzinfo=UTC),
            }
        ],
        run_id=uuid4(),
        batch_id=uuid4(),
    )

    assert result.rows_inserted == 1
    inserted = connection.execute.call_args_list[1].args[1][0]
    assert inserted["amount_raw"] == "100.50"
    assert inserted["source_system"] == "BILLING_MSSQL"
    assert inserted["source_record_id"] == "T000001"


def test_empty_load_replaces_batch_without_issuing_insert() -> None:
    engine, connection = configured_engine(0)
    batch_id = uuid4()

    result = load_crm_customers(engine, [], run_id=uuid4(), batch_id=batch_id)

    assert result.rows_received == 0
    assert result.rows_inserted == 0
    connection.execute.assert_called_once()
    assert connection.execute.call_args.args[1] == {"batch_id": batch_id}


def test_database_failure_rolls_back_entire_replacement() -> None:
    engine = MagicMock()
    transaction = engine.begin.return_value
    connection = transaction.__enter__.return_value
    connection.execute.side_effect = [
        MagicMock(),
        OperationalError("INSERT", {}, Exception("database unavailable secret-value")),
    ]

    with pytest.raises(StagingLoadError) as error:
        load_crm_customers(
            engine,
            [customer(1)],
            run_id=uuid4(),
            batch_id=uuid4(),
        )

    assert str(error.value) == "Staging load failed for CRM_PG"
    assert "secret-value" not in str(error.value)
    assert transaction.__exit__.call_args.args[0] is OperationalError


def test_replay_deletes_batch_and_reuses_stable_record_checksum() -> None:
    engine, connection = configured_engine(0, 1, 0, 1)
    batch_id = uuid4()
    source_row = customer(1)

    first = load_crm_customers(
        engine,
        [source_row],
        run_id=uuid4(),
        batch_id=batch_id,
        ingested_at=datetime(2026, 10, 27, 10, tzinfo=UTC),
    )
    second = load_crm_customers(
        engine,
        [source_row],
        run_id=uuid4(),
        batch_id=batch_id,
        ingested_at=datetime(2026, 10, 27, 11, tzinfo=UTC),
    )

    assert first.rows_inserted == second.rows_inserted == 1
    assert "DELETE FROM stg.customer_raw" in str(connection.execute.call_args_list[0].args[0])
    assert "DELETE FROM stg.customer_raw" in str(connection.execute.call_args_list[2].args[0])
    first_row = connection.execute.call_args_list[1].args[1][0]
    second_row = connection.execute.call_args_list[3].args[1][0]
    assert first_row["record_checksum"] == second_row["record_checksum"]
    assert first_row["run_id"] != second_row["run_id"]


def test_identical_rows_in_one_delivery_are_not_inserted_twice() -> None:
    engine, connection = configured_engine(0, 1)
    source_row = customer(1)

    result = load_crm_customers(
        engine,
        [source_row, source_row.copy()],
        run_id=uuid4(),
        batch_id=uuid4(),
        batch_size=10,
    )

    inserted_rows = connection.execute.call_args_list[1].args[1]
    assert result.rows_received == 2
    assert result.rows_inserted == 1
    assert inserted_rows[0]["record_checksum"] == inserted_rows[1]["record_checksum"]


def test_operational_load_preserves_file_and_row_context() -> None:
    engine, connection = configured_engine(0, 1)
    batch_id = uuid4()
    checksum = "a" * 64

    result = load_operational_events(
        engine,
        [
            {
                "source_row_number": 2,
                "event_id": "E000001",
                "customer_id": "C000001",
                "event_type": "service",
                "event_timestamp": "2026-10-20T09:00:00Z",
                "location_code": "NYC01",
                "details": "Completed",
            }
        ],
        run_id=uuid4(),
        batch_id=batch_id,
        source_file_name="operational_events_20261020.csv",
        source_file_checksum=checksum,
    )

    assert result.source_system == "OPS_CSV"
    inserted = connection.execute.call_args_list[1].args[1][0]
    assert inserted["source_file_name"] == "operational_events_20261020.csv"
    assert inserted["source_file_checksum"] == checksum
    assert inserted["source_updated_at"] == datetime(2026, 10, 20, 9, tzinfo=UTC)


@pytest.mark.parametrize("batch_size", [0, -1, 100_001, True, 1.5, "100"])
def test_invalid_batch_size_is_rejected_before_transaction(batch_size: object) -> None:
    engine = MagicMock()

    with pytest.raises(ValueError, match="batch_size"):
        load_crm_customers(
            engine,
            [],
            run_id=uuid4(),
            batch_id=uuid4(),
            batch_size=batch_size,
        )

    engine.begin.assert_not_called()
