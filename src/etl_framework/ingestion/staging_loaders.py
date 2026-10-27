"""Replay-safe, bounded writes into the PostgreSQL staging schema."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql.elements import TextClause


DEFAULT_WRITE_BATCH_SIZE = 1_000
MAX_WRITE_BATCH_SIZE = 100_000


class StagingLoadError(RuntimeError):
    """Raised when a staging batch cannot be written atomically."""


@dataclass(frozen=True, slots=True)
class StagingLoadResult:
    source_system: str
    rows_received: int
    rows_inserted: int


DELETE_CUSTOMER_BATCH = text(
    "DELETE FROM stg.customer_raw WHERE batch_id = :batch_id"
)
DELETE_TRANSACTION_BATCH = text(
    "DELETE FROM stg.transaction_raw WHERE batch_id = :batch_id"
)
DELETE_OPERATIONAL_BATCH = text(
    "DELETE FROM stg.operational_event_raw WHERE batch_id = :batch_id"
)

INSERT_CUSTOMERS = text(
    """
    INSERT INTO stg.customer_raw (
        customer_id, first_name, last_name, email, phone, country_code,
        status, created_at_raw, updated_at_raw, run_id, batch_id,
        source_system, source_record_id, source_updated_at,
        record_checksum, ingested_at
    ) VALUES (
        :customer_id, :first_name, :last_name, :email, :phone, :country_code,
        :status, :created_at_raw, :updated_at_raw, :run_id, :batch_id,
        :source_system, :source_record_id, :source_updated_at,
        :record_checksum, :ingested_at
    )
    ON CONFLICT (batch_id, record_checksum) DO NOTHING
    """
)

INSERT_TRANSACTIONS = text(
    """
    INSERT INTO stg.transaction_raw (
        transaction_id, customer_id, transaction_timestamp_raw, amount_raw,
        currency_code, transaction_status, payment_method, updated_at_raw,
        run_id, batch_id, source_system, source_record_id,
        source_updated_at, record_checksum, ingested_at
    ) VALUES (
        :transaction_id, :customer_id, :transaction_timestamp_raw, :amount_raw,
        :currency_code, :transaction_status, :payment_method, :updated_at_raw,
        :run_id, :batch_id, :source_system, :source_record_id,
        :source_updated_at, :record_checksum, :ingested_at
    )
    ON CONFLICT (batch_id, record_checksum) DO NOTHING
    """
)

INSERT_OPERATIONAL_EVENTS = text(
    """
    INSERT INTO stg.operational_event_raw (
        event_id, customer_id, event_type, event_timestamp_raw,
        location_code, details, source_file_name, source_file_checksum,
        run_id, batch_id, source_system, source_record_id,
        source_updated_at, record_checksum, ingested_at
    ) VALUES (
        :event_id, :customer_id, :event_type, :event_timestamp_raw,
        :location_code, :details, :source_file_name, :source_file_checksum,
        :run_id, :batch_id, :source_system, :source_record_id,
        :source_updated_at, :record_checksum, :ingested_at
    )
    ON CONFLICT (batch_id, record_checksum) DO NOTHING
    """
)


def load_crm_customers(
    engine: Engine,
    rows: Iterable[Mapping[str, Any]],
    *,
    run_id: UUID,
    batch_id: UUID,
    batch_size: int = DEFAULT_WRITE_BATCH_SIZE,
    ingested_at: datetime | None = None,
) -> StagingLoadResult:
    """Replace one CRM batch atomically and return its write counts."""
    timestamp = _ingestion_timestamp(ingested_at)
    return _replace_batch(
        engine,
        rows,
        run_id=run_id,
        batch_id=batch_id,
        source_system="CRM_PG",
        batch_size=batch_size,
        ingested_at=timestamp,
        delete_statement=DELETE_CUSTOMER_BATCH,
        insert_statement=INSERT_CUSTOMERS,
        mapper=_map_customer,
    )


def load_billing_transactions(
    engine: Engine,
    rows: Iterable[Mapping[str, Any]],
    *,
    run_id: UUID,
    batch_id: UUID,
    batch_size: int = DEFAULT_WRITE_BATCH_SIZE,
    ingested_at: datetime | None = None,
) -> StagingLoadResult:
    """Replace one billing batch atomically while preserving exact amounts."""
    timestamp = _ingestion_timestamp(ingested_at)
    return _replace_batch(
        engine,
        rows,
        run_id=run_id,
        batch_id=batch_id,
        source_system="BILLING_MSSQL",
        batch_size=batch_size,
        ingested_at=timestamp,
        delete_statement=DELETE_TRANSACTION_BATCH,
        insert_statement=INSERT_TRANSACTIONS,
        mapper=_map_transaction,
    )


def load_operational_events(
    engine: Engine,
    rows: Iterable[Mapping[str, Any]],
    *,
    run_id: UUID,
    batch_id: UUID,
    source_file_name: str,
    source_file_checksum: str,
    batch_size: int = DEFAULT_WRITE_BATCH_SIZE,
    ingested_at: datetime | None = None,
) -> StagingLoadResult:
    """Replace one operational-file batch atomically."""
    timestamp = _ingestion_timestamp(ingested_at)

    def mapper(
        row: Mapping[str, Any], row_run_id: UUID, row_batch_id: UUID, loaded_at: datetime
    ) -> dict[str, Any]:
        return _map_operational_event(
            row,
            row_run_id,
            row_batch_id,
            loaded_at,
            source_file_name=source_file_name,
            source_file_checksum=source_file_checksum,
        )

    return _replace_batch(
        engine,
        rows,
        run_id=run_id,
        batch_id=batch_id,
        source_system="OPS_CSV",
        batch_size=batch_size,
        ingested_at=timestamp,
        delete_statement=DELETE_OPERATIONAL_BATCH,
        insert_statement=INSERT_OPERATIONAL_EVENTS,
        mapper=mapper,
    )


def _replace_batch(
    engine: Engine,
    rows: Iterable[Mapping[str, Any]],
    *,
    run_id: UUID,
    batch_id: UUID,
    source_system: str,
    batch_size: int,
    ingested_at: datetime,
    delete_statement: TextClause,
    insert_statement: TextClause,
    mapper: Callable[[Mapping[str, Any], UUID, UUID, datetime], dict[str, Any]],
) -> StagingLoadResult:
    _validate_batch_size(batch_size)
    received = 0
    inserted = 0
    pending: list[dict[str, Any]] = []
    try:
        with engine.begin() as connection:
            # A replay rebuilds the batch inside the same transaction. A later
            # failure therefore restores the previously committed batch.
            connection.execute(delete_statement, {"batch_id": batch_id})
            for source_row in rows:
                pending.append(mapper(source_row, run_id, batch_id, ingested_at))
                received += 1
                if len(pending) >= batch_size:
                    inserted += _insert_chunk(connection, insert_statement, pending)
                    pending = []
            if pending:
                inserted += _insert_chunk(connection, insert_statement, pending)
    except SQLAlchemyError as exc:
        raise StagingLoadError(f"Staging load failed for {source_system}") from exc
    return StagingLoadResult(source_system, received, inserted)


def _insert_chunk(connection: Any, statement: TextClause, rows: list[dict[str, Any]]) -> int:
    result = connection.execute(statement, rows)
    rowcount = result.rowcount
    return rowcount if isinstance(rowcount, int) and rowcount >= 0 else len(rows)


def _map_customer(
    row: Mapping[str, Any], run_id: UUID, batch_id: UUID, ingested_at: datetime
) -> dict[str, Any]:
    raw = {
        "source_row_id": _raw_text(row.get("source_row_id")),
        "customer_id": _raw_text(row.get("customer_id")),
        "first_name": _raw_text(row.get("first_name")),
        "last_name": _raw_text(row.get("last_name")),
        "email": _raw_text(row.get("email")),
        "phone": _raw_text(row.get("phone")),
        "country_code": _raw_text(row.get("country_code")),
        "status": _raw_text(row.get("status")),
        "created_at": _raw_text(row.get("created_at")),
        "updated_at": _raw_text(row.get("updated_at")),
    }
    return {
        "customer_id": raw["customer_id"],
        "first_name": raw["first_name"],
        "last_name": raw["last_name"],
        "email": raw["email"],
        "phone": raw["phone"],
        "country_code": raw["country_code"],
        "status": raw["status"],
        "created_at_raw": raw["created_at"],
        "updated_at_raw": raw["updated_at"],
        "run_id": run_id,
        "batch_id": batch_id,
        "source_system": "CRM_PG",
        "source_record_id": raw["customer_id"],
        "source_updated_at": _source_timestamp(row.get("updated_at")),
        "record_checksum": _record_checksum("CRM_PG", raw),
        "ingested_at": ingested_at,
    }


def _map_transaction(
    row: Mapping[str, Any], run_id: UUID, batch_id: UUID, ingested_at: datetime
) -> dict[str, Any]:
    raw = {
        "source_row_id": _raw_text(row.get("source_row_id")),
        "transaction_id": _raw_text(row.get("transaction_id")),
        "customer_id": _raw_text(row.get("customer_id")),
        "transaction_timestamp": _raw_text(row.get("transaction_timestamp")),
        "amount": _raw_text(row.get("amount")),
        "currency_code": _raw_text(row.get("currency_code")),
        "transaction_status": _raw_text(row.get("transaction_status")),
        "payment_method": _raw_text(row.get("payment_method")),
        "updated_at": _raw_text(row.get("updated_at")),
    }
    return {
        "transaction_id": raw["transaction_id"],
        "customer_id": raw["customer_id"],
        "transaction_timestamp_raw": raw["transaction_timestamp"],
        "amount_raw": raw["amount"],
        "currency_code": raw["currency_code"],
        "transaction_status": raw["transaction_status"],
        "payment_method": raw["payment_method"],
        "updated_at_raw": raw["updated_at"],
        "run_id": run_id,
        "batch_id": batch_id,
        "source_system": "BILLING_MSSQL",
        "source_record_id": raw["transaction_id"],
        "source_updated_at": _source_timestamp(row.get("updated_at")),
        "record_checksum": _record_checksum("BILLING_MSSQL", raw),
        "ingested_at": ingested_at,
    }


def _map_operational_event(
    row: Mapping[str, Any],
    run_id: UUID,
    batch_id: UUID,
    ingested_at: datetime,
    *,
    source_file_name: str,
    source_file_checksum: str,
) -> dict[str, Any]:
    raw = {
        "source_row_number": row.get("source_row_number"),
        "event_id": _raw_text(row.get("event_id")),
        "customer_id": _raw_text(row.get("customer_id")),
        "event_type": _raw_text(row.get("event_type")),
        "event_timestamp": _raw_text(row.get("event_timestamp")),
        "location_code": _raw_text(row.get("location_code")),
        "details": _raw_text(row.get("details")),
    }
    return {
        "event_id": raw["event_id"],
        "customer_id": raw["customer_id"],
        "event_type": raw["event_type"],
        "event_timestamp_raw": raw["event_timestamp"],
        "location_code": raw["location_code"],
        "details": raw["details"],
        "source_file_name": source_file_name,
        "source_file_checksum": source_file_checksum,
        "run_id": run_id,
        "batch_id": batch_id,
        "source_system": "OPS_CSV",
        "source_record_id": raw["event_id"],
        "source_updated_at": _source_timestamp(row.get("event_timestamp")),
        "record_checksum": _record_checksum("OPS_CSV", raw),
        "ingested_at": ingested_at,
    }


def _record_checksum(source_system: str, raw: Mapping[str, Any]) -> str:
    canonical = json.dumps(
        {"source_system": source_system, "record": raw},
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
        default=_raw_text,
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def _raw_text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Decimal):
        return format(value, "f")
    return str(value)


def _source_timestamp(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(timezone.utc)


def _ingestion_timestamp(value: datetime | None) -> datetime:
    timestamp = datetime.now(timezone.utc) if value is None else value
    if timestamp.tzinfo is None or timestamp.utcoffset() is None:
        raise ValueError("ingested_at must include a timezone")
    return timestamp.astimezone(timezone.utc)


def _validate_batch_size(batch_size: int) -> None:
    if (
        isinstance(batch_size, bool)
        or not isinstance(batch_size, int)
        or not 1 <= batch_size <= MAX_WRITE_BATCH_SIZE
    ):
        raise ValueError(
            f"batch_size must be between 1 and {MAX_WRITE_BATCH_SIZE}"
        )
