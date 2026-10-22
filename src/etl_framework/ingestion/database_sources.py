"""Incremental, read-only extraction from the database source systems."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql.elements import TextClause


CRM_SOURCE = "CRM_PG"
BILLING_SOURCE = "BILLING_MSSQL"
DEFAULT_FETCH_SIZE = 1_000
MAX_FETCH_SIZE = 100_000


class SourceExtractionError(RuntimeError):
    """Raised when a source query cannot be completed safely."""


@dataclass(frozen=True, slots=True)
class ExtractionWindow:
    """A lower-exclusive, upper-inclusive incremental extraction window."""

    upper_watermark: datetime
    lower_watermark: datetime | None = None

    def __post_init__(self) -> None:
        _require_timezone(self.upper_watermark, "upper_watermark")
        if self.lower_watermark is not None:
            _require_timezone(self.lower_watermark, "lower_watermark")
            if self.lower_watermark > self.upper_watermark:
                raise ValueError("lower_watermark must not be later than upper_watermark")

    @property
    def delivery_key(self) -> str:
        """Return a stable identity for this exact extraction window."""
        lower = _watermark_text(self.lower_watermark) if self.lower_watermark else "BEGINNING"
        return f"{lower}/{_watermark_text(self.upper_watermark)}"

    @property
    def parameters(self) -> dict[str, datetime]:
        parameters = {"upper_watermark": self.upper_watermark}
        if self.lower_watermark is not None:
            parameters["lower_watermark"] = self.lower_watermark
        return parameters


CRM_INITIAL_QUERY = text(
    """
    SELECT
        source_row_id,
        customer_id,
        first_name,
        last_name,
        email,
        phone,
        country_code,
        status,
        created_at,
        updated_at
    FROM crm.customers
    WHERE updated_at IS NULL OR updated_at <= :upper_watermark
    ORDER BY updated_at ASC NULLS FIRST, source_row_id ASC
    """
)

CRM_INCREMENTAL_QUERY = text(
    """
    SELECT
        source_row_id,
        customer_id,
        first_name,
        last_name,
        email,
        phone,
        country_code,
        status,
        created_at,
        updated_at
    FROM crm.customers
    WHERE updated_at IS NULL
       OR (updated_at > :lower_watermark AND updated_at <= :upper_watermark)
    ORDER BY updated_at ASC NULLS FIRST, source_row_id ASC
    """
)

BILLING_INITIAL_QUERY = text(
    """
    SELECT
        source_row_id,
        transaction_id,
        customer_id,
        transaction_timestamp,
        amount,
        currency_code,
        transaction_status,
        payment_method,
        updated_at
    FROM dbo.transactions
    WHERE updated_at IS NULL OR updated_at <= :upper_watermark
    ORDER BY CASE WHEN updated_at IS NULL THEN 0 ELSE 1 END, updated_at ASC, source_row_id ASC
    """
)

BILLING_INCREMENTAL_QUERY = text(
    """
    SELECT
        source_row_id,
        transaction_id,
        customer_id,
        transaction_timestamp,
        amount,
        currency_code,
        transaction_status,
        payment_method,
        updated_at
    FROM dbo.transactions
    WHERE updated_at IS NULL
       OR (updated_at > :lower_watermark AND updated_at <= :upper_watermark)
    ORDER BY CASE WHEN updated_at IS NULL THEN 0 ELSE 1 END, updated_at ASC, source_row_id ASC
    """
)


def extract_crm_customers(
    engine: Engine,
    window: ExtractionWindow,
    *,
    fetch_size: int = DEFAULT_FETCH_SIZE,
) -> Iterator[dict[str, Any]]:
    """Stream customer source rows in deterministic watermark order."""
    statement = CRM_INCREMENTAL_QUERY if window.lower_watermark is not None else CRM_INITIAL_QUERY
    yield from _stream_rows(engine, statement, window, CRM_SOURCE, fetch_size)


def extract_billing_transactions(
    engine: Engine,
    window: ExtractionWindow,
    *,
    fetch_size: int = DEFAULT_FETCH_SIZE,
) -> Iterator[dict[str, Any]]:
    """Stream transaction source rows without converting exact decimal values."""
    statement = (
        BILLING_INCREMENTAL_QUERY
        if window.lower_watermark is not None
        else BILLING_INITIAL_QUERY
    )
    yield from _stream_rows(engine, statement, window, BILLING_SOURCE, fetch_size)


def _stream_rows(
    engine: Engine,
    statement: TextClause,
    window: ExtractionWindow,
    source_system: str,
    fetch_size: int,
) -> Iterator[dict[str, Any]]:
    _validate_fetch_size(fetch_size)
    connection = None
    try:
        connection = engine.connect()
        streaming_connection = connection.execution_options(
            stream_results=True,
            yield_per=fetch_size,
        )
        result = streaming_connection.execute(statement, window.parameters).mappings()
        for partition in result.partitions(fetch_size):
            for row in partition:
                yield dict(row)
    except SQLAlchemyError as exc:
        raise SourceExtractionError(f"Source extraction failed for {source_system}") from exc
    finally:
        if connection is not None:
            connection.close()


def _validate_fetch_size(fetch_size: int) -> None:
    if (
        isinstance(fetch_size, bool)
        or not isinstance(fetch_size, int)
        or not 1 <= fetch_size <= MAX_FETCH_SIZE
    ):
        raise ValueError(f"fetch_size must be between 1 and {MAX_FETCH_SIZE}")


def _require_timezone(value: datetime, field_name: str) -> None:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must include a timezone")


def _watermark_text(value: datetime) -> str:
    return (
        value.astimezone(timezone.utc)
        .isoformat(timespec="microseconds")
        .replace("+00:00", "Z")
    )
