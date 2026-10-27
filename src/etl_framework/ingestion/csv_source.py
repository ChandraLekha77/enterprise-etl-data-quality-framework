"""Strict operational CSV contract validation and registered ingestion."""

from __future__ import annotations

import csv
import hashlib
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import UUID

from sqlalchemy.engine import Engine

from etl_framework.control.run_tracking import RunStatus, RunTracker
from etl_framework.ingestion.staging_loaders import (
    DEFAULT_WRITE_BATCH_SIZE,
    StagingLoadResult,
    load_operational_events,
)


OPS_SOURCE = "OPS_CSV"
OPS_FILENAME = re.compile(r"^operational_events_(\d{8})\.csv$")
REQUIRED_HEADERS = frozenset(
    {"event_id", "customer_id", "event_type", "event_timestamp"}
)


class CsvContractError(ValueError):
    """Raised when an operational file violates its source contract."""


@dataclass(frozen=True, slots=True)
class OperationalCsvDelivery:
    path: Path
    file_name: str
    file_checksum: str
    rows: tuple[dict[str, Any], ...]

    @property
    def delivery_key(self) -> str:
        return f"{self.file_name}:{self.file_checksum}"


@dataclass(frozen=True, slots=True)
class OperationalCsvIngestionResult:
    batch_id: UUID
    batch_run_id: UUID
    delivery_key: str
    load: StagingLoadResult


def read_operational_csv(path: str | Path) -> OperationalCsvDelivery:
    """Validate one complete CSV delivery before any database state is changed."""
    source_path = Path(path)
    _validate_filename(source_path.name)
    checksum = _file_checksum(source_path)
    try:
        source = source_path.open(encoding="utf-8-sig", errors="strict", newline="")
    except OSError as exc:
        raise CsvContractError("Operational CSV could not be opened") from exc
    try:
        with source:
            reader = csv.reader(source, strict=True)
            try:
                header = next(reader)
            except StopIteration as exc:
                raise CsvContractError("Operational CSV is missing its header") from exc
            _validate_header(header)
            rows: list[dict[str, Any]] = []
            for line_number, values in enumerate(reader, start=2):
                if len(values) != len(header):
                    raise CsvContractError(
                        f"Operational CSV row {line_number} has {len(values)} fields; "
                        f"expected {len(header)}"
                    )
                row = dict(zip(header, values, strict=True))
                row["source_row_number"] = line_number
                rows.append(row)
    except UnicodeDecodeError as exc:
        raise CsvContractError("Operational CSV must be valid UTF-8") from exc
    except csv.Error as exc:
        raise CsvContractError("Operational CSV contains malformed CSV syntax") from exc
    return OperationalCsvDelivery(source_path, source_path.name, checksum, tuple(rows))


def ingest_operational_csv(
    engine: Engine,
    path: str | Path,
    *,
    run_id: UUID,
    batch_size: int = DEFAULT_WRITE_BATCH_SIZE,
    tracker: RunTracker | None = None,
) -> OperationalCsvIngestionResult:
    """Register and stage one validated operational file delivery."""
    delivery = read_operational_csv(path)
    active_tracker = tracker or RunTracker(engine)
    batch_id = active_tracker.register_source_batch(OPS_SOURCE, delivery.delivery_key)
    batch_run_id = active_tracker.register_batch_attempt(batch_id, run_id)
    active_tracker.transition_batch_attempt(batch_run_id, RunStatus.RUNNING)
    try:
        load = load_operational_events(
            engine,
            delivery.rows,
            run_id=run_id,
            batch_id=batch_id,
            source_file_name=delivery.file_name,
            source_file_checksum=delivery.file_checksum,
            batch_size=batch_size,
        )
    except Exception:
        active_tracker.transition_batch_attempt(
            batch_run_id,
            RunStatus.FAILED,
            error_message="Operational CSV staging failed",
        )
        raise
    return OperationalCsvIngestionResult(
        batch_id=batch_id,
        batch_run_id=batch_run_id,
        delivery_key=delivery.delivery_key,
        load=load,
    )


def _validate_filename(file_name: str) -> None:
    match = OPS_FILENAME.fullmatch(file_name)
    if match is None:
        raise CsvContractError(
            "Operational CSV filename must match operational_events_YYYYMMDD.csv"
        )
    try:
        datetime.strptime(match.group(1), "%Y%m%d")
    except ValueError as exc:
        raise CsvContractError("Operational CSV filename contains an invalid date") from exc


def _validate_header(header: list[str]) -> None:
    if not header or any(not column for column in header):
        raise CsvContractError("Operational CSV contains a blank or missing header")
    duplicates = sorted({column for column in header if header.count(column) > 1})
    if duplicates:
        raise CsvContractError(
            f"Operational CSV contains duplicate headers: {', '.join(duplicates)}"
        )
    missing = sorted(REQUIRED_HEADERS.difference(header))
    if missing:
        raise CsvContractError(
            f"Operational CSV is missing required headers: {', '.join(missing)}"
        )


def _file_checksum(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as source:
            for chunk in iter(lambda: source.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError as exc:
        raise CsvContractError("Operational CSV could not be read") from exc
    return digest.hexdigest()
