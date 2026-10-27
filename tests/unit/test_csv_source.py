from __future__ import annotations

import hashlib
from pathlib import Path
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

import etl_framework.ingestion.csv_source as csv_source
from etl_framework.control.run_tracking import RunStatus
from etl_framework.ingestion.csv_source import (
    CsvContractError,
    ingest_operational_csv,
    read_operational_csv,
)
from etl_framework.ingestion.staging_loaders import StagingLoadResult


HEADER = "event_id,customer_id,event_type,event_timestamp,location_code,details\n"
VALID_ROW = "E1,C1,service,2026-10-20T09:00:00Z,NYC01,Completed\n"


def write_delivery(
    tmp_path: Path,
    content: str,
    *,
    name: str = "operational_events_20261020.csv",
) -> Path:
    path = tmp_path / name
    path.write_text(content, encoding="utf-8", newline="")
    return path


def test_valid_file_is_parsed_with_stable_checksum_and_row_number(tmp_path: Path) -> None:
    path = write_delivery(tmp_path, HEADER + VALID_ROW)

    delivery = read_operational_csv(path)

    assert delivery.file_name == path.name
    assert delivery.file_checksum == hashlib.sha256(path.read_bytes()).hexdigest()
    assert delivery.delivery_key == f"{path.name}:{delivery.file_checksum}"
    assert delivery.rows == (
        {
            "event_id": "E1",
            "customer_id": "C1",
            "event_type": "service",
            "event_timestamp": "2026-10-20T09:00:00Z",
            "location_code": "NYC01",
            "details": "Completed",
            "source_row_number": 2,
        },
    )


def test_header_only_file_is_valid_empty_delivery(tmp_path: Path) -> None:
    delivery = read_operational_csv(write_delivery(tmp_path, HEADER))

    assert delivery.rows == ()


@pytest.mark.parametrize(
    "name",
    [
        "events_20261020.csv",
        "operational_events_2026-10-20.csv",
        "operational_events_20261340.csv",
        "operational_events_20261020.CSV",
    ],
)
def test_invalid_filename_is_rejected(tmp_path: Path, name: str) -> None:
    with pytest.raises(CsvContractError, match="filename"):
        read_operational_csv(write_delivery(tmp_path, HEADER, name=name))


def test_invalid_utf8_is_rejected(tmp_path: Path) -> None:
    path = tmp_path / "operational_events_20261020.csv"
    path.write_bytes(HEADER.encode("utf-8") + b"E1,C1,service,\xff\n")

    with pytest.raises(CsvContractError, match="valid UTF-8"):
        read_operational_csv(path)


@pytest.mark.parametrize(
    ("content", "message"),
    [
        (
            "event_id,customer_id,event_type,event_type,event_timestamp\n",
            "duplicate headers",
        ),
        ("event_id,customer_id,event_timestamp\n", "missing required headers"),
        ("event_id,customer_id,,event_timestamp\n", "blank or missing header"),
        (HEADER + "E1,C1,service\n", "has 3 fields"),
        (HEADER + "E1,C1,service,2026-10-20T09:00:00Z,NYC01,x,extra\n", "has 7 fields"),
    ],
)
def test_malformed_headers_and_rows_are_rejected(
    tmp_path: Path, content: str, message: str
) -> None:
    with pytest.raises(CsvContractError, match=message):
        read_operational_csv(write_delivery(tmp_path, content))


def test_malformed_quoted_csv_is_rejected(tmp_path: Path) -> None:
    path = write_delivery(tmp_path, HEADER + 'E1,C1,service,"unterminated\n')

    with pytest.raises(CsvContractError, match="malformed CSV syntax"):
        read_operational_csv(path)


def test_ingestion_registers_delivery_attempt_and_loads_staging(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = write_delivery(tmp_path, HEADER + VALID_ROW)
    tracker = MagicMock()
    batch_id = uuid4()
    batch_run_id = uuid4()
    run_id = uuid4()
    tracker.register_source_batch.return_value = batch_id
    tracker.register_batch_attempt.return_value = batch_run_id
    loader = MagicMock(return_value=StagingLoadResult("OPS_CSV", 1, 1))
    monkeypatch.setattr(csv_source, "load_operational_events", loader)

    result = ingest_operational_csv(
        MagicMock(), path, run_id=run_id, batch_size=25, tracker=tracker
    )

    delivery = read_operational_csv(path)
    tracker.register_source_batch.assert_called_once_with(
        "OPS_CSV", delivery.delivery_key
    )
    tracker.register_batch_attempt.assert_called_once_with(batch_id, run_id)
    tracker.transition_batch_attempt.assert_called_once_with(
        batch_run_id, RunStatus.RUNNING
    )
    assert loader.call_args.kwargs["batch_id"] == batch_id
    assert loader.call_args.kwargs["source_file_checksum"] == delivery.file_checksum
    assert loader.call_args.kwargs["batch_size"] == 25
    assert result.batch_id == batch_id
    assert result.load.rows_inserted == 1


def test_duplicate_file_delivery_reuses_registered_batch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = write_delivery(tmp_path, HEADER + VALID_ROW)
    tracker = MagicMock()
    stable_batch_id = uuid4()
    tracker.register_source_batch.return_value = stable_batch_id
    tracker.register_batch_attempt.side_effect = [uuid4(), uuid4()]
    loader = MagicMock(return_value=StagingLoadResult("OPS_CSV", 1, 1))
    monkeypatch.setattr(csv_source, "load_operational_events", loader)

    first = ingest_operational_csv(
        MagicMock(), path, run_id=uuid4(), tracker=tracker
    )
    second = ingest_operational_csv(
        MagicMock(), path, run_id=uuid4(), tracker=tracker
    )

    assert first.batch_id == second.batch_id == stable_batch_id
    first_delivery_key = tracker.register_source_batch.call_args_list[0].args[1]
    second_delivery_key = tracker.register_source_batch.call_args_list[1].args[1]
    assert first_delivery_key == second_delivery_key
    assert all(call.kwargs["batch_id"] == stable_batch_id for call in loader.call_args_list)


def test_staging_failure_marks_registered_attempt_failed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = write_delivery(tmp_path, HEADER + VALID_ROW)
    tracker = MagicMock()
    tracker.register_source_batch.return_value = uuid4()
    batch_run_id = uuid4()
    tracker.register_batch_attempt.return_value = batch_run_id
    monkeypatch.setattr(
        csv_source,
        "load_operational_events",
        MagicMock(side_effect=RuntimeError("database password must not escape")),
    )

    with pytest.raises(RuntimeError):
        ingest_operational_csv(MagicMock(), path, run_id=uuid4(), tracker=tracker)

    assert tracker.transition_batch_attempt.call_args_list[-1].args == (
        batch_run_id,
        RunStatus.FAILED,
    )
    assert tracker.transition_batch_attempt.call_args_list[-1].kwargs == {
        "error_message": "Operational CSV staging failed"
    }
