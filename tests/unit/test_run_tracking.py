from __future__ import annotations

import json
from unittest.mock import MagicMock
from uuid import uuid4

import pytest
from sqlalchemy.exc import OperationalError

from etl_framework.control.run_tracking import (
    BatchCounts,
    InvalidStatusTransitionError,
    RunStatus,
    RunTracker,
    RunTrackingError,
    StepStatus,
    mask_audit_value,
)


def tracker_connection() -> tuple[RunTracker, MagicMock, MagicMock]:
    engine = MagicMock()
    connection = engine.begin.return_value.__enter__.return_value
    return RunTracker(engine), engine, connection


def test_pipeline_registration_is_idempotent_by_airflow_run_id() -> None:
    tracker, engine, connection = tracker_connection()
    existing_run_id = uuid4()
    connection.execute.return_value.scalar_one.return_value = existing_run_id

    result = tracker.register_pipeline_run("scheduled__2026-10-26", run_id=uuid4())

    assert result == existing_run_id
    statement, parameters = connection.execute.call_args.args
    assert "ON CONFLICT (airflow_run_id)" in str(statement)
    assert parameters["airflow_run_id"] == "scheduled__2026-10-26"
    engine.begin.assert_called_once_with()


def test_source_delivery_reuses_stable_batch_id() -> None:
    tracker, _, connection = tracker_connection()
    existing_batch_id = uuid4()
    connection.execute.return_value.scalar_one.return_value = existing_batch_id

    result = tracker.register_source_batch("CRM_PG", "lower/upper")

    assert result == existing_batch_id
    statement, parameters = connection.execute.call_args.args
    assert "ON CONFLICT (source_system, delivery_key)" in str(statement)
    assert parameters["source_system"] == "CRM_PG"
    assert parameters["delivery_key"] == "lower/upper"


def test_unsupported_source_is_rejected_before_database_access() -> None:
    tracker, engine, _ = tracker_connection()

    with pytest.raises(ValueError, match="Unsupported source system"):
        tracker.register_source_batch("UNKNOWN", "delivery")

    engine.begin.assert_not_called()


def test_batch_attempt_registration_is_idempotent_for_batch_and_run() -> None:
    tracker, _, connection = tracker_connection()
    existing_attempt_id = uuid4()
    connection.execute.return_value.scalar_one.return_value = existing_attempt_id

    result = tracker.register_batch_attempt(uuid4(), uuid4())

    assert result == existing_attempt_id
    statement, _ = connection.execute.call_args.args
    assert "ON CONFLICT (batch_id, run_id)" in str(statement)


def test_pipeline_run_follows_documented_transition() -> None:
    tracker, _, connection = tracker_connection()
    select_result = MagicMock()
    select_result.scalar_one_or_none.return_value = "PENDING"
    connection.execute.side_effect = [select_result, MagicMock()]
    run_id = uuid4()

    tracker.transition_pipeline_run(run_id, RunStatus.RUNNING)

    update_statement, parameters = connection.execute.call_args.args
    assert "UPDATE control.pipeline_run" in str(update_statement)
    assert parameters == {
        "record_id": run_id,
        "new_status": "RUNNING",
        "error_message": None,
    }


def test_invalid_transition_does_not_update_pipeline_run() -> None:
    tracker, _, connection = tracker_connection()
    select_result = MagicMock()
    select_result.scalar_one_or_none.return_value = "SUCCEEDED"
    connection.execute.return_value = select_result

    with pytest.raises(
        InvalidStatusTransitionError, match="SUCCEEDED -> RUNNING"
    ):
        tracker.transition_pipeline_run(uuid4(), RunStatus.RUNNING)

    assert connection.execute.call_count == 1


def test_batch_success_requires_balanced_counts() -> None:
    with pytest.raises(ValueError, match="source count"):
        BatchCounts(source=10, accepted=8, quarantined=1, duplicate=0)


def test_batch_success_persists_accounting_totals() -> None:
    tracker, _, connection = tracker_connection()
    select_result = MagicMock()
    select_result.mappings.return_value.one_or_none.return_value = {
        "status": "RUNNING",
        "source_row_count": None,
        "accepted_row_count": None,
        "quarantined_row_count": None,
        "duplicate_row_count": None,
    }
    connection.execute.side_effect = [select_result, MagicMock()]
    counts = BatchCounts(source=10, accepted=7, quarantined=2, duplicate=1)

    tracker.transition_batch_attempt(uuid4(), RunStatus.SUCCEEDED, counts=counts)

    _, parameters = connection.execute.call_args.args
    assert parameters["source_row_count"] == 10
    assert parameters["accepted_row_count"] == 7
    assert parameters["quarantined_row_count"] == 2
    assert parameters["duplicate_row_count"] == 1


def test_terminal_batch_quality_status_requires_counts() -> None:
    tracker, engine, _ = tracker_connection()

    with pytest.raises(ValueError, match="required for QUARANTINED"):
        tracker.transition_batch_attempt(uuid4(), RunStatus.QUARANTINED)

    engine.begin.assert_not_called()


def test_processing_step_attempt_and_transition_are_tracked() -> None:
    tracker, _, connection = tracker_connection()
    step_id = uuid4()
    connection.execute.return_value.scalar_one.return_value = step_id

    result = tracker.register_processing_step(
        uuid4(), "extract_crm", batch_id=uuid4(), attempt_number=2
    )

    assert result == step_id
    statement, parameters = connection.execute.call_args.args
    assert "control.processing_step" in str(statement)
    assert parameters["attempt_number"] == 2

    connection.reset_mock()
    select_result = MagicMock()
    select_result.scalar_one_or_none.return_value = "RUNNING"
    connection.execute.side_effect = [select_result, MagicMock()]

    tracker.transition_processing_step(step_id, StepStatus.SUCCEEDED)

    _, transition_parameters = connection.execute.call_args.args
    assert transition_parameters["new_status"] == "SUCCEEDED"


def test_audit_values_mask_nested_secrets_and_contact_information() -> None:
    masked = mask_audit_value(
        {
            "email": "person@example.com",
            "phone": "+1 212 555 0198",
            "authorization": "Bearer secret-token",
            "nested": {"message": "Email person@example.com or +1 212 555 0198"},
        }
    )

    assert masked == {
        "email": "[REDACTED_EMAIL]",
        "phone": "[REDACTED_PHONE]",
        "authorization": "[REDACTED]",
        "nested": {
            "message": "Email [REDACTED_EMAIL] or [REDACTED_PHONE]"
        },
    }


def test_audit_masking_preserves_dates_and_redacts_secret_assignments() -> None:
    message = "Run date 2026-10-26 failed with token=do-not-store"

    assert mask_audit_value(message) == (
        "Run date 2026-10-26 failed with token=[REDACTED]"
    )


def test_audit_event_persists_only_masked_values() -> None:
    tracker, _, connection = tracker_connection()
    connection.execute.return_value.scalar_one.return_value = 42

    event_id = tracker.record_audit_event(
        "SOURCE_FAILURE",
        "ingestion-worker",
        "Rejected person@example.com using Bearer abc123",
        details={"password": "never-store-me", "phone": "+1 212 555 0198"},
    )

    assert event_id == 42
    _, parameters = connection.execute.call_args.args
    assert parameters["message"] == (
        "Rejected [REDACTED_EMAIL] using Bearer [REDACTED]"
    )
    assert json.loads(parameters["event_details"]) == {
        "password": "[REDACTED]",
        "phone": "[REDACTED_PHONE]",
    }
    assert "never-store-me" not in str(parameters)


def test_database_error_is_sanitized() -> None:
    tracker, _, connection = tracker_connection()
    connection.execute.side_effect = OperationalError(
        "INSERT secret", {}, Exception("driver exposed never-log-this")
    )

    with pytest.raises(RunTrackingError) as error:
        tracker.register_pipeline_run("manual__2026-10-26")

    assert str(error.value) == "Pipeline run registration failed"
    assert "never-log-this" not in str(error.value)


@pytest.mark.parametrize("attempt_number", [0, -1, True, 1.5, "1"])
def test_invalid_processing_attempt_is_rejected(attempt_number: object) -> None:
    tracker, engine, _ = tracker_connection()

    with pytest.raises(ValueError, match="attempt_number"):
        tracker.register_processing_step(
            uuid4(), "extract_crm", attempt_number=attempt_number
        )

    engine.begin.assert_not_called()
