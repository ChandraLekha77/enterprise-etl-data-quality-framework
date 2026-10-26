"""Transactional run, batch, processing-step, and audit tracking."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError


ALLOWED_SOURCE_SYSTEMS = frozenset(
    {"CRM_PG", "BILLING_MSSQL", "OPS_CSV", "STATUS_API"}
)


class RunTrackingError(RuntimeError):
    """Raised when control metadata cannot be recorded safely."""


class RecordNotFoundError(RunTrackingError):
    """Raised when a requested control record does not exist."""


class InvalidStatusTransitionError(RunTrackingError):
    """Raised when a lifecycle change violates the documented state machine."""


class RunStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    QUARANTINED = "QUARANTINED"
    FAILED = "FAILED"
    SUCCEEDED = "SUCCEEDED"
    CANCELLED = "CANCELLED"


class StepStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"
    SUCCEEDED = "SUCCEEDED"


RUN_TRANSITIONS: Mapping[RunStatus, frozenset[RunStatus]] = {
    RunStatus.PENDING: frozenset({RunStatus.RUNNING, RunStatus.CANCELLED}),
    RunStatus.RUNNING: frozenset(
        {RunStatus.FAILED, RunStatus.QUARANTINED, RunStatus.SUCCEEDED}
    ),
    RunStatus.QUARANTINED: frozenset({RunStatus.RUNNING, RunStatus.FAILED}),
    RunStatus.FAILED: frozenset({RunStatus.RUNNING}),
    RunStatus.SUCCEEDED: frozenset(),
    RunStatus.CANCELLED: frozenset(),
}

STEP_TRANSITIONS: Mapping[StepStatus, frozenset[StepStatus]] = {
    StepStatus.PENDING: frozenset({StepStatus.RUNNING, StepStatus.SKIPPED}),
    StepStatus.RUNNING: frozenset({StepStatus.FAILED, StepStatus.SUCCEEDED}),
    StepStatus.FAILED: frozenset(),
    StepStatus.SKIPPED: frozenset(),
    StepStatus.SUCCEEDED: frozenset(),
}

@dataclass(frozen=True, slots=True)
class BatchCounts:
    """Accounting totals required when a batch reaches a quality outcome."""

    source: int
    accepted: int
    quarantined: int
    duplicate: int

    def __post_init__(self) -> None:
        for name, value in (
            ("source", self.source),
            ("accepted", self.accepted),
            ("quarantined", self.quarantined),
            ("duplicate", self.duplicate),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"{name} count must be a non-negative integer")
        if self.source != self.accepted + self.quarantined + self.duplicate:
            raise ValueError(
                "source count must equal accepted + quarantined + duplicate counts"
            )

    def parameters(self) -> dict[str, int]:
        return {
            "source_row_count": self.source,
            "accepted_row_count": self.accepted,
            "quarantined_row_count": self.quarantined,
            "duplicate_row_count": self.duplicate,
        }


REGISTER_PIPELINE_RUN = text(
    """
    INSERT INTO control.pipeline_run (run_id, airflow_run_id)
    VALUES (:run_id, :airflow_run_id)
    ON CONFLICT (airflow_run_id) DO UPDATE
        SET airflow_run_id = EXCLUDED.airflow_run_id
    RETURNING run_id
    """
)

REGISTER_SOURCE_BATCH = text(
    """
    INSERT INTO control.source_batch (batch_id, source_system, delivery_key)
    VALUES (:batch_id, :source_system, :delivery_key)
    ON CONFLICT (source_system, delivery_key) DO UPDATE
        SET delivery_key = EXCLUDED.delivery_key
    RETURNING batch_id
    """
)

REGISTER_BATCH_ATTEMPT = text(
    """
    INSERT INTO control.batch_run (batch_run_id, batch_id, run_id)
    VALUES (:batch_run_id, :batch_id, :run_id)
    ON CONFLICT (batch_id, run_id) DO UPDATE
        SET batch_id = EXCLUDED.batch_id
    RETURNING batch_run_id
    """
)

SELECT_PIPELINE_STATUS = text(
    "SELECT status FROM control.pipeline_run WHERE run_id = :record_id FOR UPDATE"
)

UPDATE_PIPELINE_STATUS = text(
    """
    UPDATE control.pipeline_run
    SET status = :new_status,
        started_at = CASE
            WHEN :new_status = 'RUNNING' THEN COALESCE(started_at, CURRENT_TIMESTAMP)
            ELSE started_at
        END,
        finished_at = CASE
            WHEN :new_status IN ('QUARANTINED', 'FAILED', 'SUCCEEDED', 'CANCELLED')
                THEN CURRENT_TIMESTAMP
            ELSE NULL
        END,
        error_message = :error_message,
        updated_at = CURRENT_TIMESTAMP
    WHERE run_id = :record_id
    """
)

SELECT_BATCH_RUN = text(
    """
    SELECT status, source_row_count, accepted_row_count,
           quarantined_row_count, duplicate_row_count
    FROM control.batch_run
    WHERE batch_run_id = :record_id
    FOR UPDATE
    """
)

UPDATE_BATCH_STATUS = text(
    """
    UPDATE control.batch_run
    SET status = :new_status,
        source_row_count = COALESCE(:source_row_count, source_row_count),
        accepted_row_count = COALESCE(:accepted_row_count, accepted_row_count),
        quarantined_row_count = COALESCE(:quarantined_row_count, quarantined_row_count),
        duplicate_row_count = COALESCE(:duplicate_row_count, duplicate_row_count),
        started_at = CASE
            WHEN :new_status = 'RUNNING' THEN COALESCE(started_at, CURRENT_TIMESTAMP)
            ELSE started_at
        END,
        finished_at = CASE
            WHEN :new_status IN ('QUARANTINED', 'FAILED', 'SUCCEEDED', 'CANCELLED')
                THEN CURRENT_TIMESTAMP
            ELSE NULL
        END,
        error_message = :error_message,
        updated_at = CURRENT_TIMESTAMP
    WHERE batch_run_id = :record_id
    """
)

REGISTER_PROCESSING_STEP = text(
    """
    INSERT INTO control.processing_step
        (step_id, run_id, batch_id, step_name, attempt_number)
    VALUES (:step_id, :run_id, :batch_id, :step_name, :attempt_number)
    ON CONFLICT (run_id, batch_id, step_name, attempt_number) DO UPDATE
        SET step_name = EXCLUDED.step_name
    RETURNING step_id
    """
)

SELECT_STEP_STATUS = text(
    "SELECT status FROM control.processing_step WHERE step_id = :record_id FOR UPDATE"
)

UPDATE_STEP_STATUS = text(
    """
    UPDATE control.processing_step
    SET status = :new_status,
        started_at = CASE
            WHEN :new_status = 'RUNNING' THEN COALESCE(started_at, CURRENT_TIMESTAMP)
            ELSE started_at
        END,
        finished_at = CASE
            WHEN :new_status IN ('FAILED', 'SKIPPED', 'SUCCEEDED')
                THEN CURRENT_TIMESTAMP
            ELSE NULL
        END,
        error_message = :error_message
    WHERE step_id = :record_id
    """
)

INSERT_AUDIT_EVENT = text(
    """
    INSERT INTO control.audit_event
        (run_id, batch_id, event_type, actor, message, event_details)
    VALUES
        (:run_id, :batch_id, :event_type, :actor, :message,
         CAST(:event_details AS JSONB))
    RETURNING audit_event_id
    """
)


_EMAIL_PATTERN = re.compile(
    r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.IGNORECASE
)
_PHONE_PATTERN = re.compile(r"(?<!\w)(?:\+?\d[\d\s().-]{6,}\d)(?!\w)")
_BEARER_PATTERN = re.compile(
    r"\b(Bearer\s+)[A-Z0-9._~+/=-]+", re.IGNORECASE
)
_SECRET_ASSIGNMENT_PATTERN = re.compile(
    r"\b(password|passwd|secret|token|api[_-]?key|authorization)"
    r"(\s*[:=]\s*)([^\s,;]+)",
    re.IGNORECASE,
)
_SECRET_KEYS = frozenset(
    {
        "authorization",
        "credential",
        "credentials",
        "password",
        "passwd",
        "secret",
        "token",
        "accesstoken",
        "refreshtoken",
        "apikey",
        "api_key",
    }
)


def mask_audit_value(value: Any, *, field_name: str | None = None) -> Any:
    """Recursively remove credentials and mask common contact information."""
    normalized_field = re.sub(r"[^a-z0-9_]", "", (field_name or "").lower())
    compact_field = normalized_field.replace("_", "")
    if normalized_field in _SECRET_KEYS or compact_field in _SECRET_KEYS:
        return "[REDACTED]"
    if compact_field in {"email", "emailaddress"}:
        return "[REDACTED_EMAIL]" if value is not None else None
    if compact_field in {"phone", "phonenumber", "mobile", "mobilenumber"}:
        return "[REDACTED_PHONE]" if value is not None else None
    if isinstance(value, Mapping):
        return {str(key): mask_audit_value(item, field_name=str(key)) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [mask_audit_value(item) for item in value]
    if isinstance(value, str):
        masked = _BEARER_PATTERN.sub(r"\1[REDACTED]", value)
        masked = _SECRET_ASSIGNMENT_PATTERN.sub(r"\1\2[REDACTED]", masked)
        masked = _EMAIL_PATTERN.sub("[REDACTED_EMAIL]", masked)
        return _PHONE_PATTERN.sub(_mask_phone_match, masked)
    return value


class RunTracker:
    """Persist control-plane state through short, atomic transactions."""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def register_pipeline_run(
        self, airflow_run_id: str, *, run_id: UUID | None = None
    ) -> UUID:
        airflow_run_id = _nonblank(airflow_run_id, "airflow_run_id")
        proposed_id = run_id or uuid4()
        return self._write_scalar(
            REGISTER_PIPELINE_RUN,
            {"run_id": proposed_id, "airflow_run_id": airflow_run_id},
            "pipeline run registration",
        )

    def register_source_batch(
        self,
        source_system: str,
        delivery_key: str,
        *,
        batch_id: UUID | None = None,
    ) -> UUID:
        if source_system not in ALLOWED_SOURCE_SYSTEMS:
            raise ValueError(f"Unsupported source system: {source_system}")
        delivery_key = _nonblank(delivery_key, "delivery_key")
        proposed_id = batch_id or uuid4()
        return self._write_scalar(
            REGISTER_SOURCE_BATCH,
            {
                "batch_id": proposed_id,
                "source_system": source_system,
                "delivery_key": delivery_key,
            },
            "source batch registration",
        )

    def register_batch_attempt(
        self,
        batch_id: UUID,
        run_id: UUID,
        *,
        batch_run_id: UUID | None = None,
    ) -> UUID:
        proposed_id = batch_run_id or uuid4()
        return self._write_scalar(
            REGISTER_BATCH_ATTEMPT,
            {
                "batch_run_id": proposed_id,
                "batch_id": batch_id,
                "run_id": run_id,
            },
            "batch attempt registration",
        )

    def transition_pipeline_run(
        self,
        run_id: UUID,
        new_status: RunStatus | str,
        *,
        error_message: str | None = None,
    ) -> None:
        target = _enum_value(RunStatus, new_status, "run status")
        safe_error = _safe_error_message(error_message)
        try:
            with self._engine.begin() as connection:
                current_value = connection.execute(
                    SELECT_PIPELINE_STATUS, {"record_id": run_id}
                ).scalar_one_or_none()
                if current_value is None:
                    raise RecordNotFoundError(f"Pipeline run {run_id} was not found")
                current = _enum_value(RunStatus, current_value, "stored run status")
                _require_transition(current, target, RUN_TRANSITIONS, "pipeline run")
                connection.execute(
                    UPDATE_PIPELINE_STATUS,
                    {
                        "record_id": run_id,
                        "new_status": target.value,
                        "error_message": safe_error,
                    },
                )
        except SQLAlchemyError as exc:
            raise RunTrackingError("Pipeline run status update failed") from exc

    def transition_batch_attempt(
        self,
        batch_run_id: UUID,
        new_status: RunStatus | str,
        *,
        counts: BatchCounts | None = None,
        error_message: str | None = None,
    ) -> None:
        target = _enum_value(RunStatus, new_status, "batch status")
        if target in {RunStatus.SUCCEEDED, RunStatus.QUARANTINED} and counts is None:
            raise ValueError(f"Batch counts are required for {target.value}")
        count_parameters: dict[str, int | None] = {
            "source_row_count": None,
            "accepted_row_count": None,
            "quarantined_row_count": None,
            "duplicate_row_count": None,
        }
        if counts is not None:
            count_parameters.update(counts.parameters())
        try:
            with self._engine.begin() as connection:
                row = (
                    connection.execute(SELECT_BATCH_RUN, {"record_id": batch_run_id})
                    .mappings()
                    .one_or_none()
                )
                if row is None:
                    raise RecordNotFoundError(f"Batch attempt {batch_run_id} was not found")
                current = _enum_value(RunStatus, row["status"], "stored batch status")
                _require_transition(current, target, RUN_TRANSITIONS, "batch attempt")
                connection.execute(
                    UPDATE_BATCH_STATUS,
                    {
                        "record_id": batch_run_id,
                        "new_status": target.value,
                        "error_message": _safe_error_message(error_message),
                        **count_parameters,
                    },
                )
        except SQLAlchemyError as exc:
            raise RunTrackingError("Batch attempt status update failed") from exc

    def register_processing_step(
        self,
        run_id: UUID,
        step_name: str,
        *,
        batch_id: UUID | None = None,
        attempt_number: int = 1,
        step_id: UUID | None = None,
    ) -> UUID:
        step_name = _nonblank(step_name, "step_name", maximum=100)
        if (
            isinstance(attempt_number, bool)
            or not isinstance(attempt_number, int)
            or attempt_number < 1
        ):
            raise ValueError("attempt_number must be a positive integer")
        proposed_id = step_id or uuid4()
        return self._write_scalar(
            REGISTER_PROCESSING_STEP,
            {
                "step_id": proposed_id,
                "run_id": run_id,
                "batch_id": batch_id,
                "step_name": step_name,
                "attempt_number": attempt_number,
            },
            "processing step registration",
        )

    def transition_processing_step(
        self,
        step_id: UUID,
        new_status: StepStatus | str,
        *,
        error_message: str | None = None,
    ) -> None:
        target = _enum_value(StepStatus, new_status, "step status")
        try:
            with self._engine.begin() as connection:
                current_value = connection.execute(
                    SELECT_STEP_STATUS, {"record_id": step_id}
                ).scalar_one_or_none()
                if current_value is None:
                    raise RecordNotFoundError(f"Processing step {step_id} was not found")
                current = _enum_value(StepStatus, current_value, "stored step status")
                _require_transition(current, target, STEP_TRANSITIONS, "processing step")
                connection.execute(
                    UPDATE_STEP_STATUS,
                    {
                        "record_id": step_id,
                        "new_status": target.value,
                        "error_message": _safe_error_message(error_message),
                    },
                )
        except SQLAlchemyError as exc:
            raise RunTrackingError("Processing step status update failed") from exc

    def record_audit_event(
        self,
        event_type: str,
        actor: str,
        message: str,
        *,
        run_id: UUID | None = None,
        batch_id: UUID | None = None,
        details: Mapping[str, Any] | None = None,
    ) -> int:
        event_type = _nonblank(event_type, "event_type", maximum=50)
        actor = _nonblank(actor, "actor", maximum=100)
        message = _nonblank(message, "message")
        masked_message = mask_audit_value(message)
        masked_details = mask_audit_value(details or {})
        return self._write_scalar(
            INSERT_AUDIT_EVENT,
            {
                "run_id": run_id,
                "batch_id": batch_id,
                "event_type": event_type,
                "actor": actor,
                "message": masked_message,
                "event_details": json.dumps(
                    masked_details, separators=(",", ":"), sort_keys=True, default=str
                ),
            },
            "audit event write",
        )

    def _write_scalar(
        self,
        statement: Any,
        parameters: Mapping[str, Any],
        operation: str,
    ) -> Any:
        try:
            with self._engine.begin() as connection:
                return connection.execute(statement, dict(parameters)).scalar_one()
        except SQLAlchemyError as exc:
            raise RunTrackingError(f"{operation.capitalize()} failed") from exc


def _enum_value(enum_type: type[Enum], value: Enum | str, label: str) -> Any:
    try:
        return enum_type(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Unsupported {label}: {value}") from exc


def _require_transition(
    current: Any,
    target: Any,
    transitions: Mapping[Any, frozenset[Any]],
    label: str,
) -> None:
    if target not in transitions[current]:
        raise InvalidStatusTransitionError(
            f"Invalid {label} transition: {current.value} -> {target.value}"
        )


def _nonblank(value: str, label: str, *, maximum: int | None = None) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{label} must be a nonblank string")
    cleaned = value.strip()
    if maximum is not None and len(cleaned) > maximum:
        raise ValueError(f"{label} must be at most {maximum} characters")
    return cleaned


def _safe_error_message(value: str | None) -> str | None:
    if value is None:
        return None
    return str(mask_audit_value(value))[:2000]


def _mask_phone_match(match: re.Match[str]) -> str:
    candidate = match.group(0)
    return "[REDACTED_PHONE]" if sum(char.isdigit() for char in candidate) >= 10 else candidate
