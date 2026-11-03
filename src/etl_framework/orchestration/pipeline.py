"""Durable checkpoints used by Airflow tasks and interrupted-run recovery."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from etl_framework.control.run_tracking import ALLOWED_SOURCE_SYSTEMS


class PipelineOrchestrationError(RuntimeError):
    """Raised when pipeline checkpoint state cannot be changed safely."""


class ConflictingDeliveryError(PipelineOrchestrationError):
    """Raised when another active run owns the same source delivery."""


class DataQualityFailure(RuntimeError):
    """Non-retryable signal for a completed validation or reconciliation failure."""


@dataclass(frozen=True, slots=True)
class DeliveryLease:
    source_system: str
    delivery_key: str
    owner_run_id: UUID
    expires_at: datetime


SELECT_PIPELINE_FOR_UPDATE = text(
    "SELECT status FROM control.pipeline_run WHERE run_id = :run_id FOR UPDATE"
)
UPDATE_PIPELINE_RUNNING = text(
    """
    UPDATE control.pipeline_run
       SET status = 'RUNNING',
           started_at = COALESCE(started_at, CURRENT_TIMESTAMP),
           finished_at = NULL,
           error_message = NULL,
           updated_at = CURRENT_TIMESTAMP
     WHERE run_id = :run_id
    """
)
UPDATE_PIPELINE_FINAL = text(
    """
    UPDATE control.pipeline_run
       SET status = :status,
           finished_at = CURRENT_TIMESTAMP,
           error_message = :error_message,
           updated_at = CURRENT_TIMESTAMP
     WHERE run_id = :run_id
       AND status <> 'SUCCEEDED'
    """
)
ENSURE_BATCH_RUNNING = text(
    """
    UPDATE control.batch_run
       SET status = 'RUNNING',
           started_at = COALESCE(started_at, CURRENT_TIMESTAMP),
           finished_at = NULL,
           error_message = NULL,
           updated_at = CURRENT_TIMESTAMP
     WHERE batch_run_id = :batch_run_id
       AND status IN ('PENDING', 'RUNNING', 'FAILED', 'QUARANTINED')
    """
)
READ_WATERMARK = text(
    "SELECT watermark_value FROM control.source_watermark WHERE source_system = :source_system"
)
CLAIM_LEASE = text(
    """
    INSERT INTO control.delivery_lease (
        source_system, delivery_key, owner_run_id, acquired_at, heartbeat_at, expires_at
    ) VALUES (
        :source_system, :delivery_key, :owner_run_id,
        CURRENT_TIMESTAMP, CURRENT_TIMESTAMP,
        CURRENT_TIMESTAMP + (:lease_seconds * INTERVAL '1 second')
    )
    ON CONFLICT (source_system, delivery_key) DO UPDATE SET
        owner_run_id = EXCLUDED.owner_run_id,
        acquired_at = CASE
            WHEN control.delivery_lease.owner_run_id = EXCLUDED.owner_run_id
                THEN control.delivery_lease.acquired_at
            ELSE CURRENT_TIMESTAMP
        END,
        heartbeat_at = CURRENT_TIMESTAMP,
        expires_at = EXCLUDED.expires_at
    WHERE control.delivery_lease.owner_run_id = EXCLUDED.owner_run_id
       OR control.delivery_lease.expires_at <= CURRENT_TIMESTAMP
    RETURNING source_system, delivery_key, owner_run_id, expires_at
    """
)
RELEASE_LEASE = text(
    """
    DELETE FROM control.delivery_lease
     WHERE source_system = :source_system
       AND delivery_key = :delivery_key
       AND owner_run_id = :owner_run_id
    """
)


def ensure_pipeline_running(engine: Engine, run_id: UUID) -> None:
    """Start or resume a registered run without resetting a successful run."""
    try:
        with engine.begin() as connection:
            status = connection.execute(
                SELECT_PIPELINE_FOR_UPDATE, {"run_id": run_id}
            ).scalar_one_or_none()
            if status is None:
                raise PipelineOrchestrationError("Registered pipeline run was not found")
            if status == "SUCCEEDED":
                return
            if status not in {"PENDING", "RUNNING", "FAILED", "QUARANTINED"}:
                raise PipelineOrchestrationError(
                    f"Pipeline run cannot resume from status {status}"
                )
            connection.execute(UPDATE_PIPELINE_RUNNING, {"run_id": run_id})
    except PipelineOrchestrationError:
        raise
    except SQLAlchemyError as exc:
        raise PipelineOrchestrationError("Pipeline run checkpoint update failed") from exc


def ensure_batch_running(engine: Engine, batch_run_id: UUID) -> None:
    """Start a batch or resume an incomplete/corrected attempt idempotently."""
    try:
        with engine.begin() as connection:
            result = connection.execute(
                ENSURE_BATCH_RUNNING, {"batch_run_id": batch_run_id}
            )
            if result.rowcount != 1:
                raise PipelineOrchestrationError(
                    "Batch attempt is missing or already has a data-quality outcome"
                )
    except PipelineOrchestrationError:
        raise
    except SQLAlchemyError as exc:
        raise PipelineOrchestrationError("Batch checkpoint update failed") from exc


def read_source_watermark(engine: Engine, source_system: str) -> datetime | None:
    """Read the last successful UTC watermark for one source."""
    _validate_source(source_system)
    try:
        with engine.connect() as connection:
            raw = connection.execute(
                READ_WATERMARK, {"source_system": source_system}
            ).scalar_one_or_none()
    except SQLAlchemyError as exc:
        raise PipelineOrchestrationError("Source watermark read failed") from exc
    if raw is None:
        return None
    try:
        parsed = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    except ValueError as exc:
        raise PipelineOrchestrationError("Stored source watermark is invalid") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise PipelineOrchestrationError("Stored source watermark lacks a timezone")
    return parsed.astimezone(timezone.utc)


def acquire_delivery_lease(
    engine: Engine,
    *,
    source_system: str,
    delivery_key: str,
    owner_run_id: UUID,
    lease_seconds: int = 7_200,
) -> DeliveryLease:
    """Claim or renew one delivery; expired leases support interrupted-run recovery."""
    _validate_source(source_system)
    if not isinstance(delivery_key, str) or not delivery_key.strip():
        raise ValueError("delivery_key must be a nonblank string")
    if isinstance(lease_seconds, bool) or not 60 <= lease_seconds <= 86_400:
        raise ValueError("lease_seconds must be between 60 and 86400")
    try:
        with engine.begin() as connection:
            row = connection.execute(
                CLAIM_LEASE,
                {
                    "source_system": source_system,
                    "delivery_key": delivery_key.strip(),
                    "owner_run_id": owner_run_id,
                    "lease_seconds": lease_seconds,
                },
            ).mappings().one_or_none()
    except SQLAlchemyError as exc:
        raise PipelineOrchestrationError("Delivery lease claim failed") from exc
    if row is None:
        raise ConflictingDeliveryError(
            f"An active run already owns delivery {source_system}"
        )
    return DeliveryLease(
        str(row["source_system"]),
        str(row["delivery_key"]),
        UUID(str(row["owner_run_id"])),
        row["expires_at"],
    )


def release_delivery_lease(
    engine: Engine, *, source_system: str, delivery_key: str, owner_run_id: UUID
) -> bool:
    """Release only a lease owned by the calling run."""
    _validate_source(source_system)
    try:
        with engine.begin() as connection:
            result = connection.execute(
                RELEASE_LEASE,
                {
                    "source_system": source_system,
                    "delivery_key": delivery_key,
                    "owner_run_id": owner_run_id,
                },
            )
            return result.rowcount == 1
    except SQLAlchemyError as exc:
        raise PipelineOrchestrationError("Delivery lease release failed") from exc


def publish_pipeline_status(
    engine: Engine, run_id: UUID, status: str, *, error_message: str | None = None
) -> None:
    """Publish a terminal run result with a credential-safe message."""
    if status not in {"FAILED", "QUARANTINED", "SUCCEEDED", "CANCELLED"}:
        raise ValueError("Unsupported final pipeline status")
    safe_error = None if error_message is None else error_message[:2_000]
    try:
        with engine.begin() as connection:
            connection.execute(
                UPDATE_PIPELINE_FINAL,
                {"run_id": run_id, "status": status, "error_message": safe_error},
            )
    except SQLAlchemyError as exc:
        raise PipelineOrchestrationError("Final pipeline status update failed") from exc


def _validate_source(source_system: str) -> None:
    if source_system not in ALLOWED_SOURCE_SYSTEMS:
        raise ValueError(f"Unsupported source system: {source_system}")
