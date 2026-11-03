"""Concrete task services for the enterprise ETL Airflow DAG."""

from __future__ import annotations

import os
import subprocess
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Mapping
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine

from etl_framework.config.settings import AppSettings
from etl_framework.control.run_tracking import RunStatus, RunTracker, StepStatus
from etl_framework.database.connections import create_database_engines, dispose_engines
from etl_framework.ingestion import (
    CustomerStatusApiSettings,
    ExtractionWindow,
    extract_billing_transactions,
    extract_crm_customers,
    extract_customer_statuses,
    load_billing_transactions,
    load_crm_customers,
    load_customer_statuses,
    read_operational_csv,
    load_operational_events,
)
from etl_framework.orchestration.pipeline import (
    acquire_delivery_lease,
    ensure_batch_running,
    ensure_pipeline_running,
    publish_pipeline_status,
    read_source_watermark,
    release_delivery_lease,
)
from etl_framework.quality import (
    apply_release_gate,
    validate_api_statuses,
    validate_customers,
    validate_operational_events,
    validate_transactions,
)


SOURCE_ORDER = ("CRM_PG", "STATUS_API", "BILLING_MSSQL", "OPS_CSV")
TECHNICAL_FAILURE_MESSAGE = "Technical task failure; see protected Airflow logs"


def register_run(airflow_run_id: str) -> dict[str, str]:
    """Register an Airflow run and return its durable identifier for XCom."""
    with _engines() as engines:
        warehouse = engines["warehouse"]
        tracker = RunTracker(warehouse)
        run_id = tracker.register_pipeline_run(airflow_run_id)
        ensure_pipeline_running(warehouse, run_id)
        tracker.record_audit_event(
            "PIPELINE_STARTED", "airflow", "Pipeline run started", run_id=run_id
        )
        return {"run_id": str(run_id), "airflow_run_id": airflow_run_id}


def ingest_source(
    source_system: str,
    run_payload: Mapping[str, str],
    upper_watermark: str,
    *,
    attempt_number: int | None = None,
) -> dict[str, str | int]:
    """Extract and stage one source using stable delivery and batch identifiers."""
    run_id = UUID(run_payload["run_id"])
    upper = _parse_utc(upper_watermark)
    with _engines() as engines:
        warehouse = engines["warehouse"]
        tracker = RunTracker(warehouse)
        batch_id: UUID | None = None
        batch_run_id: UUID | None = None
        delivery_key: str | None = None
        try:
            if source_system == "OPS_CSV":
                delivery = read_operational_csv(_operational_csv_path(upper))
                delivery_key = delivery.delivery_key
                lower = read_source_watermark(warehouse, source_system)
            else:
                lower = read_source_watermark(warehouse, source_system)
                window = ExtractionWindow(upper_watermark=upper, lower_watermark=lower)
                delivery_key = window.delivery_key
            batch_id = tracker.register_source_batch(source_system, delivery_key)
            acquire_delivery_lease(
                warehouse,
                source_system=source_system,
                delivery_key=delivery_key,
                owner_run_id=run_id,
                lease_seconds=_lease_seconds(),
            )
            batch_run_id = tracker.register_batch_attempt(batch_id, run_id)
            ensure_batch_running(warehouse, batch_run_id)
            with tracked_step(
                warehouse,
                run_id,
                f"ingest_{source_system.lower()}",
                batch_id=batch_id,
                attempt_number=attempt_number,
            ):
                row_count = _extract_and_load(
                    source_system,
                    engines,
                    run_id=run_id,
                    batch_id=batch_id,
                    upper=upper,
                    lower=lower,
                )
        except Exception:
            if batch_run_id is not None:
                _mark_batch_failed(tracker, batch_run_id)
            raise
        return {
            "source_system": source_system,
            "run_id": str(run_id),
            "batch_id": str(batch_id),
            "batch_run_id": str(batch_run_id),
            "delivery_key": delivery_key,
            "watermark": _utc_text(upper),
            "source_rows": row_count,
        }


def run_dbt_candidates(
    run_payload: Mapping[str, str], *, attempt_number: int | None = None
) -> None:
    """Build and test normalized staging and candidate models without core promotion."""
    run_id = UUID(run_payload["run_id"])
    with _engines() as engines:
        with tracked_step(
            engines["warehouse"],
            run_id,
            "dbt_candidate_transformations",
            attempt_number=attempt_number,
        ):
            common = [
                "dbt",
                "build",
                "--project-dir",
                os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/project/dbt"),
                "--profiles-dir",
                os.environ.get("DBT_PROFILES_DIR", "/opt/airflow/project/dbt"),
            ]
            bootstrap = [
                *common,
                "--empty",
                "--quiet",
            ]
            command = [
                *common,
                "--select",
                "staging int_customer_candidates int_transaction_candidates int_operational_event_candidates",
                "--exclude",
                "dim_customer fact_transaction fact_operational_event",
                "--quiet",
            ]
            try:
                # The empty build creates all core relations on a new warehouse
                # without publishing data from a PENDING batch.
                subprocess.run(
                    bootstrap,
                    check=True,
                    capture_output=True,
                    text=True,
                    timeout=_dbt_timeout(),
                )
                subprocess.run(
                    command,
                    check=True,
                    capture_output=True,
                    text=True,
                    timeout=_dbt_timeout(),
                )
            except (OSError, subprocess.SubprocessError) as exc:
                raise RuntimeError("dbt candidate transformation failed") from exc


def validate_and_release(
    batch_payload: Mapping[str, str | int], *, attempt_number: int | None = None
) -> dict[str, Any]:
    """Validate, promote, reconcile, checkpoint, and release one staged batch."""
    source_system = str(batch_payload["source_system"])
    run_id = UUID(str(batch_payload["run_id"]))
    batch_id = UUID(str(batch_payload["batch_id"]))
    delivery_key = str(batch_payload["delivery_key"])
    with _engines() as engines:
        warehouse = engines["warehouse"]
        with tracked_step(
            warehouse,
            run_id,
            f"quality_release_{source_system.lower()}",
            batch_id=batch_id,
            attempt_number=attempt_number,
        ):
            validation = _validate_staged_batch(
                warehouse, source_system, run_id=run_id, batch_id=batch_id
            )
            promote = _promotion_callback(source_system)
            accepted_keys = {
                record.business_key
                for record in validation.records
                if record.disposition.value == "accepted" and record.business_key is not None
            }

            def load_target_keys(connection: Connection) -> list[str]:
                table, key = _target_table_and_key(source_system)
                rows = connection.execute(text(f"SELECT {key} FROM {table}")).scalars()
                return [str(value) for value in rows if str(value) in accepted_keys]

            financial_loader = None
            if source_system == "BILLING_MSSQL":
                def load_financial_rows(connection: Connection) -> list[Mapping[str, Any]]:
                    rows = connection.execute(text(
                        "SELECT transaction_id, amount, currency_code, transaction_status "
                        "FROM core.fact_transaction"
                    )).mappings()
                    return [dict(row) for row in rows if str(row["transaction_id"]) in accepted_keys]
                financial_loader = load_financial_rows

            result = apply_release_gate(
                warehouse,
                validation,
                watermark_value=str(batch_payload["watermark"]),
                promote=promote,
                load_target_keys=load_target_keys,
                load_target_financial_rows=financial_loader,
            )
            release_delivery_lease(
                warehouse,
                source_system=source_system,
                delivery_key=delivery_key,
                owner_run_id=run_id,
            )
            if not result.released:
                # A completed quality decision is data, not a technical task
                # exception. Returning QUARANTINED avoids Airflow retrying an
                # unchanged delivery and lets the other sources finish.
                return {
                    "source_system": source_system,
                    "status": result.status,
                    "batch_id": str(batch_id),
                    "reconciliations": len(result.reconciliations),
                }
            return {
                "source_system": source_system,
                "status": result.status,
                "batch_id": str(batch_id),
                "reconciliations": len(result.reconciliations),
            }


def finalize_run(run_payload: Mapping[str, str]) -> str:
    """Publish final status from durable batch outcomes, including upstream failures."""
    run_id = UUID(run_payload["run_id"])
    with _engines() as engines:
        warehouse = engines["warehouse"]
        with warehouse.connect() as connection:
            statuses = list(connection.execute(text(
                "SELECT source_system, status FROM control.batch_run "
                "JOIN control.source_batch USING (batch_id) WHERE run_id = :run_id"
            ), {"run_id": run_id}).mappings())
        by_source = {str(row["source_system"]): str(row["status"]) for row in statuses}
        if len(by_source) != len(SOURCE_ORDER) or any(
            by_source.get(source) in {None, "PENDING", "RUNNING", "FAILED"}
            for source in SOURCE_ORDER
        ):
            status = "FAILED"
            message = "One or more technical pipeline tasks failed"
        elif any(by_source[source] == "QUARANTINED" for source in SOURCE_ORDER):
            status = "QUARANTINED"
            message = "One or more batches failed mandatory data-quality checks"
        elif all(by_source[source] == "SUCCEEDED" for source in SOURCE_ORDER):
            status = "SUCCEEDED"
            message = None
        else:
            status = "FAILED"
            message = "Pipeline batches ended in an unexpected state"
        publish_pipeline_status(warehouse, run_id, status, error_message=message)
        RunTracker(warehouse).record_audit_event(
            "PIPELINE_FINISHED",
            "airflow",
            f"Pipeline run finished with status {status}",
            run_id=run_id,
            details={"status": status, "batch_statuses": by_source},
        )
        return status


@contextmanager
def tracked_step(
    engine: Engine,
    run_id: UUID,
    step_name: str,
    *,
    batch_id: UUID | None = None,
    attempt_number: int | None = None,
) -> Iterator[None]:
    """Record one Airflow task attempt and a sanitized failure outcome."""
    tracker = RunTracker(engine)
    attempt = attempt_number or int(os.environ.get("AIRFLOW_CTX_TRY_NUMBER", "1"))
    step_id = tracker.register_processing_step(
        run_id,
        step_name,
        batch_id=batch_id,
        attempt_number=attempt,
    )
    tracker.transition_processing_step(step_id, StepStatus.RUNNING)
    try:
        yield
    except Exception:
        tracker.transition_processing_step(
            step_id, StepStatus.FAILED, error_message=TECHNICAL_FAILURE_MESSAGE
        )
        raise
    else:
        tracker.transition_processing_step(step_id, StepStatus.SUCCEEDED)


@contextmanager
def _engines() -> Iterator[dict[str, Engine]]:
    engines = create_database_engines(AppSettings.from_env())
    try:
        yield engines
    finally:
        dispose_engines(engines)


def _extract_and_load(
    source_system: str,
    engines: Mapping[str, Engine],
    *,
    run_id: UUID,
    batch_id: UUID,
    upper: datetime,
    lower: datetime | None,
) -> int:
    warehouse = engines["warehouse"]
    window = ExtractionWindow(upper_watermark=upper, lower_watermark=lower)
    if source_system == "CRM_PG":
        rows = list(extract_crm_customers(engines["crm"], window))
        return load_crm_customers(
            warehouse, rows, run_id=run_id, batch_id=batch_id
        ).rows_received
    if source_system == "BILLING_MSSQL":
        rows = list(extract_billing_transactions(engines["billing"], window))
        return load_billing_transactions(
            warehouse, rows, run_id=run_id, batch_id=batch_id
        ).rows_received
    if source_system == "STATUS_API":
        rows = extract_customer_statuses(CustomerStatusApiSettings.from_env(), window)
        return load_customer_statuses(
            warehouse, rows, run_id=run_id, batch_id=batch_id
        ).rows_received
    if source_system == "OPS_CSV":
        delivery = read_operational_csv(_operational_csv_path(upper))
        return load_operational_events(
            warehouse,
            delivery.rows,
            run_id=run_id,
            batch_id=batch_id,
            source_file_name=delivery.file_name,
            source_file_checksum=delivery.file_checksum,
        ).rows_received
    raise ValueError(f"Unsupported source system: {source_system}")


def _validate_staged_batch(
    engine: Engine, source_system: str, *, run_id: UUID, batch_id: UUID
) -> Any:
    statements = {
        "CRM_PG": """
            SELECT customer_id, first_name, last_name, email, phone, country_code,
                   status, created_at_raw AS created_at, updated_at_raw AS updated_at,
                   run_id, batch_id
              FROM stg.customer_raw WHERE run_id = :run_id AND batch_id = :batch_id
        """,
        "STATUS_API": """
            SELECT customer_id, status, status_updated_at_raw AS status_updated_at,
                   run_id, batch_id
              FROM stg.customer_status_raw WHERE run_id = :run_id AND batch_id = :batch_id
        """,
        "BILLING_MSSQL": """
            SELECT transaction_id, customer_id,
                   transaction_timestamp_raw AS transaction_timestamp,
                   amount_raw AS amount, currency_code, transaction_status,
                   payment_method, updated_at_raw AS updated_at, run_id, batch_id
              FROM stg.transaction_raw WHERE run_id = :run_id AND batch_id = :batch_id
        """,
        "OPS_CSV": """
            SELECT event_id, customer_id, event_type,
                   event_timestamp_raw AS event_timestamp, location_code, details
                   , run_id, batch_id
              FROM stg.operational_event_raw WHERE run_id = :run_id AND batch_id = :batch_id
        """,
    }
    with engine.connect() as connection:
        rows = [dict(row) for row in connection.execute(
            text(statements[source_system]), {"run_id": run_id, "batch_id": batch_id}
        ).mappings()]
        customer_ids = set(connection.execute(
            text("SELECT customer_id FROM core.dim_customer")
        ).scalars()) if source_system in {"BILLING_MSSQL", "OPS_CSV"} else set()
    if source_system == "CRM_PG":
        return validate_customers(rows, run_id=run_id, batch_id=batch_id)
    if source_system == "STATUS_API":
        return validate_api_statuses(rows, run_id=run_id, batch_id=batch_id)
    if source_system == "BILLING_MSSQL":
        return validate_transactions(
            rows, customer_ids=customer_ids, run_id=run_id, batch_id=batch_id
        )
    return validate_operational_events(
        rows, customer_ids=customer_ids, run_id=run_id, batch_id=batch_id
    )


def _promotion_callback(source_system: str) -> Any:
    if source_system == "CRM_PG":
        statement = text("""
            INSERT INTO core.dim_customer (
                customer_id, first_name, last_name, email, phone, country_code,
                status, status_source, source_created_at, source_updated_at,
                run_id, batch_id, loaded_at
            ) VALUES (
                :customer_id, :first_name, :last_name, :email, :phone, :country_code,
                :status, 'CRM_PG', :created_at, :updated_at, :run_id, :batch_id,
                CURRENT_TIMESTAMP
            ) ON CONFLICT (customer_id) DO UPDATE SET
                first_name = EXCLUDED.first_name, last_name = EXCLUDED.last_name,
                email = EXCLUDED.email, phone = EXCLUDED.phone,
                country_code = EXCLUDED.country_code,
                status = EXCLUDED.status, status_source = EXCLUDED.status_source,
                source_created_at = EXCLUDED.source_created_at,
                source_updated_at = EXCLUDED.source_updated_at,
                run_id = EXCLUDED.run_id, batch_id = EXCLUDED.batch_id,
                loaded_at = CURRENT_TIMESTAMP
            WHERE EXCLUDED.source_updated_at >= core.dim_customer.source_updated_at
        """)
    elif source_system == "STATUS_API":
        statement = text("""
            UPDATE core.dim_customer SET
                status = :status, status_source = 'STATUS_API',
                source_updated_at = :status_updated_at,
                run_id = :run_id, batch_id = :batch_id, loaded_at = CURRENT_TIMESTAMP
            WHERE customer_id = :customer_id
              AND :status_updated_at >= source_updated_at
        """)
    elif source_system == "BILLING_MSSQL":
        statement = text("""
            INSERT INTO core.fact_transaction (
                transaction_id, customer_id, transaction_timestamp, amount,
                currency_code, transaction_status, payment_method,
                source_updated_at, run_id, batch_id, loaded_at
            ) VALUES (
                :transaction_id, :customer_id, :transaction_timestamp, :amount,
                :currency_code, :transaction_status, :payment_method,
                :updated_at, :run_id, :batch_id, CURRENT_TIMESTAMP
            ) ON CONFLICT (transaction_id) DO UPDATE SET
                customer_id = EXCLUDED.customer_id,
                transaction_timestamp = EXCLUDED.transaction_timestamp,
                amount = EXCLUDED.amount, currency_code = EXCLUDED.currency_code,
                transaction_status = EXCLUDED.transaction_status,
                payment_method = EXCLUDED.payment_method,
                source_updated_at = EXCLUDED.source_updated_at,
                run_id = EXCLUDED.run_id, batch_id = EXCLUDED.batch_id,
                loaded_at = CURRENT_TIMESTAMP
            WHERE EXCLUDED.source_updated_at >= core.fact_transaction.source_updated_at
        """)
    else:
        statement = text("""
            INSERT INTO core.fact_operational_event (
                event_id, customer_id, event_type, event_timestamp,
                location_code, details, run_id, batch_id, loaded_at
            ) VALUES (
                :event_id, :customer_id, :event_type, :event_timestamp,
                :location_code, :details, :run_id, :batch_id, CURRENT_TIMESTAMP
            ) ON CONFLICT (event_id) DO UPDATE SET
                customer_id = EXCLUDED.customer_id, event_type = EXCLUDED.event_type,
                event_timestamp = EXCLUDED.event_timestamp,
                location_code = EXCLUDED.location_code, details = EXCLUDED.details,
                run_id = EXCLUDED.run_id, batch_id = EXCLUDED.batch_id,
                loaded_at = CURRENT_TIMESTAMP
            WHERE EXCLUDED.event_timestamp >= core.fact_operational_event.event_timestamp
        """)

    def promote(connection: Connection, rows: tuple[dict[str, Any], ...]) -> None:
        if rows:
            connection.execute(statement, list(rows))

    return promote


def _target_table_and_key(source_system: str) -> tuple[str, str]:
    if source_system in {"CRM_PG", "STATUS_API"}:
        return "core.dim_customer", "customer_id"
    if source_system == "BILLING_MSSQL":
        return "core.fact_transaction", "transaction_id"
    return "core.fact_operational_event", "event_id"


def _mark_batch_failed(tracker: RunTracker, batch_run_id: UUID) -> None:
    try:
        tracker.transition_batch_attempt(
            batch_run_id, RunStatus.FAILED, error_message=TECHNICAL_FAILURE_MESSAGE
        )
    except Exception:
        pass


def _operational_csv_path(upper: datetime) -> Path:
    configured = os.environ.get("OPS_CSV_PATH")
    if configured:
        return Path(configured)
    directory = Path(os.environ.get("OPS_CSV_DIRECTORY", "/opt/airflow/project/data/raw/csv"))
    dated = directory / f"operational_events_{upper:%Y%m%d}.csv"
    if dated.exists():
        return dated
    deliveries = sorted(directory.glob("operational_events_*.csv"))
    if not deliveries:
        raise FileNotFoundError("No operational CSV delivery is available")
    return deliveries[-1]


def _parse_utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("watermark must include a timezone")
    return parsed.astimezone(timezone.utc)


def _utc_text(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _lease_seconds() -> int:
    return int(os.environ.get("PIPELINE_DELIVERY_LEASE_SECONDS", "7200"))


def _dbt_timeout() -> int:
    return int(os.environ.get("DBT_COMMAND_TIMEOUT_SECONDS", "1800"))
