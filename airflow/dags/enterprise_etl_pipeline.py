"""Day 20-21 enterprise ETL pipeline with bounded technical retries."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from airflow.exceptions import AirflowFailException
from airflow.sdk import dag, get_current_context, task
from airflow.utils.trigger_rule import TriggerRule

from etl_framework.orchestration.airflow_tasks import (
    finalize_run,
    ingest_source,
    register_run,
    run_dbt_candidates,
    validate_and_release,
)


TECHNICAL_RETRIES = 2
TECHNICAL_RETRY_DELAY = timedelta(minutes=2)


@dag(
    dag_id="enterprise_etl_data_quality",
    schedule="0 2 * * *",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-platform",
        "retries": TECHNICAL_RETRIES,
        "retry_delay": TECHNICAL_RETRY_DELAY,
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=10),
        "execution_timeout": timedelta(hours=1),
    },
    tags=["etl", "quality", "reconciliation"],
)
def enterprise_etl_data_quality():
    @task
    def register_pipeline() -> dict[str, str]:
        context = get_current_context()
        return register_run(context["run_id"])

    @task
    def ingest(source_system: str, run_payload: dict[str, str]) -> dict:
        context = get_current_context()
        upper = context["data_interval_end"].isoformat()
        return ingest_source(
            source_system,
            run_payload,
            upper,
            attempt_number=context["task_instance"].try_number,
        )

    @task
    def transform_candidates(run_payload: dict[str, str]) -> None:
        context = get_current_context()
        run_dbt_candidates(
            run_payload, attempt_number=context["task_instance"].try_number
        )

    @task
    def quality_release(batch_payload: dict) -> dict:
        # Mandatory quality failures are persisted and returned as a
        # QUARANTINED outcome, so Airflow retries only technical exceptions.
        context = get_current_context()
        return validate_and_release(
            batch_payload, attempt_number=context["task_instance"].try_number
        )

    @task(trigger_rule=TriggerRule.ALL_DONE, retries=0)
    def publish_final_status(run_payload: dict[str, str]) -> str:
        status = finalize_run(run_payload)
        if status != "SUCCEEDED":
            raise AirflowFailException(f"Pipeline ended with status {status}")
        return status

    run = register_pipeline()
    crm = ingest.override(task_id="ingest_crm")("CRM_PG", run)
    billing = ingest.override(task_id="ingest_billing")("BILLING_MSSQL", run)
    operations = ingest.override(task_id="ingest_operations")("OPS_CSV", run)
    api = ingest.override(task_id="ingest_status_api")("STATUS_API", run)

    candidates = transform_candidates(run)
    [crm, billing, operations, api] >> candidates

    crm_release = quality_release.override(task_id="release_crm")(crm)
    api_release = quality_release.override(task_id="release_status_api")(api)
    candidates >> crm_release >> api_release

    billing_release = quality_release.override(task_id="release_billing")(billing)
    operations_release = quality_release.override(task_id="release_operations")(operations)
    api_release >> [billing_release, operations_release]

    final = publish_final_status(run)
    [crm_release, api_release, billing_release, operations_release] >> final


enterprise_etl_data_quality()
