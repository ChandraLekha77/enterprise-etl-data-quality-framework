"""Public response models that intentionally exclude sensitive observations."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from uuid import UUID

from pydantic import BaseModel, ConfigDict


class ApiModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class HealthResponse(ApiModel):
    status: str
    database: str


class PipelineRunSummary(ApiModel):
    run_id: UUID
    airflow_run_id: str
    status: str
    started_at: datetime | None
    finished_at: datetime | None
    created_at: datetime
    batch_count: int
    total_rows: int
    accepted_rows: int
    quarantined_rows: int
    duplicate_rows: int
    totals_verified: bool
    failure_count: int
    reconciliation_failure_count: int


class PipelineRunPage(ApiModel):
    items: list[PipelineRunSummary]
    page: int
    page_size: int
    total: int


class SourceBatchSummary(ApiModel):
    batch_run_id: UUID
    run_id: UUID
    batch_id: UUID
    source_system: str
    delivery_key: str
    status: str
    total_rows: int
    accepted_rows: int
    quarantined_rows: int
    duplicate_rows: int
    totals_verified: bool
    started_at: datetime | None
    finished_at: datetime | None
    created_at: datetime


class SourceBatchPage(ApiModel):
    items: list[SourceBatchSummary]
    page: int
    page_size: int
    total: int


class ValidationFailure(ApiModel):
    validation_result_id: UUID
    run_id: UUID
    batch_id: UUID
    source_system: str
    rule_id: str
    severity: str
    field_name: str | None
    message: str
    created_at: datetime


class ValidationFailurePage(ApiModel):
    items: list[ValidationFailure]
    page: int
    page_size: int
    total: int


class FailureGroup(ApiModel):
    source_system: str
    rule_id: str
    severity: str
    failure_count: int


class ReconciliationResult(ApiModel):
    reconciliation_result_id: UUID
    run_id: UUID
    batch_id: UUID
    rule_id: str
    metric_name: str
    group_key: str | None
    source_value: Decimal
    target_value: Decimal
    difference: Decimal
    tolerance: Decimal
    status: str
    created_at: datetime


class ReconciliationPage(ApiModel):
    items: list[ReconciliationResult]
    page: int
    page_size: int
    total: int


class FinancialTotal(ApiModel):
    source_system: str
    batch_id: UUID
    rule_id: str
    metric_name: str
    currency_code: str
    source_value: Decimal
    target_value: Decimal
    difference: Decimal
    status: str


class RecurringIssue(ApiModel):
    source_system: str
    rule_id: str
    field_name: str | None
    classification: str
    first_seen_at: datetime
    last_seen_at: datetime
    affected_run_count: int
    current_failure_count: int
    updated_at: datetime


class RecurringIssuePage(ApiModel):
    items: list[RecurringIssue]
    page: int
    page_size: int
    total: int
