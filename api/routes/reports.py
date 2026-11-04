"""Read-only operational and quality reporting endpoints."""

from __future__ import annotations

from typing import Annotated, Literal
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query

from api.dependencies import get_repository
from api.schemas.reports import (
    FailureGroup,
    FinancialTotal,
    HealthResponse,
    PipelineRunPage,
    PipelineRunSummary,
    ReconciliationPage,
    RecurringIssuePage,
    SourceBatchPage,
    ValidationFailurePage,
)
from etl_framework.reporting import ReportingRepository


router = APIRouter()
Repository = Annotated[ReportingRepository, Depends(get_repository)]
Page = Annotated[int, Query(ge=1)]
PageSize = Annotated[int, Query(ge=1, le=100)]
Source = Literal["CRM_PG", "BILLING_MSSQL", "OPS_CSV", "STATUS_API"]
RunStatus = Literal[
    "PENDING", "RUNNING", "QUARANTINED", "FAILED", "SUCCEEDED", "CANCELLED"
]


def _page_response(result) -> dict:
    return {
        "items": list(result.items),
        "page": result.page,
        "page_size": result.page_size,
        "total": result.total,
    }


@router.get("/health", response_model=HealthResponse, tags=["health"])
def health(repository: Repository) -> dict[str, str]:
    if not repository.health():
        raise HTTPException(status_code=503, detail="Reporting database is unavailable")
    return {"status": "ok", "database": "available"}


@router.get("/api/v1/runs", response_model=PipelineRunPage, tags=["pipeline runs"])
def list_runs(
    repository: Repository,
    status: RunStatus | None = None,
    page: Page = 1,
    page_size: PageSize = 50,
) -> dict:
    return _page_response(
        repository.list_pipeline_runs(status=status, page=page, page_size=page_size)
    )


@router.get(
    "/api/v1/runs/{run_id}", response_model=PipelineRunSummary, tags=["pipeline runs"]
)
def get_run(run_id: UUID, repository: Repository) -> dict:
    result = repository.get_pipeline_run(run_id)
    if result is None:
        raise HTTPException(status_code=404, detail="Pipeline run was not found")
    return result


@router.get(
    "/api/v1/runs/{run_id}/source-batches",
    response_model=SourceBatchPage,
    tags=["source batches"],
)
def run_source_batches(
    run_id: UUID,
    repository: Repository,
    source_system: Source | None = None,
    status: RunStatus | None = None,
    page: Page = 1,
    page_size: PageSize = 50,
) -> dict:
    return _page_response(repository.list_source_batches(
        run_id=run_id,
        source_system=source_system,
        status=status,
        page=page,
        page_size=page_size,
    ))


@router.get(
    "/api/v1/source-batches", response_model=SourceBatchPage, tags=["source batches"]
)
def list_source_batches(
    repository: Repository,
    run_id: UUID | None = None,
    source_system: Source | None = None,
    status: RunStatus | None = None,
    page: Page = 1,
    page_size: PageSize = 50,
) -> dict:
    return _page_response(repository.list_source_batches(
        run_id=run_id,
        source_system=source_system,
        status=status,
        page=page,
        page_size=page_size,
    ))


@router.get(
    "/api/v1/validation-failures",
    response_model=ValidationFailurePage,
    tags=["validation"],
)
def list_validation_failures(
    repository: Repository,
    run_id: UUID | None = None,
    source_system: Source | None = None,
    rule_id: Annotated[str | None, Query(min_length=1, max_length=30)] = None,
    severity: Literal["ERROR", "WARNING", "INFO"] | None = None,
    page: Page = 1,
    page_size: PageSize = 50,
) -> dict:
    return _page_response(repository.list_validation_failures(
        run_id=run_id,
        source_system=source_system,
        rule_id=rule_id,
        severity=severity,
        page=page,
        page_size=page_size,
    ))


@router.get(
    "/api/v1/validation-failures/summary",
    response_model=list[FailureGroup],
    tags=["validation"],
)
def validation_failure_summary(
    repository: Repository, run_id: UUID | None = None
) -> tuple[dict, ...]:
    return repository.failure_groups(run_id=run_id)


@router.get(
    "/api/v1/reconciliations",
    response_model=ReconciliationPage,
    tags=["reconciliation"],
)
def list_reconciliations(
    repository: Repository,
    run_id: UUID | None = None,
    rule_id: Annotated[str | None, Query(min_length=1, max_length=30)] = None,
    status: Literal["PASS", "FAIL"] | None = None,
    group_key: Annotated[str | None, Query(min_length=1, max_length=50)] = None,
    page: Page = 1,
    page_size: PageSize = 50,
) -> dict:
    return _page_response(repository.list_reconciliations(
        run_id=run_id,
        rule_id=rule_id,
        status=status,
        group_key=group_key,
        page=page,
        page_size=page_size,
    ))


@router.get(
    "/api/v1/runs/{run_id}/financial-totals",
    response_model=list[FinancialTotal],
    tags=["reconciliation"],
)
def financial_totals(run_id: UUID, repository: Repository) -> tuple[dict, ...]:
    return repository.financial_totals(run_id)


@router.get(
    "/api/v1/recurring-issues",
    response_model=RecurringIssuePage,
    tags=["recurring issues"],
)
def recurring_issues(
    repository: Repository,
    source_system: Source | None = None,
    rule_id: Annotated[str | None, Query(min_length=1, max_length=30)] = None,
    classification: Literal["NEW", "RECURRING", "PERSISTENT", "RESOLVED"] | None = None,
    page: Page = 1,
    page_size: PageSize = 50,
) -> dict:
    return _page_response(repository.list_recurring_issues(
        source_system=source_system,
        rule_id=rule_id,
        classification=classification,
        page=page,
        page_size=page_size,
    ))
