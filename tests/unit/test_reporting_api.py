from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from uuid import uuid4

from fastapi.testclient import TestClient

from api.main import create_app
from etl_framework.reporting import PageResult, ReportingError


NOW = datetime(2026, 11, 4, 12, tzinfo=timezone.utc)


class FakeRepository:
    def __init__(self):
        self.filters = {}

    def health(self):
        return True

    def list_pipeline_runs(self, **filters):
        self.filters = filters
        return PageResult((run_summary(),), filters["page"], filters["page_size"], 1)

    def get_pipeline_run(self, run_id):
        return run_summary(run_id)

    def list_source_batches(self, **filters):
        self.filters = filters
        item = {
            "batch_run_id": uuid4(), "run_id": filters.get("run_id") or uuid4(),
            "batch_id": uuid4(), "source_system": "CRM_PG",
            "delivery_key": "window", "status": "SUCCEEDED", "total_rows": 3,
            "accepted_rows": 2, "quarantined_rows": 0, "duplicate_rows": 1,
            "totals_verified": True, "started_at": NOW, "finished_at": NOW,
            "created_at": NOW,
        }
        return PageResult((item,), filters["page"], filters["page_size"], 1)

    def list_validation_failures(self, **filters):
        self.filters = filters
        item = {
            "validation_result_id": uuid4(), "run_id": uuid4(), "batch_id": uuid4(),
            "source_system": "CRM_PG", "rule_id": "DQ-CUS-002",
            "severity": "ERROR", "field_name": "email",
            "message": "Email structure is invalid", "created_at": NOW,
        }
        return PageResult((item,), filters["page"], filters["page_size"], 1)

    def failure_groups(self, **filters):
        return ({"source_system": "CRM_PG", "rule_id": "DQ-CUS-002",
                 "severity": "ERROR", "failure_count": 1},)

    def list_reconciliations(self, **filters):
        self.filters = filters
        item = {
            "reconciliation_result_id": uuid4(), "run_id": uuid4(),
            "batch_id": uuid4(), "rule_id": "RC-ROW-001",
            "metric_name": "row_accounting", "group_key": None,
            "source_value": Decimal("3.0000"), "target_value": Decimal("3.0000"),
            "difference": Decimal("0.0000"), "tolerance": Decimal("0.0000"),
            "status": "PASS", "created_at": NOW,
        }
        return PageResult((item,), filters["page"], filters["page_size"], 1)

    def financial_totals(self, run_id):
        return ({"source_system": "BILLING_MSSQL", "batch_id": uuid4(),
                 "rule_id": "RC-FIN-001", "metric_name": "completed_amount",
                 "currency_code": "USD", "source_value": Decimal("10.00"),
                 "target_value": Decimal("10.00"), "difference": Decimal("0.00"),
                 "status": "PASS"},)

    def list_recurring_issues(self, **filters):
        self.filters = filters
        item = {"source_system": "CRM_PG", "rule_id": "DQ-CUS-002",
                "field_name": "email", "classification": "RECURRING",
                "first_seen_at": NOW, "last_seen_at": NOW,
                "affected_run_count": 2, "current_failure_count": 1,
                "updated_at": NOW}
        return PageResult((item,), filters["page"], filters["page_size"], 1)


def run_summary(run_id=None):
    return {
        "run_id": run_id or uuid4(), "airflow_run_id": "scheduled__2026-11-04",
        "status": "SUCCEEDED", "started_at": NOW, "finished_at": NOW,
        "created_at": NOW, "batch_count": 4, "total_rows": 10,
        "accepted_rows": 8, "quarantined_rows": 1, "duplicate_rows": 1,
        "totals_verified": True, "failure_count": 1,
        "reconciliation_failure_count": 0,
    }


def test_health_and_pipeline_summary_endpoints() -> None:
    repository = FakeRepository()
    with TestClient(create_app(repository)) as client:
        assert client.get("/health").json() == {"status": "ok", "database": "available"}
        response = client.get("/api/v1/runs?status=SUCCEEDED&page=1&page_size=20")
    assert response.status_code == 200
    assert response.json()["items"][0]["totals_verified"] is True
    assert repository.filters["status"] == "SUCCEEDED"


def test_run_source_batch_endpoint_passes_run_filter() -> None:
    repository = FakeRepository()
    run_id = uuid4()
    with TestClient(create_app(repository)) as client:
        response = client.get(f"/api/v1/runs/{run_id}/source-batches?page_size=10")
    assert response.status_code == 200
    assert repository.filters["run_id"] == run_id
    assert response.json()["items"][0]["totals_verified"] is True


def test_validation_endpoint_omits_sensitive_observations() -> None:
    with TestClient(create_app(FakeRepository())) as client:
        response = client.get(
            "/api/v1/validation-failures?source_system=CRM_PG&severity=ERROR"
        )
    assert response.status_code == 200
    item = response.json()["items"][0]
    assert "observed_value" not in item
    assert "business_key" not in item
    assert "details" not in item


def test_reconciliation_financial_and_recurring_endpoints() -> None:
    run_id = uuid4()
    with TestClient(create_app(FakeRepository())) as client:
        reconciliation = client.get("/api/v1/reconciliations?status=PASS")
        financial = client.get(f"/api/v1/runs/{run_id}/financial-totals")
        recurring = client.get("/api/v1/recurring-issues?classification=RECURRING")
    assert reconciliation.status_code == 200
    assert financial.json()[0]["source_value"] == "10.00"
    assert recurring.json()["items"][0]["classification"] == "RECURRING"


def test_invalid_requests_return_sanitized_errors() -> None:
    secret = "Bearer-super-secret-token"
    with TestClient(create_app(FakeRepository())) as client:
        response = client.get(f"/api/v1/runs?status={secret}&page_size=1000")
    assert response.status_code == 422
    assert response.json() == {"detail": "Request validation failed"}
    assert secret not in response.text


def test_database_errors_do_not_expose_internal_message() -> None:
    class BrokenRepository(FakeRepository):
        def health(self):
            raise ReportingError("password=do-not-return host=internal")

    with TestClient(create_app(BrokenRepository())) as client:
        response = client.get("/health")
    assert response.status_code == 503
    assert response.json() == {"detail": "Reporting service is unavailable"}
    assert "password" not in response.text
