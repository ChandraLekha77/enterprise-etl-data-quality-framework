"""Live acceptance assertions; skipped unless explicitly enabled."""

from __future__ import annotations

import os
from uuid import UUID

import pytest
import requests


pytestmark = [
    pytest.mark.e2e,
    pytest.mark.skipif(os.getenv("RUN_E2E_TESTS") != "1", reason="set RUN_E2E_TESTS=1"),
]

BASE = os.getenv("REPORTING_API_URL", "http://127.0.0.1:8000").rstrip("/")
HEADERS = {"Authorization": f"Bearer {os.getenv('REPORTING_VIEWER_TOKEN', '')}"}


def _get(path: str):
    response = requests.get(f"{BASE}{path}", headers=HEADERS, timeout=10)
    response.raise_for_status()
    return response.json()


def test_valid_four_source_run_and_lineage() -> None:
    run_id = UUID(os.environ["E2E_VALID_RUN_ID"])
    run = _get(f"/api/v1/runs/{run_id}")
    batches = _get(f"/api/v1/runs/{run_id}/source-batches?page_size=100")["items"]
    reconciliations = _get(f"/api/v1/reconciliations?run_id={run_id}&page_size=100")["items"]
    assert run["status"] == "SUCCEEDED"
    assert {row["source_system"] for row in batches} == {
        "CRM_PG", "BILLING_MSSQL", "OPS_CSV", "STATUS_API"
    }
    assert all(row["run_id"] == str(run_id) and row["batch_id"] for row in batches)
    assert all(row["total_rows"] == row["accepted_rows"] + row["quarantined_rows"]
               + row["duplicate_rows"] for row in batches)
    assert reconciliations and all(row["status"] == "PASS" for row in reconciliations)


def test_mixed_quality_quarantine_and_masked_reporting() -> None:
    run_id = UUID(os.environ["E2E_MIXED_RUN_ID"])
    run = _get(f"/api/v1/runs/{run_id}")
    batches = _get(f"/api/v1/runs/{run_id}/source-batches?page_size=100")["items"]
    failures = _get(f"/api/v1/validation-failures?run_id={run_id}&page_size=100")["items"]
    assert run["status"] == "QUARANTINED"
    assert sum(row["quarantined_rows"] for row in batches) > 0
    assert failures and any(row["severity"] == "ERROR" for row in failures)
    assert all("observed_value" not in row and "business_key" not in row for row in failures)
