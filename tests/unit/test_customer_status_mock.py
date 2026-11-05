from __future__ import annotations

import json

from fastapi.testclient import TestClient

from api.mock_customer_status import app


def test_mock_api_enforces_auth_window_and_pagination(tmp_path, monkeypatch) -> None:
    fixture = tmp_path / "statuses.json"
    fixture.write_text(json.dumps({"data": [
        {"customer_id": "C1", "status": "active", "status_updated_at": "2026-10-20T10:00:00Z"},
        {"customer_id": "C2", "status": "inactive", "status_updated_at": "2026-10-20T11:00:00Z"},
        {"customer_id": "C3", "status": "suspended", "status_updated_at": "2026-10-20T12:00:00Z"},
    ]}), encoding="utf-8")
    monkeypatch.setenv("CUSTOMER_STATUS_API_TOKEN", "test-token")
    monkeypatch.setenv("CUSTOMER_STATUS_FIXTURE_PATH", str(fixture))

    with TestClient(app) as client:
        unauthorized = client.get(
            "/v1/customer-status?updated_to=2026-10-20T13:00:00Z"
        )
        first = client.get(
            "/v1/customer-status?updated_from=2026-10-20T09:00:00Z&updated_to=2026-10-20T13:00:00Z",
            headers={"Authorization": "Bearer test-token"},
        )
        second = client.get(
            "/v1/customer-status?updated_from=2026-10-20T09:00:00Z&updated_to=2026-10-20T13:00:00Z&page=2",
            headers={"Authorization": "Bearer test-token"},
        )

    assert unauthorized.status_code == 401
    assert len(first.json()["data"]) == 2
    assert first.json()["next_page"] == "2"
    assert [row["customer_id"] for row in second.json()["data"]] == ["C3"]
    assert second.json()["next_page"] is None
