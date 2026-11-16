from __future__ import annotations

from fastapi.testclient import TestClient

from api.auth import AuthSettings
from api.main import create_app
from tests.unit.test_reporting_api import FakeRepository


SETTINGS = AuthSettings(
    enabled=True,
    dashboard_username="dashboard",
    dashboard_password="safe-password",
    admin_token="admin-token",
    operator_token="operator-token",
    viewer_token="viewer-token",
)


def test_api_requires_auth_and_accepts_all_documented_roles() -> None:
    with TestClient(create_app(FakeRepository(), SETTINGS)) as client:
        assert client.get("/api/v1/runs").status_code == 401
        for token, role in (("admin-token", "administrator"),
                            ("operator-token", "operator"),
                            ("viewer-token", "read-only")):
            response = client.get("/api/v1/runs", headers={"Authorization": f"Bearer {token}"})
            assert response.status_code == 200
            assert response.headers["X-Reporting-Role"] == role


def test_dashboard_has_login_and_operational_views() -> None:
    with TestClient(create_app(FakeRepository(), SETTINGS)) as client:
        unauthorized = client.get("/dashboard")
        response = client.get("/dashboard", auth=("dashboard", "safe-password"))
    assert unauthorized.status_code == 401
    assert unauthorized.headers["www-authenticate"] == "Basic"
    assert response.status_code == 200
    assert "Pipeline runs" in response.text
    assert "Validation failures" in response.text
    assert "Reconciliation differences" in response.text
    assert "financial-totals" in response.text
    assert "page_size=10" in response.text
    assert "Export CSV" in response.text
    assert "Refresh 30s" in response.text
    assert "All sources" in response.text
    assert "Trigger pipeline" in response.text
    assert "Replay" in response.text


def test_invalid_credentials_are_rejected_without_echoing_secrets() -> None:
    with TestClient(create_app(FakeRepository(), SETTINGS)) as client:
        response = client.get("/api/v1/runs", headers={"Authorization": "Bearer wrong-secret"})
    assert response.status_code == 401
    assert "wrong-secret" not in response.text


def test_metrics_are_authenticated_and_avoid_dynamic_path_labels() -> None:
    with TestClient(create_app(FakeRepository(), SETTINGS)) as client:
        assert client.get("/metrics").status_code == 401
        headers = {"Authorization": "Bearer viewer-token"}
        assert client.get("/api/v1/runs", headers=headers).status_code == 200
        response = client.get("/metrics", headers=headers)
    assert response.status_code == 200
    assert "reporting_http_requests_total" in response.text
    assert 'route="/api/v1/runs"' in response.text
    assert "viewer-token" not in response.text
