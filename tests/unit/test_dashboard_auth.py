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


def test_invalid_credentials_are_rejected_without_echoing_secrets() -> None:
    with TestClient(create_app(FakeRepository(), SETTINGS)) as client:
        response = client.get("/api/v1/runs", headers={"Authorization": "Bearer wrong-secret"})
    assert response.status_code == 401
    assert "wrong-secret" not in response.text
