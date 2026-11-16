from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

from fastapi.testclient import TestClient

from api.auth import AuthSettings
from api.main import create_app
from api.operator import AirflowClient, AirflowSettings, OperatorRequest
from tests.unit.test_reporting_api import FakeRepository


AUTH = AuthSettings(
    enabled=True,
    dashboard_username="admin",
    dashboard_password="dashboard-password",
    admin_token="admin-token",
    operator_token="operator-token",
    viewer_token="viewer-token",
)


class FakeAirflowClient:
    def __init__(self):
        self.payloads: list[OperatorRequest] = []

    def trigger(self, payload: OperatorRequest):
        self.payloads.append(payload)
        return {"dag_id": "enterprise_etl_data_quality", "dag_run_id": "manual__safe", "state": "queued"}


def test_operator_and_admin_can_trigger_but_viewer_cannot() -> None:
    airflow = FakeAirflowClient()
    with TestClient(create_app(FakeRepository(), AUTH, airflow)) as client:
        denied = client.post(
            "/api/v1/operator/runs", json={},
            headers={"Authorization": "Bearer viewer-token"},
        )
        operator = client.post(
            "/api/v1/operator/runs",
            json={"upper_watermark": "2026-10-21T00:00:00Z"},
            headers={"Authorization": "Bearer operator-token"},
        )
        admin = client.post(
            "/api/v1/operator/runs", json={},
            headers={"Authorization": "Bearer admin-token"},
        )
    assert denied.status_code == 403
    assert operator.status_code == admin.status_code == 202
    assert operator.json()["dag_run_id"] == "manual__safe"
    assert airflow.payloads[0].upper_watermark == datetime(2026, 10, 21, tzinfo=timezone.utc)


def test_replay_requires_source_run_and_rejects_naive_watermark() -> None:
    airflow = FakeAirflowClient()
    headers = {"Authorization": "Bearer operator-token"}
    run_id = uuid4()
    with TestClient(create_app(FakeRepository(), AUTH, airflow)) as client:
        missing = client.post("/api/v1/operator/replays", json={}, headers=headers)
        naive = client.post(
            "/api/v1/operator/runs",
            json={"upper_watermark": "2026-10-21T00:00:00"}, headers=headers,
        )
        replay = client.post(
            "/api/v1/operator/replays",
            json={"replay_of_run_id": str(run_id)}, headers=headers,
        )
    assert missing.status_code == naive.status_code == 422
    assert replay.status_code == 202
    assert airflow.payloads[-1].replay_of_run_id == run_id


class FakeResponse:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


class FakeSession:
    def __init__(self):
        self.calls = []

    def post(self, url, **kwargs):
        self.calls.append((url, kwargs))
        if url.endswith("/auth/token"):
            return FakeResponse({"access_token": "short-lived-jwt"})
        return FakeResponse({"dag_run_id": "manual__verified", "state": "queued"})


def test_airflow_client_uses_json_login_and_stable_v2_trigger_contract() -> None:
    session = FakeSession()
    client = AirflowClient(
        AirflowSettings("http://airflow:8080", "service-user", "secret"), session
    )
    result = client.trigger(OperatorRequest(upper_watermark="2026-10-21T00:00:00Z"))
    login_url, login = session.calls[0]
    trigger_url, trigger = session.calls[1]
    assert login_url.endswith("/auth/token")
    assert login["json"] == {"username": "service-user", "password": "secret"}
    assert trigger_url.endswith("/api/v2/dags/enterprise_etl_data_quality/dagRuns")
    assert trigger["json"]["logical_date"] is None
    assert trigger["json"]["conf"]["upper_watermark"].endswith("+00:00")
    assert trigger["headers"] == {"Authorization": "Bearer short-lived-jwt"}
    assert result["dag_run_id"] == "manual__verified"
