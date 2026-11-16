"""Role-gated Airflow trigger and corrected-replay controls."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Annotated, Any
from urllib.parse import quote
from uuid import UUID

import requests
from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, ConfigDict, field_validator


DAG_ID = "enterprise_etl_data_quality"


class OperatorRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    upper_watermark: datetime | None = None
    replay_of_run_id: UUID | None = None

    @field_validator("upper_watermark")
    @classmethod
    def timezone_required(cls, value: datetime | None) -> datetime | None:
        if value is not None and (value.tzinfo is None or value.utcoffset() is None):
            raise ValueError("upper_watermark must include a timezone")
        return value


@dataclass(frozen=True, slots=True)
class AirflowSettings:
    base_url: str
    username: str
    password: str
    timeout_seconds: float = 10.0

    @classmethod
    def from_env(cls, environment: Mapping[str, str] | None = None) -> "AirflowSettings":
        env = os.environ if environment is None else environment
        values = {
            "base_url": env.get("AIRFLOW_OPERATOR_BASE_URL", "").rstrip("/"),
            "username": env.get("AIRFLOW_OPERATOR_USERNAME", "").strip(),
            "password": env.get("AIRFLOW_OPERATOR_PASSWORD", ""),
        }
        if not all(values.values()):
            raise ValueError("Airflow operator credentials are not configured")
        return cls(**values)


class AirflowOperatorError(RuntimeError):
    pass


class AirflowClient:
    def __init__(self, settings: AirflowSettings, session: requests.Session | None = None):
        self.settings = settings
        self.session = session or requests.Session()

    def trigger(self, request: OperatorRequest) -> dict[str, Any]:
        try:
            token_response = self.session.post(
                f"{self.settings.base_url}/auth/token",
                json={"username": self.settings.username, "password": self.settings.password},
                timeout=self.settings.timeout_seconds,
            )
            token_response.raise_for_status()
            access_token = token_response.json()["access_token"]
        except (requests.RequestException, KeyError, ValueError) as exc:
            raise AirflowOperatorError("Airflow authentication failed") from exc
        conf: dict[str, str] = {}
        if request.upper_watermark is not None:
            conf["upper_watermark"] = request.upper_watermark.isoformat()
        if request.replay_of_run_id is not None:
            conf["replay_of_run_id"] = str(request.replay_of_run_id)
        try:
            response = self.session.post(
                f"{self.settings.base_url}/api/v2/dags/{quote(DAG_ID, safe='')}/dagRuns",
                json={"logical_date": None, "conf": conf},
                headers={"Authorization": f"Bearer {access_token}"},
                timeout=self.settings.timeout_seconds,
            )
            response.raise_for_status()
            payload = response.json()
        except (requests.RequestException, ValueError) as exc:
            raise AirflowOperatorError("Airflow rejected the pipeline request") from exc
        return {
            "dag_id": DAG_ID,
            "dag_run_id": payload.get("dag_run_id") or payload.get("run_id"),
            "state": payload.get("state", "queued"),
        }


def require_operator(request: Request) -> str:
    role = getattr(request.state, "reporting_role", None)
    if role not in {"administrator", "operator"}:
        raise HTTPException(status_code=403, detail="Operator role is required")
    return role


def get_airflow_client(request: Request) -> AirflowClient:
    client = request.app.state.airflow_client
    if client is None:
        try:
            client = AirflowClient(AirflowSettings.from_env())
        except ValueError as exc:
            raise HTTPException(status_code=503, detail="Operator service is unavailable") from exc
        request.app.state.airflow_client = client
    return client


router = APIRouter(prefix="/api/v1/operator", tags=["operator controls"])
Client = Annotated[AirflowClient, Depends(get_airflow_client)]
Role = Annotated[str, Depends(require_operator)]


@router.post("/runs", status_code=202)
def trigger_run(payload: OperatorRequest, client: Client, _role: Role) -> dict[str, Any]:
    try:
        return client.trigger(payload)
    except AirflowOperatorError as exc:
        raise HTTPException(status_code=502, detail="Airflow request failed") from exc


@router.post("/replays", status_code=202)
def replay_run(payload: OperatorRequest, client: Client, _role: Role) -> dict[str, Any]:
    if payload.replay_of_run_id is None:
        raise HTTPException(status_code=422, detail="replay_of_run_id is required")
    try:
        return client.trigger(payload)
    except AirflowOperatorError as exc:
        raise HTTPException(status_code=502, detail="Airflow request failed") from exc
