"""Secure, bounded ingestion for the customer-status REST API."""

from __future__ import annotations

import hashlib
import json
import logging
import os
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlsplit, urlunsplit
from uuid import UUID

import requests
from requests.adapters import HTTPAdapter
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from urllib3.util.retry import Retry

from etl_framework.config.settings import ConfigurationError
from etl_framework.ingestion.database_sources import ExtractionWindow
from etl_framework.ingestion.staging_loaders import StagingLoadError, StagingLoadResult


LOGGER = logging.getLogger(__name__)
REQUIRED_FIELDS = ("customer_id", "status", "status_updated_at")
ALLOWED_STATUSES = frozenset({"active", "inactive", "suspended"})


class CustomerStatusApiError(RuntimeError):
    """Raised for a transport, pagination, or response-contract failure."""


@dataclass(frozen=True, slots=True)
class CustomerStatusApiSettings:
    endpoint: str
    token: str = field(repr=False)
    connect_timeout_seconds: float = 5.0
    read_timeout_seconds: float = 30.0
    max_retries: int = 3
    backoff_seconds: float = 0.5

    @classmethod
    def from_env(cls, environment: Mapping[str, str] | None = None) -> "CustomerStatusApiSettings":
        env = os.environ if environment is None else environment
        endpoint = env.get("CUSTOMER_STATUS_API_URL", "").strip()
        token = env.get("CUSTOMER_STATUS_API_TOKEN", "").strip()
        if not endpoint:
            raise ConfigurationError("Required environment variable CUSTOMER_STATUS_API_URL is missing")
        parsed = urlsplit(endpoint)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ConfigurationError("CUSTOMER_STATUS_API_URL must be an absolute HTTP(S) URL")
        if parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise ConfigurationError("CUSTOMER_STATUS_API_URL must not contain credentials, query, or fragment")
        if not token:
            raise ConfigurationError("Required environment variable CUSTOMER_STATUS_API_TOKEN is missing")
        if token.upper().startswith("CHANGE_ME"):
            raise ConfigurationError("CUSTOMER_STATUS_API_TOKEN still contains a placeholder")
        try:
            connect = float(env.get("CUSTOMER_STATUS_API_CONNECT_TIMEOUT_SECONDS", "5"))
            read = float(env.get("CUSTOMER_STATUS_API_READ_TIMEOUT_SECONDS", "30"))
            retries = int(env.get("CUSTOMER_STATUS_API_MAX_RETRIES", "3"))
            backoff = float(env.get("CUSTOMER_STATUS_API_BACKOFF_SECONDS", "0.5"))
        except ValueError as exc:
            raise ConfigurationError("Customer-status API timeout/retry settings must be numeric") from exc
        if not 0 < connect <= 120 or not 0 < read <= 300:
            raise ConfigurationError("Customer-status API timeouts are outside the allowed range")
        if not 0 <= retries <= 10 or not 0 <= backoff <= 60:
            raise ConfigurationError("Customer-status API retry settings are outside the allowed range")
        endpoint = urlunsplit((parsed.scheme, parsed.netloc, parsed.path, "", ""))
        return cls(endpoint, token, connect, read, retries, backoff)


def create_customer_status_session(settings: CustomerStatusApiSettings) -> requests.Session:
    """Create a session that retries only safe GET requests and transient failures."""
    retry = Retry(
        total=settings.max_retries,
        connect=settings.max_retries,
        read=settings.max_retries,
        status=settings.max_retries,
        backoff_factor=settings.backoff_seconds,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
        respect_retry_after_header=True,
        raise_on_status=False,
    )
    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {settings.token}", "Accept": "application/json"})
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.mount("http://", HTTPAdapter(max_retries=retry))
    return session


def extract_customer_statuses(
    settings: CustomerStatusApiSettings,
    window: ExtractionWindow,
    *,
    session: requests.Session | None = None,
) -> list[dict[str, Any]]:
    """Extract every validated page for an inclusive/exclusive UTC window."""
    owned_session = session is None
    client = session or create_customer_status_session(settings)
    page_token: str | None = None
    seen_tokens: set[str] = set()
    rows: list[dict[str, Any]] = []
    try:
        while True:
            params = {"updated_to": _api_timestamp(window.upper_watermark)}
            if window.lower_watermark is not None:
                params["updated_from"] = _api_timestamp(window.lower_watermark)
            if page_token is not None:
                params["page"] = page_token
            try:
                response = client.get(
                    settings.endpoint,
                    params=params,
                    timeout=(settings.connect_timeout_seconds, settings.read_timeout_seconds),
                )
                response.raise_for_status()
            except requests.RequestException as exc:
                raise CustomerStatusApiError("Customer-status API request failed") from exc
            try:
                payload = response.json()
            except (ValueError, json.JSONDecodeError) as exc:
                raise CustomerStatusApiError("Customer-status API returned invalid JSON") from exc
            page_rows, next_token = _validate_page(payload)
            rows.extend({**row, "_page_token": page_token} for row in page_rows)
            if next_token is None:
                break
            if next_token in seen_tokens:
                raise CustomerStatusApiError("Customer-status API repeated a pagination token")
            seen_tokens.add(next_token)
            page_token = next_token
    finally:
        if owned_session:
            client.close()
    LOGGER.info("Customer-status API extraction completed with %d rows", len(rows))
    return rows


def load_customer_statuses(
    engine: Engine,
    rows: Iterable[Mapping[str, Any]],
    *,
    run_id: UUID,
    batch_id: UUID,
    ingested_at: datetime | None = None,
) -> StagingLoadResult:
    """Atomically replace one API-window batch in stg.customer_status_raw."""
    loaded_at = ingested_at or datetime.now(timezone.utc)
    if loaded_at.tzinfo is None:
        raise ValueError("ingested_at must include a timezone")
    mapped = [_map_status(row, run_id, batch_id, loaded_at.astimezone(timezone.utc)) for row in rows]
    try:
        with engine.begin() as connection:
            connection.execute(text("DELETE FROM stg.customer_status_raw WHERE batch_id = :batch_id"), {"batch_id": batch_id})
            result = connection.execute(text("""
                INSERT INTO stg.customer_status_raw (
                    customer_id, status, status_updated_at_raw, page_token, raw_payload,
                    run_id, batch_id, source_system, source_record_id,
                    source_updated_at, record_checksum, ingested_at
                ) VALUES (
                    :customer_id, :status, :status_updated_at_raw, :page_token,
                    CAST(:raw_payload AS JSONB), :run_id, :batch_id, 'STATUS_API',
                    :source_record_id, :source_updated_at, :record_checksum, :ingested_at
                ) ON CONFLICT (batch_id, record_checksum) DO NOTHING
            """), mapped) if mapped else None
    except SQLAlchemyError as exc:
        raise StagingLoadError("Staging load failed for STATUS_API") from exc
    inserted = 0 if result is None else (result.rowcount if result.rowcount >= 0 else len(mapped))
    return StagingLoadResult("STATUS_API", len(mapped), inserted)


def _validate_page(payload: Any) -> tuple[list[dict[str, Any]], str | None]:
    if not isinstance(payload, dict) or not isinstance(payload.get("data"), list):
        raise CustomerStatusApiError("Customer-status API response must contain a data array")
    validated: list[dict[str, Any]] = []
    for index, row in enumerate(payload["data"]):
        if not isinstance(row, dict):
            raise CustomerStatusApiError(f"Customer-status API data item {index} must be an object")
        missing = [name for name in REQUIRED_FIELDS if name not in row or row[name] is None]
        if missing:
            raise CustomerStatusApiError(f"Customer-status API data item {index} is missing required fields: {', '.join(missing)}")
        if not isinstance(row["customer_id"], str) or not row["customer_id"].strip():
            raise CustomerStatusApiError(f"Customer-status API data item {index} has an invalid customer_id")
        if not isinstance(row["status"], str) or row["status"].strip().lower() not in ALLOWED_STATUSES:
            raise CustomerStatusApiError(f"Customer-status API data item {index} has an invalid status")
        _parse_timestamp(row["status_updated_at"], index)
        validated.append(dict(row))
    token = payload.get("next_page")
    if token is not None and (not isinstance(token, str) or not token):
        raise CustomerStatusApiError("Customer-status API next_page must be a nonblank string or null")
    return validated, token


def _map_status(row: Mapping[str, Any], run_id: UUID, batch_id: UUID, ingested_at: datetime) -> dict[str, Any]:
    raw_payload = {key: value for key, value in row.items() if key != "_page_token"}
    canonical = json.dumps(raw_payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return {
        "customer_id": str(row["customer_id"]), "status": str(row["status"]),
        "status_updated_at_raw": str(row["status_updated_at"]),
        "page_token": row.get("_page_token"), "raw_payload": canonical,
        "run_id": run_id, "batch_id": batch_id, "source_record_id": str(row["customer_id"]),
        "source_updated_at": _parse_timestamp(row["status_updated_at"], 0),
        "record_checksum": hashlib.sha256(canonical.encode("utf-8")).hexdigest(),
        "ingested_at": ingested_at,
    }


def _parse_timestamp(value: Any, index: int) -> datetime:
    if not isinstance(value, str):
        raise CustomerStatusApiError(f"Customer-status API data item {index} has an invalid status_updated_at")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise CustomerStatusApiError(f"Customer-status API data item {index} has an invalid status_updated_at") from exc
    if parsed.tzinfo is None:
        raise CustomerStatusApiError(f"Customer-status API data item {index} status_updated_at lacks a timezone")
    return parsed.astimezone(timezone.utc)


def _api_timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
