"""Local acceptance-only customer-status API backed by the JSON fixture."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Annotated, Any

from fastapi import FastAPI, Header, HTTPException, Query


app = FastAPI(title="Customer Status Acceptance Fixture", docs_url=None, redoc_url=None)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/v1/customer-status")
def customer_statuses(
    updated_to: Annotated[datetime, Query()],
    updated_from: Annotated[datetime | None, Query()] = None,
    page: Annotated[str | None, Query()] = None,
    authorization: Annotated[str | None, Header()] = None,
) -> dict[str, Any]:
    expected = os.environ.get("CUSTOMER_STATUS_API_TOKEN", "")
    if not expected or authorization != f"Bearer {expected}":
        raise HTTPException(status_code=401, detail="Unauthorized")
    upper = _utc(updated_to, "updated_to")
    lower = _utc(updated_from, "updated_from") if updated_from is not None else None
    rows = []
    for row in _fixture_rows():
        changed = _utc(datetime.fromisoformat(row["status_updated_at"].replace("Z", "+00:00")), "fixture timestamp")
        if changed <= upper and (lower is None or changed > lower):
            rows.append(row)
    try:
        offset = 0 if page is None else int(page)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid page token") from exc
    if offset < 0 or offset > len(rows):
        raise HTTPException(status_code=400, detail="Invalid page token")
    page_size = 2
    selected = rows[offset : offset + page_size]
    next_offset = offset + len(selected)
    return {
        "data": selected,
        "next_page": str(next_offset) if next_offset < len(rows) else None,
    }


def _fixture_rows() -> list[dict[str, Any]]:
    fixture_path = Path(
        os.environ.get(
            "CUSTOMER_STATUS_FIXTURE_PATH",
            "/app/data/raw/json/customer_status_20261020.json",
        )
    )
    payload = json.loads(fixture_path.read_text(encoding="utf-8"))
    data = payload.get("data")
    if not isinstance(data, list):
        raise RuntimeError("Customer-status fixture is invalid")
    return [dict(row) for row in data]


def _utc(value: datetime, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise HTTPException(status_code=422, detail=f"{field_name} requires a timezone")
    return value.astimezone(timezone.utc)
