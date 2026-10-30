from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock
from uuid import uuid4

import pytest
import requests

from etl_framework.ingestion.customer_status_api import (
    CustomerStatusApiError,
    CustomerStatusApiSettings,
    create_customer_status_session,
    extract_customer_statuses,
    load_customer_statuses,
)
from etl_framework.ingestion.database_sources import ExtractionWindow


UTC = timezone.utc


def settings() -> CustomerStatusApiSettings:
    return CustomerStatusApiSettings("https://status.example.test/v1/customer-status", "top-secret", 2, 9, 2, 0.1)


def window() -> ExtractionWindow:
    return ExtractionWindow(
        upper_watermark=datetime(2026, 10, 2, tzinfo=UTC),
        lower_watermark=datetime(2026, 10, 1, tzinfo=UTC),
    )


def response(payload: object) -> MagicMock:
    item = MagicMock()
    item.json.return_value = payload
    item.raise_for_status.return_value = None
    return item


def test_extract_sends_window_follows_pages_and_never_places_token_in_url() -> None:
    client = MagicMock()
    client.get.side_effect = [
        response({"data": [{"customer_id": "C1", "status": "active", "status_updated_at": "2026-10-01T12:00:00Z"}], "next_page": "opaque-page"}),
        response({"data": [{"customer_id": "C2", "status": "inactive", "status_updated_at": "2026-10-01T13:00:00+00:00"}], "next_page": None}),
    ]

    rows = extract_customer_statuses(settings(), window(), session=client)

    assert [row["customer_id"] for row in rows] == ["C1", "C2"]
    assert rows[0]["_page_token"] is None
    assert rows[1]["_page_token"] == "opaque-page"
    first_call, second_call = client.get.call_args_list
    assert first_call.args == (settings().endpoint,)
    assert first_call.kwargs["params"] == {"updated_from": "2026-10-01T00:00:00Z", "updated_to": "2026-10-02T00:00:00Z"}
    assert second_call.kwargs["params"]["page"] == "opaque-page"
    assert first_call.kwargs["timeout"] == (2, 9)


def test_repeated_pagination_token_fails_without_disclosing_token() -> None:
    client = MagicMock()
    client.get.side_effect = [
        response({"data": [], "next_page": "sensitive-token"}),
        response({"data": [], "next_page": "sensitive-token"}),
    ]

    with pytest.raises(CustomerStatusApiError) as error:
        extract_customer_statuses(settings(), window(), session=client)

    assert "repeated" in str(error.value)
    assert "sensitive-token" not in str(error.value)
    assert "top-secret" not in str(error.value)


@pytest.mark.parametrize("payload", [None, [], {}, {"data": {}}, {"data": [{"status": "active", "status_updated_at": "2026-10-01T00:00:00Z"}]}])
def test_invalid_response_contract_is_rejected(payload: object) -> None:
    client = MagicMock()
    client.get.return_value = response(payload)
    with pytest.raises(CustomerStatusApiError):
        extract_customer_statuses(settings(), window(), session=client)


def test_request_exception_is_sanitized() -> None:
    client = MagicMock()
    client.get.side_effect = requests.RequestException("Authorization: Bearer top-secret")
    with pytest.raises(CustomerStatusApiError) as error:
        extract_customer_statuses(settings(), window(), session=client)
    assert str(error.value) == "Customer-status API request failed"
    assert "top-secret" not in str(error.value)


def test_unknown_nonblank_status_is_preserved_for_business_rule_quarantine() -> None:
    client = MagicMock()
    client.get.return_value = response(
        {"data": [{"customer_id": "C1", "status": "archived", "status_updated_at": "2026-10-01T12:00:00Z"}], "next_page": None}
    )
    rows = extract_customer_statuses(settings(), window(), session=client)
    assert rows[0]["status"] == "archived"


def test_session_has_bounded_retry_policy_and_auth_header() -> None:
    session = create_customer_status_session(settings())
    try:
        assert session.headers["Authorization"] == "Bearer top-secret"
        retry = session.get_adapter("https://").max_retries
        assert retry.total == 2
        assert retry.allowed_methods == frozenset({"GET"})
        assert 429 in retry.status_forcelist
    finally:
        session.close()


def test_load_replaces_batch_and_preserves_payload() -> None:
    engine = MagicMock()
    connection = engine.begin.return_value.__enter__.return_value
    connection.execute.side_effect = [MagicMock(rowcount=0), MagicMock(rowcount=1)]
    run_id, batch_id = uuid4(), uuid4()
    result = load_customer_statuses(
        engine,
        [{"customer_id": "C1", "status": "active", "status_updated_at": "2026-10-01T12:00:00Z", "extra": "kept", "_page_token": "page-2"}],
        run_id=run_id,
        batch_id=batch_id,
        ingested_at=datetime(2026, 10, 2, tzinfo=UTC),
    )
    assert result.rows_received == result.rows_inserted == 1
    inserted = connection.execute.call_args_list[1].args[1][0]
    assert inserted["page_token"] == "page-2"
    assert '"extra":"kept"' in inserted["raw_payload"]
    assert "_page_token" not in inserted["raw_payload"]
    assert len(inserted["record_checksum"]) == 64
