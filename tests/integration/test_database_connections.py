from __future__ import annotations

import os
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from etl_framework.config.settings import AppSettings
from etl_framework.database.connections import (
    check_connection,
    create_database_engines,
    dispose_engines,
)
from etl_framework.ingestion.database_sources import (
    ExtractionWindow,
    extract_billing_transactions,
    extract_crm_customers,
)


pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("RUN_DATABASE_INTEGRATION_TESTS") != "1",
        reason="set RUN_DATABASE_INTEGRATION_TESTS=1 with running database services",
    ),
]


def test_all_configured_databases_accept_health_query() -> None:
    engines = create_database_engines(AppSettings.from_env())
    try:
        for name, engine in engines.items():
            check_connection(name, engine)
    finally:
        dispose_engines(engines)


def test_database_sources_stream_seed_records_with_native_types() -> None:
    engines = create_database_engines(AppSettings.from_env())
    window = ExtractionWindow(upper_watermark=datetime(2100, 1, 1, tzinfo=timezone.utc))
    try:
        customers = list(extract_crm_customers(engines["crm"], window, fetch_size=2))
        transactions = list(
            extract_billing_transactions(engines["billing"], window, fetch_size=2)
        )
    finally:
        dispose_engines(engines)

    assert set(range(1, 8)) <= {row["source_row_id"] for row in customers}
    assert set(range(1, 10)) <= {row["source_row_id"] for row in transactions}
    assert all(isinstance(row["amount"], Decimal) for row in transactions)
