from __future__ import annotations

import os

import pytest

from etl_framework.config.settings import AppSettings
from etl_framework.database.connections import (
    check_connection,
    create_database_engines,
    dispose_engines,
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
