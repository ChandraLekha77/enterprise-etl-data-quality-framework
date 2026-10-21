from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy.exc import OperationalError

from etl_framework.config.settings import DatabaseSettings, PoolSettings
from etl_framework.database.connections import (
    DatabaseConnectionError,
    check_connection,
    create_database_engine,
    dispose_engines,
    transaction_scope,
)


def postgres_settings() -> DatabaseSettings:
    return DatabaseSettings(
        name="warehouse",
        drivername="postgresql+psycopg",
        username="etl_user",
        password="never-log-this",
        host="warehouse-postgres",
        port=5432,
        database="quality_framework",
        query={"sslmode": "disable"},
    )


def sqlserver_settings() -> DatabaseSettings:
    return DatabaseSettings(
        name="billing",
        drivername="mssql+pyodbc",
        username="billing_reader",
        password="never-log-this",
        host="billing-sqlserver",
        port=1433,
        database="billing_source",
        query={
            "driver": "ODBC Driver 18 for SQL Server",
            "Encrypt": "yes",
            "TrustServerCertificate": "yes",
        },
    )


def test_create_postgres_engine_uses_bounded_resilient_pool() -> None:
    pool = PoolSettings(
        size=4,
        max_overflow=6,
        timeout_seconds=20,
        recycle_seconds=900,
        connect_timeout_seconds=7,
    )

    with patch("etl_framework.database.connections.create_engine") as create_engine:
        create_database_engine(postgres_settings(), pool)

    _, kwargs = create_engine.call_args
    assert kwargs == {
        "pool_pre_ping": True,
        "pool_size": 4,
        "max_overflow": 6,
        "pool_timeout": 20,
        "pool_recycle": 900,
        "connect_args": {
            "connect_timeout": 7,
            "application_name": "enterprise_etl_quality",
        },
    }


def test_create_sqlserver_engine_sets_login_timeout() -> None:
    pool = PoolSettings(connect_timeout_seconds=9)

    with patch("etl_framework.database.connections.create_engine") as create_engine:
        create_database_engine(sqlserver_settings(), pool)

    _, kwargs = create_engine.call_args
    assert kwargs["connect_args"] == {"timeout": 9}
    assert kwargs["pool_pre_ping"] is True


def test_unsupported_driver_is_rejected() -> None:
    settings = DatabaseSettings(
        name="unsupported",
        drivername="sqlite",
        username="user",
        password="password",
        host="host",
        port=1,
        database="database",
    )

    with pytest.raises(ValueError, match="Unsupported database driver"):
        create_database_engine(settings, PoolSettings())


def test_connection_check_executes_select_one() -> None:
    engine = MagicMock()
    connection = engine.connect.return_value.__enter__.return_value
    connection.execute.return_value.scalar_one.return_value = 1

    check_connection("warehouse", engine)

    connection.execute.assert_called_once()


def test_connection_error_does_not_leak_credentials() -> None:
    engine = MagicMock()
    engine.connect.return_value.__enter__.side_effect = OperationalError(
        "SELECT 1", {}, Exception("driver rejected never-log-this")
    )

    with pytest.raises(DatabaseConnectionError) as error:
        check_connection("warehouse", engine)

    assert str(error.value) == "Database connection check failed for warehouse"
    assert "never-log-this" not in str(error.value)


def test_transaction_scope_uses_engine_transaction() -> None:
    engine = MagicMock()
    connection = engine.begin.return_value.__enter__.return_value

    with transaction_scope(engine) as yielded:
        assert yielded is connection

    engine.begin.assert_called_once_with()


def test_dispose_engines_releases_each_pool() -> None:
    first = MagicMock()
    second = MagicMock()

    dispose_engines({"first": first, "second": second})

    first.dispose.assert_called_once_with()
    second.dispose.assert_called_once_with()
