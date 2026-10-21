"""SQLAlchemy connection lifecycle without import-time side effects."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from contextlib import contextmanager

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import SQLAlchemyError

from etl_framework.config.settings import AppSettings, DatabaseSettings, PoolSettings


class DatabaseConnectionError(RuntimeError):
    """Raised when a configured database cannot pass its health query."""


def create_database_engine(settings: DatabaseSettings, pool: PoolSettings) -> Engine:
    """Create a bounded, pre-ping SQLAlchemy engine for one endpoint."""
    connect_args: dict[str, object]
    if settings.drivername.startswith("postgresql"):
        connect_args = {
            "connect_timeout": pool.connect_timeout_seconds,
            "application_name": "enterprise_etl_quality",
        }
    elif settings.drivername.startswith("mssql"):
        connect_args = {"timeout": pool.connect_timeout_seconds}
    else:
        raise ValueError(f"Unsupported database driver for {settings.name}")

    return create_engine(
        settings.url,
        pool_pre_ping=True,
        pool_size=pool.size,
        max_overflow=pool.max_overflow,
        pool_timeout=pool.timeout_seconds,
        pool_recycle=pool.recycle_seconds,
        connect_args=connect_args,
    )


def create_database_engines(settings: AppSettings) -> dict[str, Engine]:
    """Create all engines and dispose partial results if any creation fails."""
    engines: dict[str, Engine] = {}
    try:
        for database in (settings.crm, settings.warehouse, settings.billing):
            engines[database.name] = create_database_engine(database, settings.pool)
    except Exception:
        dispose_engines(engines)
        raise
    return engines


def check_connection(name: str, engine: Engine) -> None:
    """Run a side-effect-free health query without exposing credentials."""
    try:
        with engine.connect() as connection:
            result = connection.execute(text("SELECT 1")).scalar_one()
    except SQLAlchemyError as exc:
        raise DatabaseConnectionError(f"Database connection check failed for {name}") from exc
    if result != 1:
        raise DatabaseConnectionError(f"Database connection check returned an invalid result for {name}")


@contextmanager
def transaction_scope(engine: Engine) -> Iterator[Connection]:
    """Commit on success and roll back automatically on failure."""
    with engine.begin() as connection:
        yield connection


def dispose_engines(engines: Mapping[str, Engine]) -> None:
    """Release every pool owned by the caller."""
    for engine in engines.values():
        engine.dispose()
