"""Lazy, warehouse-only dependencies for the reporting API."""

from __future__ import annotations

import os
from collections.abc import Mapping

from fastapi import Request

from etl_framework.config.settings import DatabaseSettings, PoolSettings
from etl_framework.database.connections import create_database_engine
from etl_framework.reporting import ReportingRepository


def create_reporting_repository(
    environment: Mapping[str, str] | None = None,
) -> ReportingRepository:
    """Create a bounded warehouse repository without loading source credentials."""
    env = os.environ if environment is None else environment
    warehouse = DatabaseSettings(
        name="warehouse_reporting",
        drivername="postgresql+psycopg",
        username=_required(env, "WAREHOUSE_POSTGRES_USER"),
        password=_required(env, "WAREHOUSE_POSTGRES_PASSWORD"),
        host=_required(env, "WAREHOUSE_POSTGRES_HOST"),
        port=_integer(env, "WAREHOUSE_POSTGRES_PORT", 5432, 1, 65535),
        database=_required(env, "WAREHOUSE_POSTGRES_DB"),
        query={"sslmode": env.get("WAREHOUSE_POSTGRES_SSL_MODE", "require")},
    )
    pool = PoolSettings.from_env(env)
    return ReportingRepository(create_database_engine(warehouse, pool))


def get_repository(request: Request) -> ReportingRepository:
    repository = request.app.state.reporting_repository
    if repository is None:
        repository = create_reporting_repository()
        request.app.state.reporting_repository = repository
        request.app.state.owns_reporting_repository = True
    return repository


def _required(environment: Mapping[str, str], name: str) -> str:
    value = environment.get(name, "").strip()
    if not value:
        raise ValueError(f"Required reporting environment variable {name} is missing")
    if "PASSWORD" in name and value.upper().startswith("CHANGE_ME"):
        raise ValueError(f"Required reporting environment variable {name} is not configured")
    return value


def _integer(
    environment: Mapping[str, str], name: str, default: int, minimum: int, maximum: int
) -> int:
    try:
        value = int(environment.get(name, str(default)))
    except ValueError as exc:
        raise ValueError(f"Reporting environment variable {name} must be numeric") from exc
    if not minimum <= value <= maximum:
        raise ValueError(f"Reporting environment variable {name} is outside its allowed range")
    return value
