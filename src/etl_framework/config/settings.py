"""Validated settings for the framework database connections."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass, field

from sqlalchemy.engine import URL


class ConfigurationError(ValueError):
    """Raised when required runtime configuration is absent or unsafe."""


def _required(environment: Mapping[str, str], name: str, *, secret: bool = False) -> str:
    value = environment.get(name, "").strip()
    if not value:
        raise ConfigurationError(f"Required environment variable {name} is missing")
    if secret and value.upper().startswith("CHANGE_ME"):
        raise ConfigurationError(f"Required environment variable {name} still contains a placeholder")
    return value


def _integer(
    environment: Mapping[str, str],
    name: str,
    default: int,
    *,
    minimum: int,
    maximum: int,
) -> int:
    raw_value = environment.get(name, str(default)).strip()
    try:
        value = int(raw_value)
    except ValueError as exc:
        raise ConfigurationError(f"Environment variable {name} must be an integer") from exc
    if not minimum <= value <= maximum:
        raise ConfigurationError(
            f"Environment variable {name} must be between {minimum} and {maximum}"
        )
    return value


def _boolean(environment: Mapping[str, str], name: str, default: bool) -> bool:
    raw_value = environment.get(name, str(default)).strip().lower()
    if raw_value in {"1", "true", "yes"}:
        return True
    if raw_value in {"0", "false", "no"}:
        return False
    raise ConfigurationError(f"Environment variable {name} must be true or false")


@dataclass(frozen=True, slots=True)
class DatabaseSettings:
    """A validated SQLAlchemy database endpoint."""

    name: str
    drivername: str
    username: str
    password: str = field(repr=False)
    host: str
    port: int
    database: str
    query: Mapping[str, str] = field(default_factory=dict)

    @property
    def url(self) -> URL:
        return URL.create(
            drivername=self.drivername,
            username=self.username,
            password=self.password,
            host=self.host,
            port=self.port,
            database=self.database,
            query=dict(self.query),
        )

    @property
    def safe_url(self) -> str:
        return self.url.render_as_string(hide_password=True)


@dataclass(frozen=True, slots=True)
class PoolSettings:
    """Bounded SQLAlchemy connection-pool settings."""

    size: int = 5
    max_overflow: int = 10
    timeout_seconds: int = 30
    recycle_seconds: int = 1800
    connect_timeout_seconds: int = 10

    @classmethod
    def from_env(cls, environment: Mapping[str, str]) -> PoolSettings:
        return cls(
            size=_integer(environment, "DB_POOL_SIZE", 5, minimum=1, maximum=50),
            max_overflow=_integer(
                environment, "DB_MAX_OVERFLOW", 10, minimum=0, maximum=100
            ),
            timeout_seconds=_integer(
                environment, "DB_POOL_TIMEOUT_SECONDS", 30, minimum=1, maximum=300
            ),
            recycle_seconds=_integer(
                environment, "DB_POOL_RECYCLE_SECONDS", 1800, minimum=60, maximum=86400
            ),
            connect_timeout_seconds=_integer(
                environment, "DB_CONNECT_TIMEOUT_SECONDS", 10, minimum=1, maximum=120
            ),
        )


@dataclass(frozen=True, slots=True)
class AppSettings:
    """All database settings required by the ingestion and reporting layers."""

    crm: DatabaseSettings
    warehouse: DatabaseSettings
    billing: DatabaseSettings
    pool: PoolSettings

    @classmethod
    def from_env(cls, environment: Mapping[str, str] | None = None) -> AppSettings:
        env = os.environ if environment is None else environment
        crm = DatabaseSettings(
            name="crm",
            drivername="postgresql+psycopg",
            username=_required(env, "CRM_POSTGRES_USER"),
            password=_required(env, "CRM_POSTGRES_PASSWORD", secret=True),
            host=_required(env, "CRM_POSTGRES_HOST"),
            port=_integer(env, "CRM_POSTGRES_PORT", 5432, minimum=1, maximum=65535),
            database=_required(env, "CRM_POSTGRES_DB"),
            query={"sslmode": env.get("CRM_POSTGRES_SSL_MODE", "require")},
        )
        warehouse = DatabaseSettings(
            name="warehouse",
            drivername="postgresql+psycopg",
            username=_required(env, "WAREHOUSE_POSTGRES_USER"),
            password=_required(env, "WAREHOUSE_POSTGRES_PASSWORD", secret=True),
            host=_required(env, "WAREHOUSE_POSTGRES_HOST"),
            port=_integer(env, "WAREHOUSE_POSTGRES_PORT", 5432, minimum=1, maximum=65535),
            database=_required(env, "WAREHOUSE_POSTGRES_DB"),
            query={"sslmode": env.get("WAREHOUSE_POSTGRES_SSL_MODE", "require")},
        )
        billing = DatabaseSettings(
            name="billing",
            drivername="mssql+pyodbc",
            username=_required(env, "MSSQL_APP_USER"),
            password=_required(env, "MSSQL_APP_PASSWORD", secret=True),
            host=_required(env, "MSSQL_HOST"),
            port=_integer(env, "MSSQL_PORT", 1433, minimum=1, maximum=65535),
            database=_required(env, "MSSQL_DATABASE"),
            query={
                "driver": env.get("MSSQL_ODBC_DRIVER", "ODBC Driver 18 for SQL Server"),
                "Encrypt": "yes" if _boolean(env, "MSSQL_ENCRYPT", True) else "no",
                "TrustServerCertificate": (
                    "yes" if _boolean(env, "MSSQL_TRUST_SERVER_CERTIFICATE", False) else "no"
                ),
            },
        )
        return cls(crm=crm, warehouse=warehouse, billing=billing, pool=PoolSettings.from_env(env))
