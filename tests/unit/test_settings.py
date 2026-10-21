from __future__ import annotations

import pytest

from etl_framework.config.settings import AppSettings, ConfigurationError


def valid_environment() -> dict[str, str]:
    return {
        "CRM_POSTGRES_USER": "crm_user",
        "CRM_POSTGRES_PASSWORD": "crm:p@ss/word",
        "CRM_POSTGRES_HOST": "crm-postgres",
        "CRM_POSTGRES_PORT": "5432",
        "CRM_POSTGRES_DB": "crm_source",
        "CRM_POSTGRES_SSL_MODE": "disable",
        "WAREHOUSE_POSTGRES_USER": "warehouse_user",
        "WAREHOUSE_POSTGRES_PASSWORD": "warehouse:p@ss/word",
        "WAREHOUSE_POSTGRES_HOST": "warehouse-postgres",
        "WAREHOUSE_POSTGRES_PORT": "5432",
        "WAREHOUSE_POSTGRES_DB": "quality_framework",
        "WAREHOUSE_POSTGRES_SSL_MODE": "disable",
        "MSSQL_APP_USER": "billing_reader",
        "MSSQL_APP_PASSWORD": "billing:p@ss/word",
        "MSSQL_HOST": "billing-sqlserver",
        "MSSQL_PORT": "1433",
        "MSSQL_DATABASE": "billing_source",
        "MSSQL_ODBC_DRIVER": "ODBC Driver 18 for SQL Server",
        "MSSQL_ENCRYPT": "true",
        "MSSQL_TRUST_SERVER_CERTIFICATE": "true",
    }


def test_settings_build_encoded_urls_without_exposing_passwords() -> None:
    settings = AppSettings.from_env(valid_environment())

    assert settings.crm.url.password == "crm:p@ss/word"
    assert settings.crm.safe_url.startswith("postgresql+psycopg://crm_user:***@")
    assert "crm:p@ss/word" not in settings.crm.safe_url
    assert settings.billing.url.query["driver"] == "ODBC Driver 18 for SQL Server"
    assert settings.billing.url.query["Encrypt"] == "yes"
    assert settings.billing.url.query["TrustServerCertificate"] == "yes"
    assert "billing:p@ss/word" not in repr(settings.billing)


def test_missing_required_value_names_variable_without_secret() -> None:
    environment = valid_environment()
    del environment["WAREHOUSE_POSTGRES_PASSWORD"]

    with pytest.raises(ConfigurationError, match="WAREHOUSE_POSTGRES_PASSWORD") as error:
        AppSettings.from_env(environment)

    assert "warehouse:p@ss/word" not in str(error.value)


def test_placeholder_secret_is_rejected() -> None:
    environment = valid_environment()
    environment["MSSQL_APP_PASSWORD"] = "CHANGE_ME_NOW"

    with pytest.raises(ConfigurationError, match="placeholder"):
        AppSettings.from_env(environment)


@pytest.mark.parametrize("port", ["0", "65536", "not-a-port"])
def test_invalid_port_is_rejected(port: str) -> None:
    environment = valid_environment()
    environment["CRM_POSTGRES_PORT"] = port

    with pytest.raises(ConfigurationError, match="CRM_POSTGRES_PORT"):
        AppSettings.from_env(environment)


def test_invalid_boolean_is_rejected() -> None:
    environment = valid_environment()
    environment["MSSQL_ENCRYPT"] = "sometimes"

    with pytest.raises(ConfigurationError, match="MSSQL_ENCRYPT"):
        AppSettings.from_env(environment)
