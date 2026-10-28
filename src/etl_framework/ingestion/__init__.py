"""Read-only source extraction interfaces."""

from etl_framework.ingestion.database_sources import (
    ExtractionWindow,
    SourceExtractionError,
    extract_billing_transactions,
    extract_crm_customers,
)
from etl_framework.ingestion.csv_source import (
    CsvContractError,
    OperationalCsvDelivery,
    OperationalCsvIngestionResult,
    ingest_operational_csv,
    read_operational_csv,
)
from etl_framework.ingestion.staging_loaders import (
    StagingLoadError,
    StagingLoadResult,
    load_billing_transactions,
    load_crm_customers,
    load_operational_events,
)
from etl_framework.ingestion.customer_status_api import (
    CustomerStatusApiError,
    CustomerStatusApiSettings,
    create_customer_status_session,
    extract_customer_statuses,
    load_customer_statuses,
)

__all__ = [
    "ExtractionWindow",
    "SourceExtractionError",
    "extract_billing_transactions",
    "extract_crm_customers",
    "CsvContractError",
    "OperationalCsvDelivery",
    "OperationalCsvIngestionResult",
    "StagingLoadError",
    "StagingLoadResult",
    "ingest_operational_csv",
    "load_billing_transactions",
    "load_crm_customers",
    "load_operational_events",
    "read_operational_csv",
    "CustomerStatusApiError",
    "CustomerStatusApiSettings",
    "create_customer_status_session",
    "extract_customer_statuses",
    "load_customer_statuses",
]
