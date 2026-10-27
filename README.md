# Enterprise ETL Data Quality and Source-to-Target Reconciliation Framework

A reusable validation and reconciliation platform for customer, transaction, and operational data received from PostgreSQL, SQL Server, CSV files, and REST APIs.

The platform stages source records, applies documented transformations, validates data quality, reconciles source and target results, and publishes centralized quality results before data is released to downstream dashboards.

## Approved technology stack

- Python, Pandas, and NumPy
- SQL, PostgreSQL, and SQL Server
- SQLAlchemy
- Apache Airflow
- dbt Core
- Great Expectations
- FastAPI
- REST APIs, JSON, and CSV
- pytest
- Git and Docker

No additional application frameworks, orchestration engines, databases, or validation platforms are planned.

## Documentation

- [System architecture](docs/architecture/system_architecture.md)
- [Source-system contracts](docs/architecture/source_systems.md)
- [Transformation and quality rules](docs/architecture/transformation_rules.md)
- [Data dictionary](docs/data_dictionary/data_dictionary.md)
- [Source-to-target mappings](docs/source_to_target_mappings/source_to_target_mapping.md)
- [Local Docker environment runbook](docs/runbooks/local_environment.md)

## Local infrastructure

The Docker environment provides isolated services for:

- PostgreSQL CRM source on `127.0.0.1:5433`
- PostgreSQL warehouse on `127.0.0.1:5432`
- SQL Server billing source on `127.0.0.1:1433`
- Airflow API and UI on `127.0.0.1:8080`
- Internal PostgreSQL storage for Airflow metadata

Copy `.env.example` to `.env`, replace every `CHANGE_ME` value, and follow the [local environment runbook](docs/runbooks/local_environment.md). The Compose configuration fails early when a required secret or account value is missing.

## Python development

Create a Python 3.12 virtual environment and install the pinned development dependencies:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements-dev.txt
.\.venv\Scripts\python.exe -m pytest
```

Runtime settings are read from environment variables only. `AppSettings.from_env()` validates required database endpoints, rejects placeholder secrets, builds encoded SQLAlchemy URLs, and keeps passwords out of object representations and connection errors.

## Delivery status

- Day 1: repository structure complete
- Day 2: architecture, contracts, rules, dictionary, and mappings complete
- Day 3: Docker services for PostgreSQL, SQL Server, and Airflow complete
- Day 4: control, staging, audit, quality, reconciliation, and status schemas complete
- Day 5: deterministic synthetic source fixtures and expected reconciliation outputs complete
- Day 6: validated SQLAlchemy configuration, pooled connections, health checks, and tests complete
- Day 7: incremental PostgreSQL and SQL Server source extractors complete
- Day 8: transactional run, stable batch, processing-step, lifecycle, and masked audit tracking complete
- Day 9: replay-safe CRM and billing database-to-staging loading complete
- Day 10: validated operational CSV registration and staging ingestion complete
