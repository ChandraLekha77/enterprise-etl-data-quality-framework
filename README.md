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
- Read-only quality reporting API on `127.0.0.1:8000`
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

Install dbt separately, copy its example profile, and build the warehouse models:

```powershell
.\.venv\Scripts\python.exe -m pip install -r dbt\requirements.txt
Copy-Item dbt\profiles.yml.example dbt\profiles.yml
dbt build --project-dir dbt --profiles-dir dbt
```

The profile reads the existing `WAREHOUSE_POSTGRES_*` variables. REST status
ingestion also requires `CUSTOMER_STATUS_API_URL` and
`CUSTOMER_STATUS_API_TOKEN`; bounded timeout and retry settings are documented
in `.env.example`. Credentials and pagination tokens must not be placed in the
endpoint URL.

After installing runtime dependencies and setting
`GX_WAREHOUSE_CONNECTION_STRING`, register the Great Expectations SQL assets,
validation definitions, and checkpoint with:

```powershell
.\.venv\Scripts\python.exe scripts\configure_great_expectations.py
```

The reporting API exposes OpenAPI documentation at
`http://127.0.0.1:8000/docs`. Run summaries, batch totals, validation failure
groups, reconciliation differences, exact currency totals, and recurring
issues are available below `/api/v1`. Requests are paginated with `page` and
`page_size` (maximum 100), and database work is forced into read-only
transactions.

The operational dashboard is available at `http://127.0.0.1:8000/dashboard`.
It includes run history, row-count cards, validation and reconciliation charts,
filters, pagination, and run drill-down views. The browser uses HTTP Basic
authentication with `REPORTING_DASHBOARD_USERNAME` and
`REPORTING_DASHBOARD_PASSWORD`. API clients use a Bearer token from
`REPORTING_ADMIN_TOKEN`, `REPORTING_OPERATOR_TOKEN`, or
`REPORTING_VIEWER_TOKEN`. These define administrator, operator, and read-only
roles. The reporting API remains read-only for every role.

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
- Day 11: secure, paginated customer-status API ingestion and staging load complete
- Day 12: dbt PostgreSQL profile, normalized staging models, safe conversions, and tests complete
- Day 13: customer normalization, deduplication, status precedence, and promotion complete
- Day 14: transaction and operational candidate models and fact promotion complete
- Day 15: Great Expectations rule mapping and safe idempotent validation storage complete
- Day 16: business-rule validation, record classification, and fixture comparison complete
- Day 17: atomic quality gating, quarantine accounting, and corrected replay support complete
- Day 18: row-count and key-set reconciliation with persisted differences complete
- Day 19: exact financial reconciliation and transactional release gating complete
- Day 20: four-source Airflow ingestion, dbt candidates, validation, promotion, reconciliation, and status publication complete
- Day 21: bounded technical retries, non-retryable quality failures, durable checkpoints, delivery leases, and replay recovery complete
- Day 22: verified run, batch, failure, reconciliation, financial, and recurring-issue reports complete
- Day 23: read-only FastAPI health and reporting endpoints with filters, pagination, and sanitized errors complete
- Day 24: Docker service health, live source connectivity, warehouse loading, dbt, and validation tooling complete
- Day 25: valid and mixed-quality acceptance fixtures and end-to-end execution workflow complete; fresh live rerun requires Docker
- Day 26: failure, replay, rollback, watermark, and duplicate-prevention coverage complete; live outage/performance rerun requires Docker
- Day 27: installation, operation, quarantine, replay, API, limits, and final verification documentation complete; host suite verified

## Final verification

The final acceptance matrix is intentionally split into a fast host suite and a
Docker-backed live suite. Run it from a clean checkout as follows:

```powershell
Copy-Item .env.example .env
# Replace every CHANGE_ME value before continuing.
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements-dev.txt
.\.venv\Scripts\python.exe -m pytest -q tests/unit
docker compose up -d
docker compose ps
```

After every required service is healthy, run the database integration tests,
`dbt build`, and the valid and mixed-quality Airflow deliveries using the
commands in the [local environment runbook](docs/runbooks/local_environment.md).
The successful demonstration must show a `SUCCEEDED` valid run, quarantined
mandatory-error records in the mixed delivery, persisted reconciliation
differences for a deliberately failing comparison, and a corrected replay that
does not duplicate core or quality results and advances watermarks only after
success.
