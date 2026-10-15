# System Architecture

## Purpose

This framework prevents incomplete, duplicated, invalid, or financially inconsistent data from reaching business dashboards. It provides one repeatable processing path for database, CSV, and REST API sources and one central place for analysts to investigate failures.

## Architecture principles

1. **Source fidelity:** Raw source values and source identifiers remain available in staging for traceability.
2. **Deterministic processing:** The same source batch and rules must produce the same result.
3. **Idempotent reruns:** Reprocessing a `batch_id` must not create duplicate business records or duplicate quality results.
4. **Quality gates:** Invalid data is recorded and quarantined before promotion to core tables.
5. **Explicit reconciliation:** Row counts, distinct keys, rejected rows, and financial totals are balanced for every run.
6. **Auditable lineage:** Every staged, transformed, validation, and reconciliation record is tied to a `run_id`, `batch_id`, and source system.
7. **Least data exposure:** Logs and API responses must not expose raw email addresses, phone numbers, credentials, or API tokens.

## Logical flow

```text
PostgreSQL CRM ---------+
SQL Server Billing -----+
CSV Operations ---------+--> Airflow ingestion --> PostgreSQL staging
REST Customer Status ---+          |                       |
                                   |                       v
                                   |              Great Expectations
                                   |                 quality gate
                                   |                       |
                                   v                       v
                              control tables       dbt transformations
                                                           |
                                                           v
                                                   PostgreSQL core tables
                                                           |
                                    +----------------------+
                                    v
                         Python/SQL reconciliation
                                    |
                                    v
                    centralized quality and run reports
                                    |
                                    v
                             FastAPI read endpoints
```

## Component responsibilities

| Component | Responsibility |
|---|---|
| Apache Airflow | Schedules and coordinates extraction, staging, validation, dbt execution, reconciliation, and report publication. It owns workflow state, not business transformations. |
| Python | Implements source connectors, batch controls, file/API handling, reconciliation, and shared processing services. |
| SQLAlchemy | Provides managed PostgreSQL and SQL Server connections, transactions, and parameterized database access. |
| Pandas and NumPy | Parse source files and JSON payloads, normalize tabular values, calculate comparison metrics, and support deterministic reconciliation. |
| PostgreSQL | Stores staging data, transformed core data, control metadata, validation failures, and reconciliation results. |
| SQL Server | Acts as the billing transaction source for the sample enterprise workflow. |
| dbt Core | Applies versioned SQL transformations and tests staging-to-core models. |
| Great Expectations | Enforces structural and business-quality expectations before core promotion. |
| FastAPI | Exposes read-only processing status, validation failures, reconciliation differences, and recurring-issue summaries. |
| Docker | Provides reproducible local services and execution environments. |
| pytest | Verifies units, database integration, source contracts, and the end-to-end workflow. |

## PostgreSQL schema boundaries

| Schema | Contents | Write owner |
|---|---|---|
| `control` | Pipeline runs, batches, checkpoints, and processing status | Python/Airflow |
| `stg` | Immutable-per-batch representations of source records plus ingestion metadata | Python ingestion |
| `core` | Conformed customer, transaction, and operational models | dbt Core |
| `quality` | Validation failures, reconciliation results, and recurring-issue summaries | Great Expectations, dbt, and Python reconciliation |

Services must write only to their owned schema. FastAPI is read-only across `control` and `quality`.

## Run lifecycle

| State | Meaning | Allowed next states |
|---|---|---|
| `PENDING` | Run registered but not started | `RUNNING`, `CANCELLED` |
| `RUNNING` | At least one pipeline step is active | `FAILED`, `QUARANTINED`, `SUCCEEDED` |
| `QUARANTINED` | Blocking quality or reconciliation rule failed | `RUNNING` after corrected rerun, `FAILED` |
| `FAILED` | Technical failure prevented completion | `RUNNING` after rerun |
| `SUCCEEDED` | All mandatory steps and blocking gates passed | Terminal |
| `CANCELLED` | Run was intentionally stopped before processing | Terminal |

Each execution receives a unique `run_id`. Each independently replayable source delivery receives a stable `batch_id`. A rerun creates a new `run_id` but reuses the original `batch_id`.

## Failure and recovery behavior

- Source extraction fails closed: unavailable or malformed required sources stop their batch.
- Database writes use transactions; partial table writes are rolled back.
- Invalid business records are written to quality results with rule, key, observed value, severity, and run metadata.
- A batch is promoted only if all `ERROR`-severity validation rules and reconciliation tolerances pass.
- Airflow retries technical failures. Data-quality failures are not blindly retried.
- Reruns remove or replace artifacts for the same `batch_id` inside the affected processing layer before rebuilding them.

## Security and privacy boundaries

- Database passwords and API tokens are supplied at runtime and never committed to Git.
- SQL statements use bound parameters through SQLAlchemy.
- Raw PII is restricted to staging and approved core columns.
- Logs identify records by business key and rule identifier; email addresses and phone numbers are masked.
- FastAPI endpoints return summarized failures by default. Record-level details require explicit query filters and must omit credentials and secret values.
- Sample data must be synthetic and must not contain real customer information.

## Operational targets

These initial targets define acceptance criteria for the sample implementation:

| Measure | Target |
|---|---|
| Batch completeness | 100% of registered sources reach a terminal status |
| Duplicate business keys in core | 0 |
| Financial reconciliation | Exact currency-level equality at two decimal places |
| Run traceability | 100% of output rows carry source and batch lineage |
| Repeatability | Replaying a batch produces no duplicate core rows |
| API behavior | Health and summary endpoints respond without exposing sensitive fields |

## Deployment boundary

Docker will run the Python application services, Airflow, PostgreSQL, and SQL Server for the project environment. The repository will not introduce another database, workflow engine, web framework, or data-quality platform.
