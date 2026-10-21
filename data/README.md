# Synthetic Test Data

All records in this directory are fictional and safe for local development.

## Source datasets

| Source | Location | Rows | Purpose |
|---|---|---:|---|
| PostgreSQL CRM | `sql/postgresql/source/002_seed_customers.sql` | 7 | Valid customers, one resolved duplicate, invalid email, invalid country, and invalid timestamp ordering |
| SQL Server billing | `data/raw/sqlserver/002_seed_transactions.sql` | 9 | Valid financial records, one resolved duplicate, negative amount, missing customer, and invalid currency |
| Operations CSV | `data/raw/csv/operational_events_20261020.csv` | 7 | Valid events, one resolved duplicate, invalid event type, missing customer, and overlength location warning |
| Customer-status API | `data/raw/json/customer_status_20261020.json` | 4 | Newer and older status precedence plus an invalid status value |

The database scripts are idempotent by fixed source row identifier. The CSV and JSON files represent immutable deliveries and must not be edited after a batch is registered; create a new dated delivery instead.

## Expected datasets

- `customers.csv`: three accepted conformed customers.
- `transactions.csv`: five accepted transactions after validation and deduplication.
- `operational_events.csv`: four accepted events, including a warning-normalized record.
- `validation_failures.csv`: expected errors and warnings with rule identifiers and outcomes.
- `reconciliation.csv`: expected row, key, and currency-total comparisons.

The expected files intentionally exclude runtime lineage columns such as `run_id`, `batch_id`, and `loaded_at`, because those values are generated for each execution.

## Expected row accounting

| Source | Source rows | Accepted | Quarantined | Superseded duplicates |
|---|---:|---:|---:|---:|
| `CRM_PG` | 7 | 3 | 3 | 1 |
| `STATUS_API` | 4 | 3 | 1 | 0 |
| `BILLING_MSSQL` | 9 | 5 | 3 | 1 |
| `OPS_CSV` | 7 | 4 | 2 | 1 |

For every source, source rows equal accepted rows plus quarantined rows plus superseded duplicates.
