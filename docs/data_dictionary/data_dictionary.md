# Data Dictionary

## Conventions

- Target database: PostgreSQL.
- Timestamps use `TIMESTAMPTZ` and are stored in UTC.
- Monetary values use `NUMERIC(18,2)` and are never stored as floating point.
- Framework identifiers use `UUID`.
- Business keys retain their source text representation after trimming.
- `run_id` identifies an execution; `batch_id` identifies a replayable source delivery.
- Day 4 control, staging, and quality definitions are maintained in `sql/postgresql/ddl`. Core tables remain logical contracts until their dbt-model delivery and must use these documented columns.

## `control.pipeline_run`

One row per framework execution.

| Column | Type | Nullable | Description |
|---|---|---:|---|
| `run_id` | `UUID` | No | Primary key generated when the run is registered |
| `airflow_run_id` | `TEXT` | No | Airflow execution identifier |
| `status` | `VARCHAR(20)` | No | `PENDING`, `RUNNING`, `QUARANTINED`, `FAILED`, `SUCCEEDED`, or `CANCELLED` |
| `started_at` | `TIMESTAMPTZ` | Yes | Execution start time |
| `finished_at` | `TIMESTAMPTZ` | Yes | Terminal-state time |
| `error_message` | `TEXT` | Yes | Sanitized run-level technical error |
| `created_at` | `TIMESTAMPTZ` | No | Registration time |

## `control.source_batch`

One stable row per independently replayable source delivery. Execution-specific state is recorded in `control.batch_run`, allowing the same `batch_id` to be reused safely on reruns.

| Column | Type | Nullable | Description |
|---|---|---:|---|
| `batch_id` | `UUID` | No | Stable primary key for replay |
| `source_system` | `VARCHAR(30)` | No | Registered source ID |
| `delivery_key` | `TEXT` | No | Watermark, file identity, or API window identifying delivery |
| `first_seen_at` | `TIMESTAMPTZ` | No | First registration time for the delivery |

Unique contract: `(source_system, delivery_key)` identifies one source delivery.

## `control.batch_run`

One row per attempt to process a source delivery in a framework run.

| Column | Type | Nullable | Description |
|---|---|---:|---|
| `batch_run_id` | `UUID` | No | Primary key for the processing attempt |
| `batch_id` | `UUID` | No | Stable source-delivery reference |
| `run_id` | `UUID` | No | Execution reference |
| `status` | `VARCHAR(20)` | No | Same lifecycle vocabulary as the run where applicable |
| `source_row_count` | `BIGINT` | Yes | Count observed at extraction |
| `accepted_row_count` | `BIGINT` | Yes | Distinct valid records eligible for promotion |
| `quarantined_row_count` | `BIGINT` | Yes | Invalid records held from promotion |
| `duplicate_row_count` | `BIGINT` | Yes | Superseded duplicate records |
| `started_at` | `TIMESTAMPTZ` | Yes | Batch processing start |
| `finished_at` | `TIMESTAMPTZ` | Yes | Batch terminal-state time |
| `error_message` | `TEXT` | Yes | Sanitized attempt-level technical error |
| `created_at` | `TIMESTAMPTZ` | No | Attempt registration time |
| `updated_at` | `TIMESTAMPTZ` | No | Most recent state change time |

Unique contract: `(batch_id, run_id)` identifies one delivery attempt within one execution.

## Staging tables

Each staging table preserves source fields plus the common ingestion metadata defined in the source contracts.

### `stg.customer_raw`

| Column | Type | Nullable | Description |
|---|---|---:|---|
| `staged_row_id` | `BIGINT` | No | Database-generated staging row identity |
| `customer_id` | `TEXT` | Yes | Raw CRM customer key |
| `first_name` | `TEXT` | Yes | Raw first name |
| `last_name` | `TEXT` | Yes | Raw last name |
| `email` | `TEXT` | Yes | Raw email |
| `phone` | `TEXT` | Yes | Raw phone |
| `country_code` | `TEXT` | Yes | Raw country value |
| `status` | `TEXT` | Yes | Raw CRM status |
| `created_at_raw` | `TEXT` | Yes | Raw source timestamp representation |
| `updated_at_raw` | `TEXT` | Yes | Raw source timestamp representation |
| `run_id` | `UUID` | No | Execution lineage |
| `batch_id` | `UUID` | No | Delivery lineage |
| `source_system` | `VARCHAR(30)` | No | Always `CRM_PG` |
| `source_record_id` | `TEXT` | Yes | Unmodified source key representation |
| `source_updated_at` | `TIMESTAMPTZ` | Yes | Parsed source watermark when valid |
| `record_checksum` | `CHAR(64)` | No | SHA-256 digest used for idempotent batch loading |
| `ingested_at` | `TIMESTAMPTZ` | No | Staging insert time |

### `stg.transaction_raw`

Contains `staged_row_id`, raw transaction contract fields, and all common ingestion metadata. Amount and timestamp source representations are retained until validation.

### `stg.operational_event_raw`

Contains `staged_row_id`, raw operational-event contract fields, `source_file_name`, `source_file_checksum`, and all common ingestion metadata.

### `stg.customer_status_raw`

Contains `staged_row_id`, `customer_id`, `status`, `status_updated_at_raw`, `page_token`, the original JSON in `raw_payload`, and all common ingestion metadata.

## `core.dim_customer`

Current conformed customer record.

| Column | Type | Nullable | Description |
|---|---|---:|---|
| `customer_id` | `VARCHAR(50)` | No | Primary business key |
| `first_name` | `VARCHAR(100)` | No | Trimmed name |
| `last_name` | `VARCHAR(100)` | No | Trimmed name |
| `email` | `VARCHAR(320)` | No | Lowercase normalized email |
| `phone` | `VARCHAR(30)` | Yes | Trimmed phone |
| `country_code` | `CHAR(2)` | No | Uppercase country code |
| `status` | `VARCHAR(20)` | No | Conformed customer status |
| `status_source` | `VARCHAR(30)` | No | `CRM_PG` or `STATUS_API` |
| `source_created_at` | `TIMESTAMPTZ` | No | CRM creation timestamp |
| `source_updated_at` | `TIMESTAMPTZ` | No | Timestamp of selected customer version |
| `run_id` | `UUID` | No | Run that last produced this version |
| `batch_id` | `UUID` | No | Source batch lineage |
| `loaded_at` | `TIMESTAMPTZ` | No | Core load time |

## `core.fact_transaction`

| Column | Type | Nullable | Description |
|---|---|---:|---|
| `transaction_id` | `VARCHAR(50)` | No | Primary business key |
| `customer_id` | `VARCHAR(50)` | No | Reference to `core.dim_customer` |
| `transaction_timestamp` | `TIMESTAMPTZ` | No | UTC transaction time |
| `amount` | `NUMERIC(18,2)` | No | Nonnegative transaction amount |
| `currency_code` | `CHAR(3)` | No | Uppercase currency code |
| `transaction_status` | `VARCHAR(20)` | No | Normalized status |
| `payment_method` | `VARCHAR(30)` | Yes | Trimmed source category |
| `source_updated_at` | `TIMESTAMPTZ` | No | Selected source version time |
| `run_id` | `UUID` | No | Run lineage |
| `batch_id` | `UUID` | No | Batch lineage |
| `loaded_at` | `TIMESTAMPTZ` | No | Core load time |

## `core.fact_operational_event`

| Column | Type | Nullable | Description |
|---|---|---:|---|
| `event_id` | `VARCHAR(50)` | No | Primary business key |
| `customer_id` | `VARCHAR(50)` | No | Reference to `core.dim_customer` |
| `event_type` | `VARCHAR(20)` | No | Normalized event type |
| `event_timestamp` | `TIMESTAMPTZ` | No | UTC event time |
| `location_code` | `VARCHAR(30)` | Yes | Normalized location |
| `details` | `TEXT` | Yes | Trimmed operational details |
| `run_id` | `UUID` | No | Run lineage |
| `batch_id` | `UUID` | No | Batch lineage |
| `loaded_at` | `TIMESTAMPTZ` | No | Core load time |

## `quality.validation_result`

One row per evaluated rule outcome that must be reported. Successful record-level checks may be retained as aggregate metrics instead of individual rows.

| Column | Type | Nullable | Description |
|---|---|---:|---|
| `validation_result_id` | `UUID` | No | Primary key |
| `run_id` | `UUID` | No | Run lineage |
| `batch_id` | `UUID` | No | Batch lineage |
| `source_system` | `VARCHAR(30)` | No | Registered source ID |
| `rule_id` | `VARCHAR(30)` | No | Documented rule identifier |
| `severity` | `VARCHAR(10)` | No | `ERROR`, `WARNING`, or `INFO` |
| `business_key` | `TEXT` | Yes | Affected source business key |
| `field_name` | `TEXT` | Yes | Affected field or null for batch rules |
| `observed_value_masked` | `TEXT` | Yes | Safe representation with PII removed |
| `message` | `TEXT` | No | Human-readable failure description |
| `created_at` | `TIMESTAMPTZ` | No | Detection time |

## `quality.reconciliation_result`

| Column | Type | Nullable | Description |
|---|---|---:|---|
| `reconciliation_result_id` | `UUID` | No | Primary key |
| `run_id` | `UUID` | No | Run lineage |
| `batch_id` | `UUID` | No | Batch lineage |
| `rule_id` | `VARCHAR(30)` | No | Reconciliation rule identifier |
| `metric_name` | `TEXT` | No | Compared count, key set, field, or total |
| `group_key` | `TEXT` | Yes | Currency or other documented grouping |
| `source_value` | `NUMERIC(28,4)` | No | Source-side metric |
| `target_value` | `NUMERIC(28,4)` | No | Target-side metric |
| `difference` | `NUMERIC(28,4)` | No | `target_value - source_value` |
| `tolerance` | `NUMERIC(28,4)` | No | Allowed absolute difference |
| `status` | `VARCHAR(10)` | No | `PASS` or `FAIL` |
| `created_at` | `TIMESTAMPTZ` | No | Comparison time |

## `quality.issue_summary`

| Column | Type | Nullable | Description |
|---|---|---:|---|
| `source_system` | `VARCHAR(30)` | No | Registered source ID |
| `rule_id` | `VARCHAR(30)` | No | Validation rule |
| `field_name` | `TEXT` | No | Empty string for batch-level rules |
| `classification` | `VARCHAR(20)` | No | `NEW`, `RECURRING`, `PERSISTENT`, or `RESOLVED` |
| `first_seen_at` | `TIMESTAMPTZ` | No | Earliest occurrence |
| `last_seen_at` | `TIMESTAMPTZ` | No | Most recent occurrence |
| `affected_run_count` | `INTEGER` | No | Number of successful runs with the issue |
| `current_failure_count` | `BIGINT` | No | Affected records in the latest evaluated run |
| `updated_at` | `TIMESTAMPTZ` | No | Summary refresh time |

Logical key: `(source_system, rule_id, field_name)`.
