# Source-to-Target Mapping

## Mapping standards

- Target system: PostgreSQL.
- Staging columns preserve the source representation; core columns contain validated canonical values.
- `TR-*`, `DQ-*`, and `RC-*` identifiers refer to [transformation and quality rules](../architecture/transformation_rules.md).
- A required source value that fails its `ERROR` rule is quarantined and is not loaded to core.
- Framework lineage columns are populated by the pipeline and never accepted from source payloads.

## CRM customer to staging

Source: PostgreSQL `crm.customers`  
Target: `stg.customer_raw`

| Source field | Target column | Mapping | Required |
|---|---|---|---:|
| Generated | `staged_row_id` | Deterministic row identity assigned during staging | Yes |
| `customer_id` | `customer_id` | Preserve source text | Yes |
| `first_name` | `first_name` | Preserve source text | Yes |
| `last_name` | `last_name` | Preserve source text | Yes |
| `email` | `email` | Preserve source text | Yes |
| `phone` | `phone` | Preserve source text/null | No |
| `country_code` | `country_code` | Preserve source text | Yes |
| `status` | `status` | Preserve source text | Yes |
| `created_at` | `created_at` | Preserve source timestamp representation | Yes |
| `updated_at` | `updated_at` | Preserve source timestamp representation | Yes |
| Pipeline context | `run_id` | Current execution ID | Yes |
| Delivery registration | `batch_id` | Current CRM delivery ID | Yes |
| Constant | `source_system` | `CRM_PG` | Yes |
| `customer_id` | `source_record_id` | Preserve original representation | Yes |
| `updated_at` | `source_updated_at` | Parse timestamp when valid | No |
| Ingestion clock | `ingested_at` | Current UTC timestamp | Yes |

## Customer staging and API status to core

Primary source: `stg.customer_raw`  
Status enrichment source: `stg.customer_status_raw`  
Target: `core.dim_customer`

| Source field | Target column | Transformation | Quality rules |
|---|---|---|---|
| `customer_raw.customer_id` | `customer_id` | Trim; deduplicate by greatest CRM `updated_at` | `TR-COM-001`, `TR-CUS-005`, `DQ-CUS-001`, `DQ-COM-003` |
| `customer_raw.first_name` | `first_name` | Trim outer whitespace | `TR-CUS-001`, `DQ-CUS-001` |
| `customer_raw.last_name` | `last_name` | Trim outer whitespace | `TR-CUS-001`, `DQ-CUS-001` |
| `customer_raw.email` | `email` | Trim and lowercase | `TR-CUS-002`, `DQ-CUS-001`, `DQ-CUS-002` |
| `customer_raw.phone` | `phone` | Trim; blank becomes null | `TR-COM-001` |
| `customer_raw.country_code` | `country_code` | Trim and uppercase | `TR-CUS-003`, `DQ-CUS-001`, `DQ-CUS-003` |
| CRM `status`; API `status` | `status` | Select by documented timestamp precedence; lowercase | `TR-CUS-004`, `TR-CUS-006`, `DQ-CUS-004` |
| CRM/API source selection | `status_source` | `STATUS_API` when API wins precedence; otherwise `CRM_PG` | `TR-CUS-006` |
| `customer_raw.created_at` | `source_created_at` | Parse and convert to UTC | `TR-COM-002`, `DQ-CUS-001`, `DQ-CUS-005` |
| Winning status/customer timestamp | `source_updated_at` | Convert selected timestamp to UTC | `TR-COM-002`, `DQ-CUS-001` |
| Winning staged record | `run_id` | Preserve execution lineage | `DQ-COM-004` |
| Winning staged record | `batch_id` | Preserve delivery lineage | `DQ-COM-004` |
| Transformation clock | `loaded_at` | Current UTC timestamp | `DQ-COM-004` |

### Customer-status API to staging

Source: REST JSON `data[]`  
Target: `stg.customer_status_raw`

| JSON field | Target column | Mapping | Required |
|---|---|---|---:|
| Generated | `staged_row_id` | Deterministic row identity | Yes |
| `customer_id` | `customer_id` | Preserve source text | Yes |
| `status` | `status` | Preserve source text | Yes |
| `status_updated_at` | `status_updated_at` | Preserve source timestamp representation | Yes |
| Response pagination context | `page_token` | Token used to obtain the response page; null for first page | No |
| Pipeline context | `run_id` | Current execution ID | Yes |
| Delivery registration | `batch_id` | Current API request-window delivery ID | Yes |
| Constant | `source_system` | `STATUS_API` | Yes |
| `customer_id` | `source_record_id` | Preserve original representation | Yes |
| `status_updated_at` | `source_updated_at` | Parse timestamp when valid | No |
| Ingestion clock | `ingested_at` | Current UTC timestamp | Yes |

## Billing transaction to staging

Source: SQL Server `dbo.transactions`  
Target: `stg.transaction_raw`

| Source field | Target column | Mapping | Required |
|---|---|---|---:|
| Generated | `staged_row_id` | Deterministic row identity | Yes |
| `transaction_id` | `transaction_id` | Preserve source text | Yes |
| `customer_id` | `customer_id` | Preserve source text | Yes |
| `transaction_timestamp` | `transaction_timestamp` | Preserve source timestamp representation | Yes |
| `amount` | `amount` | Preserve exact decimal source representation | Yes |
| `currency_code` | `currency_code` | Preserve source text | Yes |
| `transaction_status` | `transaction_status` | Preserve source text | Yes |
| `payment_method` | `payment_method` | Preserve source text/null | No |
| `updated_at` | `updated_at` | Preserve source timestamp representation | Yes |
| Pipeline context | `run_id` | Current execution ID | Yes |
| Delivery registration | `batch_id` | Current billing delivery ID | Yes |
| Constant | `source_system` | `BILLING_MSSQL` | Yes |
| `transaction_id` | `source_record_id` | Preserve original representation | Yes |
| `updated_at` | `source_updated_at` | Parse timestamp when valid | No |
| Ingestion clock | `ingested_at` | Current UTC timestamp | Yes |

## Transaction staging to core

Source: `stg.transaction_raw`  
Target: `core.fact_transaction`

| Source field | Target column | Transformation | Quality rules |
|---|---|---|---|
| `transaction_id` | `transaction_id` | Trim; deduplicate by greatest `updated_at` | `TR-TXN-001`, `TR-TXN-005`, `DQ-TXN-001`, `DQ-COM-003` |
| `customer_id` | `customer_id` | Trim and resolve against conformed customer | `TR-TXN-001`, `DQ-TXN-001`, `DQ-TXN-005` |
| `transaction_timestamp` | `transaction_timestamp` | Parse and convert to UTC | `TR-COM-002`, `DQ-TXN-001` |
| `amount` | `amount` | Parse fixed precision and round to two decimals | `TR-COM-003`, `TR-TXN-004`, `DQ-TXN-001`, `DQ-TXN-002` |
| `currency_code` | `currency_code` | Trim and uppercase | `TR-TXN-001`, `TR-TXN-002`, `DQ-TXN-003` |
| `transaction_status` | `transaction_status` | Trim and lowercase | `TR-TXN-001`, `TR-TXN-003`, `DQ-TXN-004` |
| `payment_method` | `payment_method` | Trim; blank becomes null | `TR-TXN-001` |
| `updated_at` | `source_updated_at` | Parse and convert to UTC | `TR-COM-002`, `DQ-TXN-001` |
| Winning staged record | `run_id` | Preserve execution lineage | `DQ-COM-004` |
| Winning staged record | `batch_id` | Preserve delivery lineage | `DQ-COM-004` |
| Transformation clock | `loaded_at` | Current UTC timestamp | `DQ-COM-004` |

## Operational CSV to staging

Source: `operational_events_YYYYMMDD.csv`  
Target: `stg.operational_event_raw`

| CSV column/context | Target column | Mapping | Required |
|---|---|---|---:|
| Generated | `staged_row_id` | Deterministic row identity | Yes |
| `event_id` | `event_id` | Preserve source text | Yes |
| `customer_id` | `customer_id` | Preserve source text | Yes |
| `event_type` | `event_type` | Preserve source text | Yes |
| `event_timestamp` | `event_timestamp` | Preserve source text | Yes |
| `location_code` | `location_code` | Preserve source text/null | No |
| `details` | `details` | Preserve source text/null | No |
| File context | `source_file_name` | Exact base file name | Yes |
| File context | `source_file_checksum` | Stable content checksum | Yes |
| Pipeline context | `run_id` | Current execution ID | Yes |
| Delivery registration | `batch_id` | Current file delivery ID | Yes |
| Constant | `source_system` | `OPS_CSV` | Yes |
| `event_id` | `source_record_id` | Preserve original representation | Yes |
| `event_timestamp` | `source_updated_at` | Parsed event timestamp when valid | No |
| Ingestion clock | `ingested_at` | Current UTC timestamp | Yes |

## Operational staging to core

Source: `stg.operational_event_raw`  
Target: `core.fact_operational_event`

| Source field | Target column | Transformation | Quality rules |
|---|---|---|---|
| `event_id` | `event_id` | Trim; deduplicate by greatest event timestamp | `TR-OPS-001`, `TR-OPS-003`, `DQ-OPS-001`, `DQ-COM-003` |
| `customer_id` | `customer_id` | Trim and resolve against conformed customer | `TR-OPS-001`, `DQ-OPS-001`, `DQ-OPS-003` |
| `event_type` | `event_type` | Trim and lowercase | `TR-OPS-001`, `TR-OPS-002`, `DQ-OPS-002` |
| `event_timestamp` | `event_timestamp` | Parse and convert to UTC | `TR-COM-002`, `DQ-OPS-001` |
| `location_code` | `location_code` | Trim; blank or overlength value becomes null | `TR-OPS-001`, `DQ-OPS-004` |
| `details` | `details` | Trim; blank becomes null | `TR-OPS-001` |
| Winning staged record | `run_id` | Preserve execution lineage | `DQ-COM-004` |
| Winning staged record | `batch_id` | Preserve delivery lineage | `DQ-COM-004` |
| Transformation clock | `loaded_at` | Current UTC timestamp | `DQ-COM-004` |

## Quality-result mappings

### Validation failures

Target: `quality.validation_result`

| Producer value | Target column | Mapping |
|---|---|---|
| Generated | `validation_result_id` | Unique result ID |
| Processing context | `run_id`, `batch_id`, `source_system` | Copy lineage from evaluated record/batch |
| Rule definition | `rule_id`, `severity` | Copy registered rule metadata |
| Evaluated record | `business_key` | Use source business key; null for batch-only failures |
| Rule definition | `field_name` | Affected field; null for record-wide/batch rules |
| Evaluated value | `observed_value_masked` | Mask email/phone and omit secret values before persistence |
| Rule outcome | `message` | Deterministic human-readable failure reason |
| Validation clock | `created_at` | Current UTC timestamp |

### Reconciliation results

Target: `quality.reconciliation_result`

| Reconciliation value | Target column | Mapping |
|---|---|---|
| Generated | `reconciliation_result_id` | Unique result ID |
| Processing context | `run_id`, `batch_id` | Current comparison lineage |
| Rule definition | `rule_id`, `metric_name` | Registered comparison identity |
| Comparison group | `group_key` | Currency code or null for ungrouped metrics |
| Source calculation | `source_value` | Exact source-side count or total |
| Target calculation | `target_value` | Exact target-side count or total |
| Calculated | `difference` | `target_value - source_value` |
| Rule definition | `tolerance` | Allowed absolute difference |
| Calculated | `status` | `PASS` when `abs(difference) <= tolerance`; otherwise `FAIL` |
| Reconciliation clock | `created_at` | Current UTC timestamp |

## Batch accounting examples

### Row-count accounting

For `BILLING_MSSQL`, the source count is the number of rows extracted inside the registered watermark window. Accepted rows, quarantined rows, and superseded duplicates must sum to that exact count under `RC-ROW-001`.

### Financial accounting

For each currency and eligible transaction status, sum the accepted, canonical source amount and compare it with the corresponding core amount. The calculation must use fixed-precision values and zero tolerance under `RC-FIN-001` and `RC-FIN-002`.

### Key accounting

The set of accepted distinct source keys for a batch must equal the set of batch-attributed core keys. Missing and unexpected keys are stored as validation or reconciliation details and block promotion.

## Change-control checklist

Any source or target field change requires all applicable updates in one commit:

1. Source contract.
2. This mapping document.
3. Data dictionary.
4. Transformation or quality rule registration.
5. Great Expectations suite and dbt tests.
6. Sample fixtures and expected data.
7. Unit, integration, and end-to-end tests.
