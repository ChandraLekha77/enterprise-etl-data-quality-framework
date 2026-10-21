# Transformation and Data-Quality Rules

## Rule execution order

Rules run in the following order so that downstream comparisons use canonical values:

1. Validate the source contract and required fields.
2. Preserve the original staged value and attach lineage metadata.
3. Normalize whitespace, case, timestamps, and fixed-precision numbers.
4. Resolve duplicates deterministically.
5. Apply cross-source precedence and reference checks.
6. Build core records.
7. Reconcile row counts, keys, and totals.

A rule result records `rule_id`, severity, source system, business key, affected field, masked observed value, message, `run_id`, and `batch_id`.

## Severity and gate behavior

| Severity | Meaning | Pipeline behavior |
|---|---|---|
| `ERROR` | Record or batch cannot be trusted | Quarantine the record; fail promotion when the rule is batch-level or reconciliation-level |
| `WARNING` | Data is usable but needs investigation | Promote when no error exists; include in the quality report |
| `INFO` | Expected operational observation | Record metric without blocking promotion |

## Common rules

| Rule ID | Rule | Severity | Result |
|---|---|---|---|
| `TR-COM-001` | Trim leading/trailing whitespace from text; convert empty results to null | — | Canonical text or null |
| `TR-COM-002` | Convert all valid timestamps to UTC | — | UTC timestamp |
| `TR-COM-003` | Round financial values to two decimal places using decimal arithmetic | — | Fixed-precision amount |
| `DQ-COM-001` | Required business key must not be null or blank | `ERROR` | Quarantine record |
| `DQ-COM-002` | Required source field must be parseable as its declared type | `ERROR` | Quarantine record |
| `DQ-COM-003` | Business key must be unique after deterministic deduplication | `ERROR` | Quarantine unresolved keys |
| `DQ-COM-004` | Each output record must include run, batch, and source lineage | `ERROR` | Fail promotion |

## Customer rules

| Rule ID | Rule | Severity/behavior |
|---|---|---|
| `TR-CUS-001` | Apply `TR-COM-001` to names; preserve internal whitespace | Transform |
| `TR-CUS-002` | Trim and lowercase email addresses | Transform |
| `TR-CUS-003` | Trim and uppercase `country_code` | Transform |
| `TR-CUS-004` | Trim and lowercase status | Transform |
| `TR-CUS-005` | For repeated `customer_id`, select greatest `updated_at`; if tied, select the deterministic greatest staged row identifier | Transform |
| `TR-CUS-006` | Use API status only when its `status_updated_at` is later than or equal to the CRM `updated_at`; otherwise retain CRM status | Transform |
| `DQ-CUS-001` | `customer_id`, names, email, country, status, and source timestamps are required | `ERROR` |
| `DQ-CUS-002` | Normalized email must contain one `@`, nonblank local/domain parts, and no whitespace | `ERROR` |
| `DQ-CUS-003` | Country code must contain exactly two ASCII letters | `ERROR` |
| `DQ-CUS-004` | Status must be `active`, `inactive`, or `suspended` | `ERROR` |
| `DQ-CUS-005` | `created_at` must be earlier than or equal to `updated_at` | `ERROR` |
| `DQ-CUS-006` | More than one source row for the same customer is reported even when deduplication resolves it | `WARNING` |

## Transaction rules

| Rule ID | Rule | Severity/behavior |
|---|---|---|
| `TR-TXN-001` | Trim identifiers and category fields | Transform |
| `TR-TXN-002` | Uppercase `currency_code` | Transform |
| `TR-TXN-003` | Lowercase `transaction_status` | Transform |
| `TR-TXN-004` | Convert amount to a fixed two-decimal value without binary floating-point arithmetic | Transform |
| `TR-TXN-005` | For repeated `transaction_id`, select greatest `updated_at`; apply deterministic staged-row tie-breaker | Transform |
| `DQ-TXN-001` | Transaction ID, customer ID, timestamp, amount, currency, status, and update timestamp are required | `ERROR` |
| `DQ-TXN-002` | Amount must be greater than or equal to zero | `ERROR` |
| `DQ-TXN-003` | Currency must contain exactly three ASCII letters | `ERROR` |
| `DQ-TXN-004` | Status must be `pending`, `completed`, `failed`, or `refunded` | `ERROR` |
| `DQ-TXN-005` | Customer ID must exist in the current conformed customer set | `ERROR` |
| `DQ-TXN-006` | More than one source row for a transaction is reported after resolution | `WARNING` |

## Operational-event rules

| Rule ID | Rule | Severity/behavior |
|---|---|---|
| `TR-OPS-001` | Trim identifiers, event type, location, and details | Transform |
| `TR-OPS-002` | Lowercase `event_type` | Transform |
| `TR-OPS-003` | For repeated `event_id`, select the greatest `event_timestamp`; apply deterministic staged-row tie-breaker | Transform |
| `DQ-OPS-001` | Event ID, customer ID, event type, and timestamp are required | `ERROR` |
| `DQ-OPS-002` | Event type must be `service`, `shipment`, `support`, or `maintenance` | `ERROR` |
| `DQ-OPS-003` | Customer ID must exist in the current conformed customer set | `ERROR` |
| `DQ-OPS-004` | Location code must not exceed 30 characters after trimming | `WARNING` and set location to null |
| `DQ-OPS-005` | Duplicate file checksum must not be processed as a new batch | `ERROR` at batch level |
| `DQ-OPS-006` | More than one source row for an event is reported after resolution | `WARNING` |

## Reconciliation rules

For each source and batch, the following equation must hold:

```text
source row count = accepted distinct rows + quarantined rows + deterministically superseded duplicate rows
```

| Rule ID | Comparison | Tolerance | Failure severity |
|---|---|---:|---|
| `RC-ROW-001` | Source row count to accounted target outcomes | `0` rows | `ERROR` |
| `RC-KEY-001` | Accepted distinct source business keys to core business keys for the batch | `0` keys | `ERROR` |
| `RC-FIN-001` | Completed transaction amount by currency: accepted source to core | `0.00` | `ERROR` |
| `RC-FIN-002` | Refunded transaction amount by currency: accepted source to core | `0.00` | `ERROR` |
| `RC-FLD-001` | Canonical source values to mapped core values for every accepted record | `0` mismatches | `ERROR` |
| `RC-STA-001` | Every registered batch has one terminal processing status | `0` missing statuses | `ERROR` |

Financial reconciliation is grouped by `currency_code`; values in different currencies are never added together.

## Recurring-issue classification

An issue is identified by `(source_system, rule_id, field_name)`.

- **New:** Seen in the current run and not in the previous successful five runs for that source.
- **Recurring:** Seen in at least two of the previous successful five runs.
- **Persistent:** Seen in each of the previous successful five runs.
- **Resolved:** Previously recurring or persistent but absent from the current successful run.

Failed technical runs do not change recurring-issue classification because their quality evidence may be incomplete.

## Promotion criteria

A batch may be promoted to core only when:

1. Its source contract passed.
2. All valid records were accounted for and all invalid records were quarantined.
3. No batch-level `ERROR` rule remains open.
4. All reconciliation rules are within tolerance.
5. The dbt models and mandatory tests completed successfully.

Warnings never disappear silently: they remain visible in the centralized quality report.
