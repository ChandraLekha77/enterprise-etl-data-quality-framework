# Source-System Contracts

## Contract policy

A source contract defines the minimum structure the framework accepts. A missing required field, incompatible type, unreadable payload, or duplicate delivery identifier is a contract failure. Contract failures are recorded against the run and stop the affected source batch before transformation.

All timestamps are interpreted as UTC unless an explicit source offset is present. All sample values must be synthetic.

## Source inventory

| Source ID | Business role | Interface | Authoritative data | Delivery key |
|---|---|---|---|---|
| `CRM_PG` | Customer master | PostgreSQL table `crm.customers` | Customer identity and base profile | Maximum `updated_at` included in extraction |
| `BILLING_MSSQL` | Financial transactions | SQL Server table `dbo.transactions` | Transaction amount, currency, and settlement status | Maximum `updated_at` included in extraction |
| `OPS_CSV` | Operational events | CSV file | Customer service and operational activity | File name plus file content checksum |
| `STATUS_API` | Current customer status | REST endpoint returning JSON | Latest status and status-effective timestamp | Response retrieval timestamp plus request window |

## `CRM_PG` customer contract

Expected object: `crm.customers`

| Field | Source type | Required | Contract |
|---|---|---:|---|
| `customer_id` | `VARCHAR(50)` | Yes | Nonblank stable business key |
| `first_name` | `VARCHAR(100)` | Yes | Nonblank |
| `last_name` | `VARCHAR(100)` | Yes | Nonblank |
| `email` | `VARCHAR(320)` | Yes | One address value; validated after normalization |
| `phone` | `VARCHAR(30)` | No | May be blank or null |
| `country_code` | `CHAR(2)` | Yes | Two-letter source value |
| `status` | `VARCHAR(20)` | Yes | `active`, `inactive`, or `suspended`, case-insensitive |
| `created_at` | Timestamp with offset | Yes | Not later than `updated_at` |
| `updated_at` | Timestamp with offset | Yes | Incremental extraction watermark |

Extraction selects records with `updated_at` greater than the last successful watermark and less than or equal to the current batch cutoff. Source rows are never updated by this framework.

## `BILLING_MSSQL` transaction contract

Expected object: `dbo.transactions`

| Field | Source type | Required | Contract |
|---|---|---:|---|
| `transaction_id` | `NVARCHAR(50)` | Yes | Unique, nonblank business key |
| `customer_id` | `NVARCHAR(50)` | Yes | Must resolve to a customer before core promotion |
| `transaction_timestamp` | `DATETIMEOFFSET` | Yes | Valid timestamp |
| `amount` | `DECIMAL(18,2)` | Yes | Greater than or equal to zero |
| `currency_code` | `CHAR(3)` | Yes | Three-letter uppercase-normalizable value |
| `transaction_status` | `NVARCHAR(20)` | Yes | `pending`, `completed`, `failed`, or `refunded` |
| `payment_method` | `NVARCHAR(30)` | No | Free-text source category, trimmed during transformation |
| `updated_at` | `DATETIMEOFFSET` | Yes | Incremental extraction watermark |

Financial values are extracted without floating-point conversion. Reconciliation uses fixed two-decimal values grouped by currency.

## `OPS_CSV` operational-event contract

File-name pattern: `operational_events_YYYYMMDD.csv`

Encoding: UTF-8. Delimiter: comma. Header row: required. Quoting: standard CSV double quotes. One physical file represents one delivery batch.

Required header order is not enforced, but these columns must exist:

| Column | Required | Contract |
|---|---:|---|
| `event_id` | Yes | Unique, nonblank business key within and across files |
| `customer_id` | Yes | Nonblank customer business key |
| `event_type` | Yes | `service`, `shipment`, `support`, or `maintenance` |
| `event_timestamp` | Yes | ISO 8601 timestamp |
| `location_code` | No | Trimmed, maximum 30 characters after normalization |
| `details` | No | Text value preserved for traceability |

An empty file with the correct header is a valid zero-row delivery. A missing header, unexpected duplicate header, invalid encoding, or duplicate file checksum is a contract failure.

## `STATUS_API` customer-status contract

Request: `GET /v1/customer-status` using an inclusive `updated_from` and exclusive `updated_to` window.

Successful response: HTTP 200 with a JSON object containing `data` and optional pagination metadata.

```json
{
  "data": [
    {
      "customer_id": "C000001",
      "status": "active",
      "status_updated_at": "2026-10-15T12:00:00Z"
    }
  ],
  "next_page": null
}
```

| JSON field | Required | Contract |
|---|---:|---|
| `data` | Yes | Array; may be empty |
| `data[].customer_id` | Yes | Nonblank customer business key |
| `data[].status` | Yes | `active`, `inactive`, or `suspended` |
| `data[].status_updated_at` | Yes | ISO 8601 timestamp with timezone |
| `next_page` | No | Opaque pagination value or null |

The connector follows pagination until `next_page` is null. Non-2xx responses, invalid JSON, repeated pagination tokens, and missing required fields fail the batch. Tokens and authorization headers must never be logged.

## Common ingestion metadata

Every staged row receives the following framework-owned fields:

| Field | Meaning |
|---|---|
| `run_id` | Unique processing execution identifier |
| `batch_id` | Stable source delivery identifier used for replay |
| `source_system` | One of the registered source IDs |
| `source_record_id` | Original business key represented as text |
| `source_updated_at` | Source change timestamp when available |
| `record_checksum` | SHA-256 digest of the canonical raw record for idempotent loading |
| `ingested_at` | UTC time at which the framework persisted the row |

## Schema-change policy

- New nullable source fields are tolerated in raw ingestion but are ignored until mapped and documented.
- Removed or renamed required fields fail the source contract.
- Type changes that cannot be parsed without loss fail the source contract.
- New enum values are quarantined until the allowed-value rule and business mapping are updated.
- Contract changes require updates to this document, the mapping document, validation suites, fixtures, and affected tests in the same commit.
