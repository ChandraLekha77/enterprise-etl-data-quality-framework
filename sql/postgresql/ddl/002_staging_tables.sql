BEGIN;

CREATE TABLE IF NOT EXISTS stg.customer_raw (
    staged_row_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id TEXT,
    first_name TEXT,
    last_name TEXT,
    email TEXT,
    phone TEXT,
    country_code TEXT,
    status TEXT,
    created_at_raw TEXT,
    updated_at_raw TEXT,
    run_id UUID NOT NULL,
    batch_id UUID NOT NULL,
    source_system VARCHAR(30) NOT NULL DEFAULT 'CRM_PG',
    source_record_id TEXT,
    source_updated_at TIMESTAMPTZ,
    record_checksum CHAR(64) NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_customer_raw_batch_run FOREIGN KEY (batch_id, run_id)
        REFERENCES control.batch_run(batch_id, run_id) ON DELETE RESTRICT,
    CONSTRAINT ck_customer_raw_source CHECK (source_system = 'CRM_PG'),
    CONSTRAINT uq_customer_raw_checksum UNIQUE (batch_id, record_checksum)
);

CREATE TABLE IF NOT EXISTS stg.transaction_raw (
    staged_row_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    transaction_id TEXT,
    customer_id TEXT,
    transaction_timestamp_raw TEXT,
    amount_raw TEXT,
    currency_code TEXT,
    transaction_status TEXT,
    payment_method TEXT,
    updated_at_raw TEXT,
    run_id UUID NOT NULL,
    batch_id UUID NOT NULL,
    source_system VARCHAR(30) NOT NULL DEFAULT 'BILLING_MSSQL',
    source_record_id TEXT,
    source_updated_at TIMESTAMPTZ,
    record_checksum CHAR(64) NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_transaction_raw_batch_run FOREIGN KEY (batch_id, run_id)
        REFERENCES control.batch_run(batch_id, run_id) ON DELETE RESTRICT,
    CONSTRAINT ck_transaction_raw_source CHECK (source_system = 'BILLING_MSSQL'),
    CONSTRAINT uq_transaction_raw_checksum UNIQUE (batch_id, record_checksum)
);

CREATE TABLE IF NOT EXISTS stg.operational_event_raw (
    staged_row_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id TEXT,
    customer_id TEXT,
    event_type TEXT,
    event_timestamp_raw TEXT,
    location_code TEXT,
    details TEXT,
    source_file_name TEXT NOT NULL,
    source_file_checksum CHAR(64) NOT NULL,
    run_id UUID NOT NULL,
    batch_id UUID NOT NULL,
    source_system VARCHAR(30) NOT NULL DEFAULT 'OPS_CSV',
    source_record_id TEXT,
    source_updated_at TIMESTAMPTZ,
    record_checksum CHAR(64) NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_operational_raw_batch_run FOREIGN KEY (batch_id, run_id)
        REFERENCES control.batch_run(batch_id, run_id) ON DELETE RESTRICT,
    CONSTRAINT ck_operational_raw_source CHECK (source_system = 'OPS_CSV'),
    CONSTRAINT uq_operational_raw_checksum UNIQUE (batch_id, record_checksum)
);

CREATE TABLE IF NOT EXISTS stg.customer_status_raw (
    staged_row_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id TEXT,
    status TEXT,
    status_updated_at_raw TEXT,
    page_token TEXT,
    raw_payload JSONB NOT NULL,
    run_id UUID NOT NULL,
    batch_id UUID NOT NULL,
    source_system VARCHAR(30) NOT NULL DEFAULT 'STATUS_API',
    source_record_id TEXT,
    source_updated_at TIMESTAMPTZ,
    record_checksum CHAR(64) NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_customer_status_raw_batch_run FOREIGN KEY (batch_id, run_id)
        REFERENCES control.batch_run(batch_id, run_id) ON DELETE RESTRICT,
    CONSTRAINT ck_customer_status_raw_source CHECK (source_system = 'STATUS_API'),
    CONSTRAINT uq_customer_status_raw_checksum UNIQUE (batch_id, record_checksum)
);

COMMIT;
