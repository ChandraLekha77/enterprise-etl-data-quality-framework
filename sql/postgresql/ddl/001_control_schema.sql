BEGIN;

CREATE SCHEMA IF NOT EXISTS control;
CREATE SCHEMA IF NOT EXISTS stg;
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS quality;

CREATE TABLE IF NOT EXISTS control.pipeline_run (
    run_id UUID PRIMARY KEY,
    airflow_run_id TEXT NOT NULL UNIQUE,
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT ck_pipeline_run_status CHECK (
        status IN ('PENDING', 'RUNNING', 'QUARANTINED', 'FAILED', 'SUCCEEDED', 'CANCELLED')
    ),
    CONSTRAINT ck_pipeline_run_times CHECK (
        finished_at IS NULL OR started_at IS NULL OR finished_at >= started_at
    ),
    CONSTRAINT ck_pipeline_run_terminal_time CHECK (
        status NOT IN ('QUARANTINED', 'FAILED', 'SUCCEEDED', 'CANCELLED') OR finished_at IS NOT NULL
    )
);

CREATE TABLE IF NOT EXISTS control.source_batch (
    batch_id UUID PRIMARY KEY,
    source_system VARCHAR(30) NOT NULL,
    delivery_key TEXT NOT NULL,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_source_batch_delivery UNIQUE (source_system, delivery_key),
    CONSTRAINT ck_source_batch_system CHECK (
        source_system IN ('CRM_PG', 'BILLING_MSSQL', 'OPS_CSV', 'STATUS_API')
    )
);

CREATE TABLE IF NOT EXISTS control.batch_run (
    batch_run_id UUID PRIMARY KEY,
    batch_id UUID NOT NULL REFERENCES control.source_batch(batch_id) ON DELETE RESTRICT,
    run_id UUID NOT NULL REFERENCES control.pipeline_run(run_id) ON DELETE RESTRICT,
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    source_row_count BIGINT,
    accepted_row_count BIGINT,
    quarantined_row_count BIGINT,
    duplicate_row_count BIGINT,
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_batch_run_attempt UNIQUE (batch_id, run_id),
    CONSTRAINT ck_batch_run_status CHECK (
        status IN ('PENDING', 'RUNNING', 'QUARANTINED', 'FAILED', 'SUCCEEDED', 'CANCELLED')
    ),
    CONSTRAINT ck_batch_run_counts CHECK (
        (source_row_count IS NULL OR source_row_count >= 0)
        AND (accepted_row_count IS NULL OR accepted_row_count >= 0)
        AND (quarantined_row_count IS NULL OR quarantined_row_count >= 0)
        AND (duplicate_row_count IS NULL OR duplicate_row_count >= 0)
    ),
    CONSTRAINT ck_batch_run_times CHECK (
        finished_at IS NULL OR started_at IS NULL OR finished_at >= started_at
    ),
    CONSTRAINT ck_batch_run_started_state CHECK (
        status = 'PENDING' OR started_at IS NOT NULL
    ),
    CONSTRAINT ck_batch_run_terminal_time CHECK (
        status NOT IN ('QUARANTINED', 'FAILED', 'SUCCEEDED', 'CANCELLED')
        OR finished_at IS NOT NULL
    ),
    CONSTRAINT ck_batch_run_balanced_counts CHECK (
        status NOT IN ('QUARANTINED', 'SUCCEEDED')
        OR (
            source_row_count IS NOT NULL
            AND accepted_row_count IS NOT NULL
            AND quarantined_row_count IS NOT NULL
            AND duplicate_row_count IS NOT NULL
            AND source_row_count = accepted_row_count + quarantined_row_count + duplicate_row_count
        )
    )
);

CREATE TABLE IF NOT EXISTS control.processing_step (
    step_id UUID PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES control.pipeline_run(run_id) ON DELETE RESTRICT,
    batch_id UUID REFERENCES control.source_batch(batch_id) ON DELETE RESTRICT,
    step_name VARCHAR(100) NOT NULL,
    attempt_number INTEGER NOT NULL DEFAULT 1,
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_processing_step_attempt UNIQUE NULLS NOT DISTINCT
        (run_id, batch_id, step_name, attempt_number),
    CONSTRAINT ck_processing_step_attempt CHECK (attempt_number > 0),
    CONSTRAINT ck_processing_step_status CHECK (
        status IN ('PENDING', 'RUNNING', 'FAILED', 'SKIPPED', 'SUCCEEDED')
    ),
    CONSTRAINT ck_processing_step_times CHECK (
        finished_at IS NULL OR started_at IS NULL OR finished_at >= started_at
    )
);

CREATE TABLE IF NOT EXISTS control.source_watermark (
    source_system VARCHAR(30) PRIMARY KEY,
    watermark_value TEXT NOT NULL,
    successful_run_id UUID NOT NULL REFERENCES control.pipeline_run(run_id) ON DELETE RESTRICT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT ck_source_watermark_system CHECK (
        source_system IN ('CRM_PG', 'BILLING_MSSQL', 'OPS_CSV', 'STATUS_API')
    )
);

CREATE TABLE IF NOT EXISTS control.audit_event (
    audit_event_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id UUID REFERENCES control.pipeline_run(run_id) ON DELETE RESTRICT,
    batch_id UUID REFERENCES control.source_batch(batch_id) ON DELETE RESTRICT,
    event_type VARCHAR(50) NOT NULL,
    actor VARCHAR(100) NOT NULL,
    message TEXT NOT NULL,
    event_details JSONB NOT NULL DEFAULT '{}'::JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

COMMIT;
