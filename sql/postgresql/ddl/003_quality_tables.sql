BEGIN;

CREATE TABLE IF NOT EXISTS quality.rule_definition (
    rule_id VARCHAR(30) PRIMARY KEY,
    rule_type VARCHAR(20) NOT NULL,
    entity_name VARCHAR(50) NOT NULL,
    field_name TEXT,
    severity VARCHAR(10) NOT NULL,
    description TEXT NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT ck_rule_definition_type CHECK (
        rule_type IN ('CONTRACT', 'TRANSFORMATION', 'VALIDATION', 'RECONCILIATION')
    ),
    CONSTRAINT ck_rule_definition_severity CHECK (severity IN ('ERROR', 'WARNING', 'INFO'))
);

CREATE TABLE IF NOT EXISTS quality.validation_result (
    validation_result_id UUID PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES control.pipeline_run(run_id) ON DELETE RESTRICT,
    batch_id UUID NOT NULL REFERENCES control.source_batch(batch_id) ON DELETE RESTRICT,
    source_system VARCHAR(30) NOT NULL,
    rule_id VARCHAR(30) NOT NULL REFERENCES quality.rule_definition(rule_id) ON DELETE RESTRICT,
    severity VARCHAR(10) NOT NULL,
    business_key TEXT,
    field_name TEXT,
    observed_value_masked TEXT,
    message TEXT NOT NULL,
    details JSONB NOT NULL DEFAULT '{}'::JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT ck_validation_result_severity CHECK (severity IN ('ERROR', 'WARNING', 'INFO')),
    CONSTRAINT uq_validation_failure UNIQUE NULLS NOT DISTINCT
        (run_id, batch_id, source_system, rule_id, business_key, field_name)
);

CREATE TABLE IF NOT EXISTS quality.reconciliation_result (
    reconciliation_result_id UUID PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES control.pipeline_run(run_id) ON DELETE RESTRICT,
    batch_id UUID NOT NULL REFERENCES control.source_batch(batch_id) ON DELETE RESTRICT,
    rule_id VARCHAR(30) NOT NULL REFERENCES quality.rule_definition(rule_id) ON DELETE RESTRICT,
    metric_name TEXT NOT NULL,
    group_key TEXT,
    source_value NUMERIC(28, 4) NOT NULL,
    target_value NUMERIC(28, 4) NOT NULL,
    difference NUMERIC(28, 4) GENERATED ALWAYS AS (target_value - source_value) STORED,
    tolerance NUMERIC(28, 4) NOT NULL DEFAULT 0,
    status VARCHAR(10) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT ck_reconciliation_tolerance CHECK (tolerance >= 0),
    CONSTRAINT ck_reconciliation_status CHECK (status IN ('PASS', 'FAIL')),
    CONSTRAINT ck_reconciliation_status_matches_values CHECK (
        (status = 'PASS' AND ABS(target_value - source_value) <= tolerance)
        OR (status = 'FAIL' AND ABS(target_value - source_value) > tolerance)
    ),
    CONSTRAINT uq_reconciliation_metric UNIQUE NULLS NOT DISTINCT
        (run_id, batch_id, rule_id, metric_name, group_key)
);

CREATE TABLE IF NOT EXISTS quality.issue_summary (
    source_system VARCHAR(30) NOT NULL,
    rule_id VARCHAR(30) NOT NULL REFERENCES quality.rule_definition(rule_id) ON DELETE RESTRICT,
    field_name TEXT NOT NULL DEFAULT '',
    classification VARCHAR(20) NOT NULL,
    first_seen_at TIMESTAMPTZ NOT NULL,
    last_seen_at TIMESTAMPTZ NOT NULL,
    affected_run_count INTEGER NOT NULL,
    current_failure_count BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (source_system, rule_id, field_name),
    CONSTRAINT ck_issue_summary_classification CHECK (
        classification IN ('NEW', 'RECURRING', 'PERSISTENT', 'RESOLVED')
    ),
    CONSTRAINT ck_issue_summary_counts CHECK (
        affected_run_count >= 0 AND current_failure_count >= 0
    ),
    CONSTRAINT ck_issue_summary_times CHECK (last_seen_at >= first_seen_at)
);

COMMIT;
