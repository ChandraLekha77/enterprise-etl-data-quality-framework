BEGIN;

CREATE INDEX IF NOT EXISTS ix_pipeline_run_status_created
    ON control.pipeline_run (status, created_at DESC);
CREATE INDEX IF NOT EXISTS ix_batch_run_run_status
    ON control.batch_run (run_id, status);
CREATE INDEX IF NOT EXISTS ix_batch_run_batch_created
    ON control.batch_run (batch_id, created_at DESC);
CREATE INDEX IF NOT EXISTS ix_processing_step_run
    ON control.processing_step (run_id, step_name, attempt_number DESC);
CREATE INDEX IF NOT EXISTS ix_audit_event_run_created
    ON control.audit_event (run_id, created_at DESC);

CREATE INDEX IF NOT EXISTS ix_customer_raw_batch_key
    ON stg.customer_raw (batch_id, customer_id);
CREATE INDEX IF NOT EXISTS ix_transaction_raw_batch_key
    ON stg.transaction_raw (batch_id, transaction_id);
CREATE INDEX IF NOT EXISTS ix_operational_raw_batch_key
    ON stg.operational_event_raw (batch_id, event_id);
CREATE INDEX IF NOT EXISTS ix_customer_status_raw_batch_key
    ON stg.customer_status_raw (batch_id, customer_id);

CREATE INDEX IF NOT EXISTS ix_validation_result_run_severity
    ON quality.validation_result (run_id, severity, created_at DESC);
CREATE INDEX IF NOT EXISTS ix_validation_result_issue
    ON quality.validation_result (source_system, rule_id, field_name, created_at DESC);
CREATE INDEX IF NOT EXISTS ix_reconciliation_result_run_status
    ON quality.reconciliation_result (run_id, status, created_at DESC);
CREATE INDEX IF NOT EXISTS ix_issue_summary_classification
    ON quality.issue_summary (classification, last_seen_at DESC);

COMMIT;
