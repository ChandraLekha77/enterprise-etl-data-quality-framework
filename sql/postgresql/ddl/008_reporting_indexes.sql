BEGIN;

CREATE INDEX IF NOT EXISTS ix_batch_run_reporting
    ON control.batch_run (run_id, status, created_at DESC);
CREATE INDEX IF NOT EXISTS ix_validation_result_reporting
    ON quality.validation_result
        (run_id, source_system, rule_id, severity, created_at DESC);
CREATE INDEX IF NOT EXISTS ix_reconciliation_result_reporting
    ON quality.reconciliation_result
        (run_id, rule_id, status, group_key, created_at DESC);

COMMIT;
