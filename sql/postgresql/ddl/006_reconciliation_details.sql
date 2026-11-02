BEGIN;

ALTER TABLE quality.reconciliation_result
    ADD COLUMN IF NOT EXISTS details JSONB NOT NULL DEFAULT '{}'::JSONB;

COMMIT;
