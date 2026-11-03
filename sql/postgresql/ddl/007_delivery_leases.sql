BEGIN;

CREATE TABLE IF NOT EXISTS control.delivery_lease (
    source_system VARCHAR(30) NOT NULL,
    delivery_key TEXT NOT NULL,
    owner_run_id UUID NOT NULL REFERENCES control.pipeline_run(run_id) ON DELETE CASCADE,
    acquired_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (source_system, delivery_key),
    CONSTRAINT ck_delivery_lease_system CHECK (
        source_system IN ('CRM_PG', 'BILLING_MSSQL', 'OPS_CSV', 'STATUS_API')
    ),
    CONSTRAINT ck_delivery_lease_expiry CHECK (expires_at > acquired_at)
);

CREATE INDEX IF NOT EXISTS ix_delivery_lease_expiry
    ON control.delivery_lease (expires_at);

COMMIT;
