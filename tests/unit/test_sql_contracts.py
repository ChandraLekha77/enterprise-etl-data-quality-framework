from __future__ import annotations

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
DDL_DIRECTORY = ROOT / "sql/postgresql/ddl"


def test_warehouse_ddl_defines_required_tables() -> None:
    ddl = "\n".join(
        path.read_text(encoding="utf-8") for path in sorted(DDL_DIRECTORY.glob("*.sql"))
    ).lower()
    required_tables = {
        "control.pipeline_run",
        "control.source_batch",
        "control.batch_run",
        "control.processing_step",
        "control.audit_event",
        "stg.customer_raw",
        "stg.transaction_raw",
        "stg.operational_event_raw",
        "stg.customer_status_raw",
        "quality.validation_result",
        "quality.reconciliation_result",
        "quality.issue_summary",
    }

    assert all(f"create table if not exists {table}" in ddl for table in required_tables)


def test_warehouse_ddl_has_no_destructive_reset_statements() -> None:
    ddl = "\n".join(
        path.read_text(encoding="utf-8") for path in sorted(DDL_DIRECTORY.glob("*.sql"))
    ).upper()

    assert "DROP TABLE" not in ddl
    assert "DROP SCHEMA" not in ddl
    assert "TRUNCATE" not in ddl


def test_quality_writes_are_idempotent_and_batch_counts_balance() -> None:
    ddl = "\n".join(
        path.read_text(encoding="utf-8") for path in sorted(DDL_DIRECTORY.glob("*.sql"))
    ).upper()

    assert "UQ_VALIDATION_FAILURE UNIQUE NULLS NOT DISTINCT" in ddl
    assert "UQ_RECONCILIATION_METRIC UNIQUE NULLS NOT DISTINCT" in ddl
    assert "SOURCE_ROW_COUNT = ACCEPTED_ROW_COUNT + QUARANTINED_ROW_COUNT + DUPLICATE_ROW_COUNT" in ddl
    assert "ADD COLUMN IF NOT EXISTS DETAILS JSONB" in ddl


def test_delivery_lease_supports_conflict_prevention_and_recovery() -> None:
    ddl = (DDL_DIRECTORY / "007_delivery_leases.sql").read_text(encoding="utf-8")
    assert "CREATE TABLE IF NOT EXISTS control.delivery_lease" in ddl
    assert "PRIMARY KEY (source_system, delivery_key)" in ddl
    assert "owner_run_id UUID NOT NULL" in ddl
    assert "expires_at TIMESTAMPTZ NOT NULL" in ddl


def test_validation_upsert_casts_reused_registry_parameters() -> None:
    from etl_framework.quality.validation_results import UPSERT_VALIDATION_RESULT

    statement = str(UPSERT_VALIDATION_RESULT)
    assert statement.count("CAST(:rule_id AS VARCHAR(30))") == 2
    assert statement.count("CAST(:severity AS VARCHAR(10))") == 2


def test_reporting_indexes_cover_public_filters() -> None:
    ddl = (DDL_DIRECTORY / "008_reporting_indexes.sql").read_text(encoding="utf-8")
    assert "ix_batch_run_reporting" in ddl
    assert "ix_validation_result_reporting" in ddl
    assert "ix_reconciliation_result_reporting" in ddl


def test_every_documented_rule_is_seeded() -> None:
    documentation = (ROOT / "docs/architecture/transformation_rules.md").read_text(
        encoding="utf-8"
    )
    rule_seed = (DDL_DIRECTORY / "005_rule_definitions.sql").read_text(encoding="utf-8")
    documented_rules = set(re.findall(r"`((?:TR|DQ|RC)-[A-Z]+-\d{3})`", documentation))
    seeded_rules = set(re.findall(r"'((?:TR|DQ|RC)-[A-Z]+-\d{3})'", rule_seed))

    assert seeded_rules == documented_rules
