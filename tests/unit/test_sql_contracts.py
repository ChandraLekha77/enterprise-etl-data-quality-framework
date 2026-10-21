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


def test_every_documented_rule_is_seeded() -> None:
    documentation = (ROOT / "docs/architecture/transformation_rules.md").read_text(
        encoding="utf-8"
    )
    rule_seed = (DDL_DIRECTORY / "005_rule_definitions.sql").read_text(encoding="utf-8")
    documented_rules = set(re.findall(r"`((?:TR|DQ|RC)-[A-Z]+-\d{3})`", documentation))
    seeded_rules = set(re.findall(r"'((?:TR|DQ|RC)-[A-Z]+-\d{3})'", rule_seed))

    assert seeded_rules == documented_rules
