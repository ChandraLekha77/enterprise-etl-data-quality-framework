import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
GX_ROOT = ROOT / "great_expectations"


def test_gx_configuration_uses_environment_connection_string() -> None:
    config = (GX_ROOT / "great_expectations.yml").read_text(encoding="utf-8")
    assert "${GX_WAREHOUSE_CONNECTION_STRING}" in config
    assert "password:" not in config.lower()
    assert "validation_results_store" in config


def test_expectations_reference_seeded_rules_and_supported_severities() -> None:
    seed = (ROOT / "sql/postgresql/ddl/005_rule_definitions.sql").read_text(encoding="utf-8")
    expectations = []
    for path in (GX_ROOT / "expectations").glob("*.json"):
        expectations.extend(json.loads(path.read_text(encoding="utf-8"))["expectations"])

    assert expectations
    for expectation in expectations:
        meta = expectation["meta"]
        assert f"'{meta['rule_id']}'" in seed
        assert meta["severity"] in {"ERROR", "WARNING"}


def test_gx_setup_uses_current_validation_definition_and_checkpoint_api() -> None:
    setup = (ROOT / "scripts/configure_great_expectations.py").read_text(encoding="utf-8")
    assert "add_query_asset" in setup
    assert "add_batch_definition_whole_table" in setup
    assert "gx.ValidationDefinition" in setup
    assert "context.checkpoints.add_or_update" in setup
