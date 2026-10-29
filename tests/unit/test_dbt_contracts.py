from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def dbt_file(relative_path: str) -> str:
    return (ROOT / "dbt" / relative_path).read_text(encoding="utf-8")


def test_profile_uses_environment_variables_without_credentials() -> None:
    profile = dbt_file("profiles.yml.example")
    assert "env_var('WAREHOUSE_POSTGRES_HOST')" in profile
    assert "env_var('WAREHOUSE_POSTGRES_PASSWORD')" in profile
    assert "CHANGE_ME" not in profile


def test_normalized_staging_preserves_original_values() -> None:
    for model in ("stg_customers.sql", "stg_customer_statuses.sql", "stg_transactions.sql", "stg_operational_events.sql"):
        sql = dbt_file(f"models/staging/{model}")
        assert "_original" in sql
    transactions = dbt_file("models/staging/stg_transactions.sql")
    assert "safe_numeric" in transactions
    assert "safe_timestamptz" in transactions


def test_customer_rules_include_dedup_precedence_and_incremental_promotion() -> None:
    candidates = dbt_file("models/customers/int_customer_candidates.sql")
    promotion = dbt_file("models/customers/dim_customer.sql")
    assert "row_number() over" in candidates
    assert "status_updated_at >= crm.updated_at" in candidates
    assert "STATUS_API" in candidates
    assert "incremental_strategy='merge'" in promotion
    assert "unique_key='customer_id'" in promotion


def test_transaction_and_operational_fact_promotions_follow_documented_rules() -> None:
    transactions = dbt_file("models/transactions/int_transaction_candidates.sql")
    transaction_fact = dbt_file("models/transactions/fact_transaction.sql")
    operations = dbt_file("models/operations/int_operational_event_candidates.sql")
    operational_fact = dbt_file("models/operations/fact_operational_event.sql")

    assert "order by updated_at desc nulls last, staged_row_id desc" in transactions
    assert "customer_resolved" in transactions and "customer_resolved" in transaction_fact
    assert "amount::numeric(18, 2)" in transaction_fact
    assert "order by event_timestamp desc nulls last, staged_row_id desc" in operations
    assert "location_code_invalid" in operations
    assert "customer_resolved" in operations and "customer_resolved" in operational_fact
