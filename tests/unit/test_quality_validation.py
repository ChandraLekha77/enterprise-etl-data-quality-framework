from __future__ import annotations

import json
from unittest.mock import MagicMock
from uuid import uuid4

import pytest
from sqlalchemy.exc import OperationalError

from etl_framework.quality.validation_results import (
    Severity,
    ValidationFailure,
    ValidationResultError,
    format_expectation_failure,
    persist_expectation_suite_result,
    store_validation_failures,
)


def test_expectation_failure_maps_documented_rule_and_severity() -> None:
    failure = format_expectation_failure(
        {
            "success": False,
            "expectation_config": {
                "type": "expect_column_values_to_be_between",
                "kwargs": {"column": "amount"},
                "meta": {
                    "rule_id": "DQ-TXN-002",
                    "severity": "ERROR",
                    "message": "Amount is invalid",
                },
            },
            "result": {"partial_unexpected_list": ["-1.00"], "unexpected_count": 1},
        },
        run_id=uuid4(),
        batch_id=uuid4(),
        source_system="BILLING_MSSQL",
        business_key="T1",
    )

    assert failure is not None
    assert failure.rule_id == "DQ-TXN-002"
    assert failure.severity is Severity.ERROR
    assert failure.field_name == "amount"
    assert failure.observed_value == "-1.00"


def test_successful_expectation_does_not_create_failure() -> None:
    assert format_expectation_failure(
        {"success": True},
        run_id=uuid4(), batch_id=uuid4(), source_system="OPS_CSV",
    ) is None


@pytest.mark.parametrize("severity", ["INFO", "critical", ""])
def test_only_error_and_warning_severities_are_accepted(severity: str) -> None:
    with pytest.raises(ValidationResultError, match="ERROR or WARNING"):
        format_expectation_failure(
            {
                "success": False,
                "expectation_config": {
                    "type": "expect_column_values_to_not_be_null",
                    "kwargs": {"column": "event_id"},
                    "meta": {"rule_id": "DQ-OPS-001", "severity": severity},
                },
            },
            run_id=uuid4(), batch_id=uuid4(), source_system="OPS_CSV",
        )


def test_parameters_mask_observed_values_messages_and_details() -> None:
    failure = ValidationFailure(
        run_id=uuid4(), batch_id=uuid4(), source_system="CRM_PG",
        rule_id="DQ-CUS-002", severity=Severity.ERROR,
        message="Invalid email ada@example.com token=secret-token",
        business_key="C1", field_name="email", observed_value="ada@example.com",
        details={"authorization": "Bearer secret-token", "phone": "+1 212 555 1212"},
    )
    parameters = failure.parameters()
    details = json.loads(parameters["details"])

    assert parameters["observed_value_masked"] == "[REDACTED_EMAIL]"
    assert "ada@example.com" not in parameters["message"]
    assert "secret-token" not in parameters["message"]
    assert details["authorization"] == "[REDACTED]"
    assert details["phone"] == "[REDACTED_PHONE]"


def test_store_is_idempotent_and_uses_deterministic_result_id() -> None:
    engine = MagicMock()
    connection = engine.begin.return_value.__enter__.return_value
    connection.execute.return_value = MagicMock(rowcount=1)
    failure = ValidationFailure(
        run_id=uuid4(), batch_id=uuid4(), source_system="OPS_CSV",
        rule_id="DQ-OPS-004", severity=Severity.WARNING,
        message="Location too long", business_key="E1", field_name="location_code",
        observed_value="X" * 31,
    )

    assert store_validation_failures(engine, [failure, failure]) == 2
    first = connection.execute.call_args_list[0].args[1]
    second = connection.execute.call_args_list[1].args[1]
    assert first["validation_result_id"] == second["validation_result_id"] == failure.result_id
    assert "ON CONFLICT" in str(connection.execute.call_args_list[0].args[0])
    assert "DO UPDATE" in str(connection.execute.call_args_list[0].args[0])


def test_unknown_or_mismatched_rule_rolls_back() -> None:
    engine = MagicMock()
    connection = engine.begin.return_value.__enter__.return_value
    connection.execute.return_value = MagicMock(rowcount=0)
    failure = ValidationFailure(
        run_id=uuid4(), batch_id=uuid4(), source_system="OPS_CSV",
        rule_id="DQ-OPS-004", severity=Severity.WARNING, message="Invalid",
    )
    with pytest.raises(ValidationResultError, match="severity mismatch"):
        store_validation_failures(engine, [failure])


def test_database_error_does_not_disclose_database_details() -> None:
    engine = MagicMock()
    connection = engine.begin.return_value.__enter__.return_value
    connection.execute.side_effect = OperationalError("INSERT", {}, Exception("password=secret"))
    failure = ValidationFailure(
        run_id=uuid4(), batch_id=uuid4(), source_system="OPS_CSV",
        rule_id="DQ-OPS-001", severity=Severity.ERROR, message="Invalid",
    )
    with pytest.raises(ValidationResultError) as error:
        store_validation_failures(engine, [failure])
    assert str(error.value) == "Validation results could not be stored"
    assert "secret" not in str(error.value)


def test_suite_result_persists_only_failures() -> None:
    engine = MagicMock()
    connection = engine.begin.return_value.__enter__.return_value
    connection.execute.return_value = MagicMock(rowcount=1)
    common = {
        "expectation_config": {
            "type": "expect_column_values_to_not_be_null",
            "kwargs": {"column": "event_id"},
            "meta": {"rule_id": "DQ-OPS-001", "severity": "ERROR"},
        }
    }
    count = persist_expectation_suite_result(
        engine,
        {"results": [{**common, "success": True}, {**common, "success": False}]},
        run_id=uuid4(), batch_id=uuid4(), source_system="OPS_CSV",
    )
    assert count == 1
    connection.execute.assert_called_once()
