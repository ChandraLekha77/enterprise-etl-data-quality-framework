"""Safe formatting and idempotent storage of validation failures."""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from enum import Enum
from typing import Any
from uuid import UUID, uuid5

from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from etl_framework.control.run_tracking import mask_audit_value


VALIDATION_NAMESPACE = UUID("443220bc-67d8-4d69-aeaf-47dfa73f4a08")
ALLOWED_SOURCE_SYSTEMS = frozenset({"CRM_PG", "BILLING_MSSQL", "OPS_CSV", "STATUS_API"})


class Severity(str, Enum):
    ERROR = "ERROR"
    WARNING = "WARNING"


class ValidationResultError(RuntimeError):
    """Raised when a result is unsafe, malformed, or cannot be stored."""


@dataclass(frozen=True, slots=True)
class ValidationFailure:
    run_id: UUID
    batch_id: UUID
    source_system: str
    rule_id: str
    severity: Severity
    message: str
    business_key: str | None = None
    field_name: str | None = None
    observed_value: Any = field(default=None, repr=False)
    details: Mapping[str, Any] = field(default_factory=dict, repr=False)

    def __post_init__(self) -> None:
        if self.source_system not in ALLOWED_SOURCE_SYSTEMS:
            raise ValidationResultError("Unknown validation source system")
        if not self.rule_id.startswith("DQ-"):
            raise ValidationResultError("Validation rule_id must be a documented DQ rule")
        if not self.message.strip():
            raise ValidationResultError("Validation message must not be blank")

    @property
    def result_id(self) -> UUID:
        identity = "|".join(
            (
                str(self.run_id), str(self.batch_id), self.source_system,
                self.rule_id, self.business_key or "", self.field_name or "",
            )
        )
        return uuid5(VALIDATION_NAMESPACE, identity)

    def parameters(self) -> dict[str, Any]:
        masked_observed = mask_audit_value(self.observed_value, field_name=self.field_name)
        safe_details = mask_audit_value(dict(self.details))
        safe_message = mask_audit_value(self.message)
        return {
            "validation_result_id": self.result_id,
            "run_id": self.run_id,
            "batch_id": self.batch_id,
            "source_system": self.source_system,
            "rule_id": self.rule_id,
            "severity": self.severity.value,
            "business_key": self.business_key,
            "field_name": self.field_name,
            "observed_value_masked": _bounded_text(masked_observed, 1_000),
            "message": _bounded_text(safe_message, 2_000) or "Validation failed",
            "details": json.dumps(safe_details, sort_keys=True, default=str),
        }


UPSERT_VALIDATION_RESULT = text(
    """
    INSERT INTO quality.validation_result (
        validation_result_id, run_id, batch_id, source_system, rule_id,
        severity, business_key, field_name, observed_value_masked,
        message, details
    )
    SELECT
        :validation_result_id, :run_id, :batch_id, :source_system, :rule_id,
        :severity, :business_key, :field_name, :observed_value_masked,
        :message, CAST(:details AS JSONB)
    FROM quality.rule_definition rule
    WHERE rule.rule_id = :rule_id
      AND rule.rule_type = 'VALIDATION'
      AND rule.severity = :severity
      AND rule.enabled
    ON CONFLICT (run_id, batch_id, source_system, rule_id, business_key, field_name)
    DO UPDATE SET
        severity = EXCLUDED.severity,
        observed_value_masked = EXCLUDED.observed_value_masked,
        message = EXCLUDED.message,
        details = EXCLUDED.details
    """
)


def format_expectation_failure(
    result: Mapping[str, Any],
    *,
    run_id: UUID,
    batch_id: UUID,
    source_system: str,
    business_key: str | None = None,
) -> ValidationFailure | None:
    """Convert one failed GX result into the framework's stable result shape."""
    if result.get("success") is True:
        return None
    config = result.get("expectation_config")
    if not isinstance(config, Mapping):
        raise ValidationResultError("Expectation result is missing expectation_config")
    meta = config.get("meta")
    if not isinstance(meta, Mapping):
        raise ValidationResultError("Expectation config is missing rule metadata")
    rule_id = meta.get("rule_id")
    severity_value = meta.get("severity")
    if not isinstance(rule_id, str) or not isinstance(severity_value, str):
        raise ValidationResultError("Expectation metadata requires rule_id and severity")
    try:
        severity = Severity(severity_value.upper())
    except ValueError as exc:
        raise ValidationResultError("Expectation severity must be ERROR or WARNING") from exc
    kwargs = config.get("kwargs") if isinstance(config.get("kwargs"), Mapping) else {}
    field_name = kwargs.get("column") if isinstance(kwargs.get("column"), str) else None
    result_details = result.get("result") if isinstance(result.get("result"), Mapping) else {}
    observed = _first_observed_value(result_details)
    expectation_type = config.get("type") or config.get("expectation_type") or "expectation"
    message = meta.get("message") or f"{expectation_type} failed"
    return ValidationFailure(
        run_id=run_id,
        batch_id=batch_id,
        source_system=source_system,
        rule_id=rule_id,
        severity=severity,
        message=str(message),
        business_key=business_key,
        field_name=field_name,
        observed_value=observed,
        details={
            "expectation_type": expectation_type,
            "unexpected_count": result_details.get("unexpected_count"),
            "unexpected_percent": result_details.get("unexpected_percent"),
        },
    )


def store_validation_failures(engine: Engine, failures: Iterable[ValidationFailure]) -> int:
    """Upsert failures atomically; replaying the same results creates no duplicates."""
    rows = [failure.parameters() for failure in failures]
    if not rows:
        return 0
    try:
        with engine.begin() as connection:
            for row in rows:
                result = connection.execute(UPSERT_VALIDATION_RESULT, row)
                if result.rowcount == 0:
                    raise ValidationResultError(
                        f"Rule {row['rule_id']} is missing, disabled, or has a severity mismatch"
                    )
    except ValidationResultError:
        raise
    except SQLAlchemyError as exc:
        raise ValidationResultError("Validation results could not be stored") from exc
    return len(rows)


def persist_expectation_suite_result(
    engine: Engine,
    suite_result: Mapping[str, Any],
    *,
    run_id: UUID,
    batch_id: UUID,
    source_system: str,
) -> int:
    """Format and store all failed Expectations returned by one GX validation."""
    expectation_results = suite_result.get("results")
    if not isinstance(expectation_results, list):
        raise ValidationResultError("GX suite result must contain a results list")
    failures = []
    for result in expectation_results:
        if not isinstance(result, Mapping):
            raise ValidationResultError("GX expectation result must be an object")
        failure = format_expectation_failure(
            result,
            run_id=run_id,
            batch_id=batch_id,
            source_system=source_system,
        )
        if failure is not None:
            failures.append(failure)
    return store_validation_failures(engine, failures)


def _first_observed_value(result: Mapping[str, Any]) -> Any:
    values = result.get("partial_unexpected_list")
    if isinstance(values, list) and values:
        return values[0]
    return result.get("observed_value")


def _bounded_text(value: Any, maximum: int) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        rendered = value
    else:
        rendered = json.dumps(value, sort_keys=True, default=str)
    return rendered[:maximum]
