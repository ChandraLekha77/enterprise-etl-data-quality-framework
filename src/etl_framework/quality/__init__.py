"""Shared data-quality result formatting and persistence."""

from etl_framework.quality.validation_results import (
    Severity,
    ValidationFailure,
    ValidationResultError,
    format_expectation_failure,
    persist_expectation_suite_result,
    store_validation_failures,
)
from etl_framework.quality.business_rules import (
    BatchValidation,
    RecordDisposition,
    RecordEvaluation,
    validate_api_statuses,
    validate_customers,
    validate_operational_events,
    validate_transactions,
)
from etl_framework.quality.quality_gate import (
    QualityGateError,
    QualityGateResult,
    apply_quality_gate,
)

__all__ = [
    "Severity",
    "ValidationFailure",
    "ValidationResultError",
    "format_expectation_failure",
    "persist_expectation_suite_result",
    "store_validation_failures",
    "BatchValidation",
    "RecordDisposition",
    "RecordEvaluation",
    "validate_api_statuses",
    "validate_customers",
    "validate_operational_events",
    "validate_transactions",
    "QualityGateError",
    "QualityGateResult",
    "apply_quality_gate",
]
