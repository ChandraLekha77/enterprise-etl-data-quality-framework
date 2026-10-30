from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from etl_framework.quality.business_rules import validate_operational_events
from etl_framework.quality.quality_gate import QualityGateError, apply_quality_gate


def engine_with_rowcounts(*rowcounts: int) -> tuple[MagicMock, MagicMock]:
    engine = MagicMock()
    connection = engine.begin.return_value.__enter__.return_value
    connection.execute.side_effect = [MagicMock(rowcount=count) for count in rowcounts]
    return engine, connection


def test_mandatory_error_saves_failure_and_blocks_core_promotion() -> None:
    validation = validate_operational_events(
        [{"event_id": "E1", "customer_id": "missing", "event_type": "support", "event_timestamp": "2026-10-20T12:00:00Z"}],
        customer_ids=set(), run_id=uuid4(), batch_id=uuid4(),
    )
    engine, connection = engine_with_rowcounts(1, 1, 1)
    promote = MagicMock()

    result = apply_quality_gate(engine, validation, promote=promote)

    assert result.status == "QUARANTINED"
    assert not result.promoted
    assert (result.accepted, result.quarantined, result.duplicate) == (0, 1, 0)
    promote.assert_not_called()
    assert "quality.validation_result" in str(connection.execute.call_args_list[1].args[0])


def test_warning_only_batch_is_promoted_with_warning_saved() -> None:
    validation = validate_operational_events(
        [{"event_id": "E1", "customer_id": "C1", "event_type": "support", "event_timestamp": "2026-10-20T12:00:00Z", "location_code": "X" * 31}],
        customer_ids={"C1"}, run_id=uuid4(), batch_id=uuid4(),
    )
    engine, _ = engine_with_rowcounts(1, 1, 1)
    promote = MagicMock()

    result = apply_quality_gate(engine, validation, promote=promote)

    assert result.status == "SUCCEEDED"
    assert result.promoted
    promote.assert_called_once()
    promoted_rows = promote.call_args.args[1]
    assert promoted_rows[0]["location_code"] is None


def test_corrected_replay_replaces_failures_and_can_promote() -> None:
    run_id, batch_id = uuid4(), uuid4()
    invalid = validate_operational_events(
        [{"event_id": "E1", "customer_id": "missing", "event_type": "support", "event_timestamp": "2026-10-20T12:00:00Z"}],
        customer_ids=set(), run_id=run_id, batch_id=batch_id,
    )
    corrected = validate_operational_events(
        [{"event_id": "E1", "customer_id": "C1", "event_type": "support", "event_timestamp": "2026-10-20T12:00:00Z"}],
        customer_ids={"C1"}, run_id=run_id, batch_id=batch_id,
    )
    engine, connection = engine_with_rowcounts(1, 1, 1, 1, 1)
    promote = MagicMock()

    first = apply_quality_gate(engine, invalid, promote=promote)
    second = apply_quality_gate(engine, corrected, promote=promote)

    assert first.status == "QUARANTINED"
    assert second.status == "SUCCEEDED"
    promote.assert_called_once()
    delete_calls = [call for call in connection.execute.call_args_list if "DELETE FROM quality.validation_result" in str(call.args[0])]
    assert len(delete_calls) == 2


def test_missing_batch_attempt_rolls_back_without_promotion() -> None:
    validation = validate_operational_events(
        [], customer_ids=set(), run_id=uuid4(), batch_id=uuid4(),
    )
    engine, _ = engine_with_rowcounts(0, 0)
    promote = MagicMock()
    with pytest.raises(QualityGateError, match="not found"):
        apply_quality_gate(engine, validation, promote=promote)
    promote.assert_not_called()
