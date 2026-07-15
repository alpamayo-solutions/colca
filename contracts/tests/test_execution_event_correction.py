"""Tests for ExecutionEventCorrection semantic data contract."""

from copy import deepcopy
from datetime import datetime, timezone

import pytest

from colca_data_contracts.semantic import SEMANTIC_CONTRACTS
from colca_data_contracts.semantic.edge_execution_context import ExecutionEventType
from colca_data_contracts.semantic.execution_event_correction import (
    CorrectionAction,
    CorrectionReplacementEvent,
    CorrectionType,
    ExecutionEventCorrection,
)


AWARE_PERFORMED_AT = datetime(2026, 7, 15, 10, 12, tzinfo=timezone.utc)


def _modern_correction(
    correction_type: CorrectionType,
    **overrides,
) -> ExecutionEventCorrection:
    values = {
        "correction_id": "correction:01K0A1B2C3D4E5F6G7H8J9K0MN",
        "correction_type": correction_type,
        "oit_id": 7042,
        "asf_id": 9012,
        "reason": "Falscher Zeitbereich",
        "performed_by": "employee:99",
        "performed_at": AWARE_PERFORMED_AT,
        "before_json": {"start_ts": "2026-05-28T08:00:00+02:00"},
        "after_json": {"start_ts": "2026-05-28T10:00:00+02:00"},
    }
    if correction_type in (
        CorrectionType.REPLACE_OPERATOR_INTERVAL,
        CorrectionType.REPLACE_MACHINE_INTERVAL,
    ):
        values["target_session_id"] = 555
    elif correction_type in (
        CorrectionType.EDIT_CLOSURE_TIMESTAMP,
        CorrectionType.UNDO_FINALIZATION,
    ):
        values["target_closure_id"] = 123
    values.update(overrides)
    return ExecutionEventCorrection(**values)


def test_contract_registered():
    assert SEMANTIC_CONTRACTS["ExecutionEventCorrection"] is ExecutionEventCorrection


def test_replace_interval_round_trip():
    correction = ExecutionEventCorrection(
        correction_id="corr-001",
        action=CorrectionAction.REPLACE_INTERVAL,
        target_event_ids=["evt-1", "evt-2"],
        requested_by="alice.admin",
        requested_at=datetime(2026, 4, 1, 10, 15, 0),
        reason_code="admin_correction",
        reason_text="Operator forgot to stop disruption",
        target_event_type=ExecutionEventType.DISRUPTION_START,
        asset_id="MB0054",
        order_nr="FA-2026-0042",
        step_id="10",
        operator_id="AJU",
        shift_code="F",
        replacement_events=[
            CorrectionReplacementEvent(
                event_type=ExecutionEventType.DISRUPTION_START,
                timestamp=datetime(2026, 3, 15, 9, 15, 0),
            ),
            CorrectionReplacementEvent(
                event_type=ExecutionEventType.DISRUPTION_END,
                timestamp=datetime(2026, 3, 15, 9, 32, 0),
            ),
        ],
    )

    json_data = correction.to_json()
    assert json_data["action"] == "replace_interval"
    assert len(json_data["replacement_events"]) == 2

    restored = ExecutionEventCorrection.from_json(json_data)
    assert restored.action == CorrectionAction.REPLACE_INTERVAL
    assert restored.target_event_ids == ["evt-1", "evt-2"]
    assert restored.target_event_type == ExecutionEventType.DISRUPTION_START
    assert restored.replacement_events[0].event_type == ExecutionEventType.DISRUPTION_START
    assert restored.replacement_events[1].event_type == ExecutionEventType.DISRUPTION_END


def test_undo_finalization_correction_round_trips():
    correction = ExecutionEventCorrection(
        correction_id="correction:undo-123",
        correction_type=CorrectionType.UNDO_FINALIZATION,
        oit_id=7042,
        asf_id=9012,
        target_closure_id=123,
        reason="Falscher Abschluss",
        performed_by="employee:99",
        performed_at=AWARE_PERFORMED_AT,
        before_json={"is_active": True},
        after_json={"is_active": False},
    )

    restored = ExecutionEventCorrection.from_json(correction.to_json())

    assert restored.correction_type == CorrectionType.UNDO_FINALIZATION
    assert restored.correction_id == "correction:undo-123"
    assert restored.oit_id == 7042
    assert restored.asf_id == 9012
    assert restored.target_closure_id == 123
    assert restored.reason == "Falscher Abschluss"
    assert restored.performed_by == "employee:99"
    assert restored.performed_at == AWARE_PERFORMED_AT
    assert restored.before_json["is_active"] is True
    assert restored.after_json["is_active"] is False


def test_replace_machine_interval_correction_type_round_trips():
    correction = ExecutionEventCorrection(
        correction_id="correction:machine-555",
        correction_type=CorrectionType.REPLACE_MACHINE_INTERVAL,
        oit_id=7042,
        asf_id=9012,
        target_session_id=555,
        reason="Tabletzeit war 2 h zurück",
        performed_by="employee:99",
        performed_at=AWARE_PERFORMED_AT,
        before_json={"start_ts": "2026-05-28T08:00:00+02:00"},
        after_json={"start_ts": "2026-05-28T10:00:00+02:00"},
    )

    restored = ExecutionEventCorrection.from_json(correction.to_json())

    assert restored.correction_type == CorrectionType.REPLACE_MACHINE_INTERVAL
    assert restored.correction_id == "correction:machine-555"
    assert restored.target_session_id == 555
    assert restored.target_closure_id is None
    assert restored.before_json["start_ts"] == "2026-05-28T08:00:00+02:00"


def test_validate_requires_closure_target_for_undo_finalization():
    correction = ExecutionEventCorrection(
        correction_id="correction:undo-no-target",
        correction_type=CorrectionType.UNDO_FINALIZATION,
        oit_id=7042,
        asf_id=9012,
        performed_by="employee:99",
        performed_at=AWARE_PERFORMED_AT,
    )

    with pytest.raises(ValueError, match="target_closure_id"):
        correction.validate()


@pytest.mark.parametrize("correction_type", list(CorrectionType))
def test_modern_correction_identity_round_trips_for_every_type(correction_type):
    correction = _modern_correction(correction_type)
    original_snapshot = deepcopy(correction.before_json)

    payload = correction.to_json()
    restored = ExecutionEventCorrection.from_json(payload)

    assert payload["correction_id"] == correction.correction_id
    assert restored.correction_id == correction.correction_id
    assert restored.correction_type == correction_type
    assert restored.before_json == original_snapshot
    assert correction.before_json == original_snapshot


@pytest.mark.parametrize("correction_type", list(CorrectionType))
@pytest.mark.parametrize("correction_id", [None, "", 123])
def test_modern_correction_requires_correction_id(correction_type, correction_id):
    correction = _modern_correction(correction_type, correction_id=correction_id)

    with pytest.raises(ValueError, match="correction_id is required"):
        correction.validate()


@pytest.mark.parametrize(
    "performed_at",
    [None, datetime(2026, 7, 15, 10, 12), "2026-07-15T10:12:00+00:00"],
)
def test_modern_correction_requires_aware_performed_at(performed_at):
    correction = _modern_correction(
        CorrectionType.REPLACE_MACHINE_INTERVAL,
        performed_at=performed_at,
    )

    with pytest.raises(ValueError, match="performed_at must be timezone-aware"):
        correction.validate()


@pytest.mark.parametrize("performed_by", [None, "employee", ":99", "employee:", "", 99])
def test_modern_correction_requires_namespaced_performed_by(performed_by):
    correction = _modern_correction(
        CorrectionType.REPLACE_MACHINE_INTERVAL,
        performed_by=performed_by,
    )

    with pytest.raises(ValueError, match="performed_by must be namespaced"):
        correction.validate()


@pytest.mark.parametrize(
    ("correction_type", "field"),
    [
        (CorrectionType.REPLACE_MACHINE_INTERVAL, "oit_id"),
        (CorrectionType.REPLACE_MACHINE_INTERVAL, "asf_id"),
        (CorrectionType.REPLACE_MACHINE_INTERVAL, "target_session_id"),
        (CorrectionType.UNDO_FINALIZATION, "target_closure_id"),
    ],
)
@pytest.mark.parametrize("invalid_value", ["123", True])
def test_modern_correction_rejects_non_integer_ids(correction_type, field, invalid_value):
    correction = _modern_correction(correction_type, **{field: invalid_value})

    with pytest.raises(ValueError, match=field):
        correction.validate()


@pytest.mark.parametrize("field", ["before_json", "after_json"])
@pytest.mark.parametrize("invalid_value", [[], "snapshot", 42, True])
def test_modern_correction_rejects_non_object_snapshots(field, invalid_value):
    correction = _modern_correction(
        CorrectionType.REPLACE_MACHINE_INTERVAL,
        **{field: invalid_value},
    )

    with pytest.raises(ValueError, match=field):
        correction.validate()


@pytest.mark.parametrize(
    "correction_type",
    [CorrectionType.REPLACE_OPERATOR_INTERVAL, CorrectionType.REPLACE_MACHINE_INTERVAL],
)
def test_session_replacement_requires_target_session(correction_type):
    correction = _modern_correction(correction_type, target_session_id=None)

    with pytest.raises(ValueError, match="target_session_id"):
        correction.validate()


def test_insert_session_does_not_require_existing_target():
    correction = _modern_correction(CorrectionType.INSERT_SESSION)

    correction.validate()


@pytest.mark.parametrize(
    "correction_type",
    [CorrectionType.EDIT_CLOSURE_TIMESTAMP, CorrectionType.UNDO_FINALIZATION],
)
def test_closure_correction_requires_target_closure(correction_type):
    correction = _modern_correction(correction_type, target_closure_id=None)

    with pytest.raises(ValueError, match="target_closure_id"):
        correction.validate()


def test_validate_does_not_mutate_caller_enum_value():
    correction = _modern_correction(CorrectionType.REPLACE_MACHINE_INTERVAL)
    raw_correction_type = "replace_machine_interval"
    correction.correction_type = raw_correction_type

    correction.validate()

    assert type(correction.correction_type) is str
    assert correction.correction_type is raw_correction_type
    assert correction.to_json()["correction_type"] == raw_correction_type


def test_from_json_runs_modern_structural_validation():
    payload = _modern_correction(CorrectionType.REPLACE_MACHINE_INTERVAL).to_json()
    payload["performed_at"] = "2026-07-15T10:12:00"

    with pytest.raises(ValueError, match="performed_at must be timezone-aware"):
        ExecutionEventCorrection.from_json(payload)


def test_validate_requires_target_event_ids():
    correction = ExecutionEventCorrection(
        correction_id="corr-002",
        action=CorrectionAction.CANCEL_EVENT,
        target_event_ids=[],
        requested_by="alice.admin",
        requested_at=datetime(2026, 4, 1, 10, 15, 0),
    )

    with pytest.raises(ValueError, match="target_event_ids must contain at least one event ID"):
        correction.validate()


def test_validate_requires_replacement_events_for_replace_interval():
    correction = ExecutionEventCorrection(
        correction_id="corr-003",
        action=CorrectionAction.REPLACE_INTERVAL,
        target_event_ids=["evt-1", "evt-2"],
        requested_by="alice.admin",
        requested_at=datetime(2026, 4, 1, 10, 15, 0),
    )

    with pytest.raises(
        ValueError,
        match="replacement_events are required for insert_missing_event and replace_interval",
    ):
        correction.validate()


def test_validate_rejects_replacement_events_for_cancel():
    correction = ExecutionEventCorrection(
        correction_id="corr-004",
        action=CorrectionAction.CANCEL_EVENT,
        target_event_ids=["evt-1"],
        requested_by="alice.admin",
        requested_at=datetime(2026, 4, 1, 10, 15, 0),
        replacement_events=[
            CorrectionReplacementEvent(
                event_type=ExecutionEventType.EXECUTION_END,
                timestamp=datetime(2026, 3, 15, 14, 30, 0),
            )
        ],
    )

    with pytest.raises(ValueError, match="replacement_events are not allowed for cancel_event"):
        correction.validate()
