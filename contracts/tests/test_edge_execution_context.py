"""Tests for EdgeExecutionContext semantic data contract."""

from datetime import datetime, timezone

import pytest

from colca_data_contracts.semantic import edge_execution_context as execution_contract
from colca_data_contracts.semantic.edge_execution_context import (
    EdgeExecutionContext,
    ExecutionEventSource,
    ExecutionEventType,
)


AWARE_TIMESTAMP = datetime(2026, 7, 15, 10, 30, tzinfo=timezone.utc)
SESSION_ID = "9e2c2c0c-0000-4000-8000-000000000001"


def _enum_member(enum_name: str, member: str, fallback: str):
    enum_type = getattr(execution_contract, enum_name, None)
    return getattr(enum_type, member) if enum_type is not None else fallback


def _valid_event(event_type: ExecutionEventType, **overrides) -> EdgeExecutionContext:
    values = {
        "event_type": event_type,
        "event_id": "evt-123",
        "asset_id": 17,
        "lot_nr": None,
        "timestamp": AWARE_TIMESTAMP,
        "source": ExecutionEventSource.OPERATOR,
        "order_nr": 2042,
        "step_id": 5001,
        "operator_id": "employee:42",
    }
    if event_type in (
        ExecutionEventType.EXECUTION_START,
        ExecutionEventType.EXECUTION_END,
    ):
        values.update(
            segment_kind=_enum_member("ExecutionSegmentKind", "MACHINE", "machine"),
            session_id=SESSION_ID,
            subject_type=_enum_member("ExecutionSubjectType", "EQUIPMENT", "equipment"),
            subject_id="tcdb-aus:17",
            subject_label="Autoclave A2",
        )
    elif event_type == ExecutionEventType.DISRUPTION_END:
        values.update(order_nr=None, step_id=None)
    values.update(overrides)
    return EdgeExecutionContext(**values)


# --- Enum members ---


def test_disruption_start_enum_exists():
    assert ExecutionEventType.DISRUPTION_START == "disruption_start"


def test_disruption_end_enum_exists():
    assert ExecutionEventType.DISRUPTION_END == "disruption_end"


def test_work_step_finalized_event_type_exists():
    assert ExecutionEventType.WORK_STEP_FINALIZED == "work_step_finalized"


def test_cluster_scrapped_event_type_exists():
    assert ExecutionEventType.CLUSTER_SCRAPPED == "cluster_scrapped"


def test_execution_segment_kind_values():
    assert execution_contract.ExecutionSegmentKind.OPERATOR == "operator"
    assert execution_contract.ExecutionSegmentKind.MACHINE == "machine"


def test_execution_subject_type_values():
    assert execution_contract.ExecutionSubjectType.OPERATOR == "operator"
    assert execution_contract.ExecutionSubjectType.EQUIPMENT == "equipment"


# --- operator_id serialization round-trip ---


def test_operator_id_round_trip():
    ctx = EdgeExecutionContext(
        event_type=ExecutionEventType.EXECUTION_START,
        asset_id=1,
        lot_nr=100,
        timestamp=AWARE_TIMESTAMP,
        source=ExecutionEventSource.OPERATOR,
        event_id="evt-123",
        order_nr=42,
        step_id=7,
        operator_id="employee:abc-123",
        segment_kind=_enum_member("ExecutionSegmentKind", "OPERATOR", "operator"),
        session_id=SESSION_ID,
        subject_type=_enum_member("ExecutionSubjectType", "OPERATOR", "operator"),
        subject_id="employee:abc-123",
    )
    json_data = ctx.to_json()
    assert json_data["operator_id"] == "employee:abc-123"
    assert json_data["event_id"] == "evt-123"

    restored = EdgeExecutionContext.from_json(json_data)
    assert restored.operator_id == "employee:abc-123"
    assert restored.event_id == "evt-123"


def test_operator_id_none_omitted_from_json():
    ctx = _valid_event(
        ExecutionEventType.EXECUTION_END,
        source=ExecutionEventSource.PLC_AUTO,
        operator_id=None,
    )
    json_data = ctx.to_json()
    assert "operator_id" not in json_data


def test_operator_id_none_from_json_without_field():
    json_data = _valid_event(
        ExecutionEventType.EXECUTION_END,
        source=ExecutionEventSource.PLC_AUTO,
        operator_id=None,
    ).to_json()
    ctx = EdgeExecutionContext.from_json(json_data)
    assert ctx.operator_id is None


def test_context_round_trip():
    ctx = _valid_event(
        ExecutionEventType.EXECUTION_START,
        context={
            "traceability_order_item_id": 1234,
            "cluster_id": 77,
            "cluster_number": "C-077",
            "subprocess_name": "Lamination",
        },
    )
    json_data = ctx.to_json()
    assert json_data["context"]["traceability_order_item_id"] == 1234
    assert json_data["context"]["cluster_number"] == "C-077"

    restored = EdgeExecutionContext.from_json(json_data)
    assert restored.context == ctx.context


def test_empty_context_round_trip_is_lossless():
    ctx = _valid_event(ExecutionEventType.EXECUTION_START, context={})

    restored = EdgeExecutionContext.from_json(ctx.to_json())

    assert restored.context == {}


def test_lot_nr_is_optional_for_round_trip():
    ctx = _valid_event(ExecutionEventType.EXECUTION_START)
    json_data = ctx.to_json()
    assert "lot_nr" not in json_data

    restored = EdgeExecutionContext.from_json(json_data)
    assert restored.lot_nr is None


def test_execution_context_session_fields_round_trip():
    context = _valid_event(ExecutionEventType.EXECUTION_START)

    restored = EdgeExecutionContext.from_json(context.to_json())

    assert restored.segment_kind == execution_contract.ExecutionSegmentKind.MACHINE
    assert restored.session_id == SESSION_ID
    assert restored.subject_type == execution_contract.ExecutionSubjectType.EQUIPMENT
    assert restored.subject_id == "tcdb-aus:17"
    assert restored.subject_label == "Autoclave A2"


def test_cluster_scrapped_round_trip_has_no_session_fields():
    context = _valid_event(ExecutionEventType.CLUSTER_SCRAPPED)

    restored_json = EdgeExecutionContext.from_json(context.to_json()).to_json()

    assert restored_json["event_type"] == "cluster_scrapped"
    assert "segment_kind" not in restored_json
    assert "session_id" not in restored_json
    assert "subject_type" not in restored_json
    assert "subject_id" not in restored_json
    assert "subject_label" not in restored_json


@pytest.mark.parametrize(
    "event_type",
    [
        ExecutionEventType.EXECUTION_START,
        ExecutionEventType.EXECUTION_END,
        ExecutionEventType.WORK_STEP_FINALIZED,
        "cluster_scrapped",
    ],
)
@pytest.mark.parametrize(
    ("field", "invalid_value"),
    [
        ("event_id", None),
        ("event_id", ""),
        ("asset_id", "17"),
        ("asset_id", True),
        ("order_nr", "2042"),
        ("order_nr", True),
        ("step_id", "5001"),
        ("step_id", True),
    ],
)
def test_execution_and_lifecycle_events_require_shared_fields(event_type, field, invalid_value):
    context = _valid_event(event_type, **{field: invalid_value})

    with pytest.raises(ValueError, match=field):
        context.validate()


@pytest.mark.parametrize(
    "event_type",
    [
        ExecutionEventType.EXECUTION_START,
        ExecutionEventType.EXECUTION_END,
    ],
)
@pytest.mark.parametrize("field", ["segment_kind", "session_id", "subject_type", "subject_id"])
def test_execution_events_require_session_fields(event_type, field):
    context = _valid_event(event_type, **{field: None})

    with pytest.raises(ValueError, match=field):
        context.validate()


@pytest.mark.parametrize("event_type", [ExecutionEventType.WORK_STEP_FINALIZED, "cluster_scrapped"])
@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("segment_kind", "machine"),
        ("session_id", SESSION_ID),
        ("subject_type", "equipment"),
        ("subject_id", "tcdb-aus:17"),
        ("subject_label", "Autoclave A2"),
    ],
)
def test_lifecycle_events_reject_session_fields(event_type, field, value):
    context = _valid_event(event_type, **{field: value})

    with pytest.raises(ValueError, match=field):
        context.validate()


@pytest.mark.parametrize(
    "event_type",
    [
        ExecutionEventType.EXECUTION_START,
        ExecutionEventType.EXECUTION_END,
        ExecutionEventType.DISRUPTION_START,
        ExecutionEventType.DISRUPTION_END,
        ExecutionEventType.WORK_STEP_FINALIZED,
        "cluster_scrapped",
    ],
)
def test_validate_rejects_naive_timestamp(event_type):
    context = _valid_event(event_type, timestamp=datetime(2026, 7, 15, 10, 30))

    with pytest.raises(ValueError, match="timestamp must be timezone-aware"):
        context.validate()


@pytest.mark.parametrize("operator_id", ["employee", ":42", "employee:", ""])
def test_validate_rejects_malformed_optional_operator_id(operator_id):
    context = _valid_event(ExecutionEventType.WORK_STEP_FINALIZED, operator_id=operator_id)

    with pytest.raises(ValueError, match="operator_id must be namespaced"):
        context.validate()


@pytest.mark.parametrize("subject_id", ["employee", ":42", "employee:", ""])
def test_validate_rejects_malformed_session_subject_id(subject_id):
    context = _valid_event(ExecutionEventType.EXECUTION_START, subject_id=subject_id)

    with pytest.raises(ValueError, match="subject_id must be namespaced"):
        context.validate()


@pytest.mark.parametrize(
    ("segment_kind", "subject_type"),
    [("operator", "equipment"), ("machine", "operator")],
)
def test_validate_rejects_incoherent_segment_subject(segment_kind, subject_type):
    context = _valid_event(
        ExecutionEventType.EXECUTION_START,
        segment_kind=segment_kind,
        subject_type=subject_type,
    )

    with pytest.raises(ValueError, match="segment_kind and subject_type are incoherent"):
        context.validate()


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("event_type", "unknown"),
        ("source", "unknown"),
        ("segment_kind", "unknown"),
        ("subject_type", "unknown"),
    ],
)
def test_from_json_rejects_unknown_enum_value(field, value):
    payload = _valid_event(ExecutionEventType.EXECUTION_START).to_json()
    payload[field] = value

    with pytest.raises(ValueError):
        EdgeExecutionContext.from_json(payload)


def test_from_json_runs_structural_validation():
    payload = _valid_event(ExecutionEventType.EXECUTION_START).to_json()
    payload.pop("event_id")

    with pytest.raises(ValueError, match="event_id"):
        EdgeExecutionContext.from_json(payload)


# --- validate() requires order_nr for DISRUPTION_START ---


def test_validate_disruption_start_requires_order_nr():
    ctx = _valid_event(ExecutionEventType.DISRUPTION_START, order_nr=None)
    with pytest.raises(ValueError, match="order_nr is required for disruption_start"):
        ctx.validate()


def test_validate_disruption_start_requires_step_id():
    ctx = _valid_event(ExecutionEventType.DISRUPTION_START, step_id=None)
    with pytest.raises(ValueError, match="step_id is required for disruption_start"):
        ctx.validate()


def test_validate_disruption_start_passes_with_required_fields():
    ctx = _valid_event(ExecutionEventType.DISRUPTION_START)
    ctx.validate()  # should not raise


# --- validate() does NOT require order_nr for DISRUPTION_END ---


def test_validate_disruption_end_does_not_require_order_nr():
    ctx = _valid_event(ExecutionEventType.DISRUPTION_END)
    ctx.validate()  # should not raise


def test_validate_disruption_end_does_not_require_step_id():
    ctx = _valid_event(ExecutionEventType.DISRUPTION_END)
    ctx.validate()  # should not raise
