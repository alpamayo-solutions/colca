"""The MachineState enums and the compiled ``MachineState`` data model agree.

The ordinals are stored, so they must not change between releases or drift
from the model's ``enum`` slot values.
"""

from colca_data_contracts.data_models import compile_models
from colca_data_contracts.machine_state import (
    MachineState,
    OperatingMode,
    StateReason,
)


def _manifest() -> dict:
    for manifest in compile_models():
        if manifest["name"] == "MachineState":
            return manifest
    raise AssertionError("MachineState data model not found in builtin manifests")


def _slot(name: str) -> dict:
    manifest = _manifest()
    for slot in manifest["slots"]:
        if slot["key"] == name:
            return slot
    raise AssertionError(f"slot {name!r} not found in the MachineState manifest")


# --- ordinal contract: lock the integers, they are stored in value_number ---


def test_machine_state_ordinals():
    """The spine values are a storage contract — lock them explicitly."""
    assert MachineState.UNKNOWN == 0
    assert MachineState.OFFLINE == 1
    assert MachineState.DOWN == 2
    assert MachineState.IDLE == 3
    assert MachineState.EXECUTING == 4
    assert len(MachineState) == 5


def test_offline_not_off():
    """The de-energized state is called OFFLINE."""
    assert MachineState(1).name == "OFFLINE"
    assert not hasattr(MachineState, "OFF")


def test_unknown_is_zero_everywhere():
    """UNKNOWN and UNCLASSIFIED are zero, so an unset value means "don't know"."""
    assert MachineState.UNKNOWN == 0
    assert OperatingMode.UNKNOWN == 0
    assert StateReason.UNCLASSIFIED == 0


def test_state_reason_is_fixed_14():
    assert len(StateReason) == 14
    assert StateReason.STARVED == 3
    assert StateReason.BLOCKED == 4
    assert StateReason.GRADE_CHANGE == 13


# --- consistency: the manifest's enum slots mirror the IntEnums exactly ---


def _assert_manifest_matches_enum(slot_name: str, enum_cls):
    enum_values = _slot(slot_name)["enum"]
    # index-for-index == value-for-value, name-for-name
    assert enum_values == [member.name for member in enum_cls], f"{slot_name} enum drifted from {enum_cls.__name__}"
    for index, name in enumerate(enum_values):
        assert enum_cls[name].value == index, (
            f"{slot_name}: {name} is index {index} in the manifest but {enum_cls[name].value} in {enum_cls.__name__}"
        )


def test_machine_state_manifest_matches_enum():
    _assert_manifest_matches_enum("machine_state", MachineState)


def test_operating_mode_manifest_matches_enum():
    _assert_manifest_matches_enum("operating_mode", OperatingMode)


def test_state_reason_manifest_matches_enum():
    _assert_manifest_matches_enum("state_reason", StateReason)


# --- contract shape ---


def test_only_machine_state_is_required():
    """machine_state is the always-emittable spine; the other axes are optional
    and degrade independently."""
    manifest = _manifest()
    required = {slot["key"] for slot in manifest["slots"] if slot["required"]}
    assert required == {"machine_state", "heartbeat", "is_connected"}
    assert manifest["extends"] == ["Machine"]
