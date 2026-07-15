"""Tests for the MachineState contract enums and their consistency with the
machinestate.yaml interface.

The point of these tests: the ordinal ints are a wire/storage contract
(value_number in the historian, Grafana value-mappings, OEE routing). They
must never drift silently — not between releases, and not between the Python
IntEnums and the YAML enum_values that the UI / dm / edge-api consume.
"""

from pathlib import Path

import yaml

from colca_data_contracts.machine_state import (
    MachineState,
    OperatingMode,
    StateReason,
)

_INTERFACE_YAML = (
    Path(__file__).resolve().parent.parent
    / "src"
    / "colca_data_contracts"
    / "interfaces"
    / "machinestate.yaml"
)


def _signal(name: str) -> dict:
    spec = yaml.safe_load(_INTERFACE_YAML.read_text())
    for sig in spec["signals"]:
        if sig["name"] == name:
            return sig
    raise AssertionError(f"signal {name!r} not found in machinestate.yaml")


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
    """Naming decision (2026-06-15): the de-energized state is OFFLINE, not OFF.
    Guard against a regression to the old name."""
    assert MachineState(1).name == "OFFLINE"
    assert not hasattr(MachineState, "OFF")


def test_unknown_is_zero_everywhere():
    """UNKNOWN/UNCLASSIFIED are the honest defaults and must be the zero value
    of their axis so an unset signal degrades to 'don't know', never to a
    confident wrong state."""
    assert MachineState.UNKNOWN == 0
    assert OperatingMode.UNKNOWN == 0
    assert StateReason.UNCLASSIFIED == 0


def test_state_reason_is_fixed_14():
    assert len(StateReason) == 14
    assert StateReason.STARVED == 3
    assert StateReason.BLOCKED == 4
    assert StateReason.GRADE_CHANGE == 13


# --- consistency: the YAML enum_values mirror the IntEnums exactly ---


def _assert_yaml_matches_enum(signal_name: str, enum_cls):
    enum_values = _signal(signal_name)["enum_values"]
    # index-for-index == value-for-value, name-for-name
    assert enum_values == [member.name for member in enum_cls], (
        f"{signal_name} enum_values drifted from {enum_cls.__name__}"
    )
    for index, name in enumerate(enum_values):
        assert enum_cls[name].value == index, (
            f"{signal_name}: {name} is index {index} in YAML but "
            f"{enum_cls[name].value} in {enum_cls.__name__}"
        )


def test_machine_state_yaml_matches_enum():
    _assert_yaml_matches_enum("machine_state", MachineState)


def test_operating_mode_yaml_matches_enum():
    _assert_yaml_matches_enum("operating_mode", OperatingMode)


def test_state_reason_yaml_matches_enum():
    _assert_yaml_matches_enum("state_reason", StateReason)


# --- contract shape ---


def test_only_machine_state_is_required():
    """machine_state is the always-emittable spine; the other axes are optional
    and degrade independently."""
    spec = yaml.safe_load(_INTERFACE_YAML.read_text())
    required = {s["name"] for s in spec["signals"] if s.get("required")}
    assert required == {"machine_state"}
    assert spec["extends"] == ["Machine"]
