"""The four builtin platform YAMLs (machine, machinestate, mbmachine,
oee_producer.yaml) compile and reproduce the semantic content
authored before: same names, enums, required flags, and descriptions as before the
Python-class era. `compile_models()` with no `source_dir` loads exactly
these -- the package's own `data_models/` directory."""
from colca_data_contracts.data_models import compile_models


def _manifests() -> dict:
    return {m["name"]: m for m in compile_models()}


def _slots(manifest: dict) -> dict:
    return {s["key"]: s for s in manifest["slots"]}


def test_builtins_compile():
    manifests = _manifests()
    for name in ("Machine", "MachineState", "MBMachine", "OEEProducer"):
        assert name in manifests, name


def test_machine_slots_match_retired_yaml():
    slots = _slots(_manifests()["Machine"])
    assert set(slots) == {"heartbeat", "is_connected"}
    assert slots["heartbeat"].get("data_type") == "boolean"
    assert slots["heartbeat"]["required"] is True
    assert slots["heartbeat"]["semantic_type"] == "heartbeat"
    assert slots["is_connected"]["semantic_type"] == "connectivity"


def test_machinestate_extends_machine_and_keeps_enums():
    manifest = _manifests()["MachineState"]
    assert manifest["extends"] == ["Machine"]
    slots = _slots(manifest)
    assert {"heartbeat", "is_connected"} <= set(slots)  # inherited
    state = slots["machine_state"]
    assert state["kind"] == "computed"
    assert state["enum"] == ["UNKNOWN", "OFFLINE", "DOWN", "IDLE", "EXECUTING"]
    assert state["required"] is True
    assert slots["operating_mode"]["required"] is False
    assert slots["state_reason"]["enum"][:2] == ["UNCLASSIFIED", "LOCAL_FAULT"]


def test_mbmachine_extends_machine():
    manifest = _manifests()["MBMachine"]
    assert manifest["extends"] == ["Machine"]
    slots = _slots(manifest)
    assert {"heartbeat", "is_connected", "machine_status"} <= set(slots)
    assert slots["machine_status"]["kind"] == "measured"


def test_oee_producer_required_flags():
    slots = _slots(_manifests()["OEEProducer"])
    assert slots["part_counter"]["required"] is True
    assert slots["reject_counter"]["required"] is False
    assert slots["error_active"]["required"] is False


def test_no_slot_carries_inputs_or_trigger():
    for manifest in _manifests().values():
        for slot in manifest["slots"]:
            assert "inputs" not in slot
            assert "trigger" not in slot
