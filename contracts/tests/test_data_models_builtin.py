"""The builtin models (machine, machinestate, packmlmachine, oee_producer, faultcodes)
compile with the expected names, value definitions, required flags and descriptions.
`compile_models()` without `source_dir` loads exactly these."""

from colca_data_contracts.data_models import compile_models


def _manifests() -> dict:
    return {m["name"]: m for m in compile_models()}


def _slots(manifest: dict) -> dict:
    return {s["key"]: s for s in manifest["slots"]}


def test_builtins_compile():
    manifests = _manifests()
    for name in ("Machine", "MachineState", "PackMLMachine", "OEEProducer", "FaultCodes"):
        assert name in manifests, name


def test_machine_slots_match_retired_yaml():
    slots = _slots(_manifests()["Machine"])
    assert set(slots) == {"heartbeat", "is_connected"}
    assert slots["heartbeat"].get("data_type") == "boolean"
    assert slots["heartbeat"]["required"] is True
    assert slots["heartbeat"]["semantic_type"] == "heartbeat"
    assert slots["is_connected"]["semantic_type"] == "connectivity"


def test_machinestate_extends_machine_and_defines_its_states():
    manifest = _manifests()["MachineState"]
    assert manifest["extends"] == ["Machine"]
    slots = _slots(manifest)
    assert {"heartbeat", "is_connected"} <= set(slots)  # inherited
    state = slots["machine_state"]
    assert state["kind"] == "computed"
    assert state["value_definitions"] == {"0": "UNKNOWN", "1": "OFFLINE", "2": "DOWN", "3": "IDLE", "4": "EXECUTING"}
    assert state["required"] is True
    assert slots["operating_mode"]["required"] is False
    assert slots["state_reason"]["value_definitions"]["1"] == "LOCAL_FAULT"


def test_faultcodes_names_the_active_fault_and_leaves_meanings_to_the_signal():
    slots = _slots(_manifests()["FaultCodes"])
    assert set(slots) == {"fault_code", "warning_code", "fault_text"}
    assert slots["fault_code"]["data_type"] == "integer"
    assert slots["fault_code"]["required"] is True
    assert slots["warning_code"]["required"] is False
    assert slots["fault_text"]["data_type"] == "string"
    # Meanings differ per machine type, so the contract declares none and each
    # fault_code signal keeps its own.
    assert all(slot["value_definitions"] == {} for slot in slots.values())


def test_packmlmachine_extends_machine():
    manifest = _manifests()["PackMLMachine"]
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


def test_energy_consumer_requires_power_energy_and_rated_power():
    manifest = _manifests()["EnergyConsumer"]
    assert manifest["extends"] == []
    slots = _slots(manifest)
    assert set(slots) == {"active_power", "energy_total", "peak_power", "rated_power"}
    expected = {
        "active_power": ("measured", "kW", "active-power", True),
        "energy_total": ("measured", "kWh", "energy-consumption", True),
        "peak_power": ("measured", "kW", "active-power", False),
        "rated_power": ("constant", "kW", "rated-power", True),
    }
    for key, (kind, unit, tag, required) in expected.items():
        slot = slots[key]
        assert (slot["kind"], slot["unit"], slot["semantic_type"], slot["required"]) == (kind, unit, tag, required), key
        assert slot["data_type"] == "number"


def test_energy_supply_is_constants_only():
    slots = _slots(_manifests()["EnergySupply"])
    expected = {
        "energy_price": ("EUR/kWh", "energy-price", True),
        "demand_charge": ("EUR/kW/a", "demand-charge", True),
        "emission_factor": ("g CO2e/kWh", "emission-factor", True),
        "grid_capacity": ("kW", "grid-capacity", False),
    }
    assert set(slots) == set(expected)
    for key, (unit, tag, required) in expected.items():
        slot = slots[key]
        assert slot["kind"] == "constant", key
        assert slot["data_type"] == "number"
        assert (slot["unit"], slot["semantic_type"], slot["required"]) == (unit, tag, required), key
