"""The YAML data-model loader: resolution, validation, and determinism.

Every model source under a `compile_models(source_dir)` call is a plain
`*.yaml` file (see `data_models/loader.py`'s module docstring for the
schema). These tests build small YAML fixtures with `tmp_path` rather than
compiling the real platform models -- `test_data_models_builtin.py` is the
dedicated equivalence test for those.
"""
import json

import pytest
import yaml

from colca_data_contracts.data_models import CANONICAL_DATA_TYPES, CompileError, compile_models, manifest_json


def _write(tmp_path, filename: str, document: dict) -> None:
    (tmp_path / filename).write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")


PUMP = {
    "name": "Pump",
    "version": "1.0",
    "description": "A test pump.",
    "signals": [
        {"name": "flow", "data_type": "number", "semantic_type": "flow", "unit": "l/min"},
        {
            "name": "power",
            "data_type": "number",
            "semantic_type": "power",
            "computed": True,
        },
    ],
}


def test_manifest_shape(tmp_path):
    _write(tmp_path, "pump.yaml", PUMP)
    (manifest,) = compile_models(tmp_path)
    assert manifest["name"] == "Pump"
    assert manifest["version"] == "1.0"
    assert manifest["description"] == "A test pump."
    assert manifest["extends"] == []
    keys = [slot["key"] for slot in manifest["slots"]]
    assert keys == sorted(keys)
    power = next(s for s in manifest["slots"] if s["key"] == "power")
    assert power["kind"] == "computed"
    assert power["declared_by"] == "Pump"
    flow = next(s for s in manifest["slots"] if s["key"] == "flow")
    assert flow["kind"] == "measured"


def test_slots_carry_no_inputs_or_trigger_keys(tmp_path):
    _write(tmp_path, "pump.yaml", PUMP)
    (manifest,) = compile_models(tmp_path)
    for slot in manifest["slots"]:
        assert "inputs" not in slot
        assert "trigger" not in slot


def test_compiling_twice_is_byte_identical(tmp_path):
    _write(tmp_path, "pump.yaml", PUMP)
    first = manifest_json(compile_models(tmp_path))
    second = manifest_json(compile_models(tmp_path))
    assert first == second
    json.loads(first)  # valid JSON


def test_compiling_twice_is_byte_identical_with_child_slots(tmp_path):
    """The determinism guarantee above still holds when a manifest carries
    child slots, not just signal slots -- child_model/entity_name resolution
    must not introduce any nondeterministic ordering (e.g. dict/set iteration)."""
    _write(tmp_path, "bearing.yaml", BEARING)
    _write(tmp_path, "motor.yaml", _motor([
        {"name": "drive_end_bearing", "child_model": "Bearing", "required": False, "description": "DE bearing."},
    ]))
    first = manifest_json(compile_models(tmp_path))
    second = manifest_json(compile_models(tmp_path))
    assert first == second
    json.loads(first)  # valid JSON


def test_semantic_tags_collects_referenced_names(tmp_path):
    _write(tmp_path, "pump.yaml", PUMP)
    (manifest,) = compile_models(tmp_path)
    assert [t["name"] for t in manifest["semantic_tags"]] == ["flow", "power"]


def test_divergent_semantic_type_data_type_across_models_is_a_compile_error(tmp_path):
    """Two models referencing the same semantic_type name with different
    data_type would each seed a different tag stub; the seeder's
    existing-wins rule then makes whichever seeds first silently win. Reject
    the divergence at load time instead, naming both models and the tag."""
    _write(tmp_path, "pump.yaml", {
        "name": "Pump",
        "version": "1.0",
        "signals": [{"name": "reading", "data_type": "number", "semantic_type": "temperature"}],
    })
    _write(tmp_path, "sensor.yaml", {
        "name": "Sensor",
        "version": "1.0",
        "signals": [{"name": "reading", "data_type": "string", "semantic_type": "temperature"}],
    })
    with pytest.raises(CompileError) as exc_info:
        compile_models(tmp_path)
    message = str(exc_info.value)
    assert "'temperature'" in message
    assert "'Pump'" in message and "'Sensor'" in message
    assert "'number'" in message and "'string'" in message


def test_same_semantic_type_data_type_across_models_stays_legal(tmp_path):
    """The denominator: the same tag name declared with the SAME data_type by
    multiple models is legal and produces exactly one stub -- the check above
    is refusing the divergence, not shared tag references in general."""
    _write(tmp_path, "pump.yaml", {
        "name": "Pump",
        "version": "1.0",
        "signals": [{"name": "reading", "data_type": "number", "semantic_type": "temperature"}],
    })
    _write(tmp_path, "sensor.yaml", {
        "name": "Sensor",
        "version": "1.0",
        "signals": [{"name": "reading", "data_type": "number", "semantic_type": "temperature"}],
    })
    manifests = {m["name"]: m for m in compile_models(tmp_path)}
    for model_name in ("Pump", "Sensor"):
        (tag,) = manifests[model_name]["semantic_tags"]
        assert tag["name"] == "temperature"
        assert tag["data_type"] == "number"


def test_unknown_data_type_is_a_compile_error(tmp_path):
    _write(tmp_path, "broken.yaml", {
        "name": "Broken",
        "version": "1.0",
        "signals": [{"name": "x", "data_type": "wat"}],
    })
    with pytest.raises(CompileError, match=r"Broken\.x.*unknown data_type"):
        compile_models(tmp_path)


# --- extends: flattening, declared_by, unknown parent, cycles ---


BASE = {
    "name": "Base",
    "version": "1.0",
    "description": "A base model.",
    "signals": [{"name": "a", "data_type": "number"}],
}

CHILD = {
    "name": "Child",
    "version": "1.0",
    "description": "Extends Base.",
    "extends": ["Base"],
    "signals": [
        {"name": "b", "data_type": "number"},
        # overrides Base's "a" -- child-over-parent
        {"name": "a", "data_type": "number", "required": False},
    ],
}


def test_extends_flattens_and_records_declared_by(tmp_path):
    _write(tmp_path, "base.yaml", BASE)
    _write(tmp_path, "child.yaml", CHILD)
    manifests = {m["name"]: m for m in compile_models(tmp_path)}
    assert manifests["Child"]["extends"] == ["Base"]
    slots = {s["key"]: s for s in manifests["Child"]["slots"]}
    assert set(slots) == {"a", "b"}
    assert slots["b"]["declared_by"] == "Child"


def test_child_slot_overrides_parent_slot_and_declared_by_moves(tmp_path):
    _write(tmp_path, "base.yaml", BASE)
    _write(tmp_path, "child.yaml", CHILD)
    manifests = {m["name"]: m for m in compile_models(tmp_path)}
    a = {s["key"]: s for s in manifests["Child"]["slots"]}["a"]
    assert a["required"] is False  # Child's override, not Base's default True
    assert a["declared_by"] == "Child"
    # Base itself keeps its own value undisturbed.
    base_a = {s["key"]: s for s in manifests["Base"]["slots"]}["a"]
    assert base_a["required"] is True
    assert base_a["declared_by"] == "Base"


def test_unknown_extends_parent_is_a_compile_error(tmp_path):
    _write(tmp_path, "orphan.yaml", {
        "name": "Orphan",
        "version": "1.0",
        "extends": ["Nonexistent"],
    })
    with pytest.raises(CompileError, match=r"Orphan.*extends unknown model 'Nonexistent'"):
        compile_models(tmp_path)


def test_extends_cycle_is_a_compile_error_with_the_full_path(tmp_path):
    _write(tmp_path, "a.yaml", {"name": "A", "version": "1.0", "extends": ["B"]})
    _write(tmp_path, "b.yaml", {"name": "B", "version": "1.0", "extends": ["A"]})
    with pytest.raises(CompileError) as exc_info:
        compile_models(tmp_path)
    assert "A -> B -> A" in str(exc_info.value)


# --- children: child_model, entity_name, cycles, duplicate names ---


BEARING = {
    "name": "Bearing",
    "version": "1.0",
    "description": "A bearing.",
    "signals": [{"name": "vibration", "data_type": "number", "semantic_type": "vibration"}],
}


def _motor(children):
    return {
        "name": "Motor",
        "version": "1.0",
        "description": "A motor with mandated children.",
        "children": children,
    }


def test_child_slot_records_child_model_and_entity_name(tmp_path):
    _write(tmp_path, "bearing.yaml", BEARING)
    _write(tmp_path, "motor.yaml", _motor([
        {"name": "housing", "entity_name": "Housing"},
        {"name": "drive_end_bearing", "child_model": "Bearing"},
    ]))
    manifests = {m["name"]: m for m in compile_models(tmp_path)}
    slots = {s["key"]: s for s in manifests["Motor"]["slots"]}

    bearing_slot = slots["drive_end_bearing"]
    assert bearing_slot["kind"] == "child"
    assert bearing_slot["child_model"] == "Bearing"
    assert bearing_slot["entity_name"] == "drive_end_bearing"  # defaults to slot key
    assert bearing_slot["data_type"] == ""

    housing_slot = slots["housing"]
    assert housing_slot["kind"] == "child"
    assert housing_slot["child_model"] is None
    assert housing_slot["entity_name"] == "Housing"


def test_child_slot_required_false_and_description_are_recorded(tmp_path):
    _write(tmp_path, "bearing.yaml", BEARING)
    _write(tmp_path, "motor.yaml", _motor([
        {"name": "drive_end_bearing", "child_model": "Bearing", "required": False, "description": "DE bearing."},
    ]))
    manifests = {m["name"]: m for m in compile_models(tmp_path)}
    slot = {s["key"]: s for s in manifests["Motor"]["slots"]}["drive_end_bearing"]
    assert slot["required"] is False
    assert slot["description"] == "DE bearing."


def test_child_slot_inherited_across_extends_flattens_with_declared_by(tmp_path):
    """A child slot declared on a parent model and never overridden by the
    child still shows up in the extending model's flattened slots, with
    declared_by naming the model that actually declared it -- not the model
    that merely inherited it."""
    _write(tmp_path, "bearing.yaml", BEARING)
    _write(tmp_path, "motor.yaml", _motor([
        {"name": "drive_end_bearing", "child_model": "Bearing"},
    ]))
    _write(tmp_path, "servo.yaml", {
        "name": "ServoMotor",
        "version": "1.0",
        "description": "Extends Motor.",
        "extends": ["Motor"],
    })
    manifests = {m["name"]: m for m in compile_models(tmp_path)}
    slots = {s["key"]: s for s in manifests["ServoMotor"]["slots"]}
    child = slots["drive_end_bearing"]
    assert child["kind"] == "child"
    assert child["child_model"] == "Bearing"
    assert child["declared_by"] == "Motor"


def test_non_child_slot_carries_none_for_the_child_fields(tmp_path):
    _write(tmp_path, "bearing.yaml", BEARING)
    (manifest,) = compile_models(tmp_path)
    (vibration_slot,) = manifest["slots"]
    assert vibration_slot["child_model"] is None
    assert vibration_slot["entity_name"] is None


def test_unregistered_child_model_reference_is_a_compile_error(tmp_path):
    _write(tmp_path, "motor.yaml", _motor([{"name": "sub", "child_model": "Ghost"}]))
    with pytest.raises(CompileError, match=r"Motor\.sub.*child_model 'Ghost'"):
        compile_models(tmp_path)


def test_child_model_cycle_is_a_compile_error_with_the_full_path(tmp_path):
    _write(tmp_path, "a.yaml", {
        "name": "CycleA", "version": "1.0",
        "children": [{"name": "to_b", "child_model": "CycleB"}],
    })
    _write(tmp_path, "b.yaml", {
        "name": "CycleB", "version": "1.0",
        "children": [{"name": "to_a", "child_model": "CycleA"}],
    })
    with pytest.raises(CompileError) as exc_info:
        compile_models(tmp_path)
    assert "CycleA -> to_b -> CycleB -> to_a -> CycleA" in str(exc_info.value)


def test_two_child_slots_naming_the_same_entity_is_a_compile_error(tmp_path):
    """Two child slots of one model resolving to the same `entity_name` can
    never both be satisfied -- one physical child cannot be two slots."""
    _write(tmp_path, "bearing.yaml", BEARING)
    _write(tmp_path, "dual.yaml", {
        "name": "DualBearing",
        "version": "1.0",
        "children": [
            {"name": "left_bearing", "child_model": "Bearing", "entity_name": "Bearing"},
            {"name": "right_bearing", "child_model": "Bearing", "entity_name": "Bearing"},
        ],
    })
    with pytest.raises(CompileError) as exc_info:
        compile_models(tmp_path)
    message = str(exc_info.value)
    assert "DualBearing" in message
    assert "'left_bearing'" in message and "'right_bearing'" in message
    assert "'Bearing'" in message


def test_empty_explicit_child_entity_name_is_a_compile_error(tmp_path):
    """`entity_name: ""` is an explicit override that says nothing -- never
    intentional, so it is rejected rather than silently falling back."""
    _write(tmp_path, "bearing.yaml", BEARING)
    _write(tmp_path, "motor.yaml", _motor([
        {"name": "drive_end_bearing", "child_model": "Bearing", "entity_name": ""},
    ]))
    with pytest.raises(CompileError, match=r"Motor\.drive_end_bearing.*entity_name is explicitly empty"):
        compile_models(tmp_path)


def test_omitted_child_entity_name_still_defaults_to_the_slot_key(tmp_path):
    """The denominator for the check above: omitting `entity_name` entirely
    (as opposed to declaring it empty) is legal and keeps defaulting to the
    slot key."""
    _write(tmp_path, "bearing.yaml", BEARING)
    _write(tmp_path, "motor.yaml", _motor([
        {"name": "drive_end_bearing", "child_model": "Bearing"},
    ]))
    manifests = {m["name"]: m for m in compile_models(tmp_path)}
    slot = {s["key"]: s for s in manifests["Motor"]["slots"]}["drive_end_bearing"]
    assert slot["entity_name"] == "drive_end_bearing"


def test_distinct_child_entity_names_still_compile(tmp_path):
    """The denominator: the same shape with distinct names is legal, so the
    check above is refusing the collision and not child slots in general."""
    _write(tmp_path, "bearing.yaml", BEARING)
    _write(tmp_path, "two.yaml", {
        "name": "TwoBearings",
        "version": "1.0",
        "children": [
            {"name": "left_bearing", "child_model": "Bearing", "entity_name": "BearingLeft"},
            {"name": "right_bearing", "child_model": "Bearing", "entity_name": "BearingRight"},
        ],
    })
    manifests = {m["name"]: m for m in compile_models(tmp_path)}
    slots = {s["key"]: s for s in manifests["TwoBearings"]["slots"]}
    assert slots["left_bearing"]["entity_name"] == "BearingLeft"
    assert slots["right_bearing"]["entity_name"] == "BearingRight"


# --- load-level errors: name/version/duplicates ---


def test_missing_version_is_a_compile_error(tmp_path):
    _write(tmp_path, "draft.yaml", {"name": "Draft", "signals": [{"name": "x", "data_type": "number"}]})
    with pytest.raises(CompileError, match="Draft"):
        compile_models(tmp_path)


def test_missing_name_is_a_compile_error(tmp_path):
    (tmp_path / "nameless.yaml").write_text(
        yaml.safe_dump({"version": "1.0"}), encoding="utf-8",
    )
    with pytest.raises(CompileError, match="missing required 'name'"):
        compile_models(tmp_path)


def test_duplicate_data_model_name_is_a_compile_error(tmp_path):
    _write(tmp_path, "one.yaml", {"name": "Twin", "version": "1.0"})
    _write(tmp_path, "two.yaml", {"name": "Twin", "version": "1.0"})
    with pytest.raises(CompileError, match="duplicate data model name 'Twin'"):
        compile_models(tmp_path)


def test_canonical_data_types_matches_golden_vector():
    # test_data_models_vocabulary.py owns the golden-vector comparison; this
    # just confirms the loader package still exports the same set.
    assert CANONICAL_DATA_TYPES == frozenset({"boolean", "integer", "number", "string", "json"})
