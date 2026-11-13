"""Constant slots: a data model may require typed constants next to signals.

A ``constants:`` entry compiles to a slot of kind ``constant``, inherits
through ``extends`` like any slot, and refuses the fields only a signal has.
"""

import pytest
import yaml

from colca_data_contracts.data_models import CompileError, compile_models


def _write(tmp_path, filename: str, document: dict) -> None:
    (tmp_path / filename).write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")


DRIVE = {
    "name": "Drive",
    "version": "1.0",
    "signals": [{"name": "power", "data_type": "number", "semantic_type": "active-power", "unit": "kW"}],
    "constants": [
        {
            "name": "rated_power",
            "data_type": "number",
            "semantic_type": "rated-power",
            "unit": "kW",
            "description": "  Nameplate.  ",
        },
        {"name": "serial", "data_type": "string", "required": False},
    ],
}


def _slots(manifest: dict) -> dict:
    return {slot["key"]: slot for slot in manifest["slots"]}


def test_a_constant_compiles_to_a_constant_slot(tmp_path):
    _write(tmp_path, "drive.yaml", DRIVE)
    (manifest,) = compile_models(tmp_path)
    rated = _slots(manifest)["rated_power"]
    assert rated == {
        "key": "rated_power",
        "kind": "constant",
        "data_type": "number",
        "semantic_type": "rated-power",
        "unit": "kW",
        "required": True,
        "description": "Nameplate.",
        "declared_by": "Drive",
        "child_model": None,
        "entity_name": None,
        "access": None,
        "process_tolerance": None,
        "relationships": [],
        "value_definitions": {},
    }
    assert _slots(manifest)["serial"]["required"] is False
    assert _slots(manifest)["power"]["kind"] == "measured"


def test_a_constant_tag_stub_applies_to_constants(tmp_path):
    _write(tmp_path, "drive.yaml", DRIVE)
    (manifest,) = compile_models(tmp_path)
    stubs = {tag["name"]: tag for tag in manifest["semantic_tags"]}
    assert stubs["rated-power"]["applies_to"] == ["constant"]
    assert stubs["rated-power"]["data_type"] == "number"
    assert stubs["active-power"]["applies_to"] == ["signal"]


def test_constants_inherit_and_override_through_extends(tmp_path):
    _write(tmp_path, "drive.yaml", DRIVE)
    _write(
        tmp_path,
        "pump.yaml",
        {
            "name": "Pump",
            "version": "1.0",
            "extends": ["Drive"],
            "constants": [{"name": "serial", "data_type": "string", "required": True}],
        },
    )
    pump = _slots({m["name"]: m for m in compile_models(tmp_path)}["Pump"])
    assert pump["rated_power"]["kind"] == "constant"
    assert pump["rated_power"]["declared_by"] == "Drive"
    assert pump["serial"]["required"] is True
    assert pump["serial"]["declared_by"] == "Pump"


def test_unknown_constant_data_type_is_a_compile_error(tmp_path):
    _write(
        tmp_path,
        "bad.yaml",
        {"name": "Bad", "version": "1.0", "constants": [{"name": "c", "data_type": "float64"}]},
    )
    with pytest.raises(CompileError, match=r"Bad\.c: unknown data_type 'float64'"):
        compile_models(tmp_path)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("computed", True),
        ("access", "read_write"),
        ("process_tolerance", 1),
        ("relationships", []),
        ("value_definitions", {0: "OFF"}),
    ],
)
def test_a_constant_refuses_signal_only_fields(tmp_path, field, value):
    _write(
        tmp_path,
        "bad.yaml",
        {"name": "Bad", "version": "1.0", "constants": [{"name": "c", "data_type": "integer", field: value}]},
    )
    with pytest.raises(CompileError, match=rf"Bad\.c: a constant slot cannot declare {field}"):
        compile_models(tmp_path)


def test_one_key_in_two_sections_is_a_compile_error(tmp_path):
    _write(
        tmp_path,
        "bad.yaml",
        {
            "name": "Bad",
            "version": "1.0",
            "signals": [{"name": "x", "data_type": "number"}],
            "constants": [{"name": "x", "data_type": "number"}],
        },
    )
    with pytest.raises(CompileError, match=r"Bad\.x: slot key declared twice"):
        compile_models(tmp_path)


def test_a_signal_link_to_a_constant_slot_is_a_compile_error(tmp_path):
    _write(
        tmp_path,
        "bad.yaml",
        {
            "name": "Bad",
            "version": "1.0",
            "signals": [
                {
                    "name": "speed",
                    "data_type": "number",
                    "relationships": [{"type": "upper_limit", "target": "max_speed"}],
                }
            ],
            "constants": [{"name": "max_speed", "data_type": "number"}],
        },
    )
    with pytest.raises(CompileError, match=r"target Bad\.max_speed is not a signal"):
        compile_models(tmp_path)


def test_a_tag_shared_by_a_signal_and_a_constant_must_agree_on_data_type(tmp_path):
    _write(
        tmp_path,
        "bad.yaml",
        {
            "name": "Bad",
            "version": "1.0",
            "signals": [{"name": "s", "data_type": "number", "semantic_type": "temperature"}],
            "constants": [{"name": "c", "data_type": "integer", "semantic_type": "temperature"}],
        },
    )
    with pytest.raises(CompileError, match="semantic_type 'temperature' is declared with data_type"):
        compile_models(tmp_path)
