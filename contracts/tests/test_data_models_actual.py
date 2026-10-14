"""Setpoint -> actual links on data model signal slots.

A signal slot may name, with ``actual:``, the signal slot that carries its
actual value. The golden vectors in ``vectors/data_model_actual_links.json``
are judged here through ``compile_models`` and by colca's Go suite, so the two
validations cannot drift apart.
"""

import json
from pathlib import Path

import pytest
import yaml

from colca_data_contracts.data_models import CompileError, compile_models

VECTORS_PATH = Path(__file__).resolve().parents[1] / "src/colca_data_contracts/vectors/data_model_actual_links.json"
VECTORS = json.loads(VECTORS_PATH.read_text())["cases"]


def _write(tmp_path, filename: str, document: dict) -> None:
    (tmp_path / filename).write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")


def _document(name: str, slots: list[dict]) -> dict:
    signals, children = [], []
    for slot in slots:
        if slot["kind"] == "child":
            children.append({"name": slot["key"]})
            continue
        signal = {"name": slot["key"], "data_type": slot["data_type"], "computed": slot["kind"] == "computed"}
        if "actual" in slot:
            signal["actual"] = slot["actual"]
        signals.append(signal)
    return {"name": name, "version": "1.0", "signals": signals, "children": children}


@pytest.mark.parametrize("case", VECTORS, ids=[case["name"] for case in VECTORS])
def test_golden_actual_link_vectors(tmp_path, case):
    _write(tmp_path, "model.yaml", _document("Model", case["slots"]))
    if case["valid"]:
        (manifest,) = compile_models(tmp_path)
        slots = {slot["key"]: slot for slot in manifest["slots"]}
        for slot in case["slots"]:
            assert slots[slot["key"]]["actual"] == slot.get("actual")
    else:
        with pytest.raises(CompileError):
            compile_models(tmp_path)


def _press(signals: list[dict], children: list[dict] | None = None) -> dict:
    return {"name": "Press", "version": "1.0", "signals": signals, "children": children or []}


def test_manifest_carries_actual_on_every_slot(tmp_path):
    _write(
        tmp_path,
        "press.yaml",
        _press(
            [
                {"name": "speed", "data_type": "number"},
                {"name": "speed_setpoint", "data_type": "number", "actual": "speed"},
            ],
            [{"name": "spindle"}],
        ),
    )
    (manifest,) = compile_models(tmp_path)
    slots = {slot["key"]: slot for slot in manifest["slots"]}
    assert slots["speed_setpoint"]["actual"] == "speed"
    assert slots["speed"]["actual"] is None
    assert slots["spindle"]["actual"] is None


@pytest.mark.parametrize(
    ("signals", "children", "message"),
    [
        ([{"name": "sp", "data_type": "number", "actual": "pv"}], [], "Press.sp: actual 'pv' is not a slot of Press"),
        ([{"name": "sp", "data_type": "number", "actual": "sp"}], [], "Press.sp: actual names the slot itself"),
        (
            [{"name": "sp", "data_type": "number", "actual": "spindle"}],
            [{"name": "spindle"}],
            "Press.sp: actual 'spindle' is a child slot, not a signal slot",
        ),
        (
            [
                {"name": "sp", "data_type": "number", "actual": "mid"},
                {"name": "mid", "data_type": "number", "actual": "pv"},
                {"name": "pv", "data_type": "number"},
            ],
            [],
            "Press.sp: actual 'mid' is itself a setpoint (actual 'pv'); links cannot chain",
        ),
        (
            [{"name": "sp", "data_type": "string", "actual": "pv"}, {"name": "pv", "data_type": "number"}],
            [],
            "Press.sp: data_type 'string' is not compatible with actual 'pv' data_type 'number'",
        ),
        (
            [
                {"name": "pv", "data_type": "number"},
                {"name": "sp_a", "data_type": "number", "actual": "pv"},
                {"name": "sp_b", "data_type": "number", "actual": "pv"},
            ],
            [],
            "Press: setpoints 'sp_a' and 'sp_b' both name actual 'pv'",
        ),
        ([{"name": "sp", "data_type": "number", "actual": 7}], [], "Press.sp: actual must be the key of a signal slot"),
        (
            [{"name": "pv", "data_type": "number"}],
            [{"name": "spindle", "actual": "pv"}],
            "Press.spindle: a child slot cannot declare actual",
        ),
    ],
)
def test_broken_actual_links_name_the_model_and_slot(tmp_path, signals, children, message):
    _write(tmp_path, "press.yaml", _press(signals, children))
    with pytest.raises(CompileError) as caught:
        compile_models(tmp_path)
    assert message in str(caught.value)


def test_actual_may_name_an_inherited_slot(tmp_path):
    _write(
        tmp_path,
        "machine.yaml",
        {"name": "Machine", "version": "1.0", "signals": [{"name": "speed", "data_type": "number"}]},
    )
    _write(
        tmp_path,
        "press.yaml",
        {
            "name": "Press",
            "version": "1.0",
            "extends": ["Machine"],
            "signals": [{"name": "speed_setpoint", "data_type": "number", "actual": "speed"}],
        },
    )
    manifests = {m["name"]: m for m in compile_models(tmp_path)}
    slots = {slot["key"]: slot for slot in manifests["Press"]["slots"]}
    assert slots["speed_setpoint"]["actual"] == "speed"
    assert slots["speed_setpoint"]["declared_by"] == "Press"


def test_override_that_turns_the_actual_into_a_child_is_caught_after_flattening(tmp_path):
    _write(
        tmp_path,
        "machine.yaml",
        {
            "name": "Machine",
            "version": "1.0",
            "signals": [
                {"name": "speed", "data_type": "number"},
                {"name": "speed_setpoint", "data_type": "number", "actual": "speed"},
            ],
        },
    )
    _write(
        tmp_path,
        "press.yaml",
        {"name": "Press", "version": "1.0", "extends": ["Machine"], "children": [{"name": "speed"}]},
    )
    with pytest.raises(CompileError) as caught:
        compile_models(tmp_path)
    assert "Press.speed_setpoint: actual 'speed' is a child slot" in str(caught.value)
    assert "Machine." not in str(caught.value)
