"""Data-model signal slots: ``access`` and ``relationships``.

The relationship rules themselves are tested in test_signal_relationships.py;
these tests cover how the loader reads, emits and checks them on flattened
models.
"""

import pytest
import yaml

from colca_data_contracts.data_models import CompileError, compile_models


def _write(tmp_path, filename: str, document: dict) -> None:
    (tmp_path / filename).write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")


def _press(signals: list[dict], children: list[dict] | None = None, **extra) -> dict:
    return {"name": "Press", "version": "1.0", "signals": signals, "children": children or [], **extra}


def _slots(tmp_path) -> dict[str, dict]:
    manifests = {manifest["name"]: manifest for manifest in compile_models(tmp_path)}
    return {slot["key"]: slot for slot in manifests["Press"]["slots"]}


def test_slots_carry_access_and_relationships(tmp_path):
    _write(
        tmp_path,
        "press.yaml",
        _press(
            [
                {"name": "speed", "data_type": "number"},
                {
                    "name": "speed_setpoint",
                    "data_type": "number",
                    "access": "read_write",
                    "relationships": [
                        {"type": "upper_limit", "target": "speed_max"},
                        {"type": "actual", "target": "speed"},
                    ],
                },
                {"name": "speed_max", "data_type": "integer"},
            ],
            [{"name": "spindle"}],
        ),
    )
    slots = _slots(tmp_path)
    assert slots["speed"]["access"] == "read"
    assert slots["speed"]["relationships"] == []
    assert slots["speed_setpoint"]["access"] == "read_write"
    # Sorted by type, so the manifest is byte-stable whatever the YAML order.
    assert slots["speed_setpoint"]["relationships"] == [
        {"type": "actual", "target": "speed"},
        {"type": "upper_limit", "target": "speed_max"},
    ]
    assert slots["spindle"]["access"] is None
    assert slots["spindle"]["relationships"] == []


def test_a_relationship_may_name_an_inherited_slot(tmp_path):
    _write(
        tmp_path,
        "machine.yaml",
        {"name": "Machine", "version": "1.0", "signals": [{"name": "speed", "data_type": "number"}]},
    )
    _write(
        tmp_path,
        "press.yaml",
        _press(
            [
                {
                    "name": "speed_setpoint",
                    "data_type": "number",
                    "relationships": [{"type": "actual", "target": "speed"}],
                }
            ],
            extends=["Machine"],
        ),
    )
    assert _slots(tmp_path)["speed_setpoint"]["relationships"] == [{"type": "actual", "target": "speed"}]


def test_an_override_that_turns_the_target_into_a_child_is_caught_after_flattening(tmp_path):
    _write(
        tmp_path,
        "machine.yaml",
        {
            "name": "Machine",
            "version": "1.0",
            "signals": [
                {"name": "speed", "data_type": "number"},
                {
                    "name": "speed_setpoint",
                    "data_type": "number",
                    "relationships": [{"type": "actual", "target": "speed"}],
                },
            ],
        },
    )
    _write(tmp_path, "press.yaml", _press([], [{"name": "speed"}], extends=["Machine"]))
    with pytest.raises(CompileError) as caught:
        compile_models(tmp_path)
    assert "Press.speed_setpoint: actual target Press.speed is not a signal" in str(caught.value)
    assert "Machine." not in str(caught.value)


@pytest.mark.parametrize(
    ("signals", "children", "message"),
    [
        ([{"name": "sp", "data_type": "number", "access": "write"}], [], "Press.sp: unknown access 'write'"),
        (
            [{"name": "sp", "data_type": "number", "relationships": {"type": "actual"}}],
            [],
            "Press.sp: relationships must be a list",
        ),
        (
            [{"name": "sp", "data_type": "number", "relationships": [{"type": "actual", "target": "pv", "extra": 1}]}],
            [],
            "Press.sp: relationships[0] must have exactly a string type and target",
        ),
        (
            [{"name": "sp", "data_type": "number", "relationships": [{"type": "actual", "target": "pv"}]}],
            [],
            "Press.sp: actual target Press.pv does not exist",
        ),
        (
            [
                {"name": "pv", "data_type": "boolean"},
                {"name": "sp", "data_type": "number", "relationships": [{"type": "actual", "target": "pv"}]},
            ],
            [],
            "Press.sp: actual target Press.pv has data type 'boolean', not compatible with 'number'",
        ),
        (
            [{"name": "pv", "data_type": "number"}],
            [{"name": "spindle", "access": "read"}],
            "Press.spindle: a child slot cannot declare access",
        ),
        (
            [{"name": "pv", "data_type": "number"}],
            [{"name": "spindle", "relationships": []}],
            "Press.spindle: a child slot cannot declare relationships",
        ),
    ],
)
def test_broken_declarations_name_the_model_and_slot(tmp_path, signals, children, message):
    _write(tmp_path, "press.yaml", _press(signals, children))
    with pytest.raises(CompileError) as caught:
        compile_models(tmp_path)
    assert message in str(caught.value)
