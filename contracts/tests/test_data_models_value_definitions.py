"""Data-model signal slots: ``value_definitions``.

What the values of a slot mean. The loader turns the YAML keys into their
canonical text, judges the definition by the rules a ``_Signal`` record
follows, and compiles it into the manifest the ``_DataModel`` definition
carries.
"""

import json

import pytest
import yaml

from colca_data_contracts.data_models import CompileError, compile_models
from colca_data_contracts.data_models.loader import manifest_json
from colca_data_contracts.payload import DataModel


def _write(tmp_path, signals: list[dict], children: list[dict] | None = None) -> None:
    document = {"name": "Filler", "version": "1.0", "signals": signals, "children": children or []}
    (tmp_path / "filler.yaml").write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")


def _slots(tmp_path) -> dict[str, dict]:
    (manifest,) = compile_models(tmp_path)
    return {slot["key"]: slot for slot in manifest["slots"]}


def test_typed_yaml_keys_compile_to_their_canonical_text(tmp_path):
    _write(
        tmp_path,
        [
            {"name": "state", "data_type": "integer", "value_definitions": {0: "STOPPED", -1: "UNKNOWN"}},
            {"name": "door", "data_type": "boolean", "value_definitions": {True: "open", False: "closed"}},
            {"name": "phase", "data_type": "string", "value_definitions": {"FILL": "Filling"}},
            {"name": "speed", "data_type": "number"},
        ],
    )
    slots = _slots(tmp_path)
    assert slots["state"]["value_definitions"] == {"0": "STOPPED", "-1": "UNKNOWN"}
    assert slots["door"]["value_definitions"] == {"true": "open", "false": "closed"}
    assert slots["phase"]["value_definitions"] == {"FILL": "Filling"}
    assert slots["speed"]["value_definitions"] == {}


def test_the_definition_survives_the_data_model_record(tmp_path):
    _write(tmp_path, [{"name": "state", "data_type": "integer", "value_definitions": {1: "RUNNING"}}])
    (manifest,) = json.loads(manifest_json(compile_models(tmp_path)))
    record = DataModel.from_wire({"id": "01KZ0000000000000000000001", **manifest})
    assert record.slots[0]["value_definitions"] == {"1": "RUNNING"}


@pytest.mark.parametrize(
    ("slot", "problem"),
    [
        ({"data_type": "number", "value_definitions": {1: "one"}}, "takes no value definitions"),
        ({"data_type": "integer", "value_definitions": {"x": "one"}}, "is not an integer"),
        ({"data_type": "integer", "value_definitions": {1: ""}}, "is empty"),
        ({"data_type": "integer", "value_definitions": [1, 2]}, "must be a mapping"),
        ({"data_type": "integer", "value_definitions": {1: "a", "1": "b"}}, "names 1 twice"),
        ({"data_type": "integer", "enum": ["A", "B"]}, "enum is not a slot field"),
    ],
)
def test_a_definition_the_rules_refuse_is_a_compile_error(tmp_path, slot, problem):
    _write(tmp_path, [{"name": "state", **slot}])
    with pytest.raises(CompileError, match=problem):
        compile_models(tmp_path)


def test_a_child_slot_cannot_declare_a_definition(tmp_path):
    _write(tmp_path, [], children=[{"name": "head", "value_definitions": {1: "one"}}])
    with pytest.raises(CompileError, match="cannot declare value_definitions"):
        compile_models(tmp_path)
