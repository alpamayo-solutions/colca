"""Data-model signal slots: ``process_tolerance``.

How far the ``actual`` of a setpoint slot may differ from it in normal
operation. The loader reads it beside ``access`` and ``relationships``,
refuses a negative or non-numeric one, and compiles it into the manifest the
``_DataModel`` definition carries.
"""

import json

import pytest
import yaml

from colca_data_contracts.data_models import CompileError, compile_models
from colca_data_contracts.data_models.loader import manifest_json
from colca_data_contracts.payload import DataModel


def _write(tmp_path, document: dict) -> None:
    (tmp_path / "filler.yaml").write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")


def _filler(signals: list[dict], children: list[dict] | None = None) -> dict:
    return {"name": "Filler", "version": "1.0", "signals": signals, "children": children or []}


SETPOINT = {
    "name": "abfuelldruck",
    "data_type": "number",
    "access": "read_write",
    "process_tolerance": 100,
    "relationships": [{"type": "actual", "target": "ist_abfuelldruck"}],
}
ACTUAL = {"name": "ist_abfuelldruck", "data_type": "number"}


def _slots(tmp_path) -> dict[str, dict]:
    (manifest,) = compile_models(tmp_path)
    return {slot["key"]: slot for slot in manifest["slots"]}


def test_a_signal_slot_compiles_its_tolerance_beside_access_and_links(tmp_path):
    _write(tmp_path, _filler([SETPOINT, ACTUAL], [{"name": "valve"}]))

    slots = _slots(tmp_path)

    assert slots["abfuelldruck"]["process_tolerance"] == 100.0
    assert slots["abfuelldruck"]["access"] == "read_write"
    assert slots["abfuelldruck"]["relationships"] == [{"type": "actual", "target": "ist_abfuelldruck"}]
    assert slots["ist_abfuelldruck"]["process_tolerance"] is None
    assert slots["valve"]["process_tolerance"] is None


def test_the_tolerance_survives_the_data_model_record(tmp_path):
    _write(tmp_path, _filler([SETPOINT, ACTUAL]))
    (manifest,) = json.loads(manifest_json(compile_models(tmp_path)))

    record = DataModel(id="01HDM0000000000000000000FL", name=manifest["name"], slots=manifest["slots"])
    decoded = DataModel.decode(record.encode(), timestamp=0)

    slots = {slot["key"]: slot for slot in decoded.slots}
    assert slots["abfuelldruck"]["process_tolerance"] == 100.0


def test_an_inherited_slot_keeps_its_tolerance_and_an_override_replaces_it(tmp_path):
    (tmp_path / "base.yaml").write_text(
        yaml.safe_dump({"name": "Base", "version": "1.0", "signals": [SETPOINT, ACTUAL]}),
        encoding="utf-8",
    )
    _write(tmp_path, {**_filler([{**SETPOINT, "process_tolerance": 50}]), "extends": ["Base"]})

    manifests = {manifest["name"]: manifest for manifest in compile_models(tmp_path)}
    filler = {slot["key"]: slot for slot in manifests["Filler"]["slots"]}
    base = {slot["key"]: slot for slot in manifests["Base"]["slots"]}

    assert base["abfuelldruck"]["process_tolerance"] == 100.0
    assert filler["abfuelldruck"]["process_tolerance"] == 50.0


@pytest.mark.parametrize("value", [-1, -0.5, "100", True, float("nan")])
def test_a_tolerance_that_is_not_a_number_at_least_zero_is_refused(tmp_path, value):
    _write(tmp_path, _filler([{**SETPOINT, "process_tolerance": value}, ACTUAL]))

    with pytest.raises(CompileError) as caught:
        compile_models(tmp_path)

    assert "Filler.abfuelldruck: process_tolerance must be a number >= 0" in str(caught.value)


def test_zero_is_a_tolerance(tmp_path):
    _write(tmp_path, _filler([{**SETPOINT, "process_tolerance": 0}, ACTUAL]))

    assert _slots(tmp_path)["abfuelldruck"]["process_tolerance"] == 0.0


def test_a_child_slot_cannot_declare_a_tolerance(tmp_path):
    _write(tmp_path, _filler([ACTUAL], [{"name": "valve", "process_tolerance": 1}]))

    with pytest.raises(CompileError) as caught:
        compile_models(tmp_path)

    assert "Filler.valve: a child slot cannot declare process_tolerance" in str(caught.value)
