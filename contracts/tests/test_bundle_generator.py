"""Bundle generator gates (schema-bundle design §5.1/§12, contracts job).

Pins generator and dataclasses together: a contract change the generator
cannot express fails HERE, not at a customer door.
"""

from __future__ import annotations

import dataclasses
import datetime
import enum
import json
import sys
import typing
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import generate_bundle as gb  # noqa: E402
from franzmq.data_contracts import PAYLOAD_CLASSES  # noqa: E402

jsonschema = pytest.importorskip("jsonschema", reason="parity gate needs the jsonschema test dep")


def test_determinism_two_runs_one_digest():
    b1, d1 = gb.build_bundle("sha-x")
    b2, d2 = gb.build_bundle("sha-x")
    assert d1 == d2, "same tree must yield the same digest (design §5.1)"
    assert json.dumps(b1, sort_keys=True) == json.dumps(b2, sort_keys=True)


def test_inventory_every_registered_class_exactly_once():
    body, _ = gb.build_bundle()
    assert set(body["contracts"]) == set(PAYLOAD_CLASSES) - set(gb.NOT_ON_THE_WIRE), (
        "bundle inventory must equal the payload registry minus the contracts "
        "deliberately kept off the wire (§14.3: full set, unpruned)"
    )


def test_contracts_kept_off_the_wire_are_absent_and_say_why():
    body, _ = gb.build_bundle()
    for contract, reason in gb.NOT_ON_THE_WIRE.items():
        assert contract not in body["contracts"], f"{contract} must not be publishable"
        # A bare exclusion list rots into a junk drawer; every entry names the
        # producer or container that keeps the Python type alive.
        assert len(reason) > 40, f"{contract}: give a reason, not a label"


def test_the_retired_binding_contracts_are_off_the_wire():
    """DataTagContext is retired; a door that does not know it rejects it."""
    body, _ = gb.build_bundle()
    assert "_DataTagContext" not in body["contracts"]
    assert "_DataTagContexts" not in body["contracts"]


def test_the_catalogue_is_one_record_carrying_its_own_revision():
    body, _ = gb.build_bundle()
    assert "_DataTags" in body["contracts"], "the catalogue is a wire contract"
    assert "_DataTag" not in body["contracts"], "a single tag is not a record"
    # The content hash is what lets a connector skip republishing an unchanged
    # catalogue, which is the whole reason one fat record is affordable.
    from colca_data_contracts import DataTag, DataTags
    tags = [DataTag(id="a", name="A", is_writable=False, is_readable=True)]
    first = DataTags(data_tags=tags, connector="opcua-1")
    same = DataTags(data_tags=list(tags), connector="opcua-1")
    other = DataTags(data_tags=tags + [DataTag(id="b", name="B", is_writable=False, is_readable=True)],
                     connector="opcua-1")
    assert first.version == same.version
    assert first.version != other.version


def test_builtin_only_contracts_absent():
    body, _ = gb.build_bundle()
    for c in gb.BUILTIN_ONLY:
        assert c not in body["contracts"], f"{c} is builtin-only (design §10.2)"


def test_subset_lint_only_allowed_keywords():
    body, _ = gb.build_bundle()
    for ident, entry in body["contracts"].items():
        assert gb._lint_subset(entry["schema"], ident) == []
        assert entry["class"] in ("data", "entity", "definition", "cmd", "ack"), ident
        assert isinstance(entry["tombstone"], bool), ident


def _golden_instance(cls: type):
    """Fill every required field with a type-correct value."""
    hints = typing.get_type_hints(cls)
    kwargs = {}
    for f in dataclasses.fields(cls):
        if not (f.default is dataclasses.MISSING and f.default_factory is dataclasses.MISSING):
            continue
        kwargs[f.name] = _dummy(hints.get(f.name, typing.Any))
    return cls(**kwargs)


def _dummy(t):
    origin = typing.get_origin(t)
    args = typing.get_args(t)
    if origin is typing.Union or str(origin) == "types.UnionType":
        non_none = [a for a in args if a is not type(None)]
        return _dummy(non_none[0]) if non_none else None
    if t is typing.Any:
        return "x"
    if isinstance(t, type) and issubclass(t, enum.Enum):
        return list(t)[0]
    if t is str:
        return "x"
    if t in (int, float):
        return 1
    if t is bool:
        return True
    if t is datetime.datetime:
        return datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)
    if origin is dict or t is dict:
        return {}
    if origin is list or t is list:
        return []
    if dataclasses.is_dataclass(t):
        return _golden_instance(t)
    return "x"


def test_parity_golden_encodes_validate_and_mutants_reject():
    body, _ = gb.build_bundle()
    for ident, cls in sorted(PAYLOAD_CLASSES.items()):
        if ident in gb.NOT_ON_THE_WIRE:
            continue  # no schema to be parity-checked against
        entry = body["contracts"][ident]
        schema = entry["schema"]
        validator = jsonschema.Draft202012Validator(schema)

        inst = _golden_instance(cls)
        # Door contracts hidden behind constructor defaults (REQUIRED_EXTRA)
        # must be present in the golden instance too.
        for extra in gb.REQUIRED_EXTRA.get(ident, []):
            if not getattr(inst, extra, None):
                setattr(inst, extra, _dummy(typing.get_type_hints(cls).get(extra, str)))
        encoded = json.loads(inst.encode())
        errs = list(validator.iter_errors(encoded))
        assert not errs, f"{ident}: golden encode() fails its own schema: {errs[0].message}"

        # Mutants: drop each required field; wrong-type a string field.
        for req in schema.get("required", []):
            broken = dict(encoded)
            broken.pop(req, None)
            assert not validator.is_valid(broken), f"{ident}: missing {req} must be rejected"
        for name, prop in schema.get("properties", {}).items():
            if prop.get("type") == "string" and name in schema.get("required", []):
                broken = dict(encoded)
                broken[name] = 42
                assert not validator.is_valid(broken), f"{ident}: wrong-type {name} must be rejected"


def test_cmd_contracts_carry_the_door_contract():
    """The colca command door needs correlation_id + expires_at; created_at is
    dropped from required (publishers do not stamp it)."""
    body, _ = gb.build_bundle()
    for ident in ("_CmdParam", "_CmdOperate", "_CmdMaintain", "_CmdConfigure",
                  "_CmdAdmin", "_Cmd", "_ApiWriteCmd"):
        entry = body["contracts"][ident]
        assert entry["class"] == "cmd", ident
        req = entry["schema"].get("required", [])
        assert "correlation_id" in req and "expires_at" in req, (ident, req)
        assert "created_at" not in req, (ident, req)
        assert entry["tombstone"] is False, ident


def test_metric_real_shape():
    body, _ = gb.build_bundle()
    m = body["contracts"]["_Metric"]
    assert m["class"] == "data" and m["tombstone"] is True
    assert sorted(m["schema"]["required"]) == ["signal_id", "value"]
    assert m["schema"]["properties"]["signal_id"]["minLength"] == 1


def test_definitions_are_their_own_class_and_retractable():
    """A definition descends and is applied as state (definition-stream design
    §2): its own routing class, and an empty payload retracts it.

    The three type contracts were "entity" until this stream existed, which
    meant they replicated the wrong way — up, away from the nodes that need
    them.
    """
    body, _ = gb.build_bundle()
    for ident in ("_Group", "_MetadataType", "_AnnotationType", "_Interface"):
        entry = body["contracts"][ident]
        assert entry["class"] == "definition", (ident, entry["class"])
        assert entry["tombstone"] is True, ident


def test_every_definition_is_addressable_by_id():
    """A definition's path IS its identity, so one without an id could not be
    filed at all (definition-stream design §3)."""
    body, _ = gb.build_bundle()
    for ident, entry in body["contracts"].items():
        if entry["class"] != "definition":
            continue
        assert "id" in entry["schema"]["required"], ident
