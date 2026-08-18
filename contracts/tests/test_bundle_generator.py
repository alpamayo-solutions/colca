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
    assert set(body["contracts"]) == set(PAYLOAD_CLASSES), (
        "bundle inventory must equal the payload registry (§14.3: full set, unpruned)"
    )


def test_builtin_only_contracts_absent():
    body, _ = gb.build_bundle()
    for c in gb.BUILTIN_ONLY:
        assert c not in body["contracts"], f"{c} is builtin-only (design §10.2)"


def test_subset_lint_only_allowed_keywords():
    body, _ = gb.build_bundle()
    for ident, entry in body["contracts"].items():
        assert gb._lint_subset(entry["schema"], ident) == []
        assert entry["class"] in ("data", "entity", "cmd", "ack"), ident
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
    for ident in ("_CmdParam", "_CmdOperate", "_CmdMaintain", "_CmdAdmin", "_Cmd", "_ApiWriteCmd"):
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
