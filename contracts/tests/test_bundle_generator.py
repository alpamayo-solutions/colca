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
    tags = [DataTag(id="a", name="A", source="Sensors/A", is_writable=False, is_readable=True)]
    first = DataTags(data_tags=tags, connector="opcua-1")
    same = DataTags(data_tags=list(tags), connector="opcua-1")
    other = DataTags(data_tags=tags + [DataTag(id="b", name="B", source="Sensors/B", is_writable=False, is_readable=True)],
                     connector="opcua-1")
    assert first.version == same.version
    assert first.version != other.version


def test_builtin_only_contracts_absent():
    body, _ = gb.build_bundle()
    for c in gb.BUILTIN_ONLY:
        assert c not in body["contracts"], f"{c} is builtin-only (design §10.2)"


def test_projected_contract_catalogue_has_the_approved_direction():
    """Direction is one reviewed fact, not a per-consumer convention."""
    body, _ = gb.build_bundle()
    expected_entities = {
        "_Node",
        "_ServiceDetails",
        "_SystemElement",
        "_Signal",
        "_Constant",
        "_ExternalReference",
    }
    expected_definitions = {
        "_Group",
        "_MetadataType",
        "_AnnotationType",
        "_DataModel",
        "_ExternalSystem",
        "_SemanticTag",
    }

    assert {
        contract
        for contract in expected_entities
        if body["contracts"].get(contract, {}).get("class") == "entity"
    } == expected_entities
    assert {
        contract
        for contract in expected_definitions
        if body["contracts"].get(contract, {}).get("class") == "definition"
    } == expected_definitions


def test_constant_contract_is_positioned_retractable_and_value_typed():
    body, _ = gb.build_bundle()
    constant = body["contracts"]["_Constant"]

    assert constant["class"] == "entity"
    assert constant["tombstone"] is True
    assert set(constant["schema"]["required"]) >= {
        "id",
        "name",
        "data_type",
        "value",
    }
    assert constant["schema"]["properties"]["data_type"]["enum"] == [
        "float64",
        "int64",
        "boolean",
        "string",
        "datetime",
        "json",
    ]


def test_enrollment_contract_is_builtin_and_edge_node_is_retired():
    assert "_EnrolledIdentity" in gb.BUILTIN_ONLY
    assert "_EdgeNode" not in gb.BUILTIN_ONLY
    body, _ = gb.build_bundle()
    assert "_EnrolledIdentity" not in body["contracts"]
    assert "_EdgeNode" not in body["contracts"]


def test_subset_lint_only_allowed_keywords():
    body, _ = gb.build_bundle()
    for ident, entry in body["contracts"].items():
        assert gb._lint_subset(entry["schema"], ident) == []
        assert entry["class"] in (
            "data", "entity", "definition", "cmd", "ack", "audit", "alarm",
        ), ident
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
                  "_CmdEdit", "_CmdAdmin", "_Cmd", "_ApiWriteCmd"):
        entry = body["contracts"][ident]
        assert entry["class"] == "cmd", ident
        req = entry["schema"].get("required", [])
        assert "correlation_id" in req and "expires_at" in req, (ident, req)
        assert "created_at" not in req, (ident, req)
        assert entry["tombstone"] is False, ident


def test_edit_command_requires_one_versioned_idempotent_intent():
    body, _ = gb.build_bundle()
    schema = body["contracts"]["_CmdEdit"]["schema"]

    assert schema["properties"]["intent"] == {"type": "object"}
    assert schema["properties"]["expected_versions"] == {"type": "object"}
    assert set(schema["required"]) >= {
        "operation_id",
        "intent",
        "expected_versions",
        "correlation_id",
        "expires_at",
    }


def test_metric_real_shape():
    body, _ = gb.build_bundle()
    m = body["contracts"]["_Metric"]
    assert m["class"] == "data" and m["tombstone"] is True
    assert sorted(m["schema"]["required"]) == ["signal_id", "value"]
    assert m["schema"]["properties"]["signal_id"]["minLength"] == 1


def test_alarm_notification_contracts_have_revised_direction_and_shape():
    body, _ = gb.build_bundle()
    config = body["contracts"]["_AlarmNotificationConfig"]
    status = body["contracts"]["_NotificationConfigStatus"]
    event = body["contracts"]["_AlarmStateChange"]
    dispatch = body["contracts"]["_NotificationDispatched"]

    # The config pair stays on entities — the silence rail is unchanged. The
    # two EVENT contracts ride the alarms stream instead, so an alarm never
    # queues behind a metrics backlog (alarm-stream design §3.1).
    assert config["class"] == "entity"
    assert status["class"] == "entity"
    assert event["class"] == "alarm"
    assert dispatch["class"] == "alarm"
    # Tombstone is derived from class. An event cannot be retracted, so this
    # is what goes red if either contract drifts back to a state class.
    assert event["tombstone"] is False
    assert dispatch["tombstone"] is False
    assert {"target_node_id", "revision_id"} <= set(config["schema"]["required"])
    assert "id" in status["schema"]["required"]
    assert {"revision", "notification"} <= set(event["schema"]["required"])


def test_alarm_channel_schema_requires_sealed_secret_and_rejects_cleartext_fields():
    body, _ = gb.build_bundle()
    channel = body["contracts"]["_AlarmNotificationConfig"]["schema"]["properties"][
        "channels"
    ]["items"]
    envelope = channel["properties"]["sealed_secret"]

    assert channel["additionalProperties"] is False
    assert envelope["additionalProperties"] is False
    assert set(envelope["required"]) == {"version", "algorithm", "key_id", "ciphertext"}

    validator = jsonschema.Draft202012Validator(channel)
    valid = {
        "id": "channel-1",
        "name": "Operations",
        "kind": "smtp",
        "public_config": {},
        "sealed_secret": {
            "version": 1,
            "algorithm": "nacl-box-seal-x25519-xsalsa20-poly1305",
            "key_id": "sha256:key-1",
            "ciphertext": "ciphertext",
        },
        "rate_limit_per_minute": 60,
        "enabled": True,
    }
    assert validator.is_valid(valid)
    assert not validator.is_valid({**valid, "password": "cleartext"})
    assert not validator.is_valid({**valid, "webhook_url": "https://secret.invalid"})


def test_audit_event_is_append_only_and_has_stable_required_fields():
    body, _ = gb.build_bundle()
    event = body["contracts"]["_AuditEvent"]

    assert event["class"] == "audit"
    assert event["tombstone"] is False
    assert set(event["schema"]["required"]) == {
        "event_id",
        "source",
        "action",
        "outcome",
        "actor_kind",
        "occurred_at",
    }
    assert event["schema"]["properties"]["source"]["enum"] == [
        "colca",
        "api",
        "keycloak",
        "projector",
        "node_manager",
    ]


def test_definitions_are_their_own_class_and_retractable():
    """A definition descends and is applied as state (definition-stream design
    §2): its own routing class, and an empty payload retracts it.

    The three type contracts were "entity" until this stream existed, which
    meant they replicated the wrong way — up, away from the nodes that need
    them.
    """
    body, _ = gb.build_bundle()
    for ident in (
        "_Group",
        "_MetadataType",
        "_AnnotationType",
        "_DataModel",
        "_ExternalSystem",
        "_SemanticTag",
    ):
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
