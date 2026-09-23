"""Bundle generator tests.

A contract change the generator cannot express fails here, not at a door.
"""

from __future__ import annotations

import dataclasses
import datetime
import enum
import json
import sys
import types
import typing
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import generate_bundle as gb
from franzmq.data_contracts import PAYLOAD_CLASSES

jsonschema = pytest.importorskip("jsonschema", reason="parity gate needs the jsonschema test dep")


def test_optional_fields_are_typed_on_every_python():
    # Python 3.14 made `X | None` a typing.Union; before it, the two spellings had
    # different origins. A check that caught only one of them left every
    # `X | None` field unvalidated in bundles built on 3.11 to 3.13.
    assert gb._schema_for_type(str | None, required=False) == {"type": ["null", "string"]}
    assert gb._schema_for_type(typing.Optional[float], required=False) == {"type": ["null", "number"]}  # noqa: UP045


def test_determinism_two_runs_one_digest():
    b1, d1 = gb.build_bundle("sha-x")
    b2, d2 = gb.build_bundle("sha-x")
    assert d1 == d2, "same tree must yield the same digest"
    assert json.dumps(b1, sort_keys=True) == json.dumps(b2, sort_keys=True)


def test_inventory_every_registered_class_exactly_once():
    body, _ = gb.build_bundle()
    assert set(body["contracts"]) == set(PAYLOAD_CLASSES) - set(gb.NOT_ON_THE_WIRE), (
        "bundle inventory must equal the payload registry minus the contracts deliberately kept off the wire"
    )


def test_contracts_kept_off_the_wire_are_absent_and_say_why():
    body, _ = gb.build_bundle()
    for contract, reason in gb.NOT_ON_THE_WIRE.items():
        assert contract not in body["contracts"], f"{contract} must not be publishable"
        # A bare exclusion list rots into a junk drawer; every entry names the
        # producer or container that keeps the Python type alive.
        assert len(reason) > 40, f"{contract}: give a reason, not a label"


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
    other = DataTags(
        data_tags=[*tags, DataTag(id="b", name="B", source="Sensors/B", is_writable=False, is_readable=True)],
        connector="opcua-1",
    )
    assert first.version == same.version
    assert first.version != other.version


def test_builtin_only_contracts_absent():
    body, _ = gb.build_bundle()
    for c in gb.BUILTIN_ONLY:
        assert c not in body["contracts"], f"{c} is builtin-only"


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
        contract for contract in expected_entities if body["contracts"].get(contract, {}).get("class") == "entity"
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
    }
    # `value` is validated when sent but not required: a JSON constant may be
    # null or left out.
    assert "value" not in constant["schema"]["required"]
    assert "value" in constant["schema"]["properties"]
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
            "data",
            "entity",
            "definition",
            "cmd",
            "ack",
            "audit",
            "alarm",
            "annotation",
            # A log line is an event on its own stream — see uns.ClassLog.
            "log",
        ), ident
        assert isinstance(entry["tombstone"], bool), ident


#: A syntactically valid ULID for golden instances of pattern-constrained fields.
GOLDEN_ULID = "01ARZ3NDEKTSV4RRFFQ69G5FAV"


def _golden_instance(cls: type):
    """Fill every required field with a type-correct value."""
    hints = typing.get_type_hints(cls, include_extras=True)
    kwargs = {}
    for f in dataclasses.fields(cls):
        if not (f.default is dataclasses.MISSING and f.default_factory is dataclasses.MISSING):
            continue
        kwargs[f.name] = _dummy(hints.get(f.name, typing.Any))
    return cls(**kwargs)


def _dummy(t):
    origin = typing.get_origin(t)
    args = typing.get_args(t)
    if origin is typing.Annotated:
        if any(isinstance(m, gb.Pattern) for m in args[1:]):
            return GOLDEN_ULID
        return _dummy(args[0])
    if origin is typing.Union or origin is types.UnionType:
        non_none = [a for a in args if a is not type(None)]
        return _dummy(non_none[0]) if non_none else None
    if t is typing.Any:
        return "x"
    if isinstance(t, type) and issubclass(t, enum.Enum):
        return next(iter(t))
    if t is str:
        return "x"
    if t in (int, float):
        return 1
    if t is bool:
        return True
    if t is datetime.datetime:
        return datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC)
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
                setattr(inst, extra, _dummy(typing.get_type_hints(cls, include_extras=True).get(extra, str)))
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
            if "pattern" in prop:
                # A 31-character id must be refused at the door.
                broken = dict(encoded)
                broken[name] = GOLDEN_ULID + "EXTRA"
                assert not validator.is_valid(broken), f"{ident}: 31-char {name} must be rejected"
                broken[name] = GOLDEN_ULID.lower()
                assert not validator.is_valid(broken), f"{ident}: lowercase {name} must be rejected"


def test_cmd_contracts_carry_the_door_contract():
    """The colca command door needs correlation_id + expires_at; created_at is
    dropped from required (publishers do not stamp it)."""
    body, _ = gb.build_bundle()
    for ident in ("_CmdParam", "_CmdOperate", "_CmdMaintain", "_CmdConfigure", "_CmdEdit", "_CmdAdmin", "_Cmd"):
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
    # A metric is a value at a time; the "now" default is for constructors only.
    assert sorted(m["schema"]["required"]) == ["signal_id", "timestamp", "value"]
    assert m["schema"]["properties"]["signal_id"]["minLength"] == 1


def test_alarm_notification_contracts_have_revised_direction_and_shape():
    body, _ = gb.build_bundle()
    config = body["contracts"]["_AlarmNotificationConfig"]
    status = body["contracts"]["_NotificationConfigStatus"]
    event = body["contracts"]["_AlarmStateChange"]
    dispatch = body["contracts"]["_NotificationDispatched"]

    # Alarm configuration stays on entities; the two event contracts use the
    # alarms stream so they never queue behind metrics.
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


def test_a_finding_is_retained_and_carries_its_own_handling():
    """`_Finding` is what a service stands behind, with the handling attached.

    Two properties make the model work, and both are asserted here because
    losing either turns it back into the thing it replaces:

      * retained with a tombstone, so "still broken" is a cheap republish and
        "gone" is a deletion. A service needs no memory of what it said.
      * the handling travels WITH the observation. The service that wrote the
        check is the only one that knows whether its condition flaps or whether
        an operator may silence it; a rule kept elsewhere has to be matched to
        the finding, and the matching is what drifts.

    What is deliberately absent: everything about the lifecycle. No
    acknowledgement, no silence, no status. A service republishing its
    observation must never be able to overwrite an operator's acknowledgement,
    and the only way to guarantee that is to give it nowhere to write it.
    """
    body, _ = gb.build_bundle()
    finding = body["contracts"]["_Finding"]
    schema = finding["schema"]

    assert finding["class"] == "entity"
    assert finding["tombstone"] is True
    assert set(schema["required"]) == {
        "reason",
        "summary",
        "observed_at",
        "suggested_severity",
    }
    # The handling the finder attaches, all optional: a check that cannot flap
    # and may always be silenced carries none of it.
    for knob in ("silenceable", "dwell_on_s", "dwell_off_s", "min_repeat_s", "remedy"):
        assert knob in schema["properties"], knob
        assert knob not in schema["required"], knob
    # Severity is a proposal here and a decision in `_AlarmState`; the names
    # differ so nobody reads one for the other.
    assert schema["properties"]["suggested_severity"]["enum"] == ["info", "warning", "critical"]
    assert "severity" not in schema["properties"]
    # Open, like the alarm's: a service that diagnoses something new names it.
    assert "enum" not in schema["properties"]["reason"]
    # The lifecycle belongs to the manager, and a writer cannot reach it.
    for owned_by_the_manager in ("status", "acknowledged_by", "silenced_by", "silenced_until", "since"):
        assert owned_by_the_manager not in schema["properties"], owned_by_the_manager


def test_standing_alarm_is_retained_state_with_a_closed_status_vocabulary():
    body, _ = gb.build_bundle()
    standing = body["contracts"]["_AlarmState"]
    schema = standing["schema"]

    # One record per alarm definition: it overwrites itself, and the tombstone
    # is how an alarm goes away.
    assert standing["class"] == "entity"
    assert standing["tombstone"] is True
    # `signal_id` is NOT required: an alarm can stand on a finding about an
    # element -- a station commissioned that the line definition does not know,
    # a stale configuration -- and there is no signal to name. Relaxed
    # deliberately; a manager inventing one would be worse than its absence.
    assert set(schema["required"]) == {
        "alarm_id",
        "status",
        "severity",
        "since",
        "reason",
    }
    # Status and severity are closed; reason stays open so a derived diagnosis
    # can name its own.
    assert schema["properties"]["status"]["enum"] == ["pending", "firing", "unknown"]
    assert schema["properties"]["severity"]["enum"] == ["info", "warning", "critical"]
    assert "enum" not in schema["properties"]["reason"]

    validator = jsonschema.Draft202012Validator(schema)
    standing_now = {
        "alarm_id": "01H0000000000000000000ARM1",
        "status": "firing",
        "severity": "critical",
        "since": 1710000000.0,
        "signal_id": "01H0000000000000000000SGN2",
        "reason": "threshold",
        "value": 82.4,
        "op": ">",
        "threshold": 80.0,
    }
    validator.validate(standing_now)
    # There is no "normal": an alarm that no longer stands is deleted.
    assert not validator.is_valid({**standing_now, "status": "normal"})
    assert not validator.is_valid({**standing_now, "status": "recovered"})
    assert validator.is_valid({**standing_now, "reason": "compressor_stall"})


def test_alarm_channel_schema_requires_sealed_secret_and_rejects_cleartext_fields():
    body, _ = gb.build_bundle()
    channel = body["contracts"]["_AlarmNotificationConfig"]["schema"]["properties"]["channels"]["items"]
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
    """Definitions have their own routing class, and an empty payload retracts one."""
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


def test_annotation_contract_is_its_own_class_and_never_tombstoned():
    """Annotations are append-only on their own stream. A delete is a record with
    `deleted=True`, so `tombstone` stays False."""
    body, _ = gb.build_bundle()
    annotation = body["contracts"]["_Annotation"]

    assert annotation["class"] == "annotation"
    assert annotation["tombstone"] is False
    assert set(annotation["schema"]["required"]) == {
        "annotation_id",
        "annotation_type_id",
        "time_start",
    }


def test_annotation_schema_takes_placement_and_relations():
    """`system_element_id` and `related_annotation_ids` are optional ULIDs: a
    record without them is valid, one with a malformed id is not."""
    body, _ = gb.build_bundle()
    validator = jsonschema.Draft202012Validator(body["contracts"]["_Annotation"]["schema"])
    head_pass = {
        "annotation_id": "01M2AB5YWM56SH9B2EQ3S5VNXF",
        "annotation_type_id": "01M2AB5YWSZTAFBKYYYA5C0TE7",
        "time_start": 1710000000.0,
        "signal_ids": ["01BX5ZZKBKACTAV9WEVGEMMVRZ"],
    }
    validator.validate(head_pass)
    validator.validate(
        {
            **head_pass,
            "system_element_id": "01BX5ZZKBKACTAV9WEVGEMMVRA",
            "related_annotation_ids": ["01M2AB5YWM56SH9B2EQ3S5VNXG"],
        }
    )
    validator.validate({**head_pass, "system_element_id": None})
    assert not validator.is_valid({**head_pass, "system_element_id": "line-1"})
    assert not validator.is_valid({**head_pass, "related_annotation_ids": ["panel-1"]})
    assert not validator.is_valid({**head_pass, "related_annotation_ids": "01M2AB5YWM56SH9B2EQ3S5VNXG"})


def test_every_definition_is_addressable_by_id():
    """A definition's path is its id, so every definition needs one."""
    body, _ = gb.build_bundle()
    for ident, entry in body["contracts"].items():
        if entry["class"] != "definition":
            continue
        assert "id" in entry["schema"]["required"], ident


#: Every (contract, JSON pointer) the ULID pattern must reach, so a field that
#: loses its `ULID` annotation, or a new id field without one, fails here.
ULID_CONSTRAINED_FIELDS = {
    "_SystemElement": ["id", "parent_id", "semantic_type_id"],
    "_Signal": ["id", "system_element_id", "data_tag", "semantic_type_id"],
    "_Constant": ["id", "system_element_id", "semantic_type_id"],
    "_Resource": ["id", "system_element_id"],
    "_ExternalReference": ["id", "external_system_id"],
    "_Finding": ["signal_id"],
    "_AlarmState": ["alarm_id", "signal_id"],
    # annotation_type_id stays unconstrained until the golden annotation-id
    # vectors derive from ULID type ids — see payload.Annotation.
    "_Annotation": [
        "annotation_id",
        "signal_ids/items",
        "system_element_id",
        "related_annotation_ids/items",
    ],
    "_AnnotationType": ["id"],
    "_MetadataType": ["id"],
    "_SemanticTag": ["id"],
    "_DataModel": ["id"],
    "_ExternalSystem": ["id"],
    "_PersonalAccessToken": ["id"],
    "_DataTags": ["data_tags/items/id"],
}


def _walk(schema: dict, pointer: str) -> dict:
    node = schema
    for step in pointer.split("/"):
        node = node["items"] if step == "items" else node["properties"][step]
    return node


def _pointers_with_pattern(schema: dict, prefix: str = "") -> list[str]:
    out: list[str] = []
    for name, prop in schema.get("properties", {}).items():
        here = f"{prefix}{name}"
        if "pattern" in prop:
            out.append(here)
        if "pattern" in prop.get("items", {}):
            out.append(f"{here}/items")
        out += _pointers_with_pattern(prop, f"{here}/")
        out += _pointers_with_pattern(prop.get("items", {}), f"{here}/items/")
    return out


def test_ulid_fields_carry_the_one_pattern():
    """Every ULID field carries `payload.ULID_PATTERN`, and every constrained
    field is in the table."""
    from colca_data_contracts import ULID_PATTERN

    body, _ = gb.build_bundle()
    seen: dict[str, list[str]] = {}
    for ident, entry in body["contracts"].items():
        for pointer in _pointers_with_pattern(entry["schema"]):
            seen.setdefault(ident, []).append(pointer)
            assert _walk(entry["schema"], pointer)["pattern"] == ULID_PATTERN, (ident, pointer)
    assert {k: sorted(v) for k, v in seen.items()} == {k: sorted(v) for k, v in ULID_CONSTRAINED_FIELDS.items()}
