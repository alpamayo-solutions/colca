"""A payload survives a field it does not know.

Nodes of one tree run their own versions, so a record written by a newer node
carries fields an older one has never heard of. Decoding has to ignore them:
a refusal breaks the older node silently, because a consumer sets the record
aside as poison and the entity then goes missing while everything reports
healthy. The same rule is what lets a field be retired, since the retained
records that still carry it stay readable.
"""

import dataclasses
import datetime
import enum
import json
import types
import typing

import pytest
from franzmq.data_contracts import PAYLOAD_CLASSES

from colca_data_contracts import (
    AlarmNotificationConfigSnapshot,
    AnnotationPayload,
    ConstantPayload,
    DataTag,
    DataTags,
    GroupPayload,
    Metric,
    NodePayload,
    ServiceDetails,
    SignalPayload,
    SystemElementPayload,
)
from colca_data_contracts.payload import CmdConfigure

#: A key no version of any contract has ever had.
UNKNOWN = "a_field_from_a_newer_node"

#: Every payload class this package owns, taken from the registry so a new
#: contract is covered the day it is registered rather than when someone
#: remembers to add it here.
OWNED = sorted(
    (identifier, cls) for identifier, cls in PAYLOAD_CLASSES.items() if cls.__module__ == "colca_data_contracts.payload"
)


def _wire_value(annotation: object) -> object:
    """A value the wire could carry for ``annotation``."""

    origin = typing.get_origin(annotation)
    args = typing.get_args(annotation)

    if origin is typing.Annotated:
        return _wire_value(args[0])
    if origin is typing.Union or origin is types.UnionType:
        # Nullable is the simplest value that is always in range.
        if type(None) in args:
            return None
        return _wire_value(args[0])
    if isinstance(annotation, type) and issubclass(annotation, enum.Enum):
        return str(next(iter(annotation)).value)
    if annotation is bool:
        return True
    if annotation in (int, float):
        return 1
    if annotation is str:
        return "x"
    if annotation is datetime.datetime:
        return datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC).isoformat()
    if origin is list or annotation is list:
        return []
    if origin is dict or annotation is dict:
        return {}
    if dataclasses.is_dataclass(annotation) and isinstance(annotation, type):
        return _wire_record(annotation)
    # typing.Any and anything unmapped: a null is always acceptable.
    return None


def _wire_record(cls: type) -> dict:
    """A record ``cls`` accepts, with every field it knows about present."""

    hints = typing.get_type_hints(cls, include_extras=True)
    return {f.name: _wire_value(hints.get(f.name, typing.Any)) for f in dataclasses.fields(cls)}


@pytest.mark.parametrize(
    "cls",
    [
        SystemElementPayload,
        SignalPayload,
        ConstantPayload,
        NodePayload,
        ServiceDetails,
        GroupPayload,
        AnnotationPayload,
        DataTags,
        Metric,
        CmdConfigure,
    ],
    ids=lambda cls: cls.get_identifier(),
)
def test_a_record_from_a_newer_node_decodes_and_the_extra_field_is_ignored(cls):
    known = _wire_record(cls)

    decoded = cls.decode(json.dumps(known | {UNKNOWN: "whatever this means"}), timestamp=0)

    # Ignored, not smuggled in: the record decodes to exactly what it would
    # have decoded to without the field.
    assert decoded == cls.decode(json.dumps(known), timestamp=0)
    assert not hasattr(decoded, UNKNOWN)


@pytest.mark.parametrize("identifier, cls", OWNED, ids=[identifier for identifier, _ in OWNED])
def test_every_payload_this_package_owns_tolerates_an_unknown_field(identifier, cls):
    """The architectural claim, not a sample of it.

    A new contract, or an old one that grows a ``decode`` of its own, is
    covered here the moment it is registered. Fifteen contracts once
    re-implemented ``decode`` as ``cls(**json.loads(json_str))`` and only
    ``_Metric`` filtered; this is the check that would have gone red.
    """

    record = _wire_record(cls) | {UNKNOWN: 1}

    assert cls.decode(json.dumps(record), timestamp=0) is not None


def test_the_owned_set_is_the_whole_package():
    """The denominator: the test above proves nothing over an empty set."""

    identifiers = {identifier for identifier, _ in OWNED}
    assert len(identifiers) > 25
    assert {"_SystemElement", "_Signal", "_Constant", "_Group", "_DataTags", "_CmdConfigure"} <= identifiers


@pytest.mark.parametrize(
    "cls, required",
    [
        (SystemElementPayload, "name"),
        (SignalPayload, "id"),
        (GroupPayload, "name"),
        (DataTags, "connector"),
        (Metric, "timestamp"),
    ],
    ids=lambda value: value if isinstance(value, str) else value.get_identifier(),
)
def test_a_record_missing_a_field_the_contract_requires_still_fails(cls, required):
    """Tolerance is about fields that arrive, not fields that are owed."""

    record = _wire_record(cls)
    del record[required]

    with pytest.raises((TypeError, KeyError)):
        cls.decode(json.dumps(record), timestamp=0)

    # The denominator: the same record decodes once the field is back, so the
    # refusal above is about the missing field and not about the record.
    assert cls.decode(json.dumps(_wire_record(cls)), timestamp=0) is not None


def test_a_payload_keeps_the_names_only_its_own_constructor_accepts():
    """Which keys survive is what the constructor takes, not what the fields are.

    `_AlarmNotificationConfig` accepts `generated_at`, the schema-version-1
    spelling of `issued_at`, and has no field by that name. Filtering on the
    dataclass fields would drop it and then refuse the record for the very
    value it had just thrown away.
    """

    version_one = json.dumps(
        {
            "schema_version": 1,
            "generated_at": 1.5,
            "alarms": [],
            "channels": [],
            "recipients": [],
            "policies": [],
            "policy_targets": [],
            UNKNOWN: 7,
        }
    )

    assert AlarmNotificationConfigSnapshot.decode(version_one, timestamp=0).issued_at == 1.5


def test_a_catalogue_entry_from_a_newer_node_does_not_cost_the_whole_catalogue():
    """``_DataTags`` rebuilds its entries, so the rule has to reach them too."""

    catalogue = DataTags(
        data_tags=[DataTag(id="01H", name="speed", source="ns=2;s=Speed", is_writable=False, is_readable=True)],
        connector="modbus-reader",
    )
    record = json.loads(catalogue.encode())
    record["data_tags"][0][UNKNOWN] = "sampling_hint"

    decoded = DataTags.decode(json.dumps(record), timestamp=0)

    assert decoded == catalogue
