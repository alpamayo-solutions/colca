"""Generate the colca contracts bundle.

Introspects the registered payload classes and emits one canonical-JSON bundle:
a manifest plus a JSON Schema (a draft 2020-12 subset) per contract. The same
source tree always produces the same digest; `generated_at` is not part of it.

Rules the dataclasses cannot express live in the explicit tables below.

Usage:
    python scripts/generate_bundle.py [OUT_PATH]
prints "<sha256>  <path>" on success. Library surface: build_bundle().
"""

from __future__ import annotations

import datetime
import enum
import hashlib
import json
import sys
import typing
from dataclasses import MISSING, fields, is_dataclass

from franzmq.data_contracts import PAYLOAD_CLASSES
from franzmq.data_contracts.base import Ack, Cmd

import colca_data_contracts  # noqa: F401  (import populates the registry)
from colca_data_contracts.payload import Pattern
from colca_data_contracts.routing import CLASS_TABLE

# ---------------------------------------------------------------------------
# Explicit tables
# ---------------------------------------------------------------------------

# Routing class per contract, imported so the bundle and the Python consumers
# cannot disagree.

# Fields the door requires although the dataclass gives them a default for
# constructor convenience (Metric.signal_id, Ack.result_code).
REQUIRED_EXTRA: dict[str, list[str]] = {
    # A metric is a value at a time: the "now" default is for constructors,
    # and Metric.decode refuses a record without a timestamp.
    "_Metric": ["signal_id", "timestamp"],
    "_Ack": ["result_code"],
    "_CmdEdit": ["operation_id", "intent", "expected_versions"],
    # Defaulted in the constructor, required by version 2 of the wire contract.
    "_AlarmNotificationConfig": ["target_node_id", "revision_id"],
    "_AlarmStateChange": ["revision", "notification"],
}

# Fields not required on the wire although the dataclass has no default.
REQUIRED_DROP: dict[str, list[str]] = {}


def _required_drop_for(identifier: str, cls: type) -> list[str]:
    if issubclass(cls, Cmd):
        return ["created_at"]
    return REQUIRED_DROP.get(identifier, [])


# Tombstone capability per contract; without an entry the routing class decides.
TOMBSTONE_OVERRIDES: dict[str, bool] = {}

# Contracts colcad produces and validates itself. They never appear in a
# bundle; the loader refuses one that declares them.
BUILTIN_ONLY = {"_StreamGap", "_EnrolledIdentity", "_TimeSync"}

# Registered payload classes that nothing may publish, each with the reason it
# still exists as a Python type. A door rejects a contract it does not know
# (0x90). An entry leaves when its reason does.
NOT_ON_THE_WIRE: dict[str, str] = {
    "_DataTag": (
        "an element of the _DataTags catalogue, never a record of its own: "
        "discovery is atomic, so a single tag is not a valid state"
    ),
}

# The JSON Schema keywords the loader accepts. `pattern` and `maxLength` are
# there because a ULID is 26 characters of one alphabet.
ALLOWED_KEYWORDS = {
    "type",
    "properties",
    "required",
    "enum",
    "items",
    "minLength",
    "maxLength",
    "pattern",
    "minimum",
    "maximum",
    "minItems",
    "additionalProperties",
}

# Nested value objects that refuse unknown keys, so no extra field can smuggle
# in a cleartext password or an outcome consumers read differently.
STRICT_NESTED_DATACLASSES = {
    "HealthMetricDeclaration",
    "NetworkInterface",
    "SealedSecretEnvelope",
    "NotificationChannelConfig",
    "NotificationChannelOutcome",
    "AlarmNotificationSummary",
}


# ---------------------------------------------------------------------------
# Type to schema mapping
# ---------------------------------------------------------------------------


def _schema_for_type(t: object, *, required: bool) -> dict:
    origin = typing.get_origin(t)
    args = typing.get_args(t)

    # Annotated[str, Pattern(...)], as payload.ULID uses. The pattern already
    # rules out an empty string, so no minLength.
    if origin is typing.Annotated:
        inner = _schema_for_type(args[0], required=required)
        for meta in args[1:]:
            if isinstance(meta, Pattern):
                inner["pattern"] = meta.regex
                inner.pop("minLength", None)
        return inner

    # Optional[X] / X | None → nullable schema of X
    if origin is typing.Union or str(origin) == "types.UnionType":
        non_none = [a for a in args if a is not type(None)]
        nullable = len(non_none) != len(args)
        if len(non_none) != 1:
            return {}  # heterogeneous union → accept anything
        inner = _schema_for_type(non_none[0], required=False)
        if nullable and "type" in inner:
            it = inner["type"]
            inner["type"] = sorted(set((it if isinstance(it, list) else [it]) + ["null"]))
            if "enum" in inner and None not in inner["enum"]:
                inner["enum"] = inner["enum"] + [None]
        return inner

    if t is typing.Any:
        return {}
    if isinstance(t, type) and issubclass(t, enum.Enum):
        return {"type": "string", "enum": [str(m.value) for m in t]}
    if t is str:
        s: dict = {"type": "string"}
        if required:
            s["minLength"] = 1
        return s
    if t in (int, float):
        return {"type": "number"}
    if t is bool:
        return {"type": "boolean"}
    if t is datetime.datetime:
        return {"type": "string"}
    if origin is dict or t is dict:
        return {"type": "object"}
    if origin is list or t is list:
        item = _schema_for_type(args[0], required=False) if args else {}
        return {"type": "array", "items": item}
    if is_dataclass(t):
        return _schema_for_dataclass(t, required_extra=[], required_drop=[])
    return {}  # unmappable → accept (never invent constraints)


def _schema_for_dataclass(cls: type, *, required_extra: list[str], required_drop: list[str]) -> dict:
    hints = typing.get_type_hints(cls, include_extras=True)  # keep Annotated[...] metadata
    props: dict = {}
    required: list[str] = []
    for f in fields(cls):
        is_req = f.default is MISSING and f.default_factory is MISSING
        if f.name in required_extra:
            is_req = True
        if f.name in required_drop:
            is_req = False
        props[f.name] = _schema_for_type(hints.get(f.name, typing.Any), required=is_req)
        if is_req:
            required.append(f.name)
    schema: dict = {"type": "object", "properties": props}
    if cls.__name__ in STRICT_NESTED_DATACLASSES:
        schema["additionalProperties"] = False
    if required:
        schema["required"] = sorted(required)
    return schema


def _lint_subset(schema: object, path: str = "") -> list[str]:
    """Name every keyword outside ALLOWED_KEYWORDS; each one is a generator bug."""
    bad: list[str] = []
    if isinstance(schema, dict):
        for k, v in schema.items():
            if path.endswith("properties"):
                bad += _lint_subset(v, f"{path}.{k}")
                continue
            if k not in ALLOWED_KEYWORDS:
                bad.append(f"{path}.{k}")
            if k in ("properties",):
                bad += _lint_subset(v, f"{path}.{k}")
            elif k == "items":
                bad += _lint_subset(v, f"{path}.items")
    return bad


# ---------------------------------------------------------------------------
# Bundle assembly
# ---------------------------------------------------------------------------


def _class_of(identifier: str, cls: type) -> str:
    if issubclass(cls, Cmd):
        return "cmd"
    if issubclass(cls, Ack):
        return "ack"
    if identifier in CLASS_TABLE:
        return CLASS_TABLE[identifier]
    raise SystemExit(
        f"generate_bundle: contract {identifier} ({cls.__name__}) has no routing class — "
        "derive it from Cmd/Ack or add it to CLASS_TABLE"
    )


def build_bundle(git_sha: str = "unknown") -> tuple[dict, str]:
    """Returns (bundle_dict_without_generated_at, sha256_digest)."""
    try:
        from importlib.metadata import version as _pkg_version

        bundle_version = _pkg_version("colca-data-contracts")
    except Exception:  # editable/source runs without metadata
        bundle_version = "0.0.0"

    contracts: dict = {}
    for identifier in sorted(PAYLOAD_CLASSES):
        cls = PAYLOAD_CLASSES[identifier]
        if identifier in BUILTIN_ONLY:
            raise SystemExit(
                f"generate_bundle: {identifier} is builtin-only and must never "
                f"be a registered payload class"
            )
        if identifier in NOT_ON_THE_WIRE:
            continue
        klass = _class_of(identifier, cls)
        schema = _schema_for_dataclass(
            cls,
            required_extra=REQUIRED_EXTRA.get(identifier, []),
            required_drop=_required_drop_for(identifier, cls),
        )
        if bad := _lint_subset(schema, identifier):
            raise SystemExit(f"generate_bundle: schema outside the supported subset: {bad}")
        # State classes, definitions included, are retracted by an empty payload.
        tombstone = TOMBSTONE_OVERRIDES.get(identifier, klass in ("data", "entity", "definition"))
        contracts[identifier] = {"class": klass, "tombstone": tombstone, "schema": schema}

    body = {
        "bundle_version": bundle_version,
        "source": {"package": "colca-data-contracts", "git_sha": git_sha},
        "contracts": contracts,
    }
    digest = hashlib.sha256(json.dumps(body, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    return body, digest


def write_bundle(out_path: str, git_sha: str = "unknown") -> str:
    body, digest = build_bundle(git_sha)
    artifact = dict(body)
    artifact["digest"] = digest
    artifact["generated_at"] = datetime.datetime.now(datetime.UTC).isoformat()
    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump(artifact, fh, sort_keys=True, separators=(",", ":"))
        fh.write("\n")
    return digest


if __name__ == "__main__":
    out = sys.argv[1] if len(sys.argv) > 1 else "colca-contracts-bundle.json"
    sha = sys.argv[2] if len(sys.argv) > 2 else "unknown"
    d = write_bundle(out, sha)
    print(f"{d}  {out}")
