"""Generate the colca contracts bundle (schema-bundle design §5).

Introspects the registered payload classes (the same PAYLOAD_CLASSES registry
the package builds on import) and emits ONE canonical-JSON bundle: a manifest
plus a restricted JSON-Schema (draft 2020-12 subset, design §4.1) per
contract. The bundle is deterministic — same source tree ⇒ byte-identical
canonical region ⇒ same sha256 digest; `generated_at` sits OUTSIDE the digest
input.

Every mapping rule that is not derivable from the dataclasses lives in the
explicit tables below — visible in review, never inferred by magic (§5.1).

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

import colca_data_contracts  # noqa: F401  (import populates the registry)
from franzmq.data_contracts import PAYLOAD_CLASSES
from franzmq.data_contracts.base import Ack, Cmd

# ---------------------------------------------------------------------------
# Explicit tables (design §5.1: "an explicit small table in the generator")
# ---------------------------------------------------------------------------

# Routing class for everything not derivable from the class hierarchy
# (Cmd subclasses → "cmd", Ack subclasses → "ack" — derived, not listed).
CLASS_TABLE: dict[str, str] = {
    "_Metric": "data",
    "_Log": "data",
    "_AlarmStateChange": "data",
    "_NotificationDispatched": "data",
    "_DBEvent": "data",
    "_DBDump": "data",
    "_SystemElement": "entity",
    "_Signal": "entity",
    "_Constant": "entity",
    "_EditOperation": "entity",
    "_DataTags": "entity",
    "_Node": "entity",
    "_ServiceDetails": "entity",
    "_ExternalReference": "entity",
    # Definitions: authored once, needed everywhere below the author, and the
    # same thing at every node — so they descend and are applied as state
    # (definition-stream design §2). They were "entity" only because there was
    # no downward flow to put them on, which meant they replicated the wrong
    # way, away from the nodes that need them.
    "_Group": "definition",
    "_AnnotationType": "definition",
    "_MetadataType": "definition",
    "_Interface": "definition",
    "_ExternalSystem": "definition",
    "_AlarmNotificationConfig": "entity",
}

# Fields required beyond the no-default rule: door contracts the dataclass
# defaults hide (Metric.signal_id defaults to "" for constructor convenience,
# but the door demands it; Ack.result_code likewise).
REQUIRED_EXTRA: dict[str, list[str]] = {
    "_Metric": ["signal_id"],
    "_Ack": ["result_code"],
    "_CmdEdit": ["operation_id", "intent", "expected_versions"],
}

# Fields DROPPED from required although the dataclass has no default:
# colca publishers do not stamp created_at on commands — expires_at is the
# door contract (cmdadmin design §2).
REQUIRED_DROP: dict[str, list[str]] = {}


def _required_drop_for(identifier: str, cls: type) -> list[str]:
    if issubclass(cls, Cmd):
        return ["created_at"]
    return REQUIRED_DROP.get(identifier, [])


# Tombstone capability (retention §7 / design §10.1): default = class rule.
TOMBSTONE_OVERRIDES: dict[str, bool] = {}

# Builtin-only contracts (design §7.1/§10.2): produced and validated by the
# colca binary itself — they must never appear in a bundle, and the loader
# refuses a bundle that declares them.
BUILTIN_ONLY = {"_StreamGap", "_EnrolledIdentity", "_TimeSync"}

# Registered payload classes that are deliberately NOT offered to the broker,
# with the reason each one is here. They stay Python types because something
# still constructs them; they are absent from the bundle because nothing may
# publish them, and a door that does not know a contract rejects it (0x90).
#
# This is not a place to park contracts that are merely unused — every entry
# names a concrete producer or container, and the entry leaves when that does.
NOT_ON_THE_WIRE: dict[str, str] = {
    "_DataTag": (
        "an element of the _DataTags catalogue, never a record of its own: "
        "discovery is atomic, so a single tag is not a valid state "
        "(data-model binding design §3.1)"
    ),
    "_DataTagContext": (
        "retired from the wire with the DataTagContext model; still serialized "
        "by the un-migrated Django publisher until both are deleted "
        "(data-model binding design §2, §11)"
    ),
    "_DataTagContexts": "as _DataTagContext — the list form of the same retired contract",
}

# The JSON-Schema keyword subset the loader enforces (design §4.1).
ALLOWED_KEYWORDS = {
    "type", "properties", "required", "enum", "items",
    "minLength", "minimum", "maximum", "minItems", "additionalProperties",
}


# ---------------------------------------------------------------------------
# Type → schema mapping (design §5.1 rules)
# ---------------------------------------------------------------------------


def _schema_for_type(t: object, *, required: bool) -> dict:
    origin = typing.get_origin(t)
    args = typing.get_args(t)

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
    hints = typing.get_type_hints(cls)
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
    if required:
        schema["required"] = sorted(required)
    return schema


def _lint_subset(schema: object, path: str = "") -> list[str]:
    """Every keyword outside the §4.1 subset is a generator bug — name it."""
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
        f"derive it from Cmd/Ack or add it to CLASS_TABLE (design §5.1: explicit, reviewed)"
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
                f"generate_bundle: {identifier} is builtin-only (design §10.2) and must never "
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
            raise SystemExit(f"generate_bundle: schema outside the §4.1 subset: {bad}")
        # State classes are retractable: an empty payload retires the path.
        # Definitions are state too — a group or a type has to be withdrawable,
        # and a tombstone is how (definition-stream design §4).
        tombstone = TOMBSTONE_OVERRIDES.get(identifier, klass in ("data", "entity", "definition"))
        contracts[identifier] = {"class": klass, "tombstone": tombstone, "schema": schema}

    body = {
        "bundle_version": bundle_version,
        "source": {"package": "colca-data-contracts", "git_sha": git_sha},
        "contracts": contracts,
    }
    digest = hashlib.sha256(
        json.dumps(body, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return body, digest


def write_bundle(out_path: str, git_sha: str = "unknown") -> str:
    body, digest = build_bundle(git_sha)
    artifact = dict(body)
    artifact["digest"] = digest
    artifact["generated_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump(artifact, fh, sort_keys=True, separators=(",", ":"))
        fh.write("\n")
    return digest


if __name__ == "__main__":
    out = sys.argv[1] if len(sys.argv) > 1 else "colca-contracts-bundle.json"
    sha = sys.argv[2] if len(sys.argv) > 2 else "unknown"
    d = write_bundle(out, sha)
    print(f"{d}  {out}")
