"""YAML data-model loader and manifest compiler.

A Data Model is a structural contract: what a SystemElement must offer to
satisfy it (measured/computed signals, mandated child elements) -- never HOW
an engineered point is computed. Authoring is YAML again
(the data model yaml authoring design); this module
is the loader/compiler front-end. ``yaml`` is imported locally inside the
loading function rather than at module scope, mirroring
``colca_data_contracts.platform_tags``'s convention: every consumer of this
package installs it ``--no-deps`` and declares its own YAML parser, so a
service that never calls ``compile_models()`` never needs PyYAML installed.

``compile_models(source_dir=None)`` loads every ``*.yaml`` file directly
under ``source_dir`` (default: this package's own ``data_models/``
directory, i.e. the four builtin platform models), resolves ``extends``,
validates, and emits a deterministic list of manifests. Manifests are
generated, never hand-edited -- ids are NOT assigned here (whoever seeds
the definitions derives them).

YAML schema (one file per model)::

    name: MachineState
    version: "1.0"
    description: |
      Platform-default machine-state contract. ...
    extends: [Machine]
    signals:
      - name: machine_state          # slot key AND signal name
        data_type: integer           # canonical: boolean|integer|number|string|json
        semantic_type: machine-state # optional tag name; the loader emits a
                                      # seedable stub for any tag referenced
                                      # here that has no existing definition
        enum: [UNKNOWN, OFFLINE, DOWN, IDLE, EXECUTING]
        computed: true               # -> kind "computed"; default false -> "measured"
        required: true               # default true
        unit: null                   # optional
        description: |
          0 UNKNOWN | 1 OFFLINE | ...
    children:
      - name: drive_end_bearing      # slot key
        child_model: BearingModel    # optional; omitted -> plain structural child
        entity_name: "M-101 DE Bearing"  # optional; defaults to the slot key
        required: true
        description: ""

Resolution rules:

- ``name`` and ``version`` are required on every model. A model missing
  either is a load error. Two files declaring the same ``name`` is a load
  error (duplicate data model name).
- A signal's ``data_type`` must be one of ``CANONICAL_DATA_TYPES``.
- ``extends`` names sibling models by their ``name``; an unresolvable name
  is a load error. Slots are flattened topologically: a model's own slots
  always win over any inherited one (child-over-parent), and among multiple
  parents a later entry in the ``extends`` list wins over an earlier one for
  the same slot key. ``declared_by`` on a merged slot names the model whose
  YAML originally declared it -- an override changes the value AND the
  owner. The manifest's own ``extends`` field is the model's directly
  declared parent list, not the transitive ancestry.
- A cycle in ``extends`` (A extends B extends A) is a load error naming the
  full cycle.
- A ``children:`` entry whose ``child_model`` names an unknown model is a
  load error naming the declaring model and the slot. A cycle across
  ``child_model`` references is a load error naming the full path.
- Two child slots of one (flattened) model resolving to the same
  ``entity_name`` is a load error -- one physical child cannot satisfy two
  slots.
- A ``children:`` entry's explicit ``entity_name: ""`` is a load error -- an
  empty override is never intentional. Omitting the key entirely still
  defaults to the slot key, as before.
- Two signals -- in the same model or across different models -- referencing
  the same ``semantic_type`` name with a different ``data_type`` is a load
  error naming every model involved and the ``data_type`` each one declared.
  A semantic tag name seeds exactly one stub (``_semantic_tag_stub``); a
  divergent second declaration would otherwise seed a conflicting one, which
  the seeder's existing-wins rule then resolves by silent first-write-wins.
- Every problem found during a single ``compile_models()`` call is
  accumulated and raised together as one ``CompileError``.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

# Public alias (one owner per fact, architecture principle 2): every other
# place that needs the canonical slot data_type vocabulary -- the API's
# `edge.edit.model_rules.MODEL_SLOT_DATA_TYPES` and colca's
# `plugins/uns.slotDataTypes` -- pins its key set against this frozenset
# rather than re-declaring it.
CANONICAL_DATA_TYPES = frozenset({"boolean", "integer", "number", "string", "json"})


class CompileError(ValueError):
    pass


def _default_source_dir() -> Path:
    return Path(__file__).resolve().parent


def _clean(text: str | None) -> str:
    return (text or "").strip()


def _load_documents(source_dir: Path) -> tuple[dict[str, dict[str, Any]], list[str]]:
    """Read every *.yaml file in source_dir; key by the model's own ``name``."""
    import yaml  # local: see module docstring

    documents: dict[str, dict[str, Any]] = {}
    problems: list[str] = []
    for path in sorted(source_dir.glob("*.yaml")):
        raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        name = raw.get("name")
        if not name:
            problems.append(f"{path.name}: missing required 'name'")
            continue
        if not raw.get("version"):
            problems.append(f"{name}: has no version and cannot be published")
            continue
        if name in documents:
            problems.append(f"duplicate data model name {name!r} ({path.name})")
            continue
        documents[name] = raw
    return documents, problems


def _own_slots(name: str, raw: dict[str, Any]) -> tuple[dict[str, dict[str, Any]], list[str]]:
    """Slots this model's own YAML declares, stamped declared_by=name."""
    slots: dict[str, dict[str, Any]] = {}
    problems: list[str] = []
    for item in raw.get("signals") or []:
        key = item["name"]
        data_type = item.get("data_type")
        if data_type not in CANONICAL_DATA_TYPES:
            problems.append(
                f"{name}.{key}: unknown data_type {data_type!r}; "
                f"use one of {sorted(CANONICAL_DATA_TYPES)}"
            )
        slots[key] = {
            "key": key,
            "kind": "computed" if item.get("computed", False) else "measured",
            "data_type": data_type,
            "semantic_type": item.get("semantic_type"),
            "unit": item.get("unit"),
            "enum": list(item["enum"]) if item.get("enum") else None,
            "required": item.get("required", True),
            "description": _clean(item.get("description")),
            "declared_by": name,
            "child_model": None,
            "entity_name": None,
        }
    for item in raw.get("children") or []:
        key = item["name"]
        entity_name = item.get("entity_name")
        if entity_name is not None and not entity_name.strip():
            problems.append(
                f"{name}.{key}: entity_name is explicitly empty; omit the key entirely "
                f"to default to the slot key"
            )
            entity_name = key
        else:
            entity_name = entity_name or key
        slots[key] = {
            "key": key,
            "kind": "child",
            "data_type": "",
            "semantic_type": None,
            "unit": None,
            "enum": None,
            "required": item.get("required", True),
            "description": _clean(item.get("description")),
            "declared_by": name,
            "child_model": item.get("child_model"),
            "entity_name": entity_name,
        }
    return slots, problems


def _detect_cycles(nodes: list[str], edges: dict[str, list[tuple[str, ...]]]) -> list[str]:
    """DFS over an edge map (node -> [(*labels, target), ...]); reports every
    cycle found as one "trail -> ... -> trail[start]" string, full path
    included. Edges to a node outside `nodes` are ignored here -- an
    unresolved reference is reported by its own dedicated check."""
    problems: list[str] = []
    WHITE, GRAY, BLACK = 0, 1, 2
    color: dict[str, int] = dict.fromkeys(nodes, WHITE)
    trail: list[str] = []

    def visit(node: str) -> None:
        color[node] = GRAY
        trail.append(node)
        for *labels, target in edges.get(node, []):
            if target not in color:
                continue
            if color[target] == GRAY:
                start = trail.index(target)
                problems.append(" -> ".join([*trail[start:], *labels, target]))
            elif color[target] == WHITE:
                trail.extend(labels)
                visit(target)
                if labels:
                    del trail[-len(labels):]
        trail.pop()
        color[node] = BLACK

    for node in sorted(nodes):
        if color[node] == WHITE:
            visit(node)
    return problems


def _flatten(
    name: str,
    own_slots: dict[str, dict[str, dict[str, Any]]],
    parents: dict[str, list[str]],
    cache: dict[str, dict[str, dict[str, Any]]],
    resolving: set[str],
) -> dict[str, dict[str, Any]]:
    """Topologically merge a model's slots: parents (in extends order, later
    wins over earlier), then this model's own slots (always win)."""
    if name in cache:
        return cache[name]
    if name in resolving:
        return dict(own_slots.get(name, {}))  # cycle -- already reported separately
    resolving.add(name)
    merged: dict[str, dict[str, Any]] = {}
    for parent in parents.get(name, []):
        merged.update(_flatten(parent, own_slots, parents, cache, resolving))
    merged.update(own_slots.get(name, {}))
    resolving.discard(name)
    cache[name] = merged
    return merged


def _semantic_tag_stub(tag_name: str, slot: dict[str, Any]) -> dict[str, Any]:
    """Minimal seedable tag spec for a referenced tag. The seeder only uses
    this when no live tag of that name exists (existing wins)."""
    return {
        "name": tag_name,
        "i18n_name": tag_name.replace("-", " ").capitalize(),
        "description": "",
        "applies_to": ["signal"],
        "icon": "category",
        "data_type": slot["data_type"],
    }


def _tag_stub_conflicts(own_slots: dict[str, dict[str, dict[str, Any]]]) -> list[str]:
    """Two signals -- in the same model or across different models -- that
    reference the same `semantic_type` name with different `data_type` would
    each seed a different tag stub (`_semantic_tag_stub`). The seeder's
    existing-wins rule then makes whichever happens to seed first silently
    win, so the loser's data_type is never enforced. One tag name is one fact
    (architecture principle 2: one owner per fact); reject the divergence at
    load time instead, naming every model that declared it and the data_type
    each one chose.
    """
    problems: list[str] = []
    first_seen: dict[str, tuple[str, str]] = {}  # tag_name -> (data_type, model_name)
    for model_name in sorted(own_slots):
        for key in sorted(own_slots[model_name]):
            slot = own_slots[model_name][key]
            tag_name = slot["semantic_type"]
            if not tag_name:
                continue
            data_type = slot["data_type"]
            seen = first_seen.setdefault(tag_name, (data_type, model_name))
            seen_data_type, seen_model = seen
            if seen_data_type != data_type:
                problems.append(
                    f"semantic_type {tag_name!r} is declared with data_type {seen_data_type!r} "
                    f"by {seen_model!r} and with data_type {data_type!r} by {model_name!r}"
                )
    return problems


def _child_edges(slots: list[dict[str, Any]]) -> list[tuple[str, str]]:
    """(slot_key, child_model_name) pairs for a manifest's child slots."""
    return [(s["key"], s["child_model"]) for s in slots if s["kind"] == "child" and s["child_model"]]


def _duplicate_child_names(model_name: str, slots: list[dict[str, Any]]) -> list[str]:
    """Two child slots of ONE model resolving to the same `entity_name`.

    A child slot's entity_name is the name of the physical child element the
    slot resolves to, so two different keys naming the same one describe two
    slots that can never both be satisfied.
    """
    problems: list[str] = []
    owner_of: dict[str, str] = {}
    for slot in slots:
        if slot["kind"] != "child":
            continue
        entity_name = slot["entity_name"] or slot["key"]
        owner = owner_of.get(entity_name)
        if owner is None:
            owner_of[entity_name] = slot["key"]
            continue
        problems.append(
            f"{model_name}: child slots {owner!r} and {slot['key']!r} both resolve to "
            f"entity_name {entity_name!r}"
        )
    return problems


def compile_models(source_dir: Path | None = None) -> list[dict[str, Any]]:
    directory = source_dir if source_dir is not None else _default_source_dir()
    documents, problems = _load_documents(directory)

    own_slots: dict[str, dict[str, dict[str, Any]]] = {}
    for name, raw in documents.items():
        slots, slot_problems = _own_slots(name, raw)
        own_slots[name] = slots
        problems.extend(slot_problems)

    problems.extend(_tag_stub_conflicts(own_slots))

    parents: dict[str, list[str]] = {}
    for name, raw in documents.items():
        valid_parents = []
        for parent in raw.get("extends") or []:
            if parent not in documents:
                problems.append(f"{name}: extends unknown model {parent!r}")
                continue
            valid_parents.append(parent)
        parents[name] = valid_parents

    extends_edges = {name: [(p,) for p in parents[name]] for name in documents}
    problems.extend(_detect_cycles(list(documents), extends_edges))

    cache: dict[str, dict[str, dict[str, Any]]] = {}
    manifests: dict[str, dict[str, Any]] = {}
    for name in sorted(documents):
        raw = documents[name]
        flattened = _flatten(name, own_slots, parents, cache, set())
        tags: dict[str, dict[str, Any]] = {}
        for slot in flattened.values():
            if slot["semantic_type"]:
                tags.setdefault(slot["semantic_type"], _semantic_tag_stub(slot["semantic_type"], slot))
        manifests[name] = {
            "name": name,
            "version": raw["version"],
            "description": _clean(raw.get("description")),
            "extends": list(raw.get("extends") or []),
            "slots": [flattened[key] for key in sorted(flattened)],
            "semantic_tags": [tags[key] for key in sorted(tags)],
        }

    for name, manifest in manifests.items():
        for slot_key, child_name in _child_edges(manifest["slots"]):
            if child_name not in documents:
                problems.append(
                    f"{name}.{slot_key}: child_model {child_name!r} is not a known data model"
                )
        problems.extend(_duplicate_child_names(name, manifest["slots"]))

    child_edges = {
        name: [
            (slot_key, child_name)
            for slot_key, child_name in _child_edges(manifests[name]["slots"])
            if child_name in documents
        ]
        for name in documents
    }
    problems.extend(_detect_cycles(list(documents), child_edges))

    if problems:
        raise CompileError("; ".join(problems))

    return [manifests[name] for name in sorted(manifests)]


def manifest_json(manifests: list[dict[str, Any]]) -> str:
    return json.dumps(manifests, sort_keys=True, indent=2, ensure_ascii=False) + "\n"
