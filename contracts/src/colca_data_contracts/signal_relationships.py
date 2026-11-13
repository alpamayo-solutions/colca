"""The rules signal relationships follow, wherever they are declared.

A relationship is a directed link from one signal (the source) to another (the
target) with a ``SignalRelationshipType``. The same rules hold for the links a
node's ``_Signal`` records carry, for the links data-model slots declare, and
for the links a plant model declares, so this module states them once over an
abstract graph: ``relationship_problems`` takes every end keyed by whatever
identifies it (a signal id, a slot key, a signal path) and returns one message
per broken rule. colca's Go suite checks its copy against
``vectors/signal_relationships.json``.

Rules, per type (``RULES``):

- Every type: the target is another end of the graph (never the source
  itself), and a source holds at most one link of each type.
- ``actual``: a target is the actual value of at most one setpoint, and a
  target is not itself a setpoint (no chains). Data types are equal, or both
  numeric.
- ``upper_limit`` / ``lower_limit``: both data types numeric. A limit may have
  limits of its own, but the links of one type form no cycle.
- ``feedback``: any data types; no chains.

A data type that is not known (``None`` or ``""``) passes the data type rule:
the link is checked again when the type is set.
"""

from __future__ import annotations

from collections.abc import Callable, Collection, Mapping, Sequence
from dataclasses import dataclass
from typing import Literal

from colca_data_contracts.payload import SignalRelationshipType

#: Data types that count as numeric, in both vocabularies: the data-model
#: slot types (``integer``, ``number``) and the signal types (``int``,
#: ``float``).
NUMERIC_DATA_TYPES = frozenset({"integer", "number", "int", "float"})

DataTypeRule = Literal["same_or_numeric", "numeric", "any"]


@dataclass(frozen=True)
class RelationshipRule:
    #: At most this many setpoints may name one target with this type; None
    #: for no limit.
    max_sources_per_target: int | None
    #: Whether a target may itself be the source of a link of this type.
    #: Without chains there can be no cycle either.
    chains: bool
    data_types: DataTypeRule


RULES: dict[SignalRelationshipType, RelationshipRule] = {
    SignalRelationshipType.ACTUAL: RelationshipRule(
        max_sources_per_target=1, chains=False, data_types="same_or_numeric"
    ),
    SignalRelationshipType.UPPER_LIMIT: RelationshipRule(
        max_sources_per_target=None, chains=True, data_types="numeric"
    ),
    SignalRelationshipType.LOWER_LIMIT: RelationshipRule(
        max_sources_per_target=None, chains=True, data_types="numeric"
    ),
    SignalRelationshipType.FEEDBACK: RelationshipRule(max_sources_per_target=None, chains=False, data_types="any"),
}


@dataclass(frozen=True)
class RelationshipEnd:
    """One signal (or slot) as the rules see it: its data type and the links
    it holds, as ``(relationship_type, target key)`` pairs."""

    data_type: str | None
    relationships: Sequence[tuple[str, str]] = ()


def _types_compatible(rule: DataTypeRule, source: str | None, target: str | None) -> bool:
    if rule == "any" or not source or not target:
        return True
    both_numeric = source in NUMERIC_DATA_TYPES and target in NUMERIC_DATA_TYPES
    if rule == "numeric":
        return both_numeric
    return source == target or both_numeric


def relationship_problems(
    ends: Mapping[str, RelationshipEnd],
    *,
    label: Callable[[str], str] = str,
    checked: Callable[[str], bool] | None = None,
    not_signals: Collection[str] = (),
) -> list[str]:
    """Every broken rule in ``ends``, as one message each, in a stable order.

    ``label`` turns a key into the name a message uses. ``checked`` limits the
    per-source rules to the sources it accepts (the rules across sources still
    see every end), so a caller validating one write is not refused for links
    that were already there. ``not_signals`` names keys that exist but are no
    signal (a child slot), so a link to one reads as that rather than as a
    missing target.
    """
    problems: list[str] = []
    known_types = {str(member) for member in SignalRelationshipType}
    sources_of: dict[tuple[str, str], list[str]] = {}
    edges: dict[str, dict[str, str]] = {}  # type -> source -> target

    for key in sorted(ends):
        end = ends[key]
        report = checked is None or checked(key)
        seen_types: set[str] = set()
        for relationship_type, target in end.relationships:
            if relationship_type not in known_types:
                if report:
                    problems.append(
                        f"{label(key)}: unknown relationship type {relationship_type!r}; "
                        f"use one of {sorted(known_types)}"
                    )
                continue
            if relationship_type in seen_types:
                if report:
                    problems.append(f"{label(key)}: holds more than one {relationship_type} relationship")
                continue
            seen_types.add(relationship_type)
            if target == key:
                if report:
                    problems.append(f"{label(key)}: {relationship_type} names the signal itself")
                continue
            target_end = ends.get(target)
            if target_end is None:
                if report:
                    reason = "is not a signal" if target in not_signals else "does not exist"
                    problems.append(f"{label(key)}: {relationship_type} target {label(target)} {reason}")
                continue
            rule = RULES[SignalRelationshipType(relationship_type)]
            if report and not _types_compatible(rule.data_types, end.data_type, target_end.data_type):
                problems.append(
                    f"{label(key)}: {relationship_type} target {label(target)} has data type "
                    f"{target_end.data_type!r}, not compatible with {end.data_type!r}"
                )
            sources_of.setdefault((relationship_type, target), []).append(key)
            edges.setdefault(relationship_type, {})[key] = target

    for relationship_type in sorted(edges):
        rule = RULES[SignalRelationshipType(relationship_type)]
        links = edges[relationship_type]
        if rule.max_sources_per_target is not None:
            for (kind, target), sources in sorted(sources_of.items()):
                if kind != relationship_type or len(sources) <= rule.max_sources_per_target:
                    continue
                if checked is not None and not any(checked(source) for source in sources):
                    continue
                names = ", ".join(label(source) for source in sources)
                problems.append(f"{label(target)} is the {relationship_type} target of more than one signal: {names}")
        if not rule.chains:
            for source in sorted(links):
                target = links[source]
                if target in links and (checked is None or checked(source) or checked(target)):
                    problems.append(
                        f"{label(source)}: {relationship_type} target {label(target)} holds a "
                        f"{relationship_type} relationship itself; {relationship_type} links do not chain"
                    )
            continue
        problems.extend(_cycles(relationship_type, links, label, checked))
    return problems


def _cycles(
    relationship_type: str,
    links: Mapping[str, str],
    label: Callable[[str], str],
    checked: Callable[[str], bool] | None,
) -> list[str]:
    """One message per cycle in a type's links. Each source holds at most one
    link of a type, so the links are a functional graph: following them from
    any source either ends or enters exactly one cycle."""
    problems: list[str] = []
    reported: set[str] = set()
    for start in sorted(links):
        trail: list[str] = []
        node = start
        while node in links and node not in trail and node not in reported:
            trail.append(node)
            node = links[node]
        if node in trail:
            cycle = trail[trail.index(node) :]
            reported.update(cycle)
            if checked is None or any(checked(member) for member in cycle):
                path = " -> ".join(label(member) for member in [*cycle, node])
                problems.append(f"{relationship_type} links form a cycle: {path}")
        reported.update(trail)
    return problems


def wire_relationships(raw: object) -> list[tuple[str, str]]:
    """``(relationship_type, target_signal_id)`` pairs of a ``_Signal`` record's
    ``relationships`` field; anything unreadable is skipped (the door's schema
    refuses it)."""
    if not isinstance(raw, list):
        return []
    pairs: list[tuple[str, str]] = []
    for item in raw:
        if isinstance(item, dict):
            relationship_type = item.get("relationship_type")
            target = item.get("target_signal_id")
            if isinstance(relationship_type, str) and isinstance(target, str):
                pairs.append((relationship_type, target))
    return pairs
