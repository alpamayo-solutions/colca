"""The vocabularies a `_SemanticTag` definition may use.

One owner for every consumer: a command boundary refuses a definition outside
these sets, and a tool that seeds definitions validates the same fields before
it writes them. Written twice, they disagreed: one side refused
`data_type: "float"` while the other accepted it and then made every entity
using the tag unwritable.

`data_type` is one canonical vocabulary because `Signal.data_type` and
`ConstantDataType` deliberately differ; the API maps canonical -> each model's
own (semantic-types design section 3).
"""

#: What a tag may classify. A tag declares a non-empty subset.
APPLIES_TO_KINDS: frozenset[str] = frozenset({
    "system_element",
    "signal",
    "constant",
})

#: The canonical type a tag may require of the entity it classifies.
SEMANTIC_DATA_TYPES: frozenset[str] = frozenset({
    "boolean",
    "integer",
    "number",
    "string",
    "datetime",
    "json",
})

#: Topic groups: the words a person uses for a KIND of data, mapped to the
#: semantic tags that carry it. This is operator vocabulary — "pull up the
#: quality data" selects by tag, never by signal-name matching — and it lives
#: here because the tags themselves do: one owner for the vocabulary and for
#: how it is grouped. A plant that lacks a tag simply contributes nothing for
#: it; consumers also lexically match the topic word against tag names, so a
#: plant-authored tag like `quality-index` is found under "quality" even if
#: this table never names it.
SEMANTIC_TOPIC_GROUPS: dict[str, frozenset[str]] = {
    "quality": frozenset({
        "quality-index", "quality-rate", "moisture", "viscosity", "particle-size",
    }),
    "oee": frozenset({"oee", "availability", "performance", "quality-rate"}),
    "throughput": frozenset({"throughput", "parts-per-minute", "cycle-time", "batch"}),
    "state": frozenset({"machine-state", "state-reason", "connectivity", "heartbeat"}),
    "process": frozenset({"temperature", "pressure", "speed", "level", "setpoint", "recipe"}),
    "energy": frozenset({"power", "energy", "rated-power"}),
    "downtime": frozenset({"downtime", "machine-state", "state-reason"}),
}
