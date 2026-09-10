"""The vocabularies a `_SemanticTag` definition may use.

The command boundary refuses a definition outside these sets, and tools that
seed definitions check the same fields. `data_type` is a canonical vocabulary
because `Signal.data_type` and `ConstantDataType` differ; each maps from it.
"""

#: What a tag may classify. A tag declares a non-empty subset.
APPLIES_TO_KINDS: frozenset[str] = frozenset(
    {
        "system_element",
        "signal",
        "constant",
    }
)

#: The canonical type a tag may require of the entity it classifies.
SEMANTIC_DATA_TYPES: frozenset[str] = frozenset(
    {
        "boolean",
        "integer",
        "number",
        "string",
        "datetime",
        "json",
    }
)

#: Topic groups: the words people use for a kind of data, mapped to the
#: semantic tags that carry it ("quality data" selects by tag, not by signal
#: name). Consumers also match the word against tag names, so an authored tag
#: like `quality-index` is found under "quality" too.
SEMANTIC_TOPIC_GROUPS: dict[str, frozenset[str]] = {
    "quality": frozenset(
        {
            "quality-index",
            "quality-rate",
            "moisture",
            "viscosity",
            "particle-size",
        }
    ),
    "oee": frozenset({"oee", "availability", "performance", "quality-rate"}),
    "throughput": frozenset({"throughput", "parts-per-minute", "cycle-time", "batch"}),
    "state": frozenset({"machine-state", "state-reason", "connectivity", "heartbeat"}),
    "process": frozenset({"temperature", "pressure", "speed", "level", "setpoint", "recipe"}),
    "energy": frozenset({"power", "energy", "rated-power"}),
    "downtime": frozenset({"downtime", "machine-state", "state-reason"}),
}
