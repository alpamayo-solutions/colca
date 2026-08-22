"""The vocabularies a `_SemanticTag` definition may use.

One owner, two consumers that cannot import each other: the API's command
boundary refuses a definition outside these sets, and node-manager validates
the same fields in an edge YAML's `seed.semantic_types` before it ever
generates a bootstrap manifest. Written twice, they disagreed -- node-manager
refused `data_type: "float"` while the live write endpoint accepted it and
then made every entity using the tag unwritable.

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
