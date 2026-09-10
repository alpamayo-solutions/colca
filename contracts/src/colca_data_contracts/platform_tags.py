"""The definitions the platform itself ships.

This module publishes the `name` values from the platform seed files, so a
consumer can tell a platform-owned definition from an authored one. The names
are read from the YAML, never copied.

PyYAML is not a dependency of this package: a consumer that reads the shipped
YAML declares it. `__init__` does not import this module.
"""

from pathlib import Path

import yaml

#: The seed files this module reads, the same files a bootstrap renders into
#: definitions.
PLATFORM_SEMANTIC_TAG_SEED = Path(__file__).parent / "semantic_tags" / "platform.yaml"
PLATFORM_METADATA_TYPE_SEED = Path(__file__).parent / "metadata_types" / "platform.yaml"
PLATFORM_ANNOTATION_TYPE_SEED = Path(__file__).parent / "annotation_types" / "platform.yaml"


def _seeded_names(seed: Path, section: str) -> frozenset[str]:
    document = yaml.safe_load(seed.read_text(encoding="utf-8")) or {}
    names = frozenset(item["name"] for item in document.get(section) or [])
    if not names:
        raise ValueError(f"{seed} seeds no {section}")
    return names


#: The `name` of every tag shipped in `semantic_tags/platform.yaml`. A tag
#: carrying one of these names is platform-owned: read-only in an editor,
#: re-asserted by every bootstrap.
PLATFORM_SEMANTIC_TAG_NAMES: frozenset[str] = _seeded_names(PLATFORM_SEMANTIC_TAG_SEED, "tags")

#: The `name` of every metadata type shipped in `metadata_types/platform.yaml`.
#: Services publish records keyed by these names, so they are read-only too.
PLATFORM_METADATA_TYPE_NAMES: frozenset[str] = _seeded_names(PLATFORM_METADATA_TYPE_SEED, "types")

#: The `name` of every annotation type shipped in
#: `annotation_types/platform.yaml`, available on every node.
PLATFORM_ANNOTATION_TYPE_NAMES: frozenset[str] = _seeded_names(
    PLATFORM_ANNOTATION_TYPE_SEED,
    "types",
)
