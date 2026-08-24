"""The definitions the platform itself ships.

`semantic_tags/platform.yaml` and `metadata_types/platform.yaml` are the one
owner of what Colca seeds. This module publishes the `name` values from those
files so a consumer can tell a platform-owned definition from an authored one:
the editor refuses to delete or edit one, and the catalog read reports the
flag per row.

The names are READ from the YAML, never re-typed beside it. A hand-kept copy
cannot see the seed file grow, so a tag added to `platform.yaml` would ship
unprotected while the copy stayed green -- a check that cannot go red.

PyYAML is imported here rather than added to this package's dependencies. The
shipped `interfaces/*.yaml` and `semantic_tags/*.yaml` have always been parsed
by whoever reads them (every container installs this package `--no-deps`, so a
declaration would install nothing anyway), so each consumer declares the parser
itself -- `api` and `node-manager` both list `pyyaml` outright. `__init__` does
not import this module, so a service that never asks about platform tags never
pays for it.
"""

from pathlib import Path

import yaml

#: The seed files this module reads. The same files `node_manager`'s
#: `platform_semantic_tags()` and `platform_metadata_types()` render into
#: bootstrap definitions.
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
#: carrying one of these names is platform-owned: read-only in the editor,
#: re-asserted by every `colca node bootstrap`.
PLATFORM_SEMANTIC_TAG_NAMES: frozenset[str] = _seeded_names(PLATFORM_SEMANTIC_TAG_SEED, "tags")

#: The `name` of every metadata type shipped in `metadata_types/platform.yaml`.
#: Same rule, same reason: generated services publish records keyed by these
#: names, so an operator editing or deleting one would break the projection of
#: every record that carries it — and the next bootstrap would put it back.
PLATFORM_METADATA_TYPE_NAMES: frozenset[str] = _seeded_names(PLATFORM_METADATA_TYPE_SEED, "types")

#: The `name` of every annotation type shipped in
#: `annotation_types/platform.yaml`. Same rule again: these are the entry kinds
#: Colca expects on every node, so a UI may offer them without asking whether
#: this deployment happened to declare them.
PLATFORM_ANNOTATION_TYPE_NAMES: frozenset[str] = _seeded_names(
    PLATFORM_ANNOTATION_TYPE_SEED, "types",
)
