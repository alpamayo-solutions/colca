"""The semantic tags the platform itself ships.

`semantic_tags/platform.yaml` is the one owner of what Colca seeds. This
module publishes the `name` values from that file so a consumer can tell a
platform-owned tag from an authored one: the editor refuses to delete or
edit one, and the catalog read reports the flag per row.

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

#: The seed file this module reads. The same file `node_manager`'s
#: `platform_semantic_tags()` renders into bootstrap definitions.
PLATFORM_SEMANTIC_TAG_SEED = Path(__file__).parent / "semantic_tags" / "platform.yaml"


def _seeded_names() -> frozenset[str]:
    document = yaml.safe_load(PLATFORM_SEMANTIC_TAG_SEED.read_text(encoding="utf-8")) or {}
    names = frozenset(tag["name"] for tag in document.get("tags") or [])
    if not names:
        raise ValueError(f"{PLATFORM_SEMANTIC_TAG_SEED} seeds no semantic tags")
    return names


#: The `name` of every tag shipped in `semantic_tags/platform.yaml`. A tag
#: carrying one of these names is platform-owned: read-only in the editor,
#: re-asserted by every `colca node bootstrap`.
PLATFORM_SEMANTIC_TAG_NAMES: frozenset[str] = _seeded_names()
