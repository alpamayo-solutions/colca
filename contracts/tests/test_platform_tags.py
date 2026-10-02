"""The published platform-tag names come from the seed file, not from a copy.

The assertions re-parse the YAML instead of listing names, so a hand-kept copy
would fail as soon as the seed grows.
"""

from pathlib import Path

import yaml

import colca_data_contracts
from colca_data_contracts.platform_tags import (
    PLATFORM_SEMANTIC_TAG_NAMES,
    PLATFORM_SEMANTIC_TAG_SEED,
)


def _seed_names() -> set[str]:
    path = Path(colca_data_contracts.__file__).parent / "semantic_tags" / "platform.yaml"
    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    return {tag["name"] for tag in document["tags"]}


def test_the_published_set_is_exactly_what_the_seed_ships():
    names = _seed_names()
    assert names, "the platform seed is empty, so this proves nothing"
    assert names == PLATFORM_SEMANTIC_TAG_NAMES


def test_the_set_is_read_from_the_shipped_seed_file():
    """The path is published so the read is inspectable rather than implied --
    and so a consumer can point at the same file instead of guessing it."""
    assert PLATFORM_SEMANTIC_TAG_SEED.is_file()
    assert PLATFORM_SEMANTIC_TAG_SEED.name == "platform.yaml"


def test_a_name_the_projector_joins_on_is_published():
    """Consumers join alarms to signals on "platform-alarm", so that name must stay protected."""
    assert "platform-alarm" in PLATFORM_SEMANTIC_TAG_NAMES


def test_an_authored_name_is_not_in_the_set():
    """The denominator: membership means something only if a non-seeded name
    is absent."""
    assert "catalog-round-trip" not in PLATFORM_SEMANTIC_TAG_NAMES


def _tags_the_shipped_models_require() -> dict[str, list[str]]:
    """Every `semantic_type` a shipped data model names, and which slot names it."""
    models = Path(colca_data_contracts.__file__).parent / "data_models"
    required: dict[str, list[str]] = {}
    for path in sorted(models.glob("*.yaml")):
        document = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        for signal in document.get("signals") or []:
            tag = signal.get("semantic_type")
            if tag:
                required.setdefault(tag, []).append(f"{path.stem}.{signal.get('name')}")
    return required


def test_shipped_models_name_only_shipped_tags():
    """A data model that names a tag nobody ships makes every deployment invent it.

    `machine.is_connected` requires `connectivity`, and the OEE models require
    five more. None of the six shipped, so each deployment hand-created them —
    six on one hub, every one picking a glyph, all landing on the generic
    `category`, which is what an UNCLASSIFIED System Element wears. A signal
    then drew the same icon as its parent machine.

    Assigning a shipped model is meant to be a complete act; it cannot be
    while the vocabulary it names has to be supplied by whoever assigns it.
    """
    required = _tags_the_shipped_models_require()
    # The denominator: a loader that found no models would make the assertion
    # below pass while checking nothing.
    assert len(required) >= 5, f"only found {len(required)} required tags — the loader is wrong"

    missing = {tag: slots for tag, slots in required.items() if tag not in _seed_names()}

    assert not missing, (
        "these tags are named by a shipped data model and shipped by nothing, so every "
        "deployment must invent them:\n  "
        + "\n  ".join(f"{tag} <- {', '.join(slots)}" for tag, slots in sorted(missing.items()))
    )
