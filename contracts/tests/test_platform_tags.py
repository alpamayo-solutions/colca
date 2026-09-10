"""The published platform-tag names come from the seed file, not from a copy.

The API refuses to delete or edit a tag whose name is in this set. A set typed
out beside `platform.yaml` would keep passing while the seed grew, and every
tag added after the copy was made would ship deletable. So the assertion here
re-parses the YAML rather than restating any name: re-typing the constant and
then adding a tag to the seed turns this red.
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
    """`alarm_events.py` joins `semantic_type__name="platform-alarm"`; renaming
    that tag drops the alarm-to-signal association until bootstrap restores it.
    It is the one name whose protection is load-bearing beyond tidiness."""
    assert "platform-alarm" in PLATFORM_SEMANTIC_TAG_NAMES


def test_an_authored_name_is_not_in_the_set():
    """The denominator: membership means something only if a non-seeded name
    is absent."""
    assert "catalog-round-trip" not in PLATFORM_SEMANTIC_TAG_NAMES
