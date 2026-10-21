from pathlib import Path

import pytest
import yaml

import colca_data_contracts
from colca_data_contracts.icons import ICON_SET
from colca_data_contracts.quantities import QUANTITY_KINDS, accepts_unit


def test_every_kind_accepts_its_own_base_unit():
    for name, kind in QUANTITY_KINDS.items():
        if kind.base_unit is None:
            continue
        assert accepts_unit(name, kind.base_unit), f"{name} rejects its base unit"


def test_celsius_to_kelvin_is_affine_not_a_factor():
    celsius = QUANTITY_KINDS["temperature"].units["°C"]
    assert celsius.factor == 1.0
    assert celsius.offset == pytest.approx(273.15)
    # 0 °C is 273.15 K, not 0 K. A factor-only model gets this wrong.
    assert 0.0 * celsius.factor + celsius.offset == pytest.approx(273.15)


def test_an_unknown_unit_is_refused():
    assert not accepts_unit("electric-current", "bananas")


def test_an_unknown_kind_is_refused():
    assert not accepts_unit("no-such-kind", "A")


def test_every_seeded_icon_is_in_the_set():
    """A seeded icon outside the set would render as literal text."""
    seed = yaml.safe_load(
        (Path(colca_data_contracts.__file__).parent / "semantic_tags" / "platform.yaml").read_text(encoding="utf-8")
    )
    tags = seed["tags"]
    assert tags, "the platform seed is empty, so this proves nothing"
    for tag in tags:
        assert tag["icon"] in ICON_SET, f"{tag['name']} names an unlisted icon {tag['icon']!r}"


def test_shipped_model_units_are_accepted_by_their_tag_quantity():
    """A shipped slot whose unit its tag's quantity refuses could never be implemented."""
    from colca_data_contracts.data_models import compile_models

    seed = yaml.safe_load(
        (Path(colca_data_contracts.__file__).parent / "semantic_tags" / "platform.yaml").read_text(encoding="utf-8")
    )
    kinds = {tag["name"]: tag.get("quantity_kind") for tag in seed["tags"]}
    checked = 0
    for manifest in compile_models():
        for slot in manifest["slots"]:
            kind = kinds.get(slot["semantic_type"])
            if not slot["unit"] or not kind:
                continue
            checked += 1
            assert accepts_unit(kind, slot["unit"]), f"{manifest['name']}.{slot['key']}: {slot['unit']} is not {kind}"
    assert checked >= 7
