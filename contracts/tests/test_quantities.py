from pathlib import Path

import pytest
import yaml

import colca_data_contracts
from colca_data_contracts.icons import ICON_SET
from colca_data_contracts.quantities import QUANTITY_KINDS, accepts_unit, convertible


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


CURRENCY_KINDS = ("energy-price", "demand-charge")


@pytest.mark.parametrize("kind", CURRENCY_KINDS)
@pytest.mark.parametrize("currency", ["EUR", "CHF", "USD"])
def test_a_price_kind_admits_each_currency_with_a_factor_one_unit(kind, currency):
    units = QUANTITY_KINDS[kind].units
    own = {symbol: unit for symbol, unit in units.items() if unit.currency == currency}
    assert any(unit.factor == 1.0 for unit in own.values()), f"{kind} has no {currency} basis"
    assert all(accepts_unit(kind, symbol) for symbol in own)


@pytest.mark.parametrize("kind", CURRENCY_KINDS)
def test_every_price_unit_names_a_currency_and_offsets_nothing(kind):
    for symbol, unit in QUANTITY_KINDS[kind].units.items():
        assert unit.currency in {"EUR", "CHF", "USD"}, symbol
        assert unit.offset == 0.0, symbol
        prefix = {"ct": "EUR", "Rp.": "CHF"}.get(symbol.split("/")[0], symbol.split("/")[0])
        assert prefix == unit.currency, symbol


def test_swiss_units_convert_within_chf():
    units = QUANTITY_KINDS["energy-price"].units
    # 18 Rp./kWh is 0.18 CHF/kWh.
    assert 18 * units["Rp./kWh"].factor == pytest.approx(0.18)
    assert convertible("energy-price", "Rp./kWh", "CHF/kWh")
    assert convertible("demand-charge", "CHF/kW/month", "CHF/kW/a")
    assert QUANTITY_KINDS["demand-charge"].units["CHF/kW/month"].factor == 12.0


@pytest.mark.parametrize(
    ("kind", "unit", "other"),
    [
        ("energy-price", "CHF/kWh", "EUR/kWh"),
        ("energy-price", "Rp./kWh", "ct/kWh"),
        ("energy-price", "USD/MWh", "EUR/MWh"),
        ("demand-charge", "USD/kW/a", "CHF/kW/a"),
    ],
)
def test_units_never_convert_between_currencies(kind, unit, other):
    assert not convertible(kind, unit, other)
    assert not convertible(kind, other, unit)


def test_convertible_outside_money_and_for_unknown_units():
    assert convertible("power", "kW", "W")
    assert not convertible("power", "kW", "bananas")
    assert not convertible("no-such-kind", "kW", "W")
    assert not convertible("power", "kW", "kWh")
