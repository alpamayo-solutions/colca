import json
from pathlib import Path

import jsonschema
import pytest

import colca_data_contracts
from colca_data_contracts.catalog import (
    PRODUCT_SCHEMA,
    RECIPE_SCHEMA,
    Product,
    Provenance,
    Recipe,
    product_path,
    recipe_path,
)

VECTORS = json.loads(
    (Path(colca_data_contracts.__file__).parent / "vectors" / "catalog_records.json").read_text(encoding="utf-8")
)
CASES = [
    (kind, cls, schema, case, valid)
    for kind, cls, schema in (("products", Product, PRODUCT_SCHEMA), ("recipes", Recipe, RECIPE_SCHEMA))
    for valid in (True, False)
    for case in VECTORS[kind]["valid" if valid else "invalid"]
]


@pytest.mark.parametrize(("kind", "cls", "schema", "case", "valid"), CASES)
def test_the_types_and_the_schema_agree_on_every_vector(kind, cls, schema, case, valid):
    schema_ok = jsonschema.Draft202012Validator(schema).is_valid(case)
    try:
        cls.from_dict(case)
        python_ok = True
    except ValueError:
        python_ok = False
    assert (schema_ok, python_ok) == (valid, valid), f"{kind}: {case}"


def test_a_record_survives_the_round_trip():
    for case in VECTORS["products"]["valid"]:
        again = Product.from_dict(case).to_dict()
        assert Product.from_dict(again) == Product.from_dict(case)
    full = VECTORS["products"]["valid"][0]
    assert Product.from_dict(full).to_dict() == full


def test_a_hand_entry_carries_its_own_provenance():
    product = Product.from_dict(VECTORS["products"]["valid"][0])
    assert product.provenance == Provenance("erp", "sap-bridge", "2026-09-25T08:00:00Z")
    assert product.field_provenance["density_kg_m3"].source == "manual"


def test_a_product_names_its_recipe_by_key():
    assert "recipe" not in PRODUCT_SCHEMA["properties"]
    assert PRODUCT_SCHEMA["properties"]["recipe_id"]["type"] == ["string", "null"]


def test_the_paths_are_one_topic_level_per_key():
    assert product_path("AMALQ301H6") == "catalog/products/AMALQ301H6"
    assert recipe_path("recipe-standard") == "catalog/recipes/recipe-standard"
    for bad in ("", " ", "a/b", "a+", "#"):
        with pytest.raises(ValueError):
            product_path(bad)
