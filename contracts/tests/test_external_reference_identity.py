import json
from dataclasses import fields
from importlib.resources import files

import pytest

from colca_data_contracts import ExternalReferencePayload

VECTOR = json.loads(files("colca_data_contracts.vectors").joinpath("external_reference_identity.json").read_text())


@pytest.mark.parametrize("case", VECTOR["cases"], ids=lambda case: case["name"])
def test_identity_is_the_source_the_relationship_and_the_row(case):
    base = ExternalReferencePayload.decode(json.dumps(VECTOR["base"]), 0)
    changed = ExternalReferencePayload.decode(json.dumps(VECTOR["base"] | case["change"]), 0)

    assert (changed.identity == base.identity) is case["same"]


def test_every_field_is_either_identity_or_named_as_not():
    """A field added to the contract has to be placed on one side by a vector case."""
    decided = {name for case in VECTOR["cases"] for name in case["change"]}

    assert decided == {field.name for field in fields(ExternalReferencePayload)}
    assert {name for case in VECTOR["cases"] if not case["same"] for name in case["change"]} == set(
        ExternalReferencePayload.IDENTITY_FIELDS
    )
