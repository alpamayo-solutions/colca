"""The golden field refusals agree with the limits the contracts declare.

``vectors/field_refusal.json`` is what the node answers when a record breaks a
field limit; colca's Go suite checks that the generated bundle refuses each
payload with exactly that answer, and a client parses the same strings into a
per-field message. This side checks that each refusal's limit is the one the
payload class declares, so a limit changed in ``payload.py`` cannot leave the
vectors describing the old one.
"""

import json
from importlib import resources

from franzmq.data_contracts import PAYLOAD_CLASSES

from colca_data_contracts import field_limits

VECTORS = json.loads(resources.files("colca_data_contracts").joinpath("vectors/field_refusal.json").read_text())


def test_each_refusal_names_the_limit_its_contract_declares():
    checked = 0
    for case in VECTORS["cases"]:
        limit = field_limits(PAYLOAD_CLASSES[case["contract"]]).get(case["field"])
        declared = {
            "maxLength": limit and limit.max_length,
            "minimum": limit and limit.minimum,
            "maximum": limit and limit.maximum,
        }.get(case["keyword"])
        if case["keyword"] in ("maxLength", "minimum", "maximum"):
            assert declared is not None, f"{case['contract']}.{case['field']} declares no {case['keyword']}"
            assert str(declared) == case["limit"], (case, declared)
            checked += 1
        expected = f"invalid_field: {case['field']}: {case['keyword']}"
        if case["limit"]:
            expected += f" {case['limit']}"
        assert case["refusal"] == expected, case
    assert checked >= 3, "the vectors must pin at least the unit, precision and name limits"
