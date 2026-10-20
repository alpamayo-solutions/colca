"""The value definition rules, shared with Go through golden vectors.

`value_definitions` is the definition; `vectors/value_definitions.json` carries
its key table, its limits and a set of judged definitions to colca's Go suite.
These tests keep the file equal to the definition and judge every case.
"""

import json
from pathlib import Path

import pytest

from colca_data_contracts import SignalPayload
from colca_data_contracts.value_definitions import (
    KEY_KINDS,
    MAX_ENTRIES,
    MAX_TEXT,
    canonical_key,
    key_value,
    sorted_keys,
    value_definition_problems,
)

VECTORS = json.loads(
    (Path(__file__).resolve().parents[1] / "src/colca_data_contracts/vectors/value_definitions.json").read_text()
)


def _definitions(case: dict) -> object:
    if "generated_entries" in case:
        return {str(index): "v" for index in range(case["generated_entries"])}
    return case["definitions"]


def test_the_vectors_carry_the_definition():
    assert VECTORS["key_kinds"] == KEY_KINDS
    assert VECTORS["max_entries"] == MAX_ENTRIES
    assert VECTORS["max_text"] == MAX_TEXT


@pytest.mark.parametrize("case", VECTORS["cases"], ids=lambda case: case["name"])
def test_each_case_is_judged_as_the_vectors_say(case):
    problems = value_definition_problems(case["data_type"], _definitions(case))
    first = problems[0].refusal if problems else None
    assert first == case["refusal"]


def test_every_problem_reads_as_a_sentence():
    problems = value_definition_problems("int", {"01": "x", "2": "", "3": "x" * 121, "4": " y", "5": 5})
    assert [problem.keyword for problem in problems] == [
        "propertyNames",
        "minLength",
        "maxLength",
        "whitespace",
        "type",
    ]
    assert all(problem.message for problem in problems)


@pytest.mark.parametrize(
    ("data_type", "value", "key"),
    [
        ("int", 202, "202"),
        ("integer", -1, "-1"),
        ("int", "7", "7"),
        ("boolean", True, "true"),
        ("boolean", "false", "false"),
        ("string", "E17", "E17"),
    ],
)
def test_a_typed_value_becomes_its_canonical_key_and_back(data_type, value, key):
    assert canonical_key(data_type, value) == key
    assert canonical_key(data_type, key_value(data_type, key)) == key


@pytest.mark.parametrize(
    ("data_type", "value"),
    [("int", True), ("int", 1.5), ("int", "07"), ("boolean", 1), ("string", 3), ("float", 1.0), ("string", "")],
)
def test_a_value_the_data_type_cannot_take_has_no_key(data_type, value):
    with pytest.raises(ValueError):
        canonical_key(data_type, value)


def test_int_keys_sort_by_value():
    assert sorted_keys("int", {"10": "a", "-1": "b", "2": "c"}) == ["-1", "2", "10"]
    assert sorted_keys("string", {"b": "", "a": ""}) == ["a", "b"]


def test_a_signal_record_round_trips_its_definition():
    signal = SignalPayload(id="01KZ0000000000000000000001", name="fault_code", value_definitions={"202": "Sieve"})
    decoded = SignalPayload.decode(json.dumps(signal.__dict__), 0)
    assert decoded.value_definitions == {"202": "Sieve"}
    assert SignalPayload.decode(json.dumps({"id": signal.id, "name": "x"}), 0).value_definitions == {}
