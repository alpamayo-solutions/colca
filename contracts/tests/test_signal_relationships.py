"""The signal relationship rules, shared with Go through golden vectors.

`signal_relationships.RULES` is the definition; `vectors/signal_relationships.json`
carries it and a set of judged graphs to colca's Go suite. These tests keep the
file equal to the definition and judge every case.
"""

import json
from pathlib import Path

import pytest

from colca_data_contracts import SignalAccess, SignalPayload, SignalRelationship, SignalRelationshipType
from colca_data_contracts.signal_relationships import (
    NUMERIC_DATA_TYPES,
    RULES,
    RelationshipEnd,
    relationship_problems,
    wire_relationships,
)

VECTORS = json.loads(
    (Path(__file__).resolve().parents[1] / "src/colca_data_contracts/vectors/signal_relationships.json").read_text()
)

SIGNAL_ID = "01KZ0000000000000000000001"
TARGET_ID = "01KZ0000000000000000000002"


def _ends(case: dict) -> dict[str, RelationshipEnd]:
    return {
        key: RelationshipEnd(data_type=end["data_type"], relationships=[tuple(pair) for pair in end["relationships"]])
        for key, end in case["ends"].items()
    }


def test_the_rule_table_matches_the_definition():
    assert VECTORS["rules"] == {
        str(kind): {
            "max_sources_per_target": rule.max_sources_per_target,
            "chains": rule.chains,
            "data_types": rule.data_types,
        }
        for kind, rule in RULES.items()
    }
    assert set(RULES) == set(SignalRelationshipType)
    assert VECTORS["numeric_data_types"] == sorted(NUMERIC_DATA_TYPES)
    assert VECTORS["access"] == sorted(str(member) for member in SignalAccess)


@pytest.mark.parametrize("case", VECTORS["cases"], ids=[case["name"] for case in VECTORS["cases"]])
def test_golden_cases(case):
    problems = relationship_problems(_ends(case))
    assert (problems == []) is case["valid"], problems


@pytest.mark.parametrize(
    ("ends", "message"),
    [
        ({"sp": RelationshipEnd("float", [("actual", "pv")])}, "sp: actual target pv does not exist"),
        ({"sp": RelationshipEnd("float", [("actual", "sp")])}, "sp: actual names the signal itself"),
        (
            {
                "sp": RelationshipEnd("float", [("actual", "a"), ("actual", "b")]),
                "a": RelationshipEnd("float"),
                "b": RelationshipEnd("float"),
            },
            "sp: holds more than one actual relationship",
        ),
        (
            {"pv": RelationshipEnd("boolean"), "sp": RelationshipEnd("float", [("actual", "pv")])},
            "sp: actual target pv has data type 'boolean', not compatible with 'float'",
        ),
        (
            {
                "pv": RelationshipEnd("float"),
                "a": RelationshipEnd("float", [("actual", "pv")]),
                "b": RelationshipEnd("float", [("actual", "pv")]),
            },
            "pv is the actual target of more than one signal: a, b",
        ),
        (
            {
                "a": RelationshipEnd("float", [("feedback", "b")]),
                "b": RelationshipEnd("float", [("feedback", "c")]),
                "c": RelationshipEnd("float"),
            },
            "a: feedback target b holds a feedback relationship itself; feedback links do not chain",
        ),
        (
            {
                "a": RelationshipEnd("float", [("upper_limit", "b")]),
                "b": RelationshipEnd("float", [("upper_limit", "a")]),
            },
            "upper_limit links form a cycle: a -> b -> a",
        ),
    ],
)
def test_messages_name_the_signals(ends, message):
    assert message in relationship_problems(ends)


def test_checked_limits_reports_to_the_written_sources():
    ends = {
        "pv": RelationshipEnd("float"),
        "old": RelationshipEnd("float", [("actual", "missing")]),
        "new": RelationshipEnd("float", [("actual", "pv")]),
    }
    assert relationship_problems(ends, checked=lambda key: key == "new") == []
    assert relationship_problems(ends, checked=lambda key: key == "old") == [
        "old: actual target missing does not exist"
    ]


def test_a_target_that_is_no_signal_says_so():
    ends = {"sp": RelationshipEnd("number", [("actual", "spindle")])}
    assert relationship_problems(ends, not_signals={"spindle"}) == ["sp: actual target spindle is not a signal"]


def test_signal_payload_carries_access_and_relationships_round_trip():
    signal = SignalPayload(
        id=SIGNAL_ID,
        name="speed_setpoint",
        access=SignalAccess.READ_WRITE,
        relationships=[SignalRelationship(SignalRelationshipType.ACTUAL, TARGET_ID)],
    )
    wire = signal.__dict__
    assert wire["access"] == "read_write"
    assert wire["relationships"] == [{"relationship_type": "actual", "target_signal_id": TARGET_ID}]
    decoded = SignalPayload.decode(json.dumps(wire), 0)
    assert decoded.access is SignalAccess.READ_WRITE
    assert decoded.relationships == [SignalRelationship(SignalRelationshipType.ACTUAL, TARGET_ID)]
    assert wire_relationships(wire["relationships"]) == [("actual", TARGET_ID)]


def test_a_signal_without_the_fields_is_read_only_with_no_relationships():
    decoded = SignalPayload.decode(json.dumps({"id": SIGNAL_ID, "name": "speed"}), 0)
    assert decoded.access is SignalAccess.READ
    assert decoded.relationships == []
    assert decoded.__dict__["access"] == "read"


def test_plain_dict_relationships_encode_like_the_dataclass():
    signal = SignalPayload(
        id=SIGNAL_ID,
        name="sp",
        relationships=[{"relationship_type": "upper_limit", "target_signal_id": TARGET_ID}],  # type: ignore[list-item]
    )
    assert signal.__dict__["relationships"] == [{"relationship_type": "upper_limit", "target_signal_id": TARGET_ID}]
