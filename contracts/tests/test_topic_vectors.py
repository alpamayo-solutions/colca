"""The canonical golden vectors, judged against the installed franzmq.

``vectors/topic_transformations.json`` is the single dataset both sides of the
language boundary answer to (schema-bundle design §2, tier 2): colca's Go door
reads it from the repo path, franzmq's own suite runs a copy, and this test runs
the canonical file against the franzmq release this package pins. That closes the
loop — a franzmq whose copy of the vectors drifted from this one fails here.

The mount and identity sections are door-side behavior; what this suite asserts
about them is that their inputs and outputs stay inside the grammar franzmq
speaks, which is franzmq's half of the contract.
"""
import json
from dataclasses import dataclass
from importlib import resources
from pathlib import Path

import pytest

import colca_data_contracts  # noqa: F401  — registers the Colca contracts
from franzmq.data_contracts import PAYLOAD_CLASSES
from franzmq.data_contracts.base import Cmd, Payload
from franzmq.topic import Topic

pytestmark = pytest.mark.skipif(
    not colca_data_contracts.TOPICS_CARRY_NODE_ID,
    reason=(
        "installed franzmq predates the node-id scheme (0.5.0); the vectors describe "
        "the current scheme. Un-quarantines by resolving franzmq>=0.5.0."
    ),
)

VECTORS_PATH = Path(__file__).resolve().parents[1] / (
    "src/colca_data_contracts/vectors/topic_transformations.json"
)
VECTORS = json.loads(VECTORS_PATH.read_text())


@dataclass
class EnrolledIdentity(Payload):
    """Stand-in for the enrollment contract, which the broker binary owns."""
    id: str = ""


@dataclass
class TimeSync(Payload):
    """Stand-in for the time beacon, which the broker binary owns."""
    now_ms: int = 0


@pytest.fixture(autouse=True)
def _register_broker_owned_contracts():
    added = [EnrolledIdentity, TimeSync]
    for cls in added:
        PAYLOAD_CLASSES[cls.get_identifier()] = cls
    yield
    for cls in added:
        PAYLOAD_CLASSES.pop(cls.get_identifier(), None)


def test_the_vector_file_ships_as_package_data():
    # colca reads it from the repo path; franzmq's suite gets it from the
    # installed package — a file that stops shipping breaks the far side only.
    packaged = resources.files("colca_data_contracts").joinpath(
        "vectors/topic_transformations.json"
    )
    assert json.loads(packaged.read_text()) == VECTORS


def _parse_cases(ok: bool):
    return [c for c in VECTORS["parse"] if c["ok"] is ok]


@pytest.mark.parametrize("case", _parse_cases(ok=True), ids=lambda c: c["topic"])
def test_parse_accepts_and_round_trips(case):
    topic = Topic.from_str(case["topic"])

    assert topic.prefix == case["prefix"]
    assert topic.version == case["version"]
    assert topic.payload_type.get_identifier() == case["contract"]
    assert topic.node_id == case["node_id"]
    assert "/".join(topic.context) == case["path"]
    assert str(topic) == case["topic"]


@pytest.mark.parametrize("case", _parse_cases(ok=False), ids=lambda c: c["topic"])
def test_parse_rejects(case):
    with pytest.raises(ValueError):
        Topic.from_str(case["topic"])


@pytest.mark.parametrize(
    "case",
    [c for c in VECTORS["mount_insert"] if c["out"] != c["topic"]],
    ids=lambda c: f"{c['topic']}+{c['mount']}",
)
def test_mount_insert_keeps_the_identity_and_the_grammar(case):
    before, after = Topic.from_str(case["topic"]), Topic.from_str(case["out"])
    assert after.node_id == before.node_id
    assert after.payload_type is before.payload_type
    # The mount lands at the head of the path, never at the identity level. A
    # mount is itself a path (colca's MountInsert splices it in as a raw
    # string, e.g. "site1/edge1"), so it can carry more than one segment.
    assert after.context == tuple(case["mount"].split("/")) + before.context


@pytest.mark.parametrize(
    "case",
    [c for c in VECTORS["mount_strip"] if c["ok"]],
    ids=lambda c: f"{c['topic']}-{c['mount']}",
)
def test_mount_strip_keeps_the_identity_and_the_grammar(case):
    before, after = Topic.from_str(case["topic"]), Topic.from_str(case["out"])
    assert after.node_id == before.node_id
    assert after.payload_type is before.payload_type
    assert tuple(case["mount"].split("/")) + after.context == before.context


@pytest.mark.parametrize(
    "case", VECTORS["identity_rule"], ids=lambda c: f"{c['topic']}@{c['authenticated_as']}"
)
def test_identity_rule_reads_level_four(case):
    topic = Topic.from_str(case["topic"])
    is_command = issubclass(topic.payload_type, Cmd)
    assert (topic.node_id == case["authenticated_as"] or is_command) is case["ok"]


def test_node_topic_builds_under_the_process_identity(monkeypatch):
    from colca_data_contracts import Metric, node_topic

    monkeypatch.setenv("NODE_ID", "n-edge1")
    assert str(node_topic(Metric, "line1", "temp")) == "colca/v1/_Metric/n-edge1/line1/temp"
    assert str(node_topic(Metric, "temp", node="m1")) == "colca/v1/_Metric/m1/temp"


def test_node_topic_without_an_identity_names_the_variable(monkeypatch):
    from colca_data_contracts import Metric, node_topic

    monkeypatch.delenv("NODE_ID", raising=False)
    with pytest.raises(RuntimeError, match="NODE_ID"):
        node_topic(Metric, "temp")
