"""The `_Annotation` payload contract.

Annotations live on their own append-only stream. The id is deterministic, so
create, update and delete of one annotation are appends with the same id.
"""

import json
from pathlib import Path

from franzmq import Topic

from colca_data_contracts import AnnotationPayload, derive_annotation_id

ANNOTATION_ID_VECTORS_PATH = Path(__file__).resolve().parents[1] / (
    "src/colca_data_contracts/vectors/annotation_id.json"
)


def test_annotation_topic_is_stable_contract_name():
    topic = Topic(
        payload_type=AnnotationPayload,
        node_id="n-edge1",
        context=("production", "line1", "01J000000000000000000ANNOT"),
    )

    assert topic.payload_type is AnnotationPayload
    assert AnnotationPayload.get_identifier() == "_Annotation"
    # Level 3 carries the contract name, level 4 the publishing node's identity.
    assert str(topic).split("/")[2] == "_Annotation"
    assert str(topic) == ("colca/v1/_Annotation/n-edge1/production/line1/01J000000000000000000ANNOT")


def test_annotation_encode_decode_round_trip():
    annotation = AnnotationPayload(
        annotation_id=derive_annotation_id(
            "annotation-type-1",
            "dataops/part-cycle",
            1710000000.0,
            ["signal-1", "signal-2"],
        ),
        annotation_type_id="annotation-type-1",
        time_start=1710000000.0,
        time_end=1710000030.0,
        value={"part_count": 1},
        signal_ids=["signal-1", "signal-2"],
        source="dataops/part-cycle",
        deleted=False,
        revision=1,
    )

    decoded = AnnotationPayload.decode(annotation.encode(), timestamp=0)

    assert decoded.annotation_id == annotation.annotation_id
    assert decoded.annotation_type_id == "annotation-type-1"
    assert decoded.time_start == 1710000000.0
    assert decoded.time_end == 1710000030.0
    assert decoded.value == {"part_count": 1}
    assert decoded.signal_ids == ["signal-1", "signal-2"]
    assert decoded.source == "dataops/part-cycle"
    assert decoded.deleted is False
    assert decoded.revision == 1


def test_annotation_delete_is_an_append_carrying_a_marker():
    """A delete is a record with `deleted=True` and the id of the annotation it deletes."""
    created_id = derive_annotation_id("annotation-type-1", "dataops/part-cycle", 1710000000.0, ["signal-1"])
    tombstone = AnnotationPayload(
        annotation_id=created_id,
        annotation_type_id="annotation-type-1",
        time_start=1710000000.0,
        deleted=True,
        revision=2,
    )

    decoded = AnnotationPayload.decode(tombstone.encode(), timestamp=0)

    assert decoded.annotation_id == created_id
    assert decoded.deleted is True
    assert decoded.revision == 2


def test_derive_annotation_id_is_deterministic():
    first = derive_annotation_id("annotation-type-1", "dataops/part-cycle", 1710000000.0, ["sig-1"])
    second = derive_annotation_id("annotation-type-1", "dataops/part-cycle", 1710000000.0, ["sig-1"])

    assert first == second


def test_derive_annotation_id_is_distinct_for_distinct_inputs():
    baseline = derive_annotation_id("annotation-type-1", "dataops/part-cycle", 1710000000.0, ["sig-1"])

    assert derive_annotation_id("annotation-type-2", "dataops/part-cycle", 1710000000.0, ["sig-1"]) != baseline
    assert derive_annotation_id("annotation-type-1", "dataops/other-producer", 1710000000.0, ["sig-1"]) != baseline
    assert derive_annotation_id("annotation-type-1", "dataops/part-cycle", 1710000001.0, ["sig-1"]) != baseline
    # the same author, type and instant on a different
    # machine is a different annotation, not a collision.
    assert derive_annotation_id("annotation-type-1", "dataops/part-cycle", 1710000000.0, ["sig-2"]) != baseline
    assert derive_annotation_id("annotation-type-1", "dataops/part-cycle", 1710000000.0, []) != baseline
    assert derive_annotation_id("annotation-type-1", "dataops/part-cycle", 1710000000.0, ["sig-1", "sig-2"]) != baseline


def test_derive_annotation_id_ignores_the_order_of_the_signal_set():
    """The order of the signal set does not change the id; a different set does."""
    sorted_input = derive_annotation_id("annotation-type-1", "user/u-franz", 1700000000.0, ["a", "b", "c"])
    unsorted_input = derive_annotation_id("annotation-type-1", "user/u-franz", 1700000000.0, ["c", "a", "b"])
    as_tuple = derive_annotation_id("annotation-type-1", "user/u-franz", 1700000000.0, ("b", "c", "a"))
    different_set = derive_annotation_id("annotation-type-1", "user/u-franz", 1700000000.0, ["a", "b"])

    assert sorted_input == unsorted_input == as_tuple
    assert different_set != sorted_input


def test_derive_annotation_id_rounds_the_fractional_second_boundary():
    """Starts that format to the same microsecond give one id; a different microsecond does not."""
    base = derive_annotation_id("annotation-type-1", "dataops/part-cycle", 1710000000.0000001, ["sig-1"])
    same_rounding = derive_annotation_id(
        "annotation-type-1",
        "dataops/part-cycle",
        1710000000.0000002,
        ["sig-1"],
    )
    distinct = derive_annotation_id("annotation-type-1", "dataops/part-cycle", 1710000000.000001, ["sig-1"])

    assert base == same_rounding
    assert base != distinct


def test_derive_annotation_id_is_a_valid_ulid():
    identifier = derive_annotation_id("annotation-type-1", "dataops/part-cycle", 1710000000.0, ["sig-1"])

    assert isinstance(identifier, str)
    assert len(identifier) == 26
    # Crockford base32 alphabet excludes I, L, O, U to avoid confusion with
    # 1, 1, 0, V.
    assert set(identifier) <= set("0123456789ABCDEFGHJKMNPQRSTVWXYZ")


def test_derive_annotation_id_matches_the_golden_vectors():
    """plugins/uns/exec_edit_annotation.go derives the same ids in Go; both
    sides check against vectors/annotation_id.json.
    """
    vectors = json.loads(ANNOTATION_ID_VECTORS_PATH.read_text())
    cases = vectors["cases"]
    assert cases, "golden annotation_id vectors carry no cases"
    # Cover no signals, one, and several in more than one order.
    sizes = {len(case["signal_ids"]) for case in cases}
    assert 0 in sizes and 1 in sizes and any(size > 1 for size in sizes), sizes
    by_set = {}
    for case in cases:
        key = (case["annotation_type_id"], case["source"], case["time_start"], frozenset(case["signal_ids"]))
        by_set.setdefault(key, set()).add(tuple(case["signal_ids"]))
    assert any(len(orders) > 1 for orders in by_set.values()), "no vector case gives the same signal set in two orders"
    for case in cases:
        got = derive_annotation_id(
            case["annotation_type_id"],
            case["source"],
            case["time_start"],
            case["signal_ids"],
        )
        assert got == case["id"], (
            f"derive_annotation_id({case['annotation_type_id']!r}, {case['source']!r}, "
            f"{case['time_start']!r}, {case['signal_ids']!r}) = {got!r}, vectors want {case['id']!r}"
        )
