"""`_Annotation` payload contract (dataops-evaluator design §8).

Instances ride their own append-only `annotations` stream, the same shape as
`alarm` — never KV-projected, never retained. Identity is deterministic so
that create, update (e.g. setting `time_end`), and delete of the same
logical annotation are all appends carrying the SAME id.
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
    """A delete is a record with `deleted=True`, not an absence — it round
    trips through encode/decode like any other field, and it carries the
    SAME id as the annotation it deletes."""
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
    """The signals are a SET: the caller's order is not part of the identity,
    so an update that lists the same signals differently still lands on the
    record it means to replace. Sorted input, unsorted input, any input --
    one id. The denominator is the distinct set right below it."""
    sorted_input = derive_annotation_id("annotation-type-1", "user/u-franz", 1700000000.0, ["a", "b", "c"])
    unsorted_input = derive_annotation_id("annotation-type-1", "user/u-franz", 1700000000.0, ["c", "a", "b"])
    as_tuple = derive_annotation_id("annotation-type-1", "user/u-franz", 1700000000.0, ("b", "c", "a"))
    different_set = derive_annotation_id("annotation-type-1", "user/u-franz", 1700000000.0, ["a", "b"])

    assert sorted_input == unsorted_input == as_tuple
    assert different_set != sorted_input


def test_derive_annotation_id_rounds_the_fractional_second_boundary():
    """The id is derived from a `.6f`-formatted timestamp, so two starts
    that round to the same microsecond string collide by design (identity
    is idempotency, not raw float equality) while a genuinely distinct
    microsecond does not."""
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
    """Cross-language pin (annotation-cutover design, architecture
    principle 2 tier 2): plugins/uns/exec_edit_annotation.go
    re-implements this exact rule natively in Go, because a human-authored
    annotation command derives its id at the node, never in the API. Both
    native copies answer to this one checked-in dataset
    (vectors/annotation_id.json) so a change to either side that silently
    diverges from the other fails here before a producer and a human ever
    derive different ids for the same annotation.
    """
    vectors = json.loads(ANNOTATION_ID_VECTORS_PATH.read_text())
    cases = vectors["cases"]
    assert cases, "golden annotation_id vectors carry no cases"
    # The file must exercise every shape of the signal set the rule
    # distinguishes: none, one, several -- and several in more than one
    # order, or the sort inside the rule is never pinned.
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
