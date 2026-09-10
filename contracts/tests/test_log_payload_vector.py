"""The `_Log` payload's fields, shared by Python and Go.

Python builds the payload from the `Log` dataclass; Go builds it by hand.
`vectors/log_payload.json` lists the fields: this test keeps it equal to the
contract, and a test in `door/` keeps the Go payload equal to it.
"""

from __future__ import annotations

import dataclasses
import json
import pathlib

from franzmq.data_contracts.base import Log

VECTOR = pathlib.Path(__file__).resolve().parents[1] / ("src/colca_data_contracts/vectors/log_payload.json")


def test_the_vector_matches_the_contract_it_claims_to_describe():
    """A field added to `Log` must appear here, or Go will not know to send it."""
    vector = json.loads(VECTOR.read_text())

    required = [field.name for field in dataclasses.fields(Log) if field.default is dataclasses.MISSING]
    optional = [field.name for field in dataclasses.fields(Log) if field.default is not dataclasses.MISSING]

    assert vector["contract"] == "_Log"
    assert vector["required"] == required, (
        "vectors/log_payload.json lists the wrong required fields for _Log. "
        f"The contract requires {required}; the vector says {vector['required']}. "
        "Regenerate it — a publisher that follows a stale vector has its records "
        "refused by the node, silently."
    )
    assert vector["optional"] == optional, (
        f"the contract's optional fields are {optional}, the vector says {vector['optional']}"
    )


def test_the_vector_is_not_empty():
    """The denominator: an empty required list would let any payload pass the
    Go check while looking green."""
    vector = json.loads(VECTOR.read_text())
    assert vector["required"], "a vector with no required fields proves nothing"
    assert "message" in vector["required"], (
        "a _Log payload without a message is not a log line; if this is ever "
        "false the vector is describing something else"
    )
