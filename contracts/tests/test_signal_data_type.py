"""A `_Signal` may be `json` and must decode again.

franzmq's `DataType` has no `json` member, so decoding goes through
`SignalDataType`.
"""

import sys
from pathlib import Path

import pytest

from colca_data_contracts import DataType, SignalDataType
from colca_data_contracts.payload import Signal

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import generate_bundle as gb


def _round_trip(data_type: str) -> str:
    signal = Signal(id="01H00000000000000000000001", name="probe", data_type=data_type)
    return str(Signal.decode(signal.encode(), 0).data_type)


def test_a_json_signal_survives_encode_and_decode():
    assert _round_trip("json") == "json"


@pytest.mark.parametrize("data_type", ["float", "int", "boolean", "string", "datetime"])
def test_every_other_type_still_round_trips(data_type):
    """The denominator: json was added, nothing was lost."""
    assert _round_trip(data_type) == data_type


def test_the_signal_vocabulary_is_franzmqs_plus_json():
    """Derived from franzmq's enum, so a new franzmq member shows up here too."""
    assert {member.value for member in SignalDataType} == {member.value for member in DataType} | {"json"}


def test_the_door_accepts_every_type_the_vocabulary_has():
    """The schema is derived from the same enum, so it cannot lag behind it.

    It did: `json` was missing from `_Signal`'s schema while the enum and the
    golden vector both had it, so a deployment that needed a JSON-valued
    signal had to declare `string` and lie about it.
    """
    schema = gb.build_bundle()[0]["contracts"]["_Signal"]["schema"]["properties"]["data_type"]

    offered = {value for value in schema["enum"] if value is not None}
    assert offered == {str(member.value) for member in SignalDataType}
    # Null is in there as well: a signal without a declared type is allowed.
    assert None in schema["enum"]


def test_a_type_no_signal_can_hold_is_still_refused():
    """A superset, not an open door: `float64` is the CONSTANT spelling, and
    accepting it here is exactly the drift that broke the editor."""
    with pytest.raises(ValueError):
        SignalDataType("float64")
