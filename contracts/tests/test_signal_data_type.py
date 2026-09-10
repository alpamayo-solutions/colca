"""A `_Signal` may be `json` and must decode again.

franzmq's `DataType` has no `json` member, so decoding goes through
`SignalDataType`.
"""

import pytest

from colca_data_contracts import DataType, SignalDataType
from colca_data_contracts.payload import Signal


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


def test_a_type_no_signal_can_hold_is_still_refused():
    """A superset, not an open door: `float64` is the CONSTANT spelling, and
    accepting it here is exactly the drift that broke the editor."""
    with pytest.raises(ValueError):
        SignalDataType("float64")
