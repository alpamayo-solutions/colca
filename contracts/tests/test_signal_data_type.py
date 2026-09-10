"""A `_Signal` may be `json`, and must survive being read back.

The gap this pins: `Signal.data_type` was coerced through franzmq's
`DataType`, which has no `json` member, while the API's column, colca's
`slotDataTypes` and the semantic-type table all accepted one. Encoding
succeeded -- a dataclass annotation validates nothing -- so a json signal
could be authored and written, and then `Signal.decode` raised
`ValueError: 'json' is not a valid DataType` in every Python consumer of the
contract: the projector, the connector, dataops.
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
    """Derived, never retyped -- so a franzmq release that adds or removes a
    member cannot leave this silently disagreeing with it."""
    assert {member.value for member in SignalDataType} == {member.value for member in DataType} | {"json"}


def test_a_type_no_signal_can_hold_is_still_refused():
    """A superset, not an open door: `float64` is the CONSTANT spelling, and
    accepting it here is exactly the drift that broke the editor."""
    with pytest.raises(ValueError):
        SignalDataType("float64")
