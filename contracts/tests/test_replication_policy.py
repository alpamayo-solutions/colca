import json
from importlib.resources import files

import pytest

from colca_data_contracts.payload import Signal


@pytest.mark.parametrize(
    "case", json.loads(files("colca_data_contracts.vectors").joinpath("replication_policy.json").read_text())
)
def test_replication_policy_vocabulary(case):
    payload = json.dumps({"id": "s1", "name": "probe", "replication_policy": case["policy"]})
    if case["valid"]:
        signal = Signal.decode(payload, 0)
        assert json.loads(signal.encode())["replication_policy"] == case["policy"]
    else:
        with pytest.raises(ValueError):
            Signal.decode(payload, 0)


def test_absent_policy_preserves_replication():
    signal = Signal.decode('{"id":"s1","name":"probe"}', 0)
    assert json.loads(signal.encode())["replication_policy"] == "replicate_to_parents"
