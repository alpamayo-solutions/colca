"""Tests for the topology payload contracts (SystemElement, Signal)."""

import json

from franzmq.data_contracts import PAYLOAD_CLASSES
from franzmq.data_contracts.base import DataType, IndexType
from franzmq.topic import Topic

from colca_data_contracts import SystemElementPayload, SignalPayload


def test_system_element_registered():
    assert PAYLOAD_CLASSES.get("_SystemElement") is SystemElementPayload


def test_signal_registered():
    assert PAYLOAD_CLASSES.get("_Signal") is SignalPayload


def test_system_element_topic_format():
    topic = Topic(payload_type=SystemElementPayload, context=["factory", "line1", "m6"])
    assert str(topic) == "colca/v1/_SystemElement/factory/line1/m6"


def test_signal_topic_format():
    topic = Topic(payload_type=SignalPayload, context=["factory", "line1", "m6", "machine_state"])
    assert str(topic) == "colca/v1/_Signal/factory/line1/m6/machine_state"


def test_system_element_roundtrip_minimal():
    original = SystemElementPayload(id="01H...", name="M6")
    encoded = original.encode()
    decoded = SystemElementPayload.decode(encoded, timestamp=0)
    assert decoded.id == "01H..."
    assert decoded.name == "M6"
    assert decoded.description == ""
    assert decoded.parent_topic is None
    assert decoded.implements == []
    assert decoded.interface_coverage == {}
    assert decoded.metadata == {}


def test_system_element_roundtrip_full():
    original = SystemElementPayload(
        id="01HKABC",
        name="M6",
        description="Main mixer on line 1",
        parent_topic="factory/line1",
        implements=["MBMachine"],
        interface_coverage={"MBMachine": {"machine_state": "ok"}},
        external_asset_id="WO-1234",
        external_asset_id_type="string",
        metadata={"location": "Halle A", "owner": "Production"},
        edge_node_id="edge-01",
        hub_node_id=None,
        created_at="2026-05-11T10:00:00+00:00",
        updated_at="2026-05-11T10:05:00+00:00",
    )
    encoded = original.encode()
    decoded = SystemElementPayload.decode(encoded, timestamp=0)
    assert decoded.id == original.id
    assert decoded.name == original.name
    assert decoded.description == original.description
    assert decoded.parent_topic == original.parent_topic
    assert decoded.implements == original.implements
    assert decoded.interface_coverage == original.interface_coverage
    assert decoded.external_asset_id == original.external_asset_id
    assert decoded.external_asset_id_type == original.external_asset_id_type
    assert decoded.metadata == original.metadata
    assert decoded.edge_node_id == original.edge_node_id
    assert decoded.hub_node_id is None
    assert decoded.created_at == original.created_at
    assert decoded.updated_at == original.updated_at


def test_signal_roundtrip_minimal():
    original = SignalPayload(id="01H...", name="machine_state")
    encoded = original.encode()
    decoded = SignalPayload.decode(encoded, timestamp=0)
    assert decoded.id == "01H..."
    assert decoded.name == "machine_state"
    assert decoded.source == ""
    assert decoded.data_type is None
    assert decoded.index_type is None
    assert decoded.metadata == {}
    assert decoded.config == {}


def test_signal_roundtrip_full():
    original = SignalPayload(
        id="01HSIG",
        name="machine_state",
        description="Standardised machine state enum",
        system_element_topic="factory/line1/m6",
        source="computed",
        data_type=DataType.STRING,
        index_type=IndexType.TIME,
        topic_name="factory/line1/m6/machine_state",
        unit=None,
        precision=None,
        min_value=None,
        max_value=None,
        config={"enum_values": ["RUNNING", "IDLE", "FAULT"]},
        metadata={"contract": "MerzBenteliMachineState"},
        implements_contract="MerzBenteliMachineState",
        edge_node_id="edge-01",
        created_at="2026-05-11T10:00:00+00:00",
        updated_at="2026-05-11T10:00:00+00:00",
    )
    encoded = original.encode()
    decoded = SignalPayload.decode(encoded, timestamp=0)
    assert decoded.id == original.id
    assert decoded.name == original.name
    assert decoded.description == original.description
    assert decoded.system_element_topic == original.system_element_topic
    assert decoded.source == original.source
    assert decoded.data_type == DataType.STRING
    assert decoded.index_type == IndexType.TIME
    assert decoded.topic_name == original.topic_name
    assert decoded.config == original.config
    assert decoded.metadata == original.metadata
    assert decoded.implements_contract == original.implements_contract


def test_signal_enums_serialised_as_strings():
    """Enums must serialise to strings (otherwise broker payload is unreadable for non-Python consumers)."""
    sig = SignalPayload(
        id="x",
        name="y",
        data_type=DataType.FLOAT,
        index_type=IndexType.TIME,
    )
    encoded = sig.encode()
    parsed = json.loads(encoded)
    assert parsed["data_type"] == str(DataType.FLOAT)
    assert parsed["index_type"] == str(IndexType.TIME)


def test_system_element_with_only_id_name_is_valid():
    """Minimal viable payload: only id and name are mandatory."""
    se = SystemElementPayload(id="01H", name="root")
    encoded = se.encode()
    decoded = SystemElementPayload.decode(encoded, timestamp=0)
    assert decoded == se


def test_signal_with_only_id_name_is_valid():
    sig = SignalPayload(id="01H", name="x")
    encoded = sig.encode()
    decoded = SignalPayload.decode(encoded, timestamp=0)
    assert decoded == sig
