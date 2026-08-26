"""Tests for the positioned topology payload contracts."""

import json

from franzmq.data_contracts import PAYLOAD_CLASSES
from franzmq.data_contracts.base import DataType, IndexType
from franzmq.topic import Topic

from colca_data_contracts import (
    ConstantDataType,
    ConstantPayload,
    ResourcePayload,
    SignalPayload,
    SystemElementPayload,
)


def test_system_element_registered():
    assert PAYLOAD_CLASSES.get("_SystemElement") is SystemElementPayload


def test_signal_registered():
    assert PAYLOAD_CLASSES.get("_Signal") is SignalPayload


def test_constant_registered():
    assert PAYLOAD_CLASSES.get("_Constant") is ConstantPayload


def test_resource_is_registered_under_its_contract_name():
    assert PAYLOAD_CLASSES["_Resource"] is ResourcePayload


def test_system_element_topic_format():
    topic = Topic(payload_type=SystemElementPayload, node_id="n-edge1",
                  context=["factory", "line1", "m6"])
    assert str(topic) == "colca/v1/_SystemElement/n-edge1/factory/line1/m6"


def test_signal_carries_its_binding():
    """The tag→signal binding lives on the Signal as a direct FK to the tag's
    id; DataTagContext and the (connector, tag_id) pair are both gone."""
    sig = SignalPayload(
        id="01HSIG", name="temp",
        data_tag="01HTAG",
        is_published=True, is_logged=True,
        system_element_id="01HSE",
    )
    decoded = SignalPayload.decode(sig.encode(), timestamp=0)

    assert decoded.data_tag == "01HTAG"
    assert decoded.is_published and decoded.is_logged
    assert decoded.system_element_id == "01HSE"


def test_signal_holds_no_path_references():
    """A record's topic is rewritten at every hop; its payload is not, so a path
    stored inside one means something else at an ancestor (design §3.2)."""
    fields = SignalPayload.__dataclass_fields__
    assert "topic_name" not in fields
    assert "system_element_topic" not in fields


def test_signal_topic_format():
    topic = Topic(payload_type=SignalPayload, node_id="n-edge1",
                  context=["factory", "line1", "m6", "machine_state"])
    assert str(topic) == "colca/v1/_Signal/n-edge1/factory/line1/m6/machine_state"


def test_constant_topic_format():
    topic = Topic(
        payload_type=ConstantPayload,
        node_id="n-edge1",
        context=["factory", "line1", "m6", "target_speed"],
    )
    assert str(topic) == "colca/v1/_Constant/n-edge1/factory/line1/m6/target_speed"


def test_system_element_roundtrip_minimal():
    original = SystemElementPayload(id="01H...", name="M6")
    encoded = original.encode()
    decoded = SystemElementPayload.decode(encoded, timestamp=0)
    assert decoded.id == "01H..."
    assert decoded.name == "M6"
    assert decoded.description == ""
    assert decoded.parent_id is None
    assert decoded.implements == []
    assert decoded.metadata == {}


def test_system_element_roundtrip_full():
    original = SystemElementPayload(
        id="01HKABC",
        name="M6",
        description="Main mixer on line 1",
        parent_id="01HPARENT",
        implements=["PackMLMachine"],
        external_asset_id="WO-1234",
        external_asset_id_type="string",
        metadata={"location": "Halle A", "owner": "Production"},
        created_at="2026-05-11T10:00:00+00:00",
        updated_at="2026-05-11T10:05:00+00:00",
    )
    encoded = original.encode()
    decoded = SystemElementPayload.decode(encoded, timestamp=0)
    assert decoded.id == original.id
    assert decoded.name == original.name
    assert decoded.description == original.description
    assert decoded.parent_id == original.parent_id
    assert decoded.implements == original.implements
    assert decoded.external_asset_id == original.external_asset_id
    assert decoded.external_asset_id_type == original.external_asset_id_type
    assert decoded.metadata == original.metadata
    assert decoded.created_at == original.created_at
    assert decoded.updated_at == original.updated_at


def test_signal_roundtrip_minimal():
    original = SignalPayload(id="01H...", name="machine_state")
    encoded = original.encode()
    decoded = SignalPayload.decode(encoded, timestamp=0)
    assert decoded.id == "01H..."
    assert decoded.name == "machine_state"
    assert decoded.data_tag is None
    assert decoded.is_published is False
    assert decoded.data_type is None
    assert decoded.index_type is None
    assert decoded.metadata == {}
    assert decoded.config == {}


def test_signal_roundtrip_full():
    original = SignalPayload(
        id="01HSIG",
        name="machine_state",
        description="Standardised machine state enum",
        system_element_id="01HSE",
        data_tag="01HTAG",
        is_published=True,
        is_logged=True,
        data_type=DataType.STRING,
        index_type=IndexType.TIME,
        unit=None,
        precision=None,
        min_value=None,
        max_value=None,
        config={"enum_values": ["RUNNING", "IDLE", "FAULT"]},
        metadata={"contract": "MerzBenteliMachineState"},
        created_at="2026-05-11T10:00:00+00:00",
        updated_at="2026-05-11T10:00:00+00:00",
    )
    encoded = original.encode()
    decoded = SignalPayload.decode(encoded, timestamp=0)
    assert decoded.id == original.id
    assert decoded.name == original.name
    assert decoded.description == original.description
    assert decoded.system_element_id == original.system_element_id
    assert decoded.data_tag == original.data_tag
    assert decoded.is_published and decoded.is_logged
    assert decoded.data_type == DataType.STRING
    assert decoded.index_type == IndexType.TIME
    assert decoded.config == original.config
    assert decoded.metadata == original.metadata


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


def test_constant_roundtrip_preserves_typed_value_and_metadata():
    original = ConstantPayload(
        id="01HCONSTANT",
        name="Target speed",
        description="Nominal filler speed",
        system_element_id="01HSE",
        data_type=ConstantDataType.INT64,
        value=18_000,
        unit="bph",
        precision=0,
        metadata={"owner": "Production"},
        created_at="2026-05-11T10:00:00+00:00",
        updated_at="2026-05-11T10:05:00+00:00",
    )

    decoded = ConstantPayload.decode(original.encode(), timestamp=0)

    assert decoded == original
    encoded = json.loads(original.encode())
    assert encoded["data_type"] == "int64"
    assert encoded["value"] == 18_000


def test_resource_payload_round_trips():
    payload = ResourcePayload(
        id="r1",
        system_element_id="el1",
        display_name="Press 3 manual",
        filename="manual.pdf",
        content_type="application/pdf",
        resource_type="maintenance_and_operator_documentation",
        size_bytes=1834722,
        sha256="a" * 64,
    )
    encoded = json.dumps(payload.__dict__)
    decoded = ResourcePayload.decode(encoded, timestamp=0)
    assert decoded.id == "r1"
    assert decoded.sha256 == "a" * 64
    assert decoded.size_bytes == 1834722


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


def test_system_element_names_its_parent_by_identity():
    """A record's topic is rewritten at every hop but its payload is not, so a
    path stored inside one means something else at an ancestor. The parent
    reference must be frame-invariant (binding design §3.2)."""
    se = SystemElementPayload(id="01HCHILD", name="Linie 3", parent_id="01HPARENT")
    decoded = SystemElementPayload.decode(se.encode(), timestamp=0)

    assert decoded.parent_id == "01HPARENT"
    assert "parent_topic" not in SystemElementPayload.__dataclass_fields__


def test_a_root_element_has_no_parent():
    root = SystemElementPayload(id="01HROOT", name="Werk1")
    assert SystemElementPayload.decode(root.encode(), timestamp=0).parent_id is None
