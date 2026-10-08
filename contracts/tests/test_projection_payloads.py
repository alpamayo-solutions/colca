"""Wire shapes for the entity and definition projection catalogue."""

import json

from colca_data_contracts import (
    AnnotationTypePayload,
    DataModelPayload,
    ExternalReferencePayload,
    ExternalSystemPayload,
    GroupPayload,
    MetadataTypePayload,
    NodeAccess,
    NodePayload,
    ServiceDetails,
)


def test_colca_node_is_a_real_multi_node_domain_record():
    node = NodePayload(
        id="01JNODE",
        name="line-1",
        display_name="Line 1 Colca",
        description="Acquisition for line 1",
        root_system_element_id="01JROOT",
        metadata={"01JTYPE": "Line 1"},
    )

    assert NodePayload.decode(node.encode(), timestamp=0) == node
    assert node.get_identifier() == "_Node"
    assert "parent_namespace" not in node.__dataclass_fields__


def test_service_details_names_its_identity_node_and_optional_mount():
    service = ServiceDetails(
        id="01JSERVICE",
        name="opcua-1",
        display_name="OPC UA connector",
        description="Reads the line PLC",
        service_type="connector",
        colca_node_id="01JNODE",
        system_element_id="01JROOT",
        hierarchy=[],
        is_active=True,
        metadata={"01JTYPE": "Line 1"},
        architecture_metadata={"layout": {"order": 1}},
    )

    decoded = ServiceDetails.decode(service.encode(), timestamp=0)
    assert decoded == service
    assert "is_licensed" not in service.__dataclass_fields__
    assert "display_order" not in service.__dataclass_fields__


def test_external_reference_uses_definition_identity_not_a_free_form_system():
    reference = ExternalReferencePayload(
        id="01JREFERENCE",
        source_entity="SystemElement",
        source_object_id="01JELEMENT",
        relationship_type="access:equipment",
        external_system_id="01JEXTERNAL",
        external_table="equipment",
        external_column="id",
        external_row_id="4711",
    )

    assert ExternalReferencePayload.decode(reference.encode(), timestamp=0) == reference
    assert "external_system" not in reference.__dataclass_fields__


def test_global_definition_payloads_have_state_not_operations():
    definitions = [
        MetadataTypePayload(id="m", name="work_order", data_type="string"),
        AnnotationTypePayload(id="a", name="maintenance", data_type="string"),
        DataModelPayload(id="i", name="PackMLMachine"),
        ExternalSystemPayload(
            id="e",
            key="tcdb",
            name="Technical Centre Database",
            system_type="database",
        ),
        GroupPayload(id="g", name="operators"),
    ]

    for definition in definitions:
        encoded = json.loads(definition.encode())
        assert encoded["id"]
        assert "operation" not in encoded


def test_annotation_type_is_positionless_and_owns_options():
    annotation_type = AnnotationTypePayload(
        id="01JANNOTATION",
        name="maintenance",
        data_type="string",
        options=[{"id": "01JOPTION", "name": "planned", "i18n_name": "Planned"}],
    )

    assert "system_element" not in annotation_type.__dataclass_fields__
    assert AnnotationTypePayload.decode(annotation_type.encode(), timestamp=0) == annotation_type


def test_annotation_type_carries_metadata():
    annotation_type = AnnotationTypePayload(
        id="01JANNOTATION",
        name="maintenance",
        data_type="string",
        metadata={"owner": "quality-app"},
    )

    assert AnnotationTypePayload.decode(annotation_type.encode(), timestamp=0) == annotation_type
    # Types written before the field existed still decode.
    older = AnnotationTypePayload.decode('{"id": "01JOLD", "name": "downtime", "data_type": "string"}', timestamp=0)
    assert older.metadata == {}


def test_node_carries_declared_access_and_tolerates_newer_fields():
    node = NodePayload.from_wire(
        {
            "id": "n1",
            "name": "edge-07",
            "access": {"hostname": "edge-07.example", "ui_url": "https://edge-07.example", "vpn_ip": "x"},
        }
    )
    assert node.access == NodeAccess(hostname="edge-07.example", ui_url="https://edge-07.example")
    assert NodePayload.from_wire({"id": "n1", "name": "edge-07"}).access is None


def test_node_access_round_trips_through_encode_and_decode():
    access = NodeAccess(hostname="edge-07.example", ui_url="https://edge-07.example")
    node = NodePayload(id="n1", name="edge-07", access=access)
    decoded = NodePayload.decode(node.encode(), timestamp=0)
    assert decoded == node
    assert decoded.access == access
    bare = NodePayload(id="n1", name="edge-07")
    assert NodePayload.decode(bare.encode(), timestamp=0).access is None


def test_node_decodes_access_null_as_none():
    node = NodePayload.decode('{"id": "n1", "name": "edge-07", "access": null}', timestamp=0)
    assert node.access is None
