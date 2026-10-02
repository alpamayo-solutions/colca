"""_ServiceDetails.commands: the commands a service announces it executes."""

import json

from colca_data_contracts import CommandRoute, ServiceDetails, ServiceType


def test_service_details_carry_the_announced_commands() -> None:
    details = ServiceDetails(
        id="svc",
        name="dataops-line",
        service_type=ServiceType.DATAOPS,
        colca_node_id="node-1",
        commands=[CommandRoute(contract="_CmdParam", path="line1/operator/setDensity")],
    )
    wire = json.loads(details.encode())
    assert wire["commands"] == [{"contract": "_CmdParam", "path": "line1/operator/setDensity"}]


def test_a_service_that_announces_nothing_sends_an_empty_list() -> None:
    details = ServiceDetails(id="svc", name="n", service_type=ServiceType.DATAOPS, colca_node_id="node-1")
    assert json.loads(details.encode())["commands"] == []
