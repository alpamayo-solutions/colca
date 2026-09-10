"""Reusable health declarations for containerized Colca services."""

from colca_data_contracts.payload import (
    HealthMetricDeclaration,
    HealthMetricVisualization,
)


def container_resource_health_metrics() -> list[HealthMetricDeclaration]:
    """Return bounded CPU and memory declarations for the calling service.

    The service-name pattern covers generated Compose aliases such as
    ``hub-api`` and ``connector-opcua``. The node pattern selects the local
    unlabeled series on a node and the externally labeled series upstream.
    """
    selector = (
        'container_label_com_docker_compose_service=~"(^|.*-){service_name_pattern}$",node_id=~"{node_id_pattern}"'
    )
    return [
        HealthMetricDeclaration(
            key="container_cpu",
            name="CPU",
            metric="service_cpu_pct",
            description="CPU used by this service container.",
            query=('sum({{__name__=~"(edge|hub):service_cpu_pct",' + selector + "}})"),
            visualization=HealthMetricVisualization.TIMELINE,
            unit="%",
            precision=1,
            min_value=0,
        ),
        HealthMetricDeclaration(
            key="container_memory",
            name="Memory",
            metric="service_memory_working_set_bytes",
            description="Working-set memory used by this service container.",
            query=('sum({{__name__=~"(edge|hub):service_memory_working_set_bytes",' + selector + "}})"),
            visualization=HealthMetricVisualization.TIMELINE,
            unit="bytes",
            precision=0,
            min_value=0,
        ),
    ]
