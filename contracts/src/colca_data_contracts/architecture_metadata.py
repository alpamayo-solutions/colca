"""
Helper module for adding architecture diagram metadata to ServiceDetails.

This module provides utilities for services to publish architecture-specific
metadata that will be used by the architecture diagram view.
"""

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass
class KpiThresholds:
    """Threshold configuration for KPI status evaluation."""

    mode: str = "above"  # "above" = higher is better, "below" = lower is better
    success: float | None = None
    warning: float | None = None


@dataclass
class KpiDefinition:
    """Definition of a Prometheus-based KPI for a service."""

    key: str  # unique identifier
    label: str  # display label
    query: str  # PromQL query (supports {service_name} placeholder)
    format: str  # "health" | "percent" | "rate" | "seconds" | "count" | "bytes"
    thresholds: KpiThresholds | None = None


@dataclass
class ArchitectureMetrics:
    """Metrics for the architecture diagram."""

    cpuLoad: str  # e.g., "22%"
    memory: str  # e.g., "1.2GB"
    dataRate: str  # e.g., "4,500 messages/min"
    uptime: str  # e.g., "2d 5h"


@dataclass
class ArchitectureLayout:
    """Layout configuration for the architecture diagram."""

    zone: str | None = None  # 'left' | 'right' | 'top' | 'bottom' | 'center'
    order: int | None = None  # Order within the zone (lower = earlier)
    index: int | None = None  # Global index for overall ordering (lower = earlier)
    group: str | None = None  # Group services together (e.g., 'plc-pair-1')
    offset: dict[str, float] | None = None  # { x?: number, y?: number }
    alignWith: str | None = None  # Align with another service ID
    alignment: str | None = None  # 'horizontal' | 'vertical'


@dataclass
class ArchitectureConnection:
    """Connection configuration for a specific dependency."""

    from_side: str | None = None  # 'top' | 'right' | 'bottom' | 'left'
    to_side: str | None = None  # 'top' | 'right' | 'bottom' | 'left'


@dataclass
class ArchitectureDependencies:
    """Dependencies configuration for the architecture diagram."""

    upstream: list[str] = field(default_factory=list)
    downstream: list[str] = field(default_factory=list)
    connections: dict[str, ArchitectureConnection] | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary format for metadata."""
        result: dict[str, Any] = {
            "upstream": self.upstream,
            "downstream": self.downstream,
        }
        if self.connections:
            result["connections"] = {
                target_id: {
                    "from": conn.from_side,
                    "to": conn.to_side,
                }
                for target_id, conn in self.connections.items()
            }
        return result


@dataclass
class ArchitectureMetadata:
    """Complete architecture metadata for a service."""

    status: str  # 'healthy' | 'starting' | 'unhealthy'
    description: str
    metrics: ArchitectureMetrics | None = None
    icon: str | None = None  # Icon file name (e.g., 'svc-mqtt.webp')
    layout: ArchitectureLayout | None = None
    dependencies: ArchitectureDependencies | None = None
    tag: str | None = None  # Service tag (e.g., 'Data Ingestion')
    is_central: bool | None = None
    is_auxiliary: bool | None = None
    kpis: list[KpiDefinition] | None = None
    visibility: str | None = None  # "default" | "detail" | "hidden"

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary format for ServiceDetails.metadata."""
        result: dict[str, Any] = {
            "status": self.status,
            "description": self.description,
        }

        if self.metrics:
            result["metrics"] = asdict(self.metrics)

        if self.icon:
            result["icon"] = self.icon

        if self.layout:
            layout_dict: dict[str, Any] = {}
            if self.layout.zone:
                layout_dict["zone"] = self.layout.zone
            if self.layout.order is not None:
                layout_dict["order"] = self.layout.order
            if self.layout.index is not None:
                layout_dict["index"] = self.layout.index
            if self.layout.group:
                layout_dict["group"] = self.layout.group
            if self.layout.offset:
                layout_dict["offset"] = self.layout.offset
            if self.layout.alignWith:
                layout_dict["alignWith"] = self.layout.alignWith
            if self.layout.alignment:
                layout_dict["alignment"] = self.layout.alignment
            if layout_dict:
                result["layout"] = layout_dict

        if self.dependencies:
            result["dependencies"] = self.dependencies.to_dict()

        if self.tag:
            result["tag"] = self.tag

        if self.is_central is not None:
            result["is_central"] = self.is_central

        if self.is_auxiliary is not None:
            result["is_auxiliary"] = self.is_auxiliary

        if self.kpis:
            result["kpis"] = [
                {
                    "key": kpi.key,
                    "label": kpi.label,
                    "query": kpi.query,
                    "format": kpi.format,
                    "thresholds": asdict(kpi.thresholds) if kpi.thresholds else None,
                }
                for kpi in self.kpis
            ]

        if self.visibility:
            result["visibility"] = self.visibility

        return result


def create_architecture_metadata(
    status: str,
    description: str,
    metrics: ArchitectureMetrics | None = None,
    icon: str | None = None,
    layout: ArchitectureLayout | None = None,
    dependencies: ArchitectureDependencies | None = None,
    tag: str | None = None,
    is_central: bool | None = None,
    is_auxiliary: bool | None = None,
    kpis: list[KpiDefinition | dict[str, Any]] | None = None,
    visibility: str | None = None,
) -> dict[str, Any]:
    """
    Helper function to create architecture metadata dictionary.

    Returns:
        Dictionary ready to be used in ServiceDetails.architecture_metadata
    """
    # Convert dict-form KPIs to KpiDefinition dataclasses
    kpi_objects = None
    if kpis:
        kpi_objects = []
        for kpi in kpis:
            if isinstance(kpi, dict):
                thresholds = kpi.get("thresholds")
                if isinstance(thresholds, dict):
                    thresholds = KpiThresholds(**thresholds)
                kpi_objects.append(
                    KpiDefinition(
                        key=kpi["key"],
                        label=kpi["label"],
                        query=kpi["query"],
                        format=kpi["format"],
                        thresholds=thresholds,
                    )
                )
            else:
                kpi_objects.append(kpi)

    metadata = ArchitectureMetadata(
        status=status,
        description=description,
        metrics=metrics,
        icon=icon,
        layout=layout,
        dependencies=dependencies,
        tag=tag,
        is_central=is_central,
        is_auxiliary=is_auxiliary,
        kpis=kpi_objects,
        visibility=visibility,
    )
    return metadata.to_dict()


# Baseline container-level KPIs available for all containerized services.
# Services can append their own application-specific KPIs to this list.
CADVISOR_KPIS = [
    KpiDefinition(
        key="container_cpu",
        label="CPU",
        query='sum({{__name__=~"(edge|hub):service_cpu_pct",container_label_com_docker_compose_service=~"(^|.*-){service_name_pattern}$",node_id=~"{node_id_pattern}"}})',
        format="percent",
        thresholds=KpiThresholds(mode="below", success=70, warning=90),
    ),
    KpiDefinition(
        key="container_memory",
        label="Memory",
        query='sum({{__name__=~"(edge|hub):service_memory_working_set_bytes",container_label_com_docker_compose_service=~"(^|.*-){service_name_pattern}$",node_id=~"{node_id_pattern}"}})',
        format="bytes",
        thresholds=KpiThresholds(mode="below", success=536870912, warning=1073741824),
    ),
]
