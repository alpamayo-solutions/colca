"""
Helper module for adding architecture diagram metadata to ServiceDetails.

This module provides utilities for services to publish architecture-specific
metadata that will be used by the architecture diagram view.
"""
from typing import Dict, Any, Optional, List, Union
from dataclasses import dataclass, field, asdict


@dataclass
class KpiThresholds:
    """Threshold configuration for KPI status evaluation."""
    mode: str = "above"  # "above" = higher is better, "below" = lower is better
    success: Optional[float] = None
    warning: Optional[float] = None


@dataclass
class KpiDefinition:
    """Definition of a Prometheus-based KPI for a service."""
    key: str  # unique identifier
    label: str  # display label
    query: str  # PromQL query (supports {service_name} placeholder)
    format: str  # "health" | "percent" | "rate" | "seconds" | "count" | "bytes"
    thresholds: Optional[KpiThresholds] = None


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
    zone: Optional[str] = None  # 'left' | 'right' | 'top' | 'bottom' | 'center'
    order: Optional[int] = None  # Order within the zone (lower = earlier)
    index: Optional[int] = None  # Global index for overall ordering (lower = earlier)
    group: Optional[str] = None  # Group services together (e.g., 'plc-pair-1')
    offset: Optional[Dict[str, float]] = None  # { x?: number, y?: number }
    alignWith: Optional[str] = None  # Align with another service ID
    alignment: Optional[str] = None  # 'horizontal' | 'vertical'


@dataclass
class ArchitectureConnection:
    """Connection configuration for a specific dependency."""
    from_side: Optional[str] = None  # 'top' | 'right' | 'bottom' | 'left'
    to_side: Optional[str] = None  # 'top' | 'right' | 'bottom' | 'left'


@dataclass
class ArchitectureDependencies:
    """Dependencies configuration for the architecture diagram."""
    upstream: List[str] = field(default_factory=list)
    downstream: List[str] = field(default_factory=list)
    connections: Optional[Dict[str, ArchitectureConnection]] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary format for metadata."""
        result = {
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
    metrics: Optional[ArchitectureMetrics] = None
    icon: Optional[str] = None  # Icon file name (e.g., 'svc-mqtt.webp')
    layout: Optional[ArchitectureLayout] = None
    dependencies: Optional[ArchitectureDependencies] = None
    tag: Optional[str] = None  # Service tag (e.g., 'Data Ingestion')
    is_central: Optional[bool] = None
    is_auxiliary: Optional[bool] = None
    kpis: Optional[List[KpiDefinition]] = None
    visibility: Optional[str] = None  # "default" | "detail" | "hidden"

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary format for ServiceDetails.metadata."""
        result: Dict[str, Any] = {
            "status": self.status,
            "description": self.description,
        }

        if self.metrics:
            result["metrics"] = asdict(self.metrics)

        if self.icon:
            result["icon"] = self.icon

        if self.layout:
            layout_dict = {}
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
    metrics: Optional[ArchitectureMetrics] = None,
    icon: Optional[str] = None,
    layout: Optional[ArchitectureLayout] = None,
    dependencies: Optional[ArchitectureDependencies] = None,
    tag: Optional[str] = None,
    is_central: Optional[bool] = None,
    is_auxiliary: Optional[bool] = None,
    kpis: Optional[List[Union[KpiDefinition, Dict[str, Any]]]] = None,
    visibility: Optional[str] = None,
) -> Dict[str, Any]:
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
                kpi_objects.append(KpiDefinition(
                    key=kpi["key"],
                    label=kpi["label"],
                    query=kpi["query"],
                    format=kpi["format"],
                    thresholds=thresholds,
                ))
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
        query='rate(container_cpu_usage_seconds_total{{container_label_com_docker_compose_service="{service_name}"}}[5m]) * 100',
        format="percent",
        thresholds=KpiThresholds(mode="below", success=70, warning=90),
    ),
    KpiDefinition(
        key="container_memory",
        label="Memory",
        query='container_memory_working_set_bytes{{container_label_com_docker_compose_service="{service_name}"}}',
        format="bytes",
        thresholds=KpiThresholds(mode="below", success=536870912, warning=1073741824),
    ),
]

