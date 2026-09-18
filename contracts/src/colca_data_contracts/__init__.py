"""
Colca domain-specific data contracts extending franzmq.

On import, all Colca payload classes are registered into
franzmq.data_contracts.base.PAYLOAD_CLASSES so that Topic.from_str()
can resolve Colca topic strings.
"""

import inspect
import json
from typing import cast

from franzmq.data_contracts import PAYLOAD_CLASSES
from franzmq.data_contracts.base import DataType, IndexType, Payload
from franzmq.topic import Topic

from colca_data_contracts.root import topic_prefix, topic_root

# Replace franzmq's default topic prefix, "example", with the Colca topic root
# (see root.py). Dataclass defaults are compiled into __init__, so __init__ has
# to be wrapped. The wrapper forwards its arguments untouched because franzmq's
# signature differs between releases (0.5 added node_id).
_original_topic_init = Topic.__init__
_TOPIC_FIELDS = list(Topic.__dataclass_fields__)
_PREFIX_INDEX = _TOPIC_FIELDS.index("prefix")


def _topic_init_with_root_prefix(self, *args, **kwargs):
    if len(args) <= _PREFIX_INDEX and "prefix" not in kwargs:
        kwargs["prefix"] = topic_root()
    _original_topic_init(self, *args, **kwargs)


Topic.__init__ = _topic_init_with_root_prefix

# Register all Colca payload classes into franzmq's PAYLOAD_CLASSES
# so that Topic.from_str() can resolve Colca topic strings.
from colca_data_contracts import payload as _payload_module
from colca_data_contracts.architecture_metadata import (
    ArchitectureConnection,
    ArchitectureDependencies,
    ArchitectureLayout,
    ArchitectureMetadata,
    ArchitectureMetrics,
    create_architecture_metadata,
)
from colca_data_contracts.local_service import (
    LocalServiceIdentity,
    attach_log_publisher,
    connect_local_mqtt,
    publish_local_service_details,
    resolve_local_identity,
    service_details_topic,
)
from colca_data_contracts.logging import (
    COLCA_LOG_FORMAT,
    SanitizingFormatter,
    sanitize,
    setup_logging,
)
from colca_data_contracts.machine_state import (
    MachineState,
    OperatingMode,
    StateReason,
)
from colca_data_contracts.observability import container_resource_health_metrics
from colca_data_contracts.payload import (
    ULID,
    ULID_PATTERN,
    ActorKind,
    AlarmNotificationConfigSnapshot,
    AlarmNotificationSummary,
    AlarmSeverity,
    AlarmState,
    AlarmStateChange,
    AlarmStatus,
    AuditAction,
    AuditEvent,
    AuditOutcome,
    AuditSource,
    CmdEdit,
    ConstantDataType,
    CustomEncoder,
    DataTag,
    DataTags,
    HealthMetricDeclaration,
    HealthMetricVisualization,
    Metric,
    NetworkInterface,
    NetworkInterfaceType,
    NotificationChannelConfig,
    NotificationChannelOutcome,
    NotificationConfigStatus,
    NotificationDispatched,
    Pattern,
    Result,
    SealedSecretEnvelope,
    ServiceDetails,
    ServiceType,
    SignalDataType,
    derive_annotation_id,
)
from colca_data_contracts.payload import (
    Annotation as AnnotationPayload,
)
from colca_data_contracts.payload import (
    AnnotationType as AnnotationTypePayload,
)
from colca_data_contracts.payload import (
    Constant as ConstantPayload,
)
from colca_data_contracts.payload import (
    DataModel as DataModelPayload,
)
from colca_data_contracts.payload import (
    EditOperation as EditOperationPayload,
)
from colca_data_contracts.payload import (
    ExternalReference as ExternalReferencePayload,
)
from colca_data_contracts.payload import (
    ExternalSystem as ExternalSystemPayload,
)
from colca_data_contracts.payload import (
    Group as GroupPayload,
)
from colca_data_contracts.payload import (
    MetadataType as MetadataTypePayload,
)
from colca_data_contracts.payload import (
    Node as NodePayload,
)
from colca_data_contracts.payload import (
    PersonalAccessToken as PersonalAccessTokenPayload,
)
from colca_data_contracts.payload import (
    Resource as ResourcePayload,
)
from colca_data_contracts.payload import (
    SemanticTag as SemanticTagPayload,
)
from colca_data_contracts.payload import (
    Signal as SignalPayload,
)
from colca_data_contracts.payload import (
    SystemElement as SystemElementPayload,
)
from colca_data_contracts.service_topics import service_context
from colca_data_contracts.topics import (
    TOPICS_CARRY_NODE_ID,
    node_id,
    node_topic,
)

for _name, _obj in inspect.getmembers(_payload_module, inspect.isclass):
    if issubclass(_obj, Payload) and _obj is not Payload:
        PAYLOAD_CLASSES[cast("type[Payload]", _obj).get_identifier()] = _obj

# Monkey-patch json.JSONEncoder to handle Colca enums/datetimes
_original_default = json.JSONEncoder.default


def _patched_default(self, obj):
    try:
        return CustomEncoder().default(obj)
    except TypeError:
        return _original_default(self, obj)


json.JSONEncoder.default = _patched_default  # type: ignore[method-assign]

__all__ = [  # noqa: RUF022 - grouped by topic
    # Topics
    "topic_root",
    "topic_prefix",
    "node_id",
    "node_topic",
    "TOPICS_CARRY_NODE_ID",
    # Wire-level string constraints
    "Pattern",
    "ULID",
    "ULID_PATTERN",
    # Extended base types
    "ServiceType",
    "AuditSource",
    "AuditAction",
    "AuditOutcome",
    "ActorKind",
    "AuditEvent",
    "Metric",
    "NodePayload",
    "ServiceDetails",
    "HealthMetricDeclaration",
    "HealthMetricVisualization",
    "container_resource_health_metrics",
    "NetworkInterface",
    "NetworkInterfaceType",
    "IndexType",
    "DataType",
    "AlarmNotificationConfigSnapshot",
    "SealedSecretEnvelope",
    "NotificationChannelConfig",
    "NotificationConfigStatus",
    "NotificationChannelOutcome",
    "AlarmNotificationSummary",
    "AlarmStateChange",
    "NotificationDispatched",
    # The standing alarm, one retained record per alarm definition
    "AlarmState",
    "AlarmStatus",
    "AlarmSeverity",
    # Annotation instances, on their own stream like alarms
    "AnnotationPayload",
    "derive_annotation_id",
    # Domain payloads
    "DataTag",
    "DataTags",
    "Result",
    # Hub-owned type payloads
    "AnnotationTypePayload",
    "MetadataTypePayload",
    "SemanticTagPayload",
    "GroupPayload",
    "PersonalAccessTokenPayload",
    "DataModelPayload",
    "ExternalSystemPayload",
    "ExternalReferencePayload",
    # Topology payloads (retained, edge-owned)
    "SystemElementPayload",
    "SignalPayload",
    "ConstantDataType",
    "SignalDataType",
    "ConstantPayload",
    "ResourcePayload",
    "CmdEdit",
    "EditOperationPayload",
    # MachineState contract enums
    "MachineState",
    "OperatingMode",
    "StateReason",
    # Architecture metadata
    "ArchitectureMetrics",
    "ArchitectureLayout",
    "ArchitectureConnection",
    "ArchitectureDependencies",
    "ArchitectureMetadata",
    "create_architecture_metadata",
    # Encoder
    "CustomEncoder",
    # Logging
    "setup_logging",
    "sanitize",
    "SanitizingFormatter",
    "COLCA_LOG_FORMAT",
    # Local Colca service trust
    "LocalServiceIdentity",
    "attach_log_publisher",
    "connect_local_mqtt",
    "publish_local_service_details",
    "service_context",
    "resolve_local_identity",
    "service_details_topic",
]
