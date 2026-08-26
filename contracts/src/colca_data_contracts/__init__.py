"""
Colca domain-specific data contracts extending franzmq.

On import, all Colca payload classes are registered into
franzmq.data_contracts.base.PAYLOAD_CLASSES so that Topic.from_str()
can resolve Colca topic strings.
"""
import inspect
import json

from franzmq.data_contracts.base import Payload, IndexType, DataType
from franzmq.data_contracts import PAYLOAD_CLASSES
from franzmq.topic import Topic

# Override franzmq's default topic prefix from "example" to "colca".
# All Colca services use "colca/v1/" as the topic namespace.
# Dataclass defaults are compiled into __init__ at class definition time,
# so we must replace __init__ to change the effective default.
#
# The wrapper forwards whatever it is given instead of restating franzmq's
# parameter list: franzmq 0.5.0 added `node_id` at level 4, and services move to
# it one at a time (their franzmq pin is the switch — schema-bundle design §9.2).
# A hard-coded signature here would break whichever half of the fleet it does
# not match.
_original_topic_init = Topic.__init__
_TOPIC_FIELDS = list(Topic.__dataclass_fields__)
_PREFIX_INDEX = _TOPIC_FIELDS.index("prefix")


def _topic_init_with_root_prefix(self, *args, **kwargs):
    if len(args) <= _PREFIX_INDEX and "prefix" not in kwargs:
        kwargs["prefix"] = "colca"
    _original_topic_init(self, *args, **kwargs)


Topic.__init__ = _topic_init_with_root_prefix

from colca_data_contracts.topics import (  # noqa: E402
    TOPICS_CARRY_NODE_ID,
    node_id,
    node_topic,
)

from colca_data_contracts.payload import (
    ServiceType,
    AuditSource,
    AuditAction,
    AuditOutcome,
    ActorKind,
    AuditEvent,
    CustomEncoder,
    Metric,
    Node as NodePayload,
    ServiceDetails,
    SealedSecretEnvelope,
    NotificationChannelConfig,
    AlarmNotificationConfigSnapshot,
    NotificationConfigStatus,
    NotificationChannelOutcome,
    AlarmNotificationSummary,
    AlarmStateChange,
    NotificationDispatched,
    Annotation as AnnotationPayload,
    derive_annotation_id,
    DataTag,
    DataTags,
    DataTagContext,
    DataTagContexts,
    SignalData,
    Result,
    ApiWriteCmd,
    DBEvent,
    DBDump,
    AnnotationType as AnnotationTypePayload,
    MetadataType as MetadataTypePayload,
    SemanticTag as SemanticTagPayload,
    Group as GroupPayload,
    PersonalAccessToken as PersonalAccessTokenPayload,
    DataModel as DataModelPayload,
    ExternalSystem as ExternalSystemPayload,
    ExternalReference as ExternalReferencePayload,
    SystemElement as SystemElementPayload,
    Signal as SignalPayload,
    ConstantDataType,
    SignalDataType,
    Constant as ConstantPayload,
    Resource as ResourcePayload,
    CmdEdit,
    EditOperation as EditOperationPayload,
)

from colca_data_contracts.logging import (
    setup_logging,
    sanitize,
    SanitizingFormatter,
    COLCA_LOG_FORMAT,
)

from colca_data_contracts.architecture_metadata import (
    ArchitectureMetrics,
    ArchitectureLayout,
    ArchitectureConnection,
    ArchitectureDependencies,
    ArchitectureMetadata,
    create_architecture_metadata,
)

from colca_data_contracts.machine_state import (
    MachineState,
    OperatingMode,
    StateReason,
)

from colca_data_contracts.service_topics import service_context
from colca_data_contracts.local_service import (
    LocalServiceIdentity,
    connect_local_mqtt,
    publish_local_service_details,
    resolve_local_identity,
    service_details_topic,
)

# Register all Colca payload classes into franzmq's PAYLOAD_CLASSES
# so that Topic.from_str() can resolve Colca topic strings.
from colca_data_contracts import payload as _payload_module

for _name, _obj in inspect.getmembers(_payload_module, inspect.isclass):
    if issubclass(_obj, Payload) and _obj is not Payload:
        PAYLOAD_CLASSES[_obj.get_identifier()] = _obj

# Monkey-patch json.JSONEncoder to handle Colca enums/datetimes
_original_default = json.JSONEncoder.default


def _patched_default(self, obj):
    try:
        return CustomEncoder().default(obj)
    except TypeError:
        return _original_default(self, obj)


json.JSONEncoder.default = _patched_default

__all__ = [
    # Topics
    "node_id",
    "node_topic",
    "TOPICS_CARRY_NODE_ID",
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
    # Annotation instances (own stream, mirrors alarm — design §8)
    "AnnotationPayload",
    "derive_annotation_id",
    # Domain payloads
    "DataTag",
    "DataTags",
    "DataTagContext",
    "DataTagContexts",
    "SignalData",
    "Result",
    # Hub-to-edge forwarding
    "ApiWriteCmd",
    # DB sync payloads
    "DBEvent",
    "DBDump",
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
    "connect_local_mqtt",
    "publish_local_service_details",
    "service_context",
    "resolve_local_identity",
    "service_details_topic",
]
