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
_original_topic_init = Topic.__init__


def _topic_init_with_root_prefix(self, payload_type=None, prefix="colca", version="v1", context=()):
    _original_topic_init(self, payload_type=payload_type, prefix=prefix, version=version, context=context)


Topic.__init__ = _topic_init_with_root_prefix

from colca_data_contracts.payload import (
    ServiceType,
    CustomEncoder,
    Metric,
    ServiceDetails,
    AlarmNotificationConfigSnapshot,
    AlarmStateChange,
    NotificationDispatched,
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
    SystemElement as SystemElementPayload,
    Signal as SignalPayload,
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

from colca_data_contracts.semantic import SEMANTIC_CONTRACTS

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
    # Extended base types
    "ServiceType",
    "Metric",
    "ServiceDetails",
    "IndexType",
    "DataType",
    "AlarmNotificationConfigSnapshot",
    "AlarmStateChange",
    "NotificationDispatched",
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
    # Topology payloads (retained, edge-owned)
    "SystemElementPayload",
    "SignalPayload",
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
    # Semantic contracts
    "SEMANTIC_CONTRACTS",
]
