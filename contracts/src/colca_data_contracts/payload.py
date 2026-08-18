import json
import hashlib
import datetime
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field, fields

from franzmq.data_contracts.base import (
    Payload,
    Cmd,
    Metric as BaseMetric,
    ServiceDetails as BaseServiceDetails,
    StrEnum as BaseStrEnum,
    IndexType,
    DataType,
)


class ServiceType(BaseStrEnum):
    CONNECTOR = "connector"
    NOTIFICATIONS = "notifications"
    PLATFORM_HEALTH = "platform-health"
    UI = "ui"
    DATABASE = "database"
    API = "api"
    GRAFANA = "grafana"
    BROKER = "broker"
    AGENT = "agent"
    DATAOPS = "dataops"
    REVERSE_PROXY = "reverse-proxy"
    AUTH_SERVICE = "auth-service"
    MCP_SERVER = "mcp-server"
    DOCS = "docs"
    DATABASE_VIEWER = "database-viewer"
    CA = "ca"
    ADVERTISER = "advertiser"
    TEST_RUNNER = "test-runner"
    OTHER = "other"
    UNKNOWN = "unknown"


class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, ServiceType):
            return str(obj)
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        if isinstance(obj, IndexType):
            return str(obj)
        if isinstance(obj, DataType):
            return str(obj)
        raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


@dataclass
class Metric(BaseMetric):
    signal_id: str = ""
    error: Optional[str] = None
    edge_node_id: Optional[str] = None

    def encode(self):
        data = self.__dict__.copy()
        if isinstance(data["timestamp"], str):
            data["timestamp"] = datetime.datetime.fromisoformat(data["timestamp"]).timestamp()
        if data["error"] is None:
            data.pop("error", None)
        if data["edge_node_id"] is None:
            data.pop("edge_node_id", None)
        return json.dumps(data, cls=CustomEncoder)

    @classmethod
    def decode(cls, json_str, timestamp: int):
        data = json.loads(json_str)
        if isinstance(data["timestamp"], str):
            data["timestamp"] = datetime.datetime.fromisoformat(data["timestamp"]).timestamp()
        allowed_fields = {field.name for field in fields(cls)}
        data = {key: value for key, value in data.items() if key in allowed_fields}
        return cls(**data)


@dataclass
class ServiceDetails(BaseServiceDetails):
    architecture_metadata: Dict[str, Any] = field(default_factory=dict)


def _sorted_items(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return sorted(items, key=lambda item: str(item.get("id", "")))


@dataclass
class AlarmNotificationConfigSnapshot(Payload):
    schema_version: int
    generated_at: float
    alarms: List[Dict[str, Any]]
    channels: List[Dict[str, Any]]
    recipients: List[Dict[str, Any]]
    policies: List[Dict[str, Any]]
    policy_targets: List[Dict[str, Any]]
    active_silences: List[Dict[str, Any]]

    def __init__(
        self,
        schema_version: int,
        generated_at: float,
        alarms: List[Dict[str, Any]],
        channels: List[Dict[str, Any]],
        recipients: List[Dict[str, Any]],
        policies: List[Dict[str, Any]],
        policy_targets: List[Dict[str, Any]],
        active_silences: Optional[List[Dict[str, Any]]] = None,
        revision_id: Optional[str] = None,
    ):
        self.schema_version = schema_version
        self.generated_at = generated_at
        self.alarms = alarms
        self.channels = channels
        self.recipients = recipients
        self.policies = policies
        self.policy_targets = policy_targets
        self.active_silences = active_silences or []

    @classmethod
    def get_identifier(cls) -> str:
        return "_AlarmNotificationConfig"

    @property
    def revision_id(self) -> str:
        data = {
            "schema_version": self.schema_version,
            "alarms": _sorted_items(self.alarms),
            "channels": _sorted_items(self.channels),
            "recipients": _sorted_items(self.recipients),
            "policies": _sorted_items(self.policies),
            "policy_targets": _sorted_items(self.policy_targets),
            "active_silences": _sorted_items(self.active_silences),
        }
        return hashlib.sha256(
            json.dumps(data, cls=CustomEncoder, sort_keys=True).encode()
        ).hexdigest()

    @property
    def __dict__(self):
        d = super().__dict__.copy()
        d["revision_id"] = self.revision_id
        return d


@dataclass
class AlarmStateChange(Payload):
    event_id: str
    alarm_id: str
    from_status: Optional[str]
    to_status: str
    at: float
    signal_value: Optional[Any]
    metadata_json: Dict[str, Any] = field(default_factory=dict)
    # Sanitized notification settings resolved for this alarm at publish time
    # (severity, labels, covering policies → channels/recipients), so downstream
    # MQTT consumers are self-contained. Never carries channel secrets.
    notification_settings: Optional[Dict[str, Any]] = None


@dataclass
class NotificationDispatched(Payload):
    idempotency_key: str
    policy_id: Optional[str]
    channel_id: str
    recipient_id: str
    alarm_event_id: Optional[str]
    status: str
    at: float
    attempt: int
    latency_ms: Optional[int]
    error: Optional[str] = None
    metadata_json: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DataTag(Payload):
    id: str
    name: str
    is_writable: bool
    is_readable: bool
    data_type: Optional[str] = None
    hierarchy: List[str] = field(default_factory=list)
    meta: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Result:
    value: Any
    timestamp: float = field(default_factory=lambda: datetime.datetime.now(datetime.timezone.utc).timestamp())
    error: Optional[str] = None

    def to_metric(self, signal_id: str) -> Metric:
        return Metric(value=self.value, signal_id=signal_id, timestamp=self.timestamp, error=self.error)


@dataclass
class DataTags(Payload):
    data_tags: List[DataTag]
    connector: str

    @property
    def version(self) -> str:
        sorted_tags = sorted(self.data_tags, key=lambda x: x.id)
        tags_dict = [tag.__dict__ for tag in sorted_tags]
        return hashlib.md5(json.dumps(tags_dict, cls=CustomEncoder, sort_keys=True).encode()).hexdigest()

    @classmethod
    def decode(cls, json_str: str, timestamp: int) -> 'DataTags':
        data = json.loads(json_str)
        data_tags = [DataTag(**tag) for tag in data["data_tags"]]
        data["data_tags"] = data_tags
        data.pop("version", None)
        return cls(**data)

    @property
    def __dict__(self):
        d = super().__dict__.copy()
        d["data_tags"] = [tag.__dict__ for tag in self.data_tags]
        d["version"] = self.version
        return d


@dataclass
class SignalData:
    id: str
    name: str
    source: str
    data_type: DataType
    index_type: IndexType
    topic_name: str
    system_element: Optional[str] = None
    config: Dict[str, Any] = field(default_factory=dict)
    unit: Optional[str] = None
    precision: Optional[int] = None
    min_value: Optional[float] = None
    max_value: Optional[float] = None

    @property
    def hierarchy(self) -> List[str]:
        if self.system_element:
            return self.system_element.split(" - ")
        return []

    def encode(self):
        return json.dumps(self.__dict__)


@dataclass
class DataTagContext(Payload):
    id: str
    tag_id: str
    source: str
    topic_name: str
    is_logged: bool
    is_published: bool
    signal: SignalData

    @classmethod
    def decode(cls, json_str: str, timestamp: int) -> 'DataTagContext':
        data = json.loads(json_str)
        data["signal"] = SignalData(**data["signal"])
        data.pop("version", None)
        return cls(**data)

    @property
    def __dict__(self):
        d = super().__dict__.copy()
        d["signal"] = self.signal.__dict__
        return d


@dataclass
class DataTagContexts(Payload):
    data_tag_contexts: List[DataTagContext]

    @classmethod
    def decode(cls, json_str: str, timestamp: int) -> 'DataTagContexts':
        data = json.loads(json_str)
        data_tag_contexts = [
            DataTagContext.decode(json.dumps(context), timestamp)
            for context in data["data_tag_contexts"]
        ]
        return cls(data_tag_contexts=data_tag_contexts)

    @property
    def version(self) -> str:
        sorted_contexts = sorted(self.data_tag_contexts, key=lambda x: x.tag_id)
        contexts_dict = [context.__dict__ for context in sorted_contexts]
        return hashlib.md5(
            json.dumps(contexts_dict, cls=CustomEncoder, sort_keys=True).encode()
        ).hexdigest()

    @property
    def __dict__(self):
        d = super().__dict__.copy()
        d["data_tag_contexts"] = [context.__dict__ for context in self.data_tag_contexts]
        d["version"] = self.version
        return d


@dataclass
class ApiWriteCmd(Cmd):
    """Hub-to-edge API write forwarding command.

    command dict contains:
        method: HTTP method (POST, PATCH, DELETE)
        path: API path (e.g. /api/v1/signals/{id}/)
        data: Request body (None for DELETE)
        query_params: URL query parameters
    """
    command: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DBEvent(Payload):
    """Real-time CDC event for a single model row (create/update/delete)."""
    edge_node_id: str
    model: str
    operation: str  # "upsert" or "delete"
    pk: str
    data: Optional[Dict[str, Any]] = None  # Full serialized row; None for delete

    @classmethod
    def decode(cls, json_str: str, timestamp: int) -> 'DBEvent':
        data = json.loads(json_str)
        return cls(**data)


@dataclass
class DBDump(Payload):
    """Periodic full-table dump for self-healing sync."""
    edge_node_id: str
    model: str
    rows: List[Dict[str, Any]]
    row_hash: str  # MD5 of sorted JSON for change detection

    @classmethod
    def decode(cls, json_str: str, timestamp: int) -> 'DBDump':
        data = json.loads(json_str)
        return cls(**data)


@dataclass
class AnnotationType(Payload):
    """Hub-owned AnnotationType published to edges via retained MQTT.

    Topic: colca/v1/_AnnotationType/{type_id}
    Published by the hub API on create/update/delete of hub-owned annotation types.
    Consumed by mqtt-to-api on edges to upsert/delete locally.
    """

    id: str
    name: str
    data_type: str
    operation: str  # "upsert" or "delete"
    system_element: Optional[str] = None
    i18n_name: Optional[str] = None
    description: Optional[str] = None
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    unit: Optional[str] = None
    create_option_on_input_new: bool = False
    options: List[Dict[str, Any]] = field(default_factory=list)

    @classmethod
    def decode(cls, json_str: str, timestamp: int) -> "AnnotationType":
        data = json.loads(json_str)
        return cls(**data)


@dataclass
class MetadataType(Payload):
    """Hub-owned MetadataType published to edges via retained MQTT.

    Topic: colca/v1/_MetadataType/{type_id}
    Published by the hub API on create/update/delete of hub-owned metadata types.
    Consumed by mqtt-to-api on edges to upsert/delete locally.
    """

    id: str
    name: str
    data_type: str
    operation: str  # "upsert" or "delete"
    i18n_name: Optional[str] = None
    description: Optional[str] = None
    is_mandatory: bool = False
    allowed_content_type_keys: List[str] = field(default_factory=list)

    @classmethod
    def decode(cls, json_str: str, timestamp: int) -> "MetadataType":
        data = json.loads(json_str)
        return cls(**data)


@dataclass
class Interface(Payload):
    """Interface definition published as retained MQTT.

    Topic: colca/v1/_Interface/{name}/v{version}
    Published by the api service at boot. Subscribers (UI, dataops, dm CLI)
    consume this to discover the available interface registry and the
    signals each interface requires. ``signals`` carries the *resolved*
    list (parents already merged), so consumers don't need to walk
    ``extends`` themselves.
    """

    name: str
    version: str = "1.0"
    description: str = ""
    extends: List[str] = field(default_factory=list)
    signals: List[Dict[str, Any]] = field(default_factory=list)

    @classmethod
    def decode(cls, json_str: str, timestamp: int) -> "Interface":
        data = json.loads(json_str)
        return cls(**data)


@dataclass
class SystemElement(Payload):
    """Topology entity published as retained MQTT.

    Topic: colca/v1/_SystemElement/{_topic_context_section}
    Published by the api service on SystemElement create/update; soft-delete publishes
    a retained empty payload (tombstone). Consumed by the Topology UI view and,
    in a later iteration, by hub-side mqtt-to-api for replication.
    """

    id: str
    name: str
    description: str = ""
    parent_topic: Optional[str] = None  # Parent _topic_context_section, None for root
    implements: List[str] = field(default_factory=list)  # Interface names this SE fulfils (Phase 2)
    interface_coverage: Dict[str, Dict[str, str]] = field(default_factory=dict)  # Per-interface signal coverage
    external_asset_id: Optional[str] = None
    external_asset_id_type: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    edge_node_id: Optional[str] = None
    hub_node_id: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None

    @classmethod
    def decode(cls, json_str: str, timestamp: int) -> "SystemElement":
        data = json.loads(json_str)
        return cls(**data)


@dataclass
class Signal(Payload):
    """Topology entity, authored by the node.

    Topic: ``colca/v1/_Signal/{node-id}/{path…}`` — the record's own topic is its
    position in the namespace, so nothing here restates it.

    The signal also carries its **binding**: the one data tag it reads from,
    named by ``(connector, tag_id)``. That replaces the DataTagContext, whose
    many-tags-to-one-signal relationship was never adopted (data-model binding
    design §2). The reference is a pair of identities rather than a path
    because a record's topic is rewritten at every hop while its payload is
    not — a path stored in here would silently mean something else at an
    ancestor (§3.2).

    Written by the node in response to a `_CmdConfigure` command; a retired
    signal is a retained empty payload (tombstone).
    """

    id: str
    name: str
    description: str = ""
    #: ULID of the SystemElement that owns this signal.
    system_element_id: Optional[str] = None
    #: ULID of the connector owning the bound tag, and the tag's id in its catalogue.
    connector: Optional[str] = None
    tag_id: Optional[str] = None
    #: The connector publishes metrics for this signal.
    is_published: bool = False
    #: The read side historises it (consumed by the historian bridge).
    is_logged: bool = False
    data_type: Optional[DataType] = None
    index_type: Optional[IndexType] = None
    unit: Optional[str] = None
    precision: Optional[int] = None
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    config: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)
    implements_contract: Optional[str] = None
    has_contract: bool = False
    edge_node_id: Optional[str] = None
    hub_node_id: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None

    @property
    def __dict__(self):
        d = super().__dict__.copy()
        if self.data_type is not None:
            d["data_type"] = str(self.data_type)
        if self.index_type is not None:
            d["index_type"] = str(self.index_type)
        return d

    @classmethod
    def decode(cls, json_str: str, timestamp: int) -> "Signal":
        data = json.loads(json_str)
        if data.get("data_type") is not None:
            data["data_type"] = DataType(data["data_type"])
        if data.get("index_type") is not None:
            data["index_type"] = IndexType(data["index_type"])
        return cls(**data)


# ---------------------------------------------------------------------------
# Colca command classes (schema-bundle design §3).
# The class hierarchy IS the routing information: anything deriving from Cmd
# lands in the commands stream under the hazard class its name carries
# (_CmdParam → param, _CmdOperate → operate, _CmdMaintain → maintain,
# _CmdConfigure → configure, _CmdAdmin → admin). The wire contract at the colca door is
# correlation_id + expires_at (unix milliseconds); created_at is a
# franzmq-base field colca publishers do not stamp — the bundle generator
# drops it from `required` for every cmd-class contract.
# ---------------------------------------------------------------------------


@dataclass
class CmdParam(Cmd):
    """Parameters & setpoints (reversible)."""


@dataclass
class CmdOperate(Cmd):
    """Start/stop, job control."""


@dataclass
class CmdMaintain(Cmd):
    """Calibration, config updates."""


@dataclass
class CmdConfigure(Cmd):
    """Data-model editing: signal bindings and the elements that hold them.

    Verbs (the path at the target node): ``signal/upsert``, ``signal/delete``,
    ``signal/autobind``. Executed by the target NODE, which then writes the
    resulting `_Signal` records under its own identity — a human may command
    but may not author state (data-model binding design §4).

    Its own hazard class rather than `maintain`: the four machine classes are a
    ladder of how dangerous a command is to the equipment, and editing the data
    model is not on that ladder. Someone who may bind a signal must not thereby
    be allowed to send maintenance commands to a PLC (§3.3)."""


@dataclass
class CmdAdmin(Cmd):
    """Node administration: provisioning (enroll/revoke), restart, firmware.

    Executed by the target NODE, not a machine (colca cmdadmin design §5);
    verb-specific fields (entry, ulid) ride in the open payload."""
