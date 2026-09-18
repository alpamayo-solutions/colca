import datetime
import hashlib
import json
from collections.abc import Iterable
from dataclasses import asdict, dataclass, field, fields, is_dataclass
from typing import TYPE_CHECKING, Annotated, Any

import ulid
from franzmq.data_contracts.base import (
    Cmd,
    DataType,
    IndexType,
    Payload,
)
from franzmq.data_contracts.base import (
    Metric as BaseMetric,
)

if TYPE_CHECKING:
    # franzmq publishes no types; its StrEnum is a plain (str, Enum).
    from enum import StrEnum as BaseStrEnum
else:
    from franzmq.data_contracts.base import StrEnum as BaseStrEnum


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
    OTHER = "other"
    UNKNOWN = "unknown"


class HealthMetricVisualization(BaseStrEnum):
    """Compact presentation a service prefers for one health metric."""

    GAUGE = "gauge"
    TIMELINE = "timeline"
    VALUE = "value"


class NetworkInterfaceType(BaseStrEnum):
    """Portable interface categories; names remain the node's own names."""

    ETHERNET = "ethernet"
    WIFI = "wifi"
    CELLULAR = "cellular"
    LOOPBACK = "loopback"
    BRIDGE = "bridge"
    VPN = "vpn"
    OVERLAY = "overlay"
    VIRTUAL = "virtual"
    OTHER = "other"


#: Value types a Colca ``_Signal`` can carry: franzmq's ``DataType`` plus
#: ``json``. Derived from franzmq's enum so the two cannot drift, and pinned by
#: ``vectors/signal_data_types.json``.
SignalDataType = BaseStrEnum(  # type: ignore[misc]
    "SignalDataType",
    {member.name: member.value for member in DataType} | {"JSON": "json"},
)


class ReplicationPolicy(BaseStrEnum):
    REPLICATE_TO_PARENTS = "replicate_to_parents"
    SOURCE_LOCAL_ONLY = "source_local_only"


class ConstantDataType(BaseStrEnum):
    """Value types of Colca configuration constants.

    Constants use width-explicit numeric names, while a signal's ``DataType``
    keeps ``int`` and ``float``: telemetry and configuration are separate
    contracts.
    """

    FLOAT64 = "float64"
    INT64 = "int64"
    BOOLEAN = "boolean"
    STRING = "string"
    DATETIME = "datetime"
    JSON = "json"


class AuditSource(BaseStrEnum):
    COLCA = "colca"
    API = "api"
    KEYCLOAK = "keycloak"
    PROJECTOR = "projector"
    NODE_MANAGER = "node_manager"


class AuditAction(BaseStrEnum):
    SIGN_IN = "sign_in"
    SIGN_OUT = "sign_out"
    AUTHORIZE = "authorize"
    IDENTITY_ADMIN = "identity_admin"
    CREDENTIAL_ADMIN = "credential_admin"
    EXECUTE = "execute"
    REBUILD = "rebuild"
    RESTORE = "restore"
    SECURITY_CONFIG = "security_config"


class AuditOutcome(BaseStrEnum):
    SUCCESS = "success"
    FAILURE = "failure"
    DENIED = "denied"


class ActorKind(BaseStrEnum):
    HUMAN = "human"
    SERVICE = "service"
    NODE = "node"
    SYSTEM = "system"
    ANONYMOUS = "anonymous"


class AlarmStatus(BaseStrEnum):
    """What a standing alarm is doing. No ``normal`` and no ``recovered``: an
    alarm that no longer stands is deleted, not set to a quiet status."""

    PENDING = "pending"
    FIRING = "firing"
    UNKNOWN = "unknown"


class AlarmSeverity(BaseStrEnum):
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(
            obj,
            (
                ServiceType,
                HealthMetricVisualization,
                NetworkInterfaceType,
                ConstantDataType,
                AuditSource,
                AuditAction,
                AuditOutcome,
                ActorKind,
                AlarmStatus,
                AlarmSeverity,
            ),
        ):
            return str(obj)
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        if isinstance(obj, IndexType):
            return str(obj)
        if isinstance(obj, DataType):
            return str(obj)
        if is_dataclass(obj):
            return asdict(obj)
        raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


# ---------------------------------------------------------------------------
# Wire-level string constraints
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Pattern:
    """A regular expression a string field's wire value must match.

    Attach it with ``typing.Annotated``. The bundle carries it as a JSON Schema
    ``pattern``, so the door refuses a value that does not match; Python
    constructors do not check it.
    """

    regex: str


#: A ULID: 26 Crockford base32 characters (no I, L, O, U). The bundle carries
#: it to the door, so a malformed id is refused where it is published.
ULID_PATTERN = r"^[0-9A-HJKMNP-TV-Z]{26}$"

#: A string field whose value is a ULID: an entity's own id and every
#: reference to one.
ULID = Annotated[str, Pattern(ULID_PATTERN)]


@dataclass
class Metric(BaseMetric):
    signal_id: str = ""
    error: str | None = None
    colca_node_id: str | None = None

    def encode(self):
        data = self.__dict__.copy()
        if isinstance(data["timestamp"], str):
            data["timestamp"] = datetime.datetime.fromisoformat(data["timestamp"]).timestamp()
        if data["error"] is None:
            data.pop("error", None)
        if data["colca_node_id"] is None:
            data.pop("colca_node_id", None)
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
class HealthMetricDeclaration:
    """One bounded, self-described Prometheus health signal.

    ``metric`` names the family. ``query`` may refine it with aggregation or a
    rate expression and may use the server-rendered ``{service_name}``,
    ``{service_name_pattern}``, ``{node_id}``, and ``{node_id_pattern}``
    placeholders. Raw PromQL never crosses into the browser.
    """

    key: str
    name: str
    metric: str
    description: str = ""
    query: str = ""
    visualization: HealthMetricVisualization = HealthMetricVisualization.TIMELINE
    unit: str = ""
    precision: int | None = None
    min_value: float | None = None
    max_value: float | None = None
    thresholds: dict[str, float] = field(default_factory=dict)


@dataclass
class NetworkInterface:
    """Read-only network inventory observed and authored by one Colca node."""

    name: str
    interface_type: NetworkInterfaceType
    addresses: list[str] = field(default_factory=list)
    mac_address: str = ""
    observed_at: int = 0


@dataclass
class Node(Payload):
    """A Colca node, authored by the node it describes.

    ``root_system_element_id`` is the element the node is bound to, the root
    of its subtree; the root node has none. colcad sets it once the node learns
    its position. The other fields come from the deployment or an operator.
    """

    id: str
    name: str
    root_system_element_id: str | None = None
    display_name: str = ""
    description: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)
    health_metrics: list[HealthMetricDeclaration] = field(default_factory=list)
    network_interfaces: list[NetworkInterface] = field(default_factory=list)


@dataclass
class ServiceDetails(Payload):
    """Observed service registration authored by the service identity."""

    id: str
    name: str
    service_type: ServiceType
    colca_node_id: str
    display_name: str = ""
    description: str = ""
    system_element_id: str | None = None
    hierarchy: list[str] = field(default_factory=list)
    is_active: bool = True
    metadata: dict[str, Any] = field(default_factory=dict)
    architecture_metadata: dict[str, Any] = field(default_factory=dict)
    health_metrics: list[HealthMetricDeclaration] = field(default_factory=list)


@dataclass
class AuditEvent(Payload):
    """A non-state security event that travels only toward ancestor nodes."""

    event_id: str
    source: AuditSource
    action: AuditAction
    outcome: AuditOutcome
    actor_kind: ActorKind
    occurred_at: int
    operation: str | None = None
    actor_id: str | None = None
    actor_label: str | None = None
    entity_type: str | None = None
    entity_id: str | None = None
    correlation_id: str | None = None
    reason_code: str | None = None
    changed_fields: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


def _sorted_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(items, key=lambda item: str(item.get("id", "")))


@dataclass
class SealedSecretEnvelope:
    """Provider configuration encrypted for one notifications service key."""

    version: int
    algorithm: str
    key_id: str
    ciphertext: str


@dataclass
class NotificationChannelConfig:
    """One channel in the node-scoped configuration snapshot.

    ``public_config`` may contain presentation and rate-limit settings only.
    Provider endpoints and credentials belong exclusively in ``sealed_secret``.
    """

    id: str
    name: str
    kind: str
    public_config: dict[str, Any]
    sealed_secret: SealedSecretEnvelope | None
    rate_limit_per_minute: int = 60
    enabled: bool = True


@dataclass
class AlarmNotificationConfigSnapshot(Payload):
    id: str
    schema_version: int
    issued_at: float
    target_node_id: str
    revision_id: str
    alarms: list[dict[str, Any]]
    channels: list[NotificationChannelConfig]
    recipients: list[dict[str, Any]]
    policies: list[dict[str, Any]]
    policy_targets: list[dict[str, Any]]
    active_silences: list[dict[str, Any]]

    def __init__(
        self,
        schema_version: int,
        generated_at: float | None = None,
        *,
        issued_at: float | None = None,
        alarms: list[dict[str, Any]],
        channels: list[NotificationChannelConfig],
        recipients: list[dict[str, Any]],
        policies: list[dict[str, Any]],
        policy_targets: list[dict[str, Any]],
        active_silences: list[dict[str, Any]] | None = None,
        id: str = "alarm-notification-config",
        target_node_id: str = "",
        revision_id: str | None = None,
    ):
        resolved_issued_at = issued_at if issued_at is not None else generated_at
        if resolved_issued_at is None:
            raise TypeError("issued_at is required")
        self.id = id
        self.schema_version = schema_version
        self.issued_at = resolved_issued_at
        self.target_node_id = target_node_id
        self.alarms = alarms
        self.channels = channels
        self.recipients = recipients
        self.policies = policies
        self.policy_targets = policy_targets
        self.active_silences = active_silences or []
        self.revision_id = revision_id or self.compute_revision_id()

    @classmethod
    def get_identifier(cls) -> str:
        return "_AlarmNotificationConfig"

    def compute_revision_id(self) -> str:
        data = {
            "id": self.id,
            "schema_version": self.schema_version,
            "target_node_id": self.target_node_id,
            "alarms": _sorted_items(self.alarms),
            "channels": _sorted_items(
                [asdict(channel) if is_dataclass(channel) else channel for channel in self.channels]
            ),
            "recipients": _sorted_items(self.recipients),
            "policies": _sorted_items(self.policies),
            "policy_targets": _sorted_items(self.policy_targets),
            "active_silences": _sorted_items(self.active_silences),
        }
        return hashlib.sha256(json.dumps(data, cls=CustomEncoder, sort_keys=True).encode()).hexdigest()

    @property
    def generated_at(self) -> float:
        """Source compatibility for the schema-version-1 Python publisher."""

        return self.issued_at


@dataclass
class NotificationConfigStatus(Payload):
    id: str
    config_id: str
    revision_id: str
    target_node_id: str
    status: str
    applied_at: float
    reason_code: str | None = None
    message: str | None = None


@dataclass
class NotificationChannelOutcome:
    channel_id: str
    channel_kind: str
    status: str
    sent: bool
    attempt: int = 0
    policy_id: str | None = None
    target_id: str | None = None
    recipient_id: str | None = None
    provider_message_id: str | None = None
    error_code: str | None = None


@dataclass
class AlarmNotificationSummary:
    status: str
    requested: bool
    channels: list[NotificationChannelOutcome] = field(default_factory=list)


@dataclass
class AlarmStateChange(Payload):
    event_id: str
    alarm_id: str
    from_status: str | None
    to_status: str
    at: float
    signal_value: Any | None
    metadata_json: dict[str, Any] = field(default_factory=dict)
    revision: int = 1
    notification: AlarmNotificationSummary = field(
        default_factory=lambda: AlarmNotificationSummary(
            status="not_requested",
            requested=False,
        )
    )
    # Sanitized notification settings resolved for this alarm at publish time
    # (severity, labels, covering policies → channels/recipients), so downstream
    # MQTT consumers are self-contained. Never carries channel secrets.
    notification_settings: dict[str, Any] | None = None


@dataclass
class NotificationDispatched(Payload):
    idempotency_key: str
    policy_id: str | None
    channel_id: str
    recipient_id: str
    alarm_event_id: str | None
    status: str
    at: float
    attempt: int
    latency_ms: int | None
    error: str | None = None
    metadata_json: dict[str, Any] = field(default_factory=dict)
    channel_kind: str | None = None
    provider: str | None = None
    provider_message_id: str | None = None
    retryable: bool = False
    terminal: bool = True


@dataclass
class AlarmState(Payload):
    """The alarm that stands right now: one record per alarm definition.

    State, not an event. A new record replaces the one before it, and an empty
    payload retires the path — that tombstone is "the alarm has gone". Nothing
    else says so: there is no ``normal`` status, because what is not in the
    key-value view is not standing. The transitions themselves stay events, on
    the ``alarms`` stream as ``_AlarmStateChange``.

    The record sits at the alarm's own element path,
    ``{root}/v1/_AlarmState/{node}/{element-path}/{alarm-name}``, so read
    grants and zones reach it like any signal. ``_Alarm`` stays reserved for
    the definition, which has another writer.

    Recipients are deliberately absent: another writer owns them, they are
    personal data that does not belong in the retained view, and a policy
    change must never rewrite an alarm's state. Who was reached and when is in
    ``_NotificationDispatched``. No title or message either: the name comes
    from the alarm's ``_SystemElement``, and the topic path is the link.
    """

    alarm_id: ULID
    status: AlarmStatus
    severity: AlarmSeverity
    #: Unix seconds this status has held since.
    since: float
    signal_id: ULID
    #: Why it stands. ``threshold``, ``no_data`` and ``stream_gap`` are the
    #: known values, but the vocabulary stays open so a derived diagnosis can
    #: name its own reason.
    reason: str
    #: The measurement that brought it here, with the rule's operator and
    #: threshold, so a reader can render "82.4 > 80" without the definition.
    value: Any | None = None
    op: str | None = None
    threshold: float | None = None
    #: The `_AlarmStateChange` this state came out of.
    event_id: str | None = None
    #: ``sub`` of the person, as the node witnessed it (the command's
    #: actor_id), never self-asserted.
    acknowledged_by: str | None = None
    acknowledged_at: float | None = None
    note: str | None = None
    silenced_by: str | None = None
    #: Unix seconds.
    silenced_until: float | None = None


def derive_annotation_id(
    annotation_type_id: str,
    source: str,
    time_start: float,
    signal_ids: Iterable[str],
) -> str:
    """Deterministic ``Annotation.annotation_id``.

    ULID-encodes the first 16 bytes of
    ``SHA-256(f"{annotation_type_id}|{source}|{time_start:.6f}|{','.join(sorted(signal_ids))}")``.
    Create, update and delete of one annotation are appends with the same id,
    so a re-run producer overwrites its own record and consumers keep the last
    write per id.

    The signal set is part of the identity, so the same annotation type at the
    same instant on two machines gives two ids. The set is sorted first, and
    no signals contribute an empty string.
    """
    signal_set = ",".join(sorted(signal_ids))
    digest = hashlib.sha256(f"{annotation_type_id}|{source}|{time_start:.6f}|{signal_set}".encode()).digest()
    return str(ulid.from_bytes(digest[:16]))


@dataclass
class Annotation(Payload):
    """A time-based annotation.

    Lives on the append-only ``annotations`` stream and is never kept in KV or
    retained: a part-cycle producer writes about a million a year per machine.
    ``annotation_id`` comes from ``derive_annotation_id``. A delete is a record
    with ``deleted=True``, so it replicates and replays like any other change.
    """

    annotation_id: ULID
    #: A ULID in practice, but not constrained on the wire yet: the golden
    #: vectors in `vectors/annotation_id.json` use readable type ids.
    annotation_type_id: str
    time_start: float
    time_end: float | None = None
    value: Any | None = None
    signal_ids: list[ULID] = field(default_factory=list)
    #: Producing identity for audit, e.g. ``"dataops/<producer-name>"``.
    source: str = ""
    deleted: bool = False
    revision: int = 1


@dataclass
class DataTag(Payload):
    """One entry of a connector's catalogue; the catalogue is published as a whole.

    ``id`` is a ULID the connector mints at discovery and keeps across
    rediscovery by matching ``source``. A signal's binding
    (``Signal.data_tag``) names this id, so it must outlive the discovery.

    ``source`` is the tag's full address within the source, such as an OPC UA
    browse path or a register name. ``(connector, source)`` is the natural key.
    """

    id: ULID
    name: str
    source: str
    is_writable: bool
    is_readable: bool
    data_type: str | None = None
    #: True when the tag no longer exists at the source. It stays in the
    #: catalogue so a signal bound to it stays bound.
    is_stale: bool = False
    meta: dict[str, Any] = field(default_factory=dict)


@dataclass
class Result:
    value: Any
    timestamp: float = field(default_factory=lambda: datetime.datetime.now(datetime.UTC).timestamp())
    error: str | None = None

    def to_metric(self, signal_id: str) -> Metric:
        return Metric(value=self.value, signal_id=signal_id, timestamp=self.timestamp, error=self.error)


@dataclass
class DataTags(Payload):
    data_tags: list[DataTag]
    connector: str

    @property
    def version(self) -> str:
        sorted_tags = sorted(self.data_tags, key=lambda x: x.id)
        tags_dict = [tag.__dict__ for tag in sorted_tags]
        return hashlib.md5(
            json.dumps(tags_dict, cls=CustomEncoder, sort_keys=True).encode(), usedforsecurity=False
        ).hexdigest()

    @classmethod
    def decode(cls, json_str: str, timestamp: int) -> "DataTags":
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
class AnnotationType(Payload):
    """A global annotation definition projected at every descendant node."""

    id: ULID
    name: str
    data_type: str
    i18n_name: str = ""
    description: str = ""
    min_value: float | None = None
    max_value: float | None = None
    unit: str | None = None
    create_option_on_input_new: bool = False
    options: list[dict[str, Any]] = field(default_factory=list)
    #: Keyed by metadata type, the same map elements and signals carry.
    metadata: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def decode(cls, json_str: str, timestamp: int) -> "AnnotationType":
        data = json.loads(json_str)
        return cls(**data)


@dataclass
class MetadataType(Payload):
    """A global metadata definition projected at every descendant node."""

    id: ULID
    name: str
    data_type: str
    i18n_name: str = ""
    description: str = ""
    is_mandatory: bool = False
    allowed_content_type_keys: list[str] = field(default_factory=list)

    @classmethod
    def decode(cls, json_str: str, timestamp: int) -> "MetadataType":
        data = json.loads(json_str)
        return cls(**data)


@dataclass
class SemanticTag(Payload):
    """A global semantic-type definition projected at every descendant node."""

    id: ULID
    name: str
    i18n_name: str = ""
    description: str = ""
    applies_to: list[str] = field(default_factory=list)
    icon: str = ""
    quantity_kind: str | None = None
    data_type: str | None = None

    @classmethod
    def decode(cls, json_str: str, timestamp: int) -> "SemanticTag":
        data = json.loads(json_str)
        return cls(**data)


@dataclass
class Group(Payload):
    """A group of people and the grants its members hold.

    Topic: ``colca/v1/_Group/{authoring-node}/{id}``. A definition, the same at
    every node that authorizes people. A group is not an identity: it has no
    key and never connects. A token names its bearer's groups and the node
    unions their grants, so membership lives in the identity provider.
    """

    id: str
    name: str
    #: Grant strings in the uns grammar (``read:<element>/#``,
    #: ``cmd:<element>/#:classes``, ``admin:#``). Validated at authoring time by
    #: the reconciler and again by the node that applies the definition.
    grants: list[str] = field(default_factory=list)
    description: str = ""

    @classmethod
    def decode(cls, json_str: str, timestamp: int) -> "Group":
        data = json.loads(json_str)
        return cls(**data)


@dataclass
class PersonalAccessToken(Payload):
    """Hash-only personal access token record replicated to child nodes.

    The plaintext token is never stored. Nodes compare the SHA-256 digest
    locally, then rebuild the identity and privilege ceiling captured when the
    owner created the token.
    """

    id: ULID
    hashed_secret: str
    owner_sub: str
    owner_email: str
    scopes: list[str] = field(default_factory=list)
    roles: list[str] = field(default_factory=list)
    grants: list[str] = field(default_factory=list)
    namespace_read_permissions: list[str] = field(default_factory=list)
    namespace_write_permissions: list[str] = field(default_factory=list)
    expires_at: str | None = None

    @classmethod
    def decode(cls, json_str: str, timestamp: int) -> "PersonalAccessToken":
        return cls(**json.loads(json_str))


@dataclass
class DataModel(Payload):
    """A data-model definition: the compiled shape a system element can claim
    to implement.

    Topic: ``colca/v1/_DataModel/{authoring-node}/{id}``. As a definition it
    descends the tree and is applied at every node below its author. ``slots``
    is the flattened slot list, with ``extends`` already resolved.
    """

    #: The definition's identity: its path on the wire, and what a system
    #: element references to implement this model.
    id: ULID
    name: str
    version: str = "1.0"
    description: str = ""
    extends: list[str] = field(default_factory=list)
    slots: list[dict[str, Any]] = field(default_factory=list)

    @classmethod
    def decode(cls, json_str: str, timestamp: int) -> "DataModel":
        data = json.loads(json_str)
        return cls(**data)


@dataclass
class ExternalSystem(Payload):
    """A non-secret global definition for an external integration system."""

    id: ULID
    key: str
    name: str
    system_type: str
    description: str = ""
    properties: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def decode(cls, json_str: str, timestamp: int) -> "ExternalSystem":
        return cls(**json.loads(json_str))


@dataclass
class ExternalReference(Payload):
    """An upward reference from a Colca object to an external-system row."""

    id: ULID
    source_entity: str
    source_object_id: str
    relationship_type: str
    external_system_id: ULID
    external_table: str
    external_row_id: str
    external_column: str = ""
    description: str = ""

    @classmethod
    def decode(cls, json_str: str, timestamp: int) -> "ExternalReference":
        return cls(**json.loads(json_str))


@dataclass
class SystemElement(Payload):
    """A position in the plant, and therefore in the namespace.

    Topic: ``colca/v1/_SystemElement/{node-id}/{path…}``. Elements nest;
    nodes and machines get their address by binding to one; signals are leaves.
    The topic is the position, so the payload does not repeat it.

    The parent is named by id, not path: a topic is rewritten at every hop, a
    payload is not. The node writes the record for a `_CmdConfigure`; a retired
    element is a retained empty payload.
    """

    id: ULID
    name: str
    description: str = ""
    #: ULID of the enclosing element; None for a root.
    parent_id: ULID | None = None
    implements: list[str] = field(default_factory=list)  # DataModel names this SE fulfils
    external_asset_id: str | None = None
    external_asset_id_type: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    #: ULID of the `_SemanticTag` definition saying what this entity is; None
    #: means unclassified.
    semantic_type_id: ULID | None = None
    created_at: str | None = None
    updated_at: str | None = None

    @classmethod
    def decode(cls, json_str: str, timestamp: int) -> "SystemElement":
        data = json.loads(json_str)
        return cls(**data)


@dataclass
class Signal(Payload):
    """A signal, authored by the node.

    Topic: ``colca/v1/_Signal/{node-id}/{path…}``; the topic is the position, so
    the payload does not repeat it.

    ``data_tag`` binds the signal to the one data tag it reads from, by id. The
    connector is reached through the tag, and rebinding leaves the signal's id,
    and so its metric history, unchanged. The node writes the record for a
    `_CmdConfigure`; a retired signal is a retained empty payload.
    """

    id: ULID
    name: str
    description: str = ""
    #: ULID of the SystemElement that owns this signal.
    system_element_id: ULID | None = None
    #: ULID of the DataTag this signal reads from.
    data_tag: ULID | None = None
    #: The connector publishes metrics for this signal.
    is_published: bool = False
    #: The read side historises it (consumed by the historian bridge).
    is_logged: bool = False
    #: Upload eligibility for new samples; queued samples retain their decision.
    replication_policy: ReplicationPolicy = ReplicationPolicy.REPLICATE_TO_PARENTS
    data_type: DataType | None = None
    index_type: IndexType | None = None
    unit: str | None = None
    precision: int | None = None
    min_value: float | None = None
    max_value: float | None = None
    config: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)
    has_contract: bool = False
    #: ULID of the `_SemanticTag` definition saying what this entity is; None
    #: means unclassified.
    semantic_type_id: ULID | None = None
    created_at: str | None = None
    updated_at: str | None = None

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
        if "replication_policy" in data:
            data["replication_policy"] = ReplicationPolicy(data["replication_policy"])
        if data.get("data_type") is not None:
            # SignalDataType, because a _Signal may be json and franzmq's
            # DataType has no such member.
            data["data_type"] = SignalDataType(data["data_type"])
        if data.get("index_type") is not None:
            data["index_type"] = IndexType(data["index_type"])
        return cls(**data)


@dataclass
class Constant(Payload):
    """A typed, retained configuration value positioned in the namespace.

    Topic: ``colca/v1/_Constant/{node-id}/{path…}``. A Constant is not a Signal:
    it has no acquisition binding or metric topic, and its current value is the
    value retained in this record.
    """

    id: ULID
    name: str
    data_type: ConstantDataType
    #: May be ``null`` or left out, so the schema does not require it.
    value: Any = None
    description: str = ""
    system_element_id: ULID | None = None
    unit: str | None = None
    precision: int | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    #: ULID of the `_SemanticTag` definition saying what this entity is; None
    #: means unclassified.
    semantic_type_id: ULID | None = None
    created_at: str | None = None
    updated_at: str | None = None

    @property
    def __dict__(self):
        data = super().__dict__.copy()
        data["data_type"] = str(self.data_type)
        return data

    @classmethod
    def decode(cls, json_str: str, timestamp: int) -> "Constant":
        data = json.loads(json_str)
        data["data_type"] = ConstantDataType(data["data_type"])
        return cls(**data)


@dataclass
class Resource(Payload):
    """A file-backed entity attached to a system element.

    Topic: ``colca/v1/_Resource/{node-id}/{element-path…}/{resource-id}``.
    Written by the node in response to a ``_CmdConfigure``; a retracted
    resource is a retained empty payload (tombstone).

    ``sha256`` and ``size_bytes`` are the file pointer. The bytes never travel
    in this payload — they move through the blob store — so a node holding this
    record can be certain which blob it needs and can verify it on arrival.
    """

    id: ULID
    system_element_id: ULID
    filename: str
    #: Optional for a writer, so the payload is no stricter than the data it
    #: carries: a missing value is the empty string.
    content_type: str = ""
    sha256: str = ""
    display_name: str = ""
    description: str = ""
    resource_type: str = "other"
    size_bytes: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)
    created_at: str | None = None
    updated_at: str | None = None

    @classmethod
    def decode(cls, json_str: str, timestamp: int) -> "Resource":
        data = json.loads(json_str)
        return cls(**data)


@dataclass
class EditOperation(Payload):
    """Bounded, node-local success receipt for atomic Edit replay.

    The receipt is committed after its state records in the same entity batch.
    Its own stream offset therefore reconstructs the exact consecutive offsets
    of ``topics`` after a node restart.
    """

    id: str
    digest: str
    message: str
    result: str
    topics: list[str]

    @classmethod
    def decode(cls, json_str: str, timestamp: int) -> "EditOperation":
        return cls(**json.loads(json_str))


# ---------------------------------------------------------------------------
# Colca command classes.
# The class decides the routing: a Cmd subclass lands on the commands stream
# under the hazard class its name carries (_CmdParam -> param, _CmdOperate ->
# operate, _CmdMaintain -> maintain, _CmdConfigure -> configure, _CmdAdmin ->
# admin). The door requires correlation_id and expires_at (unix milliseconds);
# created_at is not required.
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
    ``signal/autobind``. The target node executes it and writes the resulting
    `_Signal` records itself; a person may command but not author state.

    It has its own hazard class so that permission to bind a signal does not
    also allow maintenance commands to a machine."""


@dataclass
class CmdEdit(Cmd):
    """One versioned, idempotent Edit mutation intent.

    ``intent`` stays an open object at the schema-bundle boundary because its
    discriminator and operation-specific fields are validated by the target
    node's Edit executor. The command never carries raw UNS topics or a
    caller-composed list of state records.
    """

    operation_id: str = ""
    intent: dict[str, Any] = field(default_factory=dict)
    # Decimal strings keep uint64 stream offsets exact in JavaScript clients.
    expected_versions: dict[str, str] = field(default_factory=dict)


@dataclass
class CmdAdmin(Cmd):
    """Node administration: provisioning (enroll/revoke), restart, firmware.

    Executed by the target node, not a machine; verb-specific fields (entry,
    ulid) ride in the open payload."""
