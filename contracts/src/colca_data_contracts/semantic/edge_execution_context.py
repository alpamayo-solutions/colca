"""EdgeExecutionContext — semantic contract for shopfloor execution tracking.

IMPORTANT — this is a SEMANTIC DATA CONTRACT, not a topic type.

How it works:
    1. A Signal is created with data_type='json' and
       implements_contract='EdgeExecutionContext'.
    2. Events are published as STANDARD Metric payloads on that signal's
       regular _Metric topic (e.g. colca/v1/_Metric/.../execution-context).
    3. The Metric.value field contains the EdgeExecutionContext JSON (this schema).
    4. Consumers discover relevant signals via the Hub API:
       GET /api/v1/signals/?implements_contract=EdgeExecutionContext
       then subscribe to each signal's .topic (a standard _Metric topic).

What NOT to do:
    - Do NOT invent a custom topic like colca/v1/_EdgeExecutionContext/... —
      that is not a recognized Colca topic type and will break franzmq's
      topic parser.
    - Do NOT publish raw EdgeExecutionContext JSON outside a Metric envelope.
    - Do NOT hardcode topic patterns for execution events. Always discover
      signals dynamically via the Hub API.
"""

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any, ClassVar, Optional


class ExecutionEventType(StrEnum):
    EXECUTION_START = "execution_start"
    EXECUTION_END = "execution_end"
    DISRUPTION_START = "disruption_start"
    DISRUPTION_END = "disruption_end"
    WORK_STEP_FINALIZED = "work_step_finalized"
    CLUSTER_SCRAPPED = "cluster_scrapped"


class ExecutionEventSource(StrEnum):
    PLC_AUTO = "plc_auto"
    OPERATOR = "operator"


class ExecutionSegmentKind(StrEnum):
    OPERATOR = "operator"
    MACHINE = "machine"


class ExecutionSubjectType(StrEnum):
    OPERATOR = "operator"
    EQUIPMENT = "equipment"


_SESSION_EVENT_TYPES = {
    ExecutionEventType.EXECUTION_START,
    ExecutionEventType.EXECUTION_END,
}
_LIFECYCLE_EVENT_TYPES = {
    ExecutionEventType.WORK_STEP_FINALIZED,
    ExecutionEventType.CLUSTER_SCRAPPED,
}


def _is_exact_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _is_namespaced(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    namespace, separator, identifier = value.partition(":")
    return bool(separator and namespace and identifier)


@dataclass
class EdgeExecutionContext:
    """Semantic data contract for execution context events.

    This defines the schema for Metric.value_json — NOT an MQTT payload type.

    Publishing:
        signal = find_signal(implements_contract='EdgeExecutionContext')
        metric = Metric(value=context.to_json(), signal_id=signal.id, timestamp=now)
        mqtt.publish(signal.topic, metric.encode())   # topic is colca/v1/_Metric/...

    Consuming:
        signals = hub_api.get_signals(implements_contract='EdgeExecutionContext')
        for signal in signals:
            mqtt.subscribe(signal.topic)  # standard _Metric topic
        # on_message: context = EdgeExecutionContext.from_json(metric.value)
    """

    expected_data_type: ClassVar[str] = "json"

    event_type: ExecutionEventType
    asset_id: int
    lot_nr: Optional[int]
    timestamp: datetime
    source: ExecutionEventSource
    event_id: Optional[str] = None
    order_nr: Optional[int] = None  # required on execution_start, disruption_start
    step_id: Optional[int] = None  # required on execution_start
    operator_id: Optional[str] = None
    context: Optional[dict[str, Any]] = None
    segment_kind: Optional[ExecutionSegmentKind] = None
    session_id: Optional[str] = None
    subject_type: Optional[ExecutionSubjectType] = None
    subject_id: Optional[str] = None
    subject_label: Optional[str] = None

    def validate(self):
        event_type = ExecutionEventType(self.event_type)
        ExecutionEventSource(self.source)

        if not isinstance(self.event_id, str) or not self.event_id:
            raise ValueError("event_id is required")
        self._validate_integer("asset_id", self.asset_id)
        if not isinstance(self.timestamp, datetime) or self.timestamp.utcoffset() is None:
            raise ValueError("timestamp must be timezone-aware")
        if self.operator_id is not None and not _is_namespaced(self.operator_id):
            raise ValueError("operator_id must be namespaced")
        if self.subject_id is not None and not _is_namespaced(self.subject_id):
            raise ValueError("subject_id must be namespaced")

        if event_type in _SESSION_EVENT_TYPES:
            self._validate_execution_session()
        elif event_type in _LIFECYCLE_EVENT_TYPES:
            self._validate_lifecycle_event()
        elif event_type == ExecutionEventType.DISRUPTION_START:
            self._validate_disruption_start()
        elif event_type == ExecutionEventType.DISRUPTION_END:
            self._validate_optional_integer("order_nr", self.order_nr)
            self._validate_optional_integer("step_id", self.step_id)

    @staticmethod
    def _validate_integer(field_name: str, value: Any):
        if not _is_exact_int(value):
            raise ValueError(f"{field_name} must be an integer")

    @classmethod
    def _validate_optional_integer(cls, field_name: str, value: Any):
        if value is not None:
            cls._validate_integer(field_name, value)

    def _validate_execution_session(self):
        self._validate_integer("order_nr", self.order_nr)
        self._validate_integer("step_id", self.step_id)
        if self.segment_kind is None:
            raise ValueError("segment_kind is required for execution events")
        if not isinstance(self.session_id, str) or not self.session_id:
            raise ValueError("session_id is required for execution events")
        if self.subject_type is None:
            raise ValueError("subject_type is required for execution events")
        if self.subject_id is None:
            raise ValueError("subject_id is required for execution events")

        segment_kind = ExecutionSegmentKind(self.segment_kind)
        subject_type = ExecutionSubjectType(self.subject_type)
        expected_subject_type = {
            ExecutionSegmentKind.OPERATOR: ExecutionSubjectType.OPERATOR,
            ExecutionSegmentKind.MACHINE: ExecutionSubjectType.EQUIPMENT,
        }[segment_kind]
        if subject_type != expected_subject_type:
            raise ValueError("segment_kind and subject_type are incoherent")

    def _validate_lifecycle_event(self):
        self._validate_integer("order_nr", self.order_nr)
        self._validate_integer("step_id", self.step_id)
        for field_name in (
            "segment_kind",
            "session_id",
            "subject_type",
            "subject_id",
            "subject_label",
        ):
            if getattr(self, field_name) is not None:
                raise ValueError(f"{field_name} is not allowed for lifecycle events")

    def _validate_disruption_start(self):
        if self.order_nr is None:
            raise ValueError("order_nr is required for disruption_start")
        if self.step_id is None:
            raise ValueError("step_id is required for disruption_start")
        self._validate_integer("order_nr", self.order_nr)
        self._validate_integer("step_id", self.step_id)

    @classmethod
    def from_json(cls, data: dict) -> "EdgeExecutionContext":
        """Parse from Metric value_json dict."""
        instance = cls(
            event_type=ExecutionEventType(data["event_type"]),
            asset_id=data["asset_id"],
            lot_nr=data.get("lot_nr"),
            timestamp=datetime.fromisoformat(data["timestamp"]),
            source=ExecutionEventSource(data["source"]),
            event_id=data.get("event_id"),
            order_nr=data.get("order_nr"),
            step_id=data.get("step_id"),
            operator_id=data.get("operator_id"),
            context=data.get("context"),
            segment_kind=ExecutionSegmentKind(data["segment_kind"])
            if data.get("segment_kind") is not None
            else None,
            session_id=data.get("session_id"),
            subject_type=ExecutionSubjectType(data["subject_type"])
            if data.get("subject_type") is not None
            else None,
            subject_id=data.get("subject_id"),
            subject_label=data.get("subject_label"),
        )
        instance.validate()
        return instance

    def to_json(self) -> dict:
        """Serialize to Metric value_json dict."""
        result = {
            "event_type": self.event_type.value,
            "asset_id": self.asset_id,
            "timestamp": self.timestamp.isoformat(),
            "source": self.source.value,
        }
        if self.lot_nr is not None:
            result["lot_nr"] = self.lot_nr
        if self.event_id is not None:
            result["event_id"] = self.event_id
        if self.order_nr is not None:
            result["order_nr"] = self.order_nr
        if self.step_id is not None:
            result["step_id"] = self.step_id
        if self.operator_id is not None:
            result["operator_id"] = self.operator_id
        if self.context is not None:
            result["context"] = self.context
        if self.segment_kind is not None:
            result["segment_kind"] = ExecutionSegmentKind(self.segment_kind).value
        if self.session_id is not None:
            result["session_id"] = self.session_id
        if self.subject_type is not None:
            result["subject_type"] = ExecutionSubjectType(self.subject_type).value
        if self.subject_id is not None:
            result["subject_id"] = self.subject_id
        if self.subject_label is not None:
            result["subject_label"] = self.subject_label
        return result
