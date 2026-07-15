"""ExecutionEventCorrection — semantic contract for hindsight event corrections.

IMPORTANT — this is a SEMANTIC DATA CONTRACT, not a topic type.

How it works:
    1. A Signal is created with data_type='json' and
       implements_contract='ExecutionEventCorrection'.
    2. Correction commands are published as STANDARD Metric payloads on that
       signal's regular _Metric topic.
    3. The Metric.value field contains the ExecutionEventCorrection JSON.
    4. Consumers discover relevant signals via the Hub API:
       GET /api/v1/signals/?implements_contract=ExecutionEventCorrection
       then subscribe to each signal's .topic.

What NOT to do:
    - Do NOT mutate or overwrite the original EdgeExecutionContext event.
    - Do NOT invent a custom topic like colca/v1/_ExecutionEventCorrection/...
    - Do NOT publish raw correction JSON outside a Metric envelope.
"""

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any, ClassVar, Optional

from colca_data_contracts.semantic.edge_execution_context import ExecutionEventType


def _is_exact_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _is_namespaced(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    namespace, separator, identifier = value.partition(":")
    return bool(separator and namespace and identifier)


class CorrectionAction(StrEnum):
    CANCEL_EVENT = "cancel_event"
    INSERT_MISSING_EVENT = "insert_missing_event"
    REPLACE_INTERVAL = "replace_interval"
    CHANGE_METADATA = "change_metadata"


class CorrectionType(StrEnum):
    REPLACE_OPERATOR_INTERVAL = "replace_operator_interval"
    REPLACE_MACHINE_INTERVAL = "replace_machine_interval"
    INSERT_SESSION = "insert_session"
    EDIT_CLOSURE_TIMESTAMP = "edit_closure_timestamp"
    UNDO_FINALIZATION = "undo_finalization"


@dataclass
class CorrectionReplacementEvent:
    """Replacement event definition embedded inside a correction command."""

    event_type: ExecutionEventType
    timestamp: datetime

    @classmethod
    def from_json(cls, data: dict) -> "CorrectionReplacementEvent":
        return cls(
            event_type=ExecutionEventType(data["event_type"]),
            timestamp=datetime.fromisoformat(data["timestamp"]),
        )

    def to_json(self) -> dict:
        return {
            "event_type": self.event_type.value,
            "timestamp": self.timestamp.isoformat(),
        }


@dataclass
class ExecutionEventCorrection:
    """Semantic contract for admin-requested correction commands.

    This is an append-only command/audit artifact. It references existing
    canonical execution events by ID and optionally carries replacement events
    for ERP-side reconciliation.
    """

    expected_data_type: ClassVar[str] = "json"

    correction_id: Optional[str] = None
    action: Optional[CorrectionAction] = None
    target_event_ids: Optional[list[str]] = None
    requested_by: Optional[str] = None
    requested_at: Optional[datetime] = None
    reason_code: Optional[str] = None
    reason_text: Optional[str] = None
    target_event_type: Optional[ExecutionEventType] = None
    asset_id: Optional[str] = None
    order_nr: Optional[str] = None
    step_id: Optional[str] = None
    operator_id: Optional[str] = None
    shift_code: Optional[str] = None
    replacement_events: Optional[list[CorrectionReplacementEvent]] = None
    correction_type: Optional[CorrectionType] = None
    oit_id: Optional[int] = None
    asf_id: Optional[int] = None
    target_session_id: Optional[int] = None
    target_closure_id: Optional[int] = None
    target_event_id: Optional[str] = None
    reason: Optional[str] = None
    performed_by: Optional[str] = None
    performed_at: Optional[datetime] = None
    before_json: Optional[dict[str, Any]] = None
    after_json: Optional[dict[str, Any]] = None

    def validate(self):
        if self.correction_type is not None:
            correction_type = CorrectionType(self.correction_type)
            if not isinstance(self.correction_id, str) or not self.correction_id:
                raise ValueError("correction_id is required")
            self._validate_integer("oit_id", self.oit_id)
            self._validate_integer("asf_id", self.asf_id)
            self._validate_optional_integer("target_session_id", self.target_session_id)
            self._validate_optional_integer("target_closure_id", self.target_closure_id)
            if not _is_namespaced(self.performed_by):
                raise ValueError("performed_by must be namespaced")
            if not isinstance(self.performed_at, datetime) or self.performed_at.utcoffset() is None:
                raise ValueError("performed_at must be timezone-aware")
            self._validate_snapshot("before_json", self.before_json)
            self._validate_snapshot("after_json", self.after_json)
            if correction_type in (
                CorrectionType.REPLACE_OPERATOR_INTERVAL,
                CorrectionType.REPLACE_MACHINE_INTERVAL,
            ) and self.target_session_id is None:
                raise ValueError("target_session_id is required")
            if correction_type in (
                CorrectionType.EDIT_CLOSURE_TIMESTAMP,
                CorrectionType.UNDO_FINALIZATION,
            ) and self.target_closure_id is None:
                raise ValueError("target_closure_id is required")
            return
        if self.action is None:
            raise ValueError("action is required")
        if not self.correction_id:
            raise ValueError("correction_id is required")
        if not self.target_event_ids:
            raise ValueError("target_event_ids must contain at least one event ID")
        if not self.requested_by:
            raise ValueError("requested_by is required")
        if self.action in (
            CorrectionAction.INSERT_MISSING_EVENT,
            CorrectionAction.REPLACE_INTERVAL,
        ):
            if not self.replacement_events:
                raise ValueError(
                    "replacement_events are required for insert_missing_event and replace_interval"
                )
        if self.action == CorrectionAction.CANCEL_EVENT and self.replacement_events:
            raise ValueError("replacement_events are not allowed for cancel_event")

    @staticmethod
    def _validate_integer(field_name: str, value: Any):
        if not _is_exact_int(value):
            raise ValueError(f"{field_name} must be an integer")

    @classmethod
    def _validate_optional_integer(cls, field_name: str, value: Any):
        if value is not None:
            cls._validate_integer(field_name, value)

    @staticmethod
    def _validate_snapshot(field_name: str, value: Any):
        if value is not None and not isinstance(value, dict):
            raise ValueError(f"{field_name} must be an object or null")

    @classmethod
    def from_json(cls, data: dict) -> "ExecutionEventCorrection":
        if "correction_type" in data:
            instance = cls(
                correction_id=data.get("correction_id"),
                correction_type=CorrectionType(data["correction_type"]),
                oit_id=data.get("oit_id"),
                asf_id=data.get("asf_id"),
                target_session_id=data.get("target_session_id"),
                target_closure_id=data.get("target_closure_id"),
                target_event_id=data.get("target_event_id"),
                reason=data.get("reason"),
                performed_by=data.get("performed_by"),
                performed_at=datetime.fromisoformat(data["performed_at"])
                if data.get("performed_at")
                else None,
                before_json=data.get("before_json"),
                after_json=data.get("after_json"),
            )
            instance.validate()
            return instance

        replacement_events = data.get("replacement_events")
        instance = cls(
            correction_id=data["correction_id"],
            action=CorrectionAction(data["action"]),
            target_event_ids=list(data["target_event_ids"]),
            requested_by=data["requested_by"],
            requested_at=datetime.fromisoformat(data["requested_at"]),
            reason_code=data.get("reason_code"),
            reason_text=data.get("reason_text"),
            target_event_type=ExecutionEventType(data["target_event_type"])
            if data.get("target_event_type") is not None
            else None,
            asset_id=data.get("asset_id"),
            order_nr=data.get("order_nr"),
            step_id=data.get("step_id"),
            operator_id=data.get("operator_id"),
            shift_code=data.get("shift_code"),
            replacement_events=[
                CorrectionReplacementEvent.from_json(item)
                for item in replacement_events
            ]
            if replacement_events is not None
            else None,
        )
        instance.validate()
        return instance

    def to_json(self) -> dict:
        if self.correction_type is not None:
            result = {
                "correction_id": self.correction_id,
                "correction_type": CorrectionType(self.correction_type).value,
            }
            if self.oit_id is not None:
                result["oit_id"] = self.oit_id
            if self.asf_id is not None:
                result["asf_id"] = self.asf_id
            if self.target_session_id is not None:
                result["target_session_id"] = self.target_session_id
            if self.target_closure_id is not None:
                result["target_closure_id"] = self.target_closure_id
            if self.target_event_id is not None:
                result["target_event_id"] = self.target_event_id
            if self.reason is not None:
                result["reason"] = self.reason
            if self.performed_by is not None:
                result["performed_by"] = self.performed_by
            if self.performed_at is not None:
                result["performed_at"] = self.performed_at.isoformat()
            if self.before_json is not None:
                result["before_json"] = self.before_json
            if self.after_json is not None:
                result["after_json"] = self.after_json
            return result

        result = {
            "correction_id": self.correction_id,
            "action": self.action.value,
            "target_event_ids": self.target_event_ids,
            "requested_by": self.requested_by,
            "requested_at": self.requested_at.isoformat(),
        }
        if self.reason_code is not None:
            result["reason_code"] = self.reason_code
        if self.reason_text is not None:
            result["reason_text"] = self.reason_text
        if self.target_event_type is not None:
            result["target_event_type"] = self.target_event_type.value
        if self.asset_id is not None:
            result["asset_id"] = self.asset_id
        if self.order_nr is not None:
            result["order_nr"] = self.order_nr
        if self.step_id is not None:
            result["step_id"] = self.step_id
        if self.operator_id is not None:
            result["operator_id"] = self.operator_id
        if self.shift_code is not None:
            result["shift_code"] = self.shift_code
        if self.replacement_events is not None:
            result["replacement_events"] = [
                event.to_json() for event in self.replacement_events
            ]
        return result
