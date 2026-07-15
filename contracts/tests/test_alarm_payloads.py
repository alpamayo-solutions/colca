from franzmq import Topic

from colca_data_contracts import (
    AlarmNotificationConfigSnapshot,
    AlarmStateChange,
    NotificationDispatched,
    ServiceType,
)


def test_alarm_config_snapshot_topic_is_stable_contract_name():
    topic = Topic(payload_type=AlarmNotificationConfigSnapshot)

    assert topic.payload_type is AlarmNotificationConfigSnapshot
    assert topic.context == ()
    assert str(topic).endswith(AlarmNotificationConfigSnapshot.get_identifier())


def test_alarm_config_snapshot_revision_is_stable_for_sorted_items():
    first = AlarmNotificationConfigSnapshot(
        schema_version=1,
        generated_at=1710000000.0,
        alarms=[{"id": "b", "name": "B"}, {"id": "a", "name": "A"}],
        channels=[],
        recipients=[],
        policies=[],
        policy_targets=[],
    )
    second = AlarmNotificationConfigSnapshot(
        schema_version=1,
        generated_at=1710001234.0,
        alarms=[{"id": "a", "name": "A"}, {"id": "b", "name": "B"}],
        channels=[],
        recipients=[],
        policies=[],
        policy_targets=[],
    )

    assert first.revision_id == second.revision_id
    assert first.__dict__["revision_id"] == first.revision_id


def test_alarm_config_snapshot_revision_changes_when_content_changes():
    first = AlarmNotificationConfigSnapshot(
        schema_version=1,
        generated_at=1710000000.0,
        alarms=[{"id": "a", "name": "A"}],
        channels=[],
        recipients=[],
        policies=[],
        policy_targets=[],
    )
    second = AlarmNotificationConfigSnapshot(
        schema_version=1,
        generated_at=1710000000.0,
        alarms=[{"id": "a", "name": "Changed"}],
        channels=[],
        recipients=[],
        policies=[],
        policy_targets=[],
    )

    assert first.revision_id != second.revision_id


def test_alarm_config_snapshot_decodes_serialized_revision_id():
    snapshot = AlarmNotificationConfigSnapshot(
        schema_version=1,
        generated_at=1710000000.0,
        alarms=[{"id": "a", "name": "A"}],
        channels=[],
        recipients=[],
        policies=[],
        policy_targets=[],
    )

    decoded = AlarmNotificationConfigSnapshot(**snapshot.__dict__)

    assert decoded.revision_id == snapshot.revision_id


def test_alarm_events_have_idempotency_keys():
    state_change = AlarmStateChange(
        event_id="evt-1",
        alarm_id="alarm-1",
        from_status="pending",
        to_status="firing",
        at=1710000000.0,
        signal_value=True,
        metadata_json={},
    )
    dispatch = NotificationDispatched(
        idempotency_key="dispatch-1",
        policy_id="policy-1",
        channel_id="channel-1",
        recipient_id="recipient-1",
        alarm_event_id="evt-1",
        status="success",
        at=1710000001.0,
        attempt=1,
        latency_ms=12,
        error=None,
        metadata_json={},
    )

    assert state_change.__dict__["event_id"] == "evt-1"
    assert dispatch.__dict__["idempotency_key"] == "dispatch-1"


def test_service_types_include_notifications():
    assert ServiceType.NOTIFICATIONS == "notifications"
