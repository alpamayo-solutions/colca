from franzmq import Topic

from colca_data_contracts import (
    AlarmNotificationSummary,
    AlarmNotificationConfigSnapshot,
    AlarmStateChange,
    NotificationChannelConfig,
    NotificationChannelOutcome,
    NotificationConfigStatus,
    NotificationDispatched,
    SealedSecretEnvelope,
    ServiceType,
)


def test_alarm_config_snapshot_topic_is_stable_contract_name():
    topic = Topic(payload_type=AlarmNotificationConfigSnapshot, node_id="n-edge1",
                  context=("alarms",))

    assert topic.payload_type is AlarmNotificationConfigSnapshot
    # The contract name sits at level 3, the publishing node's identity at 4.
    assert str(topic).split("/")[2] == AlarmNotificationConfigSnapshot.get_identifier()
    assert str(topic) == (
        f"colca/v1/{AlarmNotificationConfigSnapshot.get_identifier()}/n-edge1/alarms"
    )


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


def test_alarm_event_idempotency_keys_survive_wire_round_trip():
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

    decoded_change = AlarmStateChange.decode(state_change.encode(), timestamp=0)
    assert decoded_change.event_id == "evt-1"
    assert decoded_change.alarm_id == "alarm-1"

    decoded_dispatch = NotificationDispatched.decode(dispatch.encode(), timestamp=0)
    assert decoded_dispatch.idempotency_key == "dispatch-1"
    assert decoded_dispatch.alarm_event_id == "evt-1"


def test_alarm_config_v2_carries_only_a_sealed_channel_secret():
    snapshot = AlarmNotificationConfigSnapshot(
        schema_version=2,
        generated_at=1710000000.0,
        target_node_id="node-leaf",
        alarms=[],
        channels=[
            NotificationChannelConfig(
                id="channel-1",
                name="Operations SMTP",
                kind="smtp",
                public_config={"from_name": "Colca"},
                sealed_secret=SealedSecretEnvelope(
                    version=1,
                    algorithm="nacl-box-seal-x25519-xsalsa20-poly1305",
                    key_id="sha256:key-1",
                    ciphertext="base64-ciphertext",
                ),
            )
        ],
        recipients=[],
        policies=[],
        policy_targets=[],
    )

    encoded = snapshot.__dict__

    assert encoded["target_node_id"] == "node-leaf"
    assert encoded["revision_id"]
    assert "password" not in snapshot.encode()
    assert "webhook_url" not in snapshot.encode()
    assert "base64-ciphertext" in snapshot.encode()


def test_notification_config_status_round_trip_is_sanitized():
    status = NotificationConfigStatus(
        id="alarm-notification-config",
        config_id="alarm-notifications",
        revision_id="revision-1",
        target_node_id="node-leaf",
        status="applied",
        applied_at=1710000001.0,
    )

    decoded = NotificationConfigStatus.decode(status.encode(), timestamp=0)

    assert decoded.config_id == "alarm-notifications"
    assert decoded.id == "alarm-notification-config"
    assert decoded.revision_id == "revision-1"
    assert decoded.target_node_id == "node-leaf"
    assert decoded.status == "applied"


def test_alarm_event_revision_contains_per_channel_delivery_outcomes():
    state_change = AlarmStateChange(
        event_id="evt-1",
        alarm_id="alarm-1",
        from_status="pending",
        to_status="firing",
        at=1710000000.0,
        signal_value=91.5,
        revision=2,
        notification=AlarmNotificationSummary(
            status="partial",
            requested=True,
            channels=[
                NotificationChannelOutcome(
                    channel_id="smtp-1",
                    channel_kind="smtp",
                    status="sent",
                    sent=True,
                    attempt=1,
                    policy_id="policy-1",
                    target_id="target-1",
                    recipient_id="ops@example.test",
                    provider_message_id="provider-1",
                ),
                NotificationChannelOutcome(
                    channel_id="teams-missing",
                    channel_kind="teams_webhook",
                    status="not_configured",
                    sent=False,
                ),
            ],
        ),
    )

    decoded = AlarmStateChange.decode(state_change.encode(), timestamp=0)

    assert decoded.event_id == "evt-1"
    assert decoded.revision == 2
    assert decoded.notification["status"] == "partial"
    assert decoded.notification["channels"][0]["sent"] is True
    assert decoded.notification["channels"][0]["policy_id"] == "policy-1"
    assert decoded.notification["channels"][0]["target_id"] == "target-1"
    assert decoded.notification["channels"][1]["status"] == "not_configured"


def test_dispatch_round_trip_includes_provider_delivery_result():
    dispatch = NotificationDispatched(
        idempotency_key="delivery-1",
        policy_id="policy-1",
        channel_id="channel-1",
        recipient_id="recipient-1",
        alarm_event_id="event-1",
        status="uncertain",
        at=1710000000.0,
        attempt=2,
        latency_ms=250,
        channel_kind="teams_webhook",
        provider="teams_webhook",
        provider_message_id=None,
        retryable=True,
        terminal=False,
        error="timeout_after_write",
    )

    decoded = NotificationDispatched.decode(dispatch.encode(), timestamp=0)

    assert decoded.channel_kind == "teams_webhook"
    assert decoded.provider == "teams_webhook"
    assert decoded.retryable is True
    assert decoded.terminal is False


def test_service_types_include_notifications():
    assert ServiceType.NOTIFICATIONS == "notifications"
