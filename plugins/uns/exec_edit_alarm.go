package uns

import (
	"encoding/json"
	"fmt"
)

// alarmConfigID is the one id an `_AlarmNotificationConfig` may carry: the
// record is a node's single alarm configuration, not one row per alarm.
const alarmConfigID = "alarm-notification-config"

// composeAlarm composes the _AlarmNotificationConfig record an alarm or
// alarm_acknowledgement intent writes. The record lives at a reserved path no
// element owns, so the person is authorized at the signal the alarm is about,
// the same way annotations are.
func (w *EditExec) composeAlarm(intent editIntent) (int, string, string, []StateRecord) {
	configuring := intent.Type == "alarm"
	switch {
	case configuring:
		switch intent.Action {
		case "create", "update", "delete":
		default:
			return 422, fmt.Sprintf(
				"alarm: action must be create, update or delete, got %q", intent.Action,
			), "invalid", nil
		}
	default:
		switch intent.Action {
		case "acknowledge", "silence", "unsilence":
		default:
			return 422, fmt.Sprintf(
				"alarm_acknowledgement: action must be acknowledge, silence or unsilence, got %q",
				intent.Action,
			), "invalid", nil
		}
	}
	if intent.Entity.Kind != "signal" || intent.Entity.ID == "" {
		return 422, "alarm: entity must name the signal the alarm is about", "invalid", nil
	}

	// A bare acknowledge is settled at the api and never reaches the node.
	// What arrives under alarm_acknowledgement is a silence or unsilence,
	// which changes the config and is an operator's act (operate).
	return w.alarmConfigRecord(intent.Type, intent.Snapshot)
}

// composeNotificationConfig composes the record a notification_config intent
// writes: the node's whole alarm configuration at once. It is the same record
// the alarm intents write, but it concerns the whole node, so it is authorized
// at the node's root (see notificationConfigPositions). The snapshot's
// target_node_id says which node, checked against this one.
func (w *EditExec) composeNotificationConfig(intent editIntent) (int, string, string, []StateRecord) {
	if intent.Action != "apply" {
		return 422, fmt.Sprintf(
			"notification_config: action must be apply, got %q", intent.Action,
		), "invalid", nil
	}
	if intent.Entity.Kind != "" || intent.Entity.ID != "" {
		return 422, "notification_config: a whole-node apply names no entity; the snapshot's target_node_id says which node",
			"invalid", nil
	}
	return w.alarmConfigRecord(intent.Type, intent.Snapshot)
}

// alarmConfigRecord validates an _AlarmNotificationConfig snapshot and composes
// the record at the node's reserved path. Every intent that writes this record
// goes through here, with the same identity rules as the configure verb: the
// one config id, targeting this node.
func (w *EditExec) alarmConfigRecord(intentType string, snapshot json.RawMessage) (int, string, string, []StateRecord) {
	if len(snapshot) == 0 {
		return 422, fmt.Sprintf("%s: snapshot is required", intentType), "invalid", nil
	}
	var config struct {
		ID           string `json:"id"`
		TargetNodeID string `json:"target_node_id"`
	}
	if err := json.Unmarshal(snapshot, &config); err != nil {
		return 422, intentType + ": snapshot is unreadable: " + err.Error(), "invalid", nil
	}
	if config.ID != "" && config.ID != alarmConfigID {
		return 422, fmt.Sprintf(
			"%s: snapshot id must be %s, got %q", intentType, alarmConfigID, config.ID,
		), "invalid", nil
	}
	if config.TargetNodeID == "" {
		return 422, intentType + ": snapshot target_node_id is required", "invalid", nil
	}
	if config.TargetNodeID != w.store.NodeID() {
		return 422, fmt.Sprintf(
			"%s: snapshot target_node_id %q must equal local node %q",
			intentType, config.TargetNodeID, w.store.NodeID(),
		), "invalid", nil
	}

	topic := Prefix() + "_AlarmNotificationConfig/" + w.store.NodeID() +
		"/_colca/alarm-notification-config/" + alarmConfigID
	return 200, "upserted 1", "ok", []StateRecord{{Topic: topic, Payload: snapshot}}
}

// alarmPositions is the alarm family's write-set: the signal the alarm is
// about. Acknowledging or silencing is an operator's act, so operate covers it.
func (w *EditExec) alarmPositions(
	intent editIntent, entities map[string]editSnapshot,
) []editTouched {
	touched := touchedEntity(entities, "signal", intent.Entity.ID)
	touched.operate = intent.Type == "alarm_acknowledgement"
	return []editTouched{touched}
}

// notificationConfigPositions is the whole-node apply's write-set: the node's
// root, the empty path. The node's own element is not in its element index
// (the parent authored it), but Scope.Reaches knows the ancestry the parent
// taught, so zoneOf resolves a grant on the node's element or an ancestor to
// "#". cmd:#:configure covers it too; a grant on a child element does not. At
// the root node only a realm-wide grant covers it. A refusal reads
// "entity_not_found: colca-node:<node>".
func (w *EditExec) notificationConfigPositions() []editTouched {
	return []editTouched{{path: "", key: entityVersionKey("colca-node", w.store.NodeID())}}
}
