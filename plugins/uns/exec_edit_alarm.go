package uns

import (
	"encoding/json"
	"fmt"
)

// alarmConfigID is the one id an `_AlarmNotificationConfig` may carry: the
// record is a node's single alarm configuration, not one row per alarm.
const alarmConfigID = "alarm-notification-config"

// composeAlarm composes the `_AlarmNotificationConfig` record an `alarm` or
// `alarm_acknowledgement` intent writes (node-side command authorization
// design §G).
//
// Alarm writes were the second family still reaching the node as
// `_CmdConfigure` under the API's own identity, which is why a person's alarm
// command was gated only by preflight on the far side of the door. The record
// itself is unchanged — same contract, same reserved path, same
// one-config-per-node identity rules `checkCommandEntityIdentity` applies —
// so a node's alarm state does not care which door authored it. What changes
// is that the person is now checked here, at the signal the alarm is about.
//
// Where it is checked matters, and it is NOT this record's own path: the
// config sits under `_colca/alarm-notification-config`, a reserved position
// no element owns, so authorizing there would demand a realm-wide grant of
// everyone. The annotation intent has exactly this shape and resolves it the
// same way — the position that governs is the entity the command is ABOUT.
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

	// Every alarm command that REACHES a node carries the configuration it
	// wants: a bare acknowledge is settled at the api and never travels
	// (`alarm_operations`), because current alarm status belongs to the
	// evaluator's own table at the node that authors the transitions, not to
	// KV state. What arrives under `alarm_acknowledgement` is a silence or an
	// unsilence, which do change the config — and are an operator's act, which
	// is what the `operate` class carries.
	if len(intent.Snapshot) == 0 {
		return 422, fmt.Sprintf("%s: snapshot is required", intent.Type), "invalid", nil
	}

	var config struct {
		ID           string `json:"id"`
		TargetNodeID string `json:"target_node_id"`
	}
	if err := json.Unmarshal(intent.Snapshot, &config); err != nil {
		return 422, "alarm: snapshot is unreadable: " + err.Error(), "invalid", nil
	}
	// The same two identity rules the configure verb enforces, so the two
	// doors cannot write two different shapes of the same record.
	if config.ID != "" && config.ID != alarmConfigID {
		return 422, fmt.Sprintf(
			"alarm: snapshot id must be %s, got %q", alarmConfigID, config.ID,
		), "invalid", nil
	}
	if config.TargetNodeID == "" {
		return 422, "alarm: snapshot target_node_id is required", "invalid", nil
	}
	if config.TargetNodeID != w.store.NodeID() {
		return 422, fmt.Sprintf(
			"alarm: snapshot target_node_id %q must equal local node %q",
			config.TargetNodeID, w.store.NodeID(),
		), "invalid", nil
	}

	topic := "colca/v1/_AlarmNotificationConfig/" + w.store.NodeID() +
		"/_colca/alarm-notification-config/" + alarmConfigID
	return 200, "upserted 1", "ok", []StateRecord{{Topic: topic, Payload: intent.Snapshot}}
}

// alarmPositions is the alarm family's write-set: the signal the alarm is
// about, never the config record's own reserved path.
//
// `operate` carries the acknowledgement family. Acknowledging or silencing an
// alarm is an operator's act, not a configuration change — the same split the
// annotation intent already makes, and the reason an operator can act on an
// alarm without holding configure anywhere.
func (w *EditExec) alarmPositions(
	intent editIntent, entities map[string]editSnapshot,
) []editTouched {
	touched := touchedEntity(entities, "signal", intent.Entity.ID)
	touched.operate = intent.Type == "alarm_acknowledgement"
	return []editTouched{touched}
}
