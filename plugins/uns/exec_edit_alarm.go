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
	return w.alarmConfigRecord(intent.Type, intent.Snapshot)
}

// composeNotificationConfig composes the record a `notification_config`
// intent writes: the node's WHOLE alarm configuration, applied at once
// (`action: "apply"`; the editor's "Apply notifications" door).
//
// It is the same `_AlarmNotificationConfig` record the `alarm` family writes
// — one contract, one reserved path, one set of identity rules — but it is a
// different act. An `alarm` command is ABOUT one signal and is authorized
// there; this one is about nothing narrower than the node, names no signal,
// and so has no position below the node to be checked at.
//
// A node is a participant bound to a position like everything else
// (architecture principle 6): the element its parent enrolled it at. In the
// node's own frame that position is its root, the empty path — see
// notificationConfigPositions for how a grant naming that element covers it.
//
// The intent carries no entity. The snapshot's `target_node_id` is the one
// statement of which node this is, checked against the local node exactly as
// for the `alarm` family; a second copy of that fact on the intent would be a
// second writer of it.
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

// alarmConfigRecord validates one `_AlarmNotificationConfig` snapshot and
// composes the record at the node's reserved path. Every door that writes
// this record composes it here, so a node's alarm configuration has one shape
// no matter which intent authored it — the same two identity rules the
// configure verb enforces (`checkCommandEntityIdentity`): the id is the one
// config id, and the target is this node.
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

// notificationConfigPositions is the whole-node apply's write-set: ONE
// position, the node itself.
//
// In the node's own frame the node is the root, so its position is the empty
// path — the position every element here sits under. Which grants cover it
// is decided by the doors' own resolution (`zoneOf`), and this is the subtle
// part: the node's own element is NOT in its element index. Its
// `_SystemElement` record was authored by the PARENT at enrollment and
// entities never descend, so `PathOf` cannot answer it here. What answers it
// is the ancestry the parent taught on the downlink, which ends at the
// element the node binds to (`Ancestry` is "from the root down to and
// including the element the node itself binds to"); `Scope.Reaches` consults
// exactly that, and `zoneOf` resolves a reaching element to "#". So:
//
//   - `cmd:<the node's own element>/#:configure` reaches → zone "#" → covers "";
//   - `cmd:<an ancestor's element>/#:configure` reaches → the same;
//   - `cmd:#:configure` → "#" → covers "";
//   - `cmd:<a child element>/#:configure` resolves to that child's path, which
//     does not cover "" (coverPath: the root is above it) → refused;
//   - at the ROOT node the ancestry is empty and the node is bound to nothing
//     above itself, so only a realm-wide grant covers it — as it should.
//
// The refusal names the node, keyed like every other colca-node entity, so
// a refused apply reads as "entity_not_found: colca-node:<node>" and says
// no more than any other refusal does.
func (w *EditExec) notificationConfigPositions() []editTouched {
	return []editTouched{{path: "", key: entityVersionKey("colca-node", w.store.NodeID())}}
}
