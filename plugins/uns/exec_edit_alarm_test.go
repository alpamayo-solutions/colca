package uns

import (
	"strings"
	"testing"
)

func alarmSnapshot(node string) map[string]any {
	return map[string]any{
		"id": alarmConfigID, "target_node_id": node, "revision_id": "rev-1",
		"alarms": []any{},
	}
}

func alarmIntent(t *testing.T, op, kind, action, signal string, extra map[string]any) []byte {
	t.Helper()
	intent := map[string]any{
		"type": kind, "action": action,
		"entity": map[string]any{"kind": "signal", "id": signal},
	}
	for k, v := range extra {
		intent[k] = v
	}
	return editBody(t, op, map[string]uint64{}, intent)
}

// Alarm configuration is authorized at the SIGNAL the alarm is about — the
// config record's own path is reserved and owned by no element, so checking
// there would demand a realm-wide grant of everyone.
func TestEditAlarmIsAuthorizedAtItsSignal(t *testing.T) {
	f, exec, _ := twoLines(t)
	anna := scopedTo("el-line1")
	before := f.offset

	outside := alarmIntent(t, "op-alarm-out", "alarm", "update", "sig-2", map[string]any{
		"snapshot": alarmSnapshot("n-edge1"),
	})
	code, message, result, writes := exec.ExecuteWithWrites(anna, "_CmdEdit", "apply", outside)
	if code != 409 || result != "conflict" || len(writes) != 0 {
		t.Fatalf("alarm on a signal outside the grant = %d %q %q writes=%d, want a refusal",
			code, message, result, len(writes))
	}
	if !strings.HasPrefix(message, "entity_not_found:") {
		t.Fatalf("refusal = %q, want the not-found shape", message)
	}
	if f.offset != before {
		t.Fatalf("a refused alarm plan wrote: offset %d → %d", before, f.offset)
	}

	inside := alarmIntent(t, "op-alarm-in", "alarm", "update", "sig-1", map[string]any{
		"snapshot": alarmSnapshot("n-edge1"),
	})
	if code, message, _, writes = exec.ExecuteWithWrites(anna, "_CmdEdit", "apply", inside); code != 200 || len(writes) != 1 {
		t.Fatalf("alarm on her own signal = %d %q writes=%d", code, message, len(writes))
	}
	if !strings.Contains(writes[0].Topic, "/_AlarmNotificationConfig/") {
		t.Fatalf("wrote %s, want the alarm configuration record", writes[0].Topic)
	}
}

// An operator holds `operate` and no configure anywhere: they may acknowledge,
// and they may not configure. That split is the whole point of the class.
func TestEditAlarmAcknowledgementTakesTheOperateClass(t *testing.T) {
	_, exec, _ := twoLines(t)
	operator := CommandContext{Actor: &Entry{
		ULID: "kc-sub-operator", Kind: KindHuman,
		Grants: []string{"cmd:el-line1/#:operate"},
	}}

	silence := alarmIntent(t, "op-silence", "alarm_acknowledgement", "silence", "sig-1", map[string]any{
		"snapshot": alarmSnapshot("n-edge1"),
	})
	if code, message, _, writes := exec.ExecuteWithWrites(operator, "_CmdEdit", "apply", silence); code != 200 || len(writes) != 1 {
		t.Fatalf("operator silencing = %d %q writes=%d, want it applied", code, message, len(writes))
	}

	configure := alarmIntent(t, "op-ack-cfg", "alarm", "update", "sig-1", map[string]any{
		"snapshot": alarmSnapshot("n-edge1"),
	})
	code, message, _, writes := exec.ExecuteWithWrites(operator, "_CmdEdit", "apply", configure)
	if code != 409 || len(writes) != 0 {
		t.Fatalf("operator configuring an alarm = %d %q writes=%d, want a refusal — operate is not configure",
			code, message, len(writes))
	}
}

// The identity rules the configure verb enforces hold here too, so one node's
// alarm configuration cannot be authored in two different shapes.
func TestEditAlarmRefusesAConfigForAnotherNode(t *testing.T) {
	f, exec, _ := twoLines(t)
	full := CommandContext{Actor: &Entry{ULID: "kc-admin", Kind: KindHuman, Grants: []string{"cmd:#:configure"}}}
	before := f.offset

	foreign := alarmIntent(t, "op-alarm-foreign", "alarm", "update", "sig-1", map[string]any{
		"snapshot": alarmSnapshot("n-somewhere-else"),
	})
	code, message, _, writes := exec.ExecuteWithWrites(full, "_CmdEdit", "apply", foreign)
	if code != 422 || len(writes) != 0 {
		t.Fatalf("alarm config for another node = %d %q writes=%d, want it refused", code, message, len(writes))
	}
	if !strings.Contains(message, "target_node_id") {
		t.Fatalf("refusal = %q, want it to name the mismatched field", message)
	}
	if f.offset != before {
		t.Fatalf("a refused alarm config wrote: offset %d → %d", before, f.offset)
	}

	wrongID := alarmIntent(t, "op-alarm-id", "alarm", "update", "sig-1", map[string]any{
		"snapshot": map[string]any{"id": "not-the-one", "target_node_id": "n-edge1"},
	})
	if code, message, _, _ = exec.ExecuteWithWrites(full, "_CmdEdit", "apply", wrongID); code != 422 {
		t.Fatalf("alarm config with a foreign id = %d %q, want it refused", code, message)
	}
}
