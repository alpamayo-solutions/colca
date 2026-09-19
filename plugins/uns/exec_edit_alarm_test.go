package uns

import (
	"fmt"
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

// Alarm configuration is authorized at the signal the alarm is about; the
// config record's own path belongs to no element.
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

// An operator's act is a _CmdOperate the alarm's evaluator applies, not an
// edit: the executor knows no alarm_acknowledgement intent, and operate is
// not configure for an alarm edit.
func TestEditAlarmIsConfigurationOnly(t *testing.T) {
	_, exec, _ := twoLines(t)
	operator := CommandContext{Actor: &Entry{
		ULID: "kc-sub-operator", Kind: KindHuman,
		Grants: []string{"cmd:el-line1/#:operate"},
	}}

	silence := alarmIntent(t, "op-silence", "alarm_acknowledgement", "silence", "sig-1", map[string]any{
		"snapshot": alarmSnapshot("n-edge1"),
	})
	if code, message, _, writes := exec.ExecuteWithWrites(operator, "_CmdEdit", "apply", silence); code != 422 || len(writes) != 0 {
		t.Fatalf("an alarm_acknowledgement edit = %d %q writes=%d, want it refused as unknown", code, message, len(writes))
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

// enrolledScope is the scope of a node enrolled at own: the element index for
// the node and below, the taught ancestry for its own element and above. own is
// not seeded as a _SystemElement, since its record lives at the parent, so only
// Reaches can resolve it, as in production.
type enrolledScope struct {
	scopeOf
	own string
}

func (s enrolledScope) Reaches(elementID string) bool { return elementID != "" && elementID == s.own }

func applyIntent(t *testing.T, op string, snapshot map[string]any, extra map[string]any) []byte {
	t.Helper()
	intent := map[string]any{"type": "notification_config", "action": "apply", "snapshot": snapshot}
	for k, v := range extra {
		intent[k] = v
	}
	return editBody(t, op, map[string]uint64{}, intent)
}

// A whole-node apply is authorized on the node's own element with configure:
// a grant on that element or a realm-wide grant covers it, a grant on a child
// element does not, and that refusal writes nothing.
func TestEditNotificationConfigApplyIsAuthorizedOnTheNodesOwnElement(t *testing.T) {
	f, exec, _ := twoLines(t)
	exec.SetScope(enrolledScope{scopeOf: scopeOf{f}, own: "el-edge1"})
	snapshot := alarmSnapshot("n-edge1")

	cases := []struct {
		name  string
		actor CommandContext
		want  int
	}{
		{"the node's own element", scopedTo("el-edge1"), 200},
		{"realm-wide", CommandContext{Actor: &Entry{
			ULID: "kc-admin", Kind: KindHuman, Grants: []string{"cmd:#:configure"},
		}}, 200},
		{"a child element of the node", scopedTo("el-line1"), 409},
		{"operate on the node's own element", CommandContext{Actor: &Entry{
			ULID: "kc-operator", Kind: KindHuman, Grants: []string{"cmd:el-edge1/#:operate"},
		}}, 409},
	}
	for i, tc := range cases {
		before := f.offset
		code, message, result, writes := exec.ExecuteWithWrites(
			tc.actor, "_CmdEdit", "apply", applyIntent(t, fmt.Sprintf("op-apply-%d", i), snapshot, nil),
		)
		if code != tc.want {
			t.Fatalf("%s: apply = %d %q %q, want %d", tc.name, code, message, result, tc.want)
		}
		if tc.want == 200 {
			if len(writes) != 1 || !strings.Contains(writes[0].Topic, "/_AlarmNotificationConfig/") {
				t.Fatalf("%s: writes=%v, want the one alarm configuration record", tc.name, writes)
			}
			continue
		}
		if result != "conflict" || message != "entity_not_found: colca-node:n-edge1" {
			t.Fatalf("%s: refusal = %q %q, want the not-found shape naming the node", tc.name, message, result)
		}
		if len(writes) != 0 || f.offset != before {
			t.Fatalf("%s: a refused apply wrote: writes=%d offset %d → %d", tc.name, len(writes), before, f.offset)
		}
	}
}

// The node's own element resolves through the ancestry, so a node whose
// scope does not reach it (the root, or a node that has not learned its
// position) needs a realm-wide grant.
func TestEditNotificationConfigApplyFailsClosedWhenTheScopeDoesNotReachTheElement(t *testing.T) {
	f, exec, _ := twoLines(t) // scopeOf: Reaches is false for everything
	before := f.offset
	code, message, _, writes := exec.ExecuteWithWrites(
		scopedTo("el-edge1"), "_CmdEdit", "apply", applyIntent(t, "op-apply-root", alarmSnapshot("n-edge1"), nil),
	)
	if code != 409 || message != "entity_not_found: colca-node:n-edge1" || len(writes) != 0 || f.offset != before {
		t.Fatalf("apply with a grant the scope does not reach = %d %q writes=%d, want a refusal with nothing written",
			code, message, len(writes))
	}
	full := CommandContext{Actor: &Entry{ULID: "kc-admin", Kind: KindHuman, Grants: []string{"cmd:#:configure"}}}
	if code, message, _, writes := exec.ExecuteWithWrites(
		full, "_CmdEdit", "apply", applyIntent(t, "op-apply-root-wide", alarmSnapshot("n-edge1"), nil),
	); code != 200 || len(writes) != 1 {
		t.Fatalf("realm-wide apply at a root node = %d %q writes=%d", code, message, len(writes))
	}
}

// The record follows the same identity rules as the alarm intents, and the
// snapshot's target_node_id is the only statement of which node it is for.
func TestEditNotificationConfigApplyKeepsTheConfigIdentityRules(t *testing.T) {
	f, exec, _ := twoLines(t)
	exec.SetScope(enrolledScope{scopeOf: scopeOf{f}, own: "el-edge1"})
	anna := scopedTo("el-edge1")
	before := f.offset

	refused := map[string][]byte{
		"another node's config": applyIntent(t, "op-foreign", alarmSnapshot("n-somewhere-else"), nil),
		"a foreign config id": applyIntent(t, "op-id",
			map[string]any{"id": "not-the-one", "target_node_id": "n-edge1"}, nil),
		"an entity on the intent": applyIntent(t, "op-entity", alarmSnapshot("n-edge1"),
			map[string]any{"entity": map[string]any{"kind": "colca-node", "id": "n-edge1"}}),
		"an action other than apply": editBody(t, "op-action", map[string]uint64{}, map[string]any{
			"type": "notification_config", "action": "update", "snapshot": alarmSnapshot("n-edge1"),
		}),
		"no snapshot": editBody(t, "op-none", map[string]uint64{}, map[string]any{
			"type": "notification_config", "action": "apply",
		}),
	}
	for name, payload := range refused {
		code, message, _, writes := exec.ExecuteWithWrites(anna, "_CmdEdit", "apply", payload)
		if code != 422 || len(writes) != 0 {
			t.Fatalf("%s = %d %q writes=%d, want 422 and nothing written", name, code, message, len(writes))
		}
	}
	if f.offset != before {
		t.Fatalf("a refused apply wrote: offset %d → %d", before, f.offset)
	}
	code, message, _, writes := exec.ExecuteWithWrites(
		anna, "_CmdEdit", "apply", applyIntent(t, "op-ok", alarmSnapshot("n-edge1"), nil),
	)
	if code != 200 || len(writes) != 1 ||
		writes[0].Topic != "colca/v1/_AlarmNotificationConfig/n-edge1/_colca/alarm-notification-config/"+alarmConfigID {
		t.Fatalf("apply = %d %q writes=%v, want the one record at the reserved path", code, message, writes)
	}
}
