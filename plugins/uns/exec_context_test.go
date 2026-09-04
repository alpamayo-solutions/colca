package uns

import (
	"strings"
	"testing"
)

// asHuman is the acting person the executor tests command as. A _CmdEdit
// is refused for any other kind of actor (node-side command authorization
// design §3A), and _CmdConfigure ignores the context, so one human context
// serves every executor test.
var asHuman = CommandContext{Actor: &Entry{
	ULID: "01HTESTHUMAN00000000000000", Kind: KindHuman, Grants: []string{"cmd:#:configure"},
}}

// A _CmdEdit is a person's intent. Any other actor — the admin door
// (no identity), the api under its own local identity, an external service —
// is refused before the executor looks at the operation, so no replay receipt
// is written for a command nobody was authorized to run: the same operation
// id, presented later by a human, executes fresh instead of replaying a
// refusal or, worse, an outcome.
func TestEditRefusesEveryNonHumanActor(t *testing.T) {
	f := newStore("n-edge1")
	parentVersion := seedEditEntity(t, f, "_SystemElement", "line1", map[string]any{
		"id": "el-line1", "name": "Line 1",
	})
	exec := NewEditExec(f, nil)
	create := editBody(t, "op-create", map[string]uint64{
		"system-element:el-line1": parentVersion,
	}, map[string]any{
		"type":      "create",
		"entity":    map[string]any{"kind": "constant", "id": "const-speed"},
		"parent_id": "el-line1",
		"attributes": map[string]any{
			"name": "Target speed", "data_type": "int64", "value": 18000,
		},
	})

	for name, ctx := range map[string]CommandContext{
		"admin door":           {},
		"unplaced local (api)": {Actor: &Entry{ULID: "svc-api", Kind: KindLocal}},
		"placed local":         {Actor: &Entry{ULID: "svc-x", Kind: KindLocal, Element: "el-line1"}},
		"external service":     {Actor: &Entry{ULID: "m1", Kind: KindExternal, Element: "el-line1"}},
		"child node":           {Actor: &Entry{ULID: "n-child", Kind: KindNode, Element: "el-line1"}},
	} {
		code, message, result, writes := exec.ExecuteWithWrites(ctx, "_CmdEdit", "apply", create)
		if code != 403 || result != "denied" || len(writes) != 0 {
			t.Fatalf("%s: _CmdEdit = %d %q %q writes=%d, want 403 denied and nothing written", name, code, message, result, len(writes))
		}
		if !strings.Contains(message, "human") {
			t.Fatalf("%s: refusal must say what was missing: %q", name, message)
		}
	}
	if f.offset != parentVersion {
		t.Fatalf("a refused actor wrote state: store offset %d, want %d (the seeded parent only)", f.offset, parentVersion)
	}

	// The same operation, now from a person, is a first execution — not a
	// replay of a receipt a refusal must never have written.
	code, message, _, writes := exec.ExecuteWithWrites(asHuman, "_CmdEdit", "apply", create)
	if code != 200 || len(writes) != 1 {
		t.Fatalf("human after refusals: %d %q writes=%d, want a fresh 200 with one write", code, message, len(writes))
	}
}

func TestIsHumanIsNilSafe(t *testing.T) {
	var none *Entry
	if none.IsHuman() || (&Entry{Kind: KindLocal}).IsHuman() {
		t.Fatal("no identity and a service are not people")
	}
	if !asHuman.Actor.IsHuman() {
		t.Fatal("a token entry is a person")
	}
}

// Humans command through _CmdEdit only (§3F): a configure grant does not
// open _CmdConfigure to a person, because that executor takes no principal.
// Every other kind keeps its contracts — placement and grants decide there.
func TestHumansCommandThroughTheEditOnly(t *testing.T) {
	human := &Entry{ULID: "kc-sub-anna", Kind: KindHuman, Grants: []string{"cmd:#:configure"}}
	if human.MayPublishContract("_CmdConfigure") {
		t.Fatal("a person may not publish _CmdConfigure, whatever they hold")
	}
	for _, contract := range []string{"_CmdEdit", "_CmdParam", "_CmdAdmin"} {
		if !human.MayPublishContract(contract) {
			t.Fatalf("a person lost %s at the door predicate", contract)
		}
	}
	for _, e := range []*Entry{
		{ULID: "svc-api", Kind: KindLocal}, {ULID: "m1", Kind: KindExternal, Element: "el"}, {ULID: "n-c", Kind: KindNode, Element: "el"},
	} {
		if !e.MayPublishContract("_CmdConfigure") {
			t.Fatalf("%s lost _CmdConfigure at the door predicate", e.Kind)
		}
	}
	var none *Entry
	if none.MayPublishContract("_CmdEdit") {
		t.Fatal("no identity publishes nothing")
	}
}

// A _Cmdeditor's topic path is the owning node's route, not an element,
// so the door admits it on the CLASS alone and leaves position to the
// executor's plan. Every other command keeps the prefix comparison.
func TestTheDoorAdmitsAEditCommandOnTheClassAlone(t *testing.T) {
	scoped := &Entry{ULID: "kc-sub-anna", Kind: KindHuman, Grants: []string{"cmd:el-line1/#:configure"}}
	for _, topic := range []string{
		"colca/v1/_CmdEdit/n-edge1/apply",
		"colca/v1/_CmdEdit/n-edge1/site1/edge2/apply",
	} {
		if !Authorize(nil, scoped, ActCmd, topic) {
			t.Fatalf("a person holding configure somewhere was refused %s at the door", topic)
		}
	}
	if Authorize(nil, scoped, ActCmd, "colca/v1/_CmdParam/n-edge1/line2/set-speed") {
		t.Fatal("a routed _CmdParam lost its prefix check")
	}
	for name, e := range map[string]*Entry{
		"param only": {ULID: "p", Kind: KindHuman, Grants: []string{"cmd:#:param"}},
		"read only":  {ULID: "r", Kind: KindHuman, Grants: []string{"read:#"}},
		"nobody":     nil,
	} {
		if Authorize(nil, e, ActCmd, "colca/v1/_CmdEdit/n-edge1/apply") {
			t.Fatalf("%s was admitted to _CmdEdit without the configure class", name)
		}
	}
}
