package uns

import (
	"strings"
	"testing"
)

// asHuman is the person the executor tests act as. _CmdEdit refuses any other
// actor and _CmdConfigure ignores the context, so one context serves all tests.
var asHuman = CommandContext{Actor: &Entry{
	ULID: "01HTESTHUMAN00000000000000", Kind: KindHuman, Grants: []string{"cmd:#:configure"},
}}

// A _CmdEdit is a person's intent. Any other actor is refused before the
// executor looks at the operation, and no replay receipt is written, so the
// same operation id from a person later runs fresh.
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

	// The same operation from a person runs for the first time, not as a
	// replay.
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

// People command through _CmdEdit only: a configure grant does not open
// _CmdConfigure to a person, since that executor takes no principal.
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

// A _CmdEdit topic names the owning node, not an element, so the door admits
// it on the class alone and the executor checks positions.
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
