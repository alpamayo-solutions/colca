package uns

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// scopeOf resolves a grant's element to its path from what the fake store
// holds — the executor's scope in production is the node's element index.
type scopeOf struct{ f *fakeStore }

func (s scopeOf) PathOf(elementID string) (string, bool) {
	for topic, payload := range s.f.records {
		if !strings.Contains(topic, "/_SystemElement/") || len(payload) == 0 {
			continue
		}
		var v struct {
			ID string `json:"id"`
		}
		if json.Unmarshal(payload, &v) == nil && v.ID == elementID {
			p, _ := Parse(topic)
			return p.Path, true
		}
	}
	return "", false
}
func (scopeOf) Reaches(string) bool { return false }

// scopedTo is a person whose configure grant names ONE element.
func scopedTo(element string) CommandContext {
	return CommandContext{Actor: &Entry{
		ULID: "kc-sub-scoped", Kind: KindHuman, Grants: []string{"cmd:" + element + "/#:configure"},
	}}
}

// twoLines seeds line1 (a signal under it) and line2 (a signal under it) and
// returns the store, the executor scoped over it, and the seeded versions.
func twoLines(t *testing.T) (*fakeStore, *EditExec, map[string]uint64) {
	t.Helper()
	f := newStore("n-edge1")
	versions := map[string]uint64{}
	versions["system-element:el-line1"] = seedEditEntity(t, f, "_SystemElement", "line1", map[string]any{"id": "el-line1", "name": "Line 1"})
	versions["system-element:el-line2"] = seedEditEntity(t, f, "_SystemElement", "line2", map[string]any{"id": "el-line2", "name": "Line 2"})
	versions["signal:sig-1"] = seedEditEntity(t, f, "_Signal", "line1/temp1", map[string]any{
		"id": "sig-1", "name": "Temp 1", "system_element_id": "el-line1", "data_type": "float",
	})
	versions["signal:sig-2"] = seedEditEntity(t, f, "_Signal", "line2/temp2", map[string]any{
		"id": "sig-2", "name": "Temp 2", "system_element_id": "el-line2", "data_type": "float",
	})
	exec := NewEditExec(f, nil)
	exec.SetScope(scopeOf{f})
	return f, exec, versions
}

func createUnder(t *testing.T, op, parent string, versions map[string]uint64) []byte {
	t.Helper()
	return editBody(t, op, map[string]uint64{"system-element:" + parent: versions["system-element:"+parent]}, map[string]any{
		"type": "create", "entity": map[string]any{"kind": "constant", "id": "const-" + op},
		"parent_id": parent, "attributes": map[string]any{"name": "Target", "data_type": "int64", "value": 1},
	})
}

// A person holding configure on line1 only: a plan that touches line2 is
// refused before any write, shaped exactly like a not-found on the entity
// they named — and the same operation under line1 is applied. Removing the
// authorizeTouched call turns every refusal here into a 200.
func TestEditRefusesAPlanOutsideThePersonsGrantWithZeroWrites(t *testing.T) {
	f, exec, versions := twoLines(t)
	anna := scopedTo("el-line1")
	before := f.offset

	code, message, result, writes := exec.ExecuteWithWrites(anna, "_CmdEdit", "apply", createUnder(t, "op-out", "el-line2", versions))
	if code != 409 || result != "conflict" || message != "entity_not_found: system-element:el-line2" || len(writes) != 0 {
		t.Fatalf("create outside the grant = %d %q %q writes=%d, want the not-found shape and nothing written", code, message, result, len(writes))
	}
	if f.offset != before {
		t.Fatalf("a refused plan wrote: offset %d → %d", before, f.offset)
	}

	code, message, _, writes = exec.ExecuteWithWrites(anna, "_CmdEdit", "apply", createUnder(t, "op-in", "el-line1", versions))
	if code != 200 || len(writes) != 1 {
		t.Fatalf("create inside the grant = %d %q writes=%d", code, message, len(writes))
	}
}

func TestEditUpdateAndDeleteAreAuthorizedOnTheEntityTouched(t *testing.T) {
	_, exec, versions := twoLines(t)
	anna := scopedTo("el-line1")
	update := func(op, signal string) []byte {
		return editBody(t, op, map[string]uint64{"signal:" + signal: versions["signal:"+signal]}, map[string]any{
			"type": "update", "entity": map[string]any{"kind": "signal", "id": signal},
			"attributes": map[string]any{"name": "Renamed"},
		})
	}
	if code, message, _, _ := exec.ExecuteWithWrites(anna, "_CmdEdit", "apply", update("op-u2", "sig-2")); code != 409 || message != "entity_not_found: signal:sig-2" {
		t.Fatalf("update outside = %d %q", code, message)
	}
	if code, message, _, _ := exec.ExecuteWithWrites(anna, "_CmdEdit", "apply", update("op-u1", "sig-1")); code != 200 {
		t.Fatalf("update inside = %d %q", code, message)
	}
	del := editBody(t, "op-d2", map[string]uint64{"signal:sig-2": versions["signal:sig-2"]}, map[string]any{
		"type": "delete", "entity": map[string]any{"kind": "signal", "id": "sig-2"},
	})
	if code, message, _, _ := exec.ExecuteWithWrites(anna, "_CmdEdit", "apply", del); code != 409 || message != "entity_not_found: signal:sig-2" {
		t.Fatalf("delete outside = %d %q", code, message)
	}
}

// A placement touches both ends: moving out of the grant names the target,
// moving in from outside names the entity being moved.
func TestEditPlacementIsAuthorizedAtBothEnds(t *testing.T) {
	_, exec, versions := twoLines(t)
	anna := scopedTo("el-line1")
	move := func(op, signal, target string) []byte {
		return editBody(t, op, map[string]uint64{
			"signal:" + signal: versions["signal:"+signal], "system-element:" + target: versions["system-element:"+target],
		}, map[string]any{
			"type": "placement", "entity": map[string]any{"kind": "signal", "id": signal}, "target_parent_id": target,
		})
	}
	if code, message, _, _ := exec.ExecuteWithWrites(anna, "_CmdEdit", "apply", move("op-out", "sig-1", "el-line2")); code != 409 || message != "entity_not_found: system-element:el-line2" {
		t.Fatalf("move out of the grant = %d %q", code, message)
	}
	if code, message, _, _ := exec.ExecuteWithWrites(anna, "_CmdEdit", "apply", move("op-in", "sig-2", "el-line1")); code != 409 || message != "entity_not_found: signal:sig-2" {
		t.Fatalf("move in from outside = %d %q", code, message)
	}
}

// An annotation touches every signal it names; naming none needs a
// realm-wide grant.
func TestEditAnnotationIsAuthorizedOnEverySignalNamed(t *testing.T) {
	_, exec, _ := twoLines(t)
	anna := scopedTo("el-line1")
	annotate := func(op string, signals []string) []byte {
		return editBody(t, op, map[string]uint64{}, map[string]any{
			"type": "annotation", "action": "create", "annotation_type_id": "at-1",
			"source": "user/anna", "time_start": 1710000000.0, "signal_ids": signals,
		})
	}
	if code, message, _, _ := exec.ExecuteWithWrites(anna, "_CmdEdit", "apply", annotate("op-a2", []string{"sig-1", "sig-2"})); code != 409 || message != "entity_not_found: signal:sig-2" {
		t.Fatalf("annotation naming a signal outside = %d %q", code, message)
	}
	if code, message, _, _ := exec.ExecuteWithWrites(anna, "_CmdEdit", "apply", annotate("op-a0", nil)); code != 409 {
		t.Fatalf("annotation naming no signal from a scoped person = %d %q, want refused", code, message)
	}
	if code, message, _, _ := exec.ExecuteWithWrites(anna, "_CmdEdit", "apply", annotate("op-a1", []string{"sig-1"})); code != 200 {
		t.Fatalf("annotation inside = %d %q", code, message)
	}
}

// Without a scope only a realm-wide grant resolves: an element-scoped grant
// covers nothing, never everything.
func TestEditWithoutAScopeFailsClosedOnScopedGrants(t *testing.T) {
	f, _, versions := twoLines(t)
	exec := NewEditExec(f, nil)
	if code, message, _, _ := exec.ExecuteWithWrites(scopedTo("el-line1"), "_CmdEdit", "apply", createUnder(t, "op-ns", "el-line1", versions)); code != 409 {
		t.Fatalf("scoped grant without a scope = %d %q, want refused", code, message)
	}
	if code, message, _, _ := exec.ExecuteWithWrites(asHuman, "_CmdEdit", "apply", createUnder(t, "op-all", "el-line1", versions)); code != 200 {
		t.Fatalf("realm-wide grant without a scope = %d %q", code, message)
	}
}

// An operator — `operate` on line1, no configure anywhere — may annotate a
// signal there and edit their own annotation, never another author's, and
// may not configure. Authorship is proven from the annotation id itself.
func TestAnOperatorAnnotatesAndEditsOnlyTheirOwn(t *testing.T) {
	_, exec, versions := twoLines(t)
	operator := CommandContext{Actor: &Entry{
		ULID: "kc-sub-op", Kind: KindHuman, Grants: []string{"cmd:el-line1/#:operate"},
	}}
	own := AnnotationSource(operator.Actor)
	annotate := func(op, action, source, id string, signals []string) []byte {
		intent := map[string]any{
			"type": "annotation", "action": action, "annotation_type_id": "at-1",
			"source": source, "time_start": 1710000000.0, "signal_ids": signals,
		}
		if id != "" {
			intent["annotation_id"] = id
		}
		return editBody(t, op, map[string]uint64{}, intent)
	}
	if code, message, _, _ := exec.ExecuteWithWrites(operator, "_CmdEdit", "apply", annotate("op-c1", "create", own, "", []string{"sig-1"})); code != 200 {
		t.Fatalf("operator create inside = %d %q", code, message)
	}
	if code, message, _, _ := exec.ExecuteWithWrites(operator, "_CmdEdit", "apply", annotate("op-c2", "create", own, "", []string{"sig-2"})); code != 409 {
		t.Fatalf("operator create outside their zone = %d %q, want refused", code, message)
	}
	ownID := deriveAnnotationID("at-1", own, 1710000000.0)
	if code, message, _, _ := exec.ExecuteWithWrites(operator, "_CmdEdit", "apply", annotate("op-u-own", "update", own, ownID, []string{"sig-1"})); code != 200 {
		t.Fatalf("operator updating their own annotation = %d %q", code, message)
	}
	othersID := deriveAnnotationID("at-1", AnnotationSource(&Entry{ULID: "kc-sub-anna", Kind: KindHuman}), 1710000000.0)
	if code, message, _, _ := exec.ExecuteWithWrites(operator, "_CmdEdit", "apply", annotate("op-d-other", "delete", own, othersID, []string{"sig-1"})); code != 409 {
		t.Fatalf("operator deleting another author's annotation = %d %q, want refused", code, message)
	}
	if code, message, _, _ := exec.ExecuteWithWrites(operator, "_CmdEdit", "apply", createUnder(t, "op-cfg", "el-line1", versions)); code != 409 {
		t.Fatalf("operator configuring = %d %q, want refused", code, message)
	}
	// The door admits an operate-only person to the Edit contract at all.
	if !Authorize(nil, operator.Actor, ActCmd, "colca/v1/_CmdEdit/n-edge1/apply") {
		t.Fatal("the door refused an operator the Edit contract")
	}
}

// The per-kind source rule has two native owners (api annotation_source and
// AnnotationSource here); the shared vectors are what keeps them equal.
func TestAnnotationSourceMatchesTheGoldenVectors(t *testing.T) {
	raw, err := os.ReadFile(filepath.Clean(annotationIDVectorPath))
	if err != nil {
		t.Fatal(err)
	}
	var v struct {
		Sources []struct {
			Kind     string `json:"kind"`
			Sub      string `json:"sub"`
			Username string `json:"preferred_username"`
			Source   string `json:"source"`
		} `json:"sources"`
	}
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatal(err)
	}
	checked := 0
	for _, c := range v.Sources {
		if c.Kind == "service" {
			continue // an mTLS service never reaches the executor as an actor; the api pins that kind
		}
		got := AnnotationSource(&Entry{ULID: c.Sub, Kind: KindHuman, Username: c.Username})
		if got != c.Source {
			t.Fatalf("%s: AnnotationSource = %q, want %q", c.Kind, got, c.Source)
		}
		checked++
	}
	if checked < 2 {
		t.Fatalf("vectors pin %d executor-side kinds, want person and service_account", checked)
	}
}
