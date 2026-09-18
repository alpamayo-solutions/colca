package uns

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// scopeOf resolves a grant's element to its path from the fake store, as the
// element index does in production.
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

// scopedTo is a person whose configure grant names one element.
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

// A person with configure on line1 only: a plan touching line2 is refused
// before any write, looking like a not-found, while the same operation under
// line1 applies.
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

// An operator (operate on line1, no configure) may annotate a signal there
// and edit their own annotation, but not another author's, and may not
// configure. The annotation id proves authorship.
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
	ownID := deriveAnnotationID("at-1", own, 1710000000.0, []string{"sig-1"})
	if code, message, _, _ := exec.ExecuteWithWrites(operator, "_CmdEdit", "apply", annotate("op-u-own", "update", own, ownID, []string{"sig-1"})); code != 200 {
		t.Fatalf("operator updating their own annotation = %d %q", code, message)
	}
	othersID := deriveAnnotationID("at-1", AnnotationSource(&Entry{ULID: "kc-sub-anna", Kind: KindHuman}), 1710000000.0, []string{"sig-1"})
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

// An operator (param on line1, no configure) may set the value and metadata of
// an existing constant there — an operator-input like a station's sandoff —
// but not create one, not touch its other attributes, not reach a constant on
// another line, and not rebind a signal. A person with configure keeps working
// exactly as before.
func TestAnOperatorWithParamSetsOnlyAConstantsValueAndMetadata(t *testing.T) {
	f, exec, versions := twoLines(t)
	versions["constant:const-sandoff-1"] = seedEditEntity(t, f, "_Constant", "line1/sta1/operator/sandoffMm", map[string]any{
		"id": "const-sandoff-1", "name": "Sandoff", "data_type": "float64", "value": 5.0,
		"system_element_id": "el-line1", "metadata": map[string]any{"source": "catalog"},
	})
	versions["constant:const-sandoff-2"] = seedEditEntity(t, f, "_Constant", "line2/sta1/operator/sandoffMm", map[string]any{
		"id": "const-sandoff-2", "name": "Sandoff", "data_type": "float64", "value": 5.0,
		"system_element_id": "el-line2", "metadata": map[string]any{"source": "catalog"},
	})
	operator := CommandContext{Actor: &Entry{
		ULID: "kc-sub-op", Kind: KindHuman, Grants: []string{"cmd:el-line1/#:param"},
	}}
	setValue := func(op, constantID string, attributes map[string]any) []byte {
		return editBody(t, op, map[string]uint64{"constant:" + constantID: versions["constant:"+constantID]}, map[string]any{
			"type": "update", "entity": map[string]any{"kind": "constant", "id": constantID},
			"attributes": attributes,
		})
	}

	// Inside the grant, value and metadata only: allowed.
	code, message, _, writes := exec.ExecuteWithWrites(operator, "_CmdEdit", "apply", setValue("op-set", "const-sandoff-1", map[string]any{
		"value": 6.5, "metadata": map[string]any{"source": "operator", "set_by": "kc-sub-op", "set_at": "2026-09-18T00:00:00Z"},
	}))
	if code != 200 || len(writes) != 1 {
		t.Fatalf("operator set inside the grant = %d %q writes=%d", code, message, len(writes))
	}
	versions["constant:const-sandoff-1"] = writes[0].Offset
	updated, _ := f.KVGet("colca/v1/_Constant/n-edge1/line1/sta1/operator/sandoffMm")
	var constant map[string]any
	if err := json.Unmarshal(updated, &constant); err != nil {
		t.Fatal(err)
	}
	if constant["value"] != 6.5 || constant["name"] != "Sandoff" {
		t.Fatalf("param update changed more than value: %+v", constant)
	}

	// Outside the grant: refused, nothing written.
	code, message, _, writes = exec.ExecuteWithWrites(operator, "_CmdEdit", "apply", setValue("op-out", "const-sandoff-2", map[string]any{"value": 9.0}))
	if code != 409 || message != "entity_not_found: constant:const-sandoff-2" || len(writes) != 0 {
		t.Fatalf("operator set outside the grant = %d %q writes=%d, want the not-found shape and nothing written", code, message, len(writes))
	}

	// Any other attribute (renaming, retyping) needs configure, which the
	// operator does not hold — refused even inside their zone.
	code, message, _, writes = exec.ExecuteWithWrites(operator, "_CmdEdit", "apply", setValue("op-rename", "const-sandoff-1", map[string]any{
		"value": 7.0, "name": "Renamed",
	}))
	if code != 409 || len(writes) != 0 {
		t.Fatalf("operator changing a non-value attribute = %d %q writes=%d, want refused", code, message, len(writes))
	}

	// Creating a constant is configure-only; param never reaches it.
	code, message, _, writes = exec.ExecuteWithWrites(operator, "_CmdEdit", "apply", createUnder(t, "op-create", "el-line1", versions))
	if code != 409 || len(writes) != 0 {
		t.Fatalf("operator creating a constant = %d %q writes=%d, want refused", code, message, len(writes))
	}

	// Rebinding a signal is configure-only; param never reaches it either.
	rebind := editBody(t, "op-rebind", map[string]uint64{"signal:sig-1": versions["signal:sig-1"]}, map[string]any{
		"type": "update", "entity": map[string]any{"kind": "signal", "id": "sig-1"},
		"attributes": map[string]any{"name": "Renamed"},
	})
	code, message, _, writes = exec.ExecuteWithWrites(operator, "_CmdEdit", "apply", rebind)
	if code != 409 || len(writes) != 0 {
		t.Fatalf("operator updating a signal = %d %q writes=%d, want refused", code, message, len(writes))
	}

	// A person with configure keeps setting anything on the constant, as
	// before param existed.
	code, message, _, writes = exec.ExecuteWithWrites(scopedTo("el-line1"), "_CmdEdit", "apply", setValue("op-cfg", "const-sandoff-1", map[string]any{
		"value": 8.0, "name": "Sandoff (renamed)",
	}))
	if code != 200 || len(writes) != 1 {
		t.Fatalf("configure set = %d %q writes=%d", code, message, len(writes))
	}

	// A param-only person still cannot reach _CmdConfigure at all — Colca
	// routes every human write through _CmdEdit, whatever they hold.
	if operator.Actor.MayPublishContract("_CmdConfigure") {
		t.Fatal("a person may not publish _CmdConfigure, whatever they hold")
	}
}

// A param grant's constant write carries the operator's own attribution
// (ActorID/ActorLabel/ActorKind) to PublishBatch, not just their authorizing
// Actor: the engine's (*entityStore).PublishBatch reads these onto the
// write's actor_* fields, which is how an operator-set constant ends up
// attributed to them on /kv rather than to the node alone.
func TestParamSetForwardsTheOperatorsAttributionToTheStore(t *testing.T) {
	f, exec, versions := twoLines(t)
	versions["constant:const-sandoff-1"] = seedEditEntity(t, f, "_Constant", "line1/sta1/operator/sandoffMm", map[string]any{
		"id": "const-sandoff-1", "name": "Sandoff", "data_type": "float64", "value": 5.0,
		"system_element_id": "el-line1",
	})
	operator := CommandContext{
		Actor:      &Entry{ULID: "kc-sub-op", Kind: KindHuman, Grants: []string{"cmd:el-line1/#:param"}},
		ActorID:    "kc-sub-op",
		ActorLabel: "op@example.com",
		ActorKind:  "human",
	}
	setValue := editBody(t, "op-attrib", map[string]uint64{
		"constant:const-sandoff-1": versions["constant:const-sandoff-1"],
	}, map[string]any{
		"type": "update", "entity": map[string]any{"kind": "constant", "id": "const-sandoff-1"},
		"attributes": map[string]any{"value": 6.5},
	})

	code, message, _, writes := exec.ExecuteWithWrites(operator, "_CmdEdit", "apply", setValue)
	if code != 200 || len(writes) != 1 {
		t.Fatalf("operator param set = %d %q writes=%d", code, message, len(writes))
	}
	if got := f.lastBatchCtx; got.ActorID != "kc-sub-op" || got.ActorLabel != "op@example.com" || got.ActorKind != "human" {
		t.Fatalf("PublishBatch ctx = %+v, want the operator's own attribution", got)
	}
}

// The source rule exists in the api and here; the shared vectors keep them
// equal.
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
			continue // mTLS services never act at the executor; that case is the api's
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
