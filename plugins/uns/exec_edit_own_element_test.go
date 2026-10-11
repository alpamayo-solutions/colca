package uns

import (
	"encoding/json"
	"testing"
)

// A child node is bound to an element its parent authored, so the node holds
// no record for it. The Workbench offers "add child" there, routes the create
// to the child (it authors everything below its mount) and names that element
// as the parent, with the version the parent's read model holds for it. The
// node creates the entity at its own root: an element with
// no parent_id, like every root it authors, and a signal standing on the bound
// element. Before, it looked the element up among its own entities, did not
// find it and refused the create.
func TestEditCreateUnderTheElementThisNodeIsBoundToCreatesAtItsRoot(t *testing.T) {
	f := newStore("n-edge1")
	exec := NewEditExec(f, nil)
	// A grant on the bound element covers the node's whole frame (zoneOf
	// resolves it to "#"), which is what the plan is authorized against.
	exec.SetScope(enrolledScope{scopeOf: scopeOf{f}, own: "el-mount"})
	person := CommandContext{Actor: &Entry{
		ULID: "kc-sub-mount", Kind: KindHuman, Grants: []string{"cmd:el-mount/#:configure"},
	}}

	// The api checked the bound element's version against the parent's read
	// model and sends it along; this node cannot check it and does not refuse.
	element := editBody(t, "op-element", map[string]uint64{"system-element:el-mount": 2}, map[string]any{
		"type":       "create",
		"entity":     map[string]any{"kind": "system-element", "id": "01M4JPH8CN21VK2MT9BM6S2N4V"},
		"parent_id":  "el-mount",
		"attributes": map[string]any{"name": "Cleaning", "parent_id": "el-mount"},
	})
	code, msg, _, writes := exec.ExecuteWithWrites(person, "_CmdEdit", "apply", element)
	if code != 200 || len(writes) != 1 {
		t.Fatalf("create under the bound element = %d %q writes=%+v", code, msg, writes)
	}
	raw, ok := f.KVGet("colca/v1/_SystemElement/n-edge1/Cleaning")
	if !ok {
		t.Fatal("the element was not created at the node's root")
	}
	var created map[string]any
	if err := json.Unmarshal(raw, &created); err != nil {
		t.Fatal(err)
	}
	if _, named := created["parent_id"]; named {
		t.Fatalf("a root the node authors names no parent; got %+v", created)
	}

	signal := editBody(t, "op-signal", map[string]uint64{}, map[string]any{
		"type":       "create",
		"entity":     map[string]any{"kind": "signal", "id": "sig-mount"},
		"parent_id":  "el-mount",
		"attributes": map[string]any{"name": "Throughput", "data_type": "float"},
	})
	if code, msg, _, _ := exec.ExecuteWithWrites(person, "_CmdEdit", "apply", signal); code != 200 {
		t.Fatalf("signal create under the bound element = %d %q", code, msg)
	}
	raw, ok = f.KVGet("colca/v1/_Signal/n-edge1/Throughput")
	if !ok {
		t.Fatal("the signal was not created at the node's root")
	}
	var standing map[string]any
	if err := json.Unmarshal(raw, &standing); err != nil {
		t.Fatal(err)
	}
	if standing["system_element_id"] != "el-mount" {
		t.Fatalf("the signal must stand on the bound element; got %+v", standing)
	}
}

// Only the element the node is bound to is its root. An element the node does
// not hold and is not bound to is still an unknown parent, and a person whose
// grant covers neither the node nor the new position is refused.
func TestEditCreateUnderAnUnknownOrUncoveredParentIsStillRefused(t *testing.T) {
	f := newStore("n-edge1")
	exec := NewEditExec(f, nil)
	exec.SetScope(enrolledScope{scopeOf: scopeOf{f}, own: "el-mount"})

	stranger := editBody(t, "op-stranger", map[string]uint64{"system-element:el-elsewhere": 3}, map[string]any{
		"type":       "create",
		"entity":     map[string]any{"kind": "signal", "id": "sig-x"},
		"parent_id":  "el-elsewhere",
		"attributes": map[string]any{"name": "X"},
	})
	if code, msg, _, _ := exec.ExecuteWithWrites(asHuman, "_CmdEdit", "apply", stranger); code != 409 {
		t.Fatalf("create under an element the node neither holds nor is bound to = %d %q, want 409", code, msg)
	}

	elsewhere := CommandContext{Actor: &Entry{
		ULID: "kc-sub-other", Kind: KindHuman, Grants: []string{"cmd:el-other/#:configure"},
	}}
	uncovered := editBody(t, "op-uncovered", map[string]uint64{}, map[string]any{
		"type":       "create",
		"entity":     map[string]any{"kind": "signal", "id": "sig-y"},
		"parent_id":  "el-mount",
		"attributes": map[string]any{"name": "Y"},
	})
	if code, msg, _, _ := exec.ExecuteWithWrites(elsewhere, "_CmdEdit", "apply", uncovered); code != 409 {
		t.Fatalf("create by a person with no grant here = %d %q, want 409 entity_not_found", code, msg)
	}
	if _, ok := f.KVGet("colca/v1/_Signal/n-edge1/Y"); ok {
		t.Fatal("a refused create wrote its record")
	}
}
