package uns

import (
	"encoding/json"
	"strings"
	"testing"
)

// The parent holds the element a child node is attached at, but everything
// below it is the child's. A move of the parent's own asset onto that element
// used to be applied: the parent wrote its record into the child's subtree,
// the read side then routed every later edit of it to the child, and the
// child refused each one because it never held the record. The parent now
// refuses to author anything below a child's mount; retiring what already
// stands there (a move out, a delete) still works.
func TestEditRefusesToWriteBelowAChildNodesMount(t *testing.T) {
	f := newStore("n-site1")
	mountVersion := seedEditEntity(t, f, "_SystemElement", "beans", map[string]any{
		"id": "el-beans", "name": "Beans",
	})
	packagingVersion := seedEditEntity(t, f, "_SystemElement", "packaging", map[string]any{
		"id": "el-packaging", "name": "Packaging",
	})
	assetVersion := seedEditEntity(t, f, "_SystemElement", "packaging/asset", map[string]any{
		"id": "el-asset", "name": "Asset", "parent_id": "el-packaging",
	})
	seedNodeAttachment(t, f, "n-beans", "el-beans")
	exec := NewEditExec(f, nil)

	move := editBody(t, "op-move-in", map[string]uint64{
		"system-element:el-asset": assetVersion,
		"system-element:el-beans": mountVersion,
	}, map[string]any{
		"type": "placement", "entity": map[string]any{"kind": "system-element", "id": "el-asset"},
		"target_parent_id": "el-beans",
	})
	code, msg, _, writes := exec.ExecuteWithWrites(asHuman, "_CmdEdit", "apply", move)
	if code != 422 || !strings.HasPrefix(msg, "cross_node_move: ") || len(writes) != 0 {
		t.Fatalf("move below a child's mount = %d %q writes=%+v, want 422 cross_node_move", code, msg, writes)
	}
	if !strings.Contains(msg, "n-beans") {
		t.Fatalf("the refusal must name the node that owns the position: %q", msg)
	}
	if _, ok := f.KVGet("colca/v1/_SystemElement/n-site1/beans/asset"); ok {
		t.Fatal("a refused move wrote its record below the mount")
	}

	create := editBody(t, "op-create-in", map[string]uint64{"system-element:el-beans": mountVersion}, map[string]any{
		"type":       "create",
		"entity":     map[string]any{"kind": "signal", "id": "sig-in"},
		"parent_id":  "el-beans",
		"attributes": map[string]any{"name": "Inside"},
	})
	code, msg, _, _ = exec.ExecuteWithWrites(asHuman, "_CmdEdit", "apply", create)
	if code != 422 || !strings.HasPrefix(msg, "foreign_position: ") {
		t.Fatalf("create below a child's mount = %d %q, want 422 foreign_position", code, msg)
	}

	// Denominator: the same move to an element no node is attached at is applied.
	elsewhere := editBody(t, "op-move-elsewhere", map[string]uint64{
		"system-element:el-asset":     assetVersion,
		"system-element:el-packaging": packagingVersion,
	}, map[string]any{
		"type": "placement", "entity": map[string]any{"kind": "system-element", "id": "el-asset"},
		"target_parent_id": "el-packaging",
	})
	if code, msg, _, _ := exec.ExecuteWithWrites(asHuman, "_CmdEdit", "apply", elsewhere); code != 409 || msg != "placement_unchanged" {
		t.Fatalf("move to its own parent = %d %q, want the ordinary placement_unchanged", code, msg)
	}
	// And renaming the mount element itself, the parent's own record, is applied.
	rename := editBody(t, "op-rename-mount", map[string]uint64{"system-element:el-beans": mountVersion}, map[string]any{
		"type": "update", "entity": map[string]any{"kind": "system-element", "id": "el-beans"},
		"attributes": map[string]any{"name": "Bean processing"},
	})
	if code, msg, _, _ := exec.ExecuteWithWrites(asHuman, "_CmdEdit", "apply", rename); code != 200 {
		t.Fatalf("rename of the mount element = %d %q, want 200", code, msg)
	}
}

// A record that already stands below a child's mount -- written before the
// refusal existed -- can be moved out at the node that holds it; a move
// only retires the record there. Renaming it in place stays refused.
func TestEditMovesARecordAlreadyBelowAChildNodesMountOut(t *testing.T) {
	f := newStore("n-site1")
	seedEditEntity(t, f, "_SystemElement", "beans", map[string]any{"id": "el-beans", "name": "Beans"})
	packagingVersion := seedEditEntity(t, f, "_SystemElement", "packaging", map[string]any{
		"id": "el-packaging", "name": "Packaging",
	})
	strayVersion := seedEditEntity(t, f, "_SystemElement", "beans/stray", map[string]any{
		"id": "el-stray", "name": "Stray", "parent_id": "el-beans",
	})
	signalVersion := seedEditEntity(t, f, "_Signal", "beans/stray/sig", map[string]any{
		"id": "sig-stray", "name": "sig", "system_element_id": "el-stray",
	})
	seedNodeAttachment(t, f, "n-beans", "el-beans")
	exec := NewEditExec(f, nil)

	rename := editBody(t, "op-rename-stray", map[string]uint64{"system-element:el-stray": strayVersion}, map[string]any{
		"type": "update", "entity": map[string]any{"kind": "system-element", "id": "el-stray"},
		"attributes": map[string]any{"name": "Still stray"},
	})
	if code, msg, _, _ := exec.ExecuteWithWrites(asHuman, "_CmdEdit", "apply", rename); code != 422 || !strings.HasPrefix(msg, "foreign_position: ") {
		t.Fatalf("rename in place below the mount = %d %q, want 422 foreign_position", code, msg)
	}

	back := editBody(t, "op-move-back", map[string]uint64{
		"system-element:el-stray":     strayVersion,
		"signal:sig-stray":            signalVersion,
		"system-element:el-packaging": packagingVersion,
	}, map[string]any{
		"type": "placement", "entity": map[string]any{"kind": "system-element", "id": "el-stray"},
		"target_parent_id": "el-packaging",
	})
	code, msg, _, _ := exec.ExecuteWithWrites(asHuman, "_CmdEdit", "apply", back)
	if code != 200 {
		t.Fatalf("move out of the mount = %d %q, want 200", code, msg)
	}
	for _, topic := range []string{
		"colca/v1/_SystemElement/n-site1/packaging/stray",
		"colca/v1/_Signal/n-site1/packaging/stray/sig",
	} {
		if _, ok := f.KVGet(topic); !ok {
			t.Fatalf("%s was not written by the move out", topic)
		}
	}
	if _, ok := f.KVGet("colca/v1/_SystemElement/n-site1/beans/stray"); ok {
		t.Fatal("the move out left the record below the mount")
	}
}

// At the child, the element it is attached at is its root. A move onto it
// makes the entity one of the child's roots, exactly like a create there; the
// version sent for that element was checked at the parent.
func TestEditMoveOntoTheElementThisNodeIsBoundToMovesToItsRoot(t *testing.T) {
	f := newStore("n-beans")
	lineVersion := seedEditEntity(t, f, "_SystemElement", "line", map[string]any{"id": "el-line", "name": "Line"})
	assetVersion := seedEditEntity(t, f, "_SystemElement", "line/asset", map[string]any{
		"id": "el-asset", "name": "Asset", "parent_id": "el-line",
	})
	signalVersion := seedEditEntity(t, f, "_Signal", "line/asset/temp", map[string]any{
		"id": "sig-temp", "name": "temp", "system_element_id": "el-asset",
	})
	exec := NewEditExec(f, nil)
	exec.SetScope(enrolledScope{scopeOf: scopeOf{f}, own: "el-mount"})
	person := CommandContext{Actor: &Entry{
		ULID: "kc-sub-mount", Kind: KindHuman, Grants: []string{"cmd:el-mount/#:configure"},
	}}

	move := editBody(t, "op-to-root", map[string]uint64{
		"system-element:el-asset": assetVersion,
		"signal:sig-temp":         signalVersion,
		"system-element:el-mount": 2,
	}, map[string]any{
		"type": "placement", "entity": map[string]any{"kind": "system-element", "id": "el-asset"},
		"target_parent_id": "el-mount",
	})
	code, msg, _, _ := exec.ExecuteWithWrites(person, "_CmdEdit", "apply", move)
	if code != 200 {
		t.Fatalf("move onto the bound element = %d %q, want 200", code, msg)
	}
	raw, ok := f.KVGet("colca/v1/_SystemElement/n-beans/asset")
	if !ok {
		t.Fatal("the element was not moved to the node's root")
	}
	var moved map[string]any
	if err := json.Unmarshal(raw, &moved); err != nil {
		t.Fatal(err)
	}
	if _, named := moved["parent_id"]; named {
		t.Fatalf("a root the node authors names no parent; got %+v", moved)
	}
	if _, ok := f.KVGet("colca/v1/_Signal/n-beans/asset/temp"); !ok {
		t.Fatal("the element's signal did not move with it")
	}

	root := editBody(t, "op-root-to-root", map[string]uint64{
		"system-element:el-line":  lineVersion,
		"system-element:el-mount": 2,
	}, map[string]any{
		"type": "placement", "entity": map[string]any{"kind": "system-element", "id": "el-line"},
		"target_parent_id": "el-mount",
	})
	if code, msg, _, _ := exec.ExecuteWithWrites(person, "_CmdEdit", "apply", root); code != 409 || msg != "placement_unchanged" {
		t.Fatalf("a root moved onto the bound element = %d %q, want placement_unchanged", code, msg)
	}
}
