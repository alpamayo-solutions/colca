package uns

import (
	"strings"
	"testing"
)

// A root's children are children even though no path says so.
//
// The publisher leaves the root out of the path it writes, so an element whose
// parent is a root is stored at its own segment alone: `Traceability` at
// "Traceability", its child `events` at "Events". By path the root holds
// nothing, and a delete that judged occupancy on paths retired it without
// complaint.
//
// Seen on a live deployment: `prekit dm deploy --prune` removed a root with
// two children and a grandchild; this check found nothing in the way and the
// single tombstone went out. The projection knows the relationship — the
// payload carries `parent_id` — so it cascaded, and four elements the node
// still held disappeared from the read model. Node and projection then
// disagreed permanently, and every later apply failed against a tree only one
// of them could see.
//
// `parent_id` is in the record. The node can see it, so the node checks it.

func parentedElement(path, id, name, parentID string) map[string]any {
	return map[string]any{"path": path, "element": map[string]any{
		"id": id, "name": name, "parent_id": parentID,
	}}
}

func rootWithChild(t *testing.T) (*fakeStore, *ConfigExec) {
	t.Helper()
	f := newStore("n-edge1")
	c := NewConfigExec(f, nil, nil, nil, nil, nil)
	c.Execute(asHuman, "_CmdConfigure", "element/upsert", elementBody(t,
		element("Traceability", "01HTRACE", "Traceability"),
		parentedElement("Events", "01HEVENTS", "events", "01HTRACE"),
	))
	return f, c
}

func TestElementDeleteRefusesWhileAChildNamesItAsParent(t *testing.T) {
	f, c := rootWithChild(t)

	code, msg, result := c.Execute(asHuman, "_CmdConfigure", "element/delete", body(t, map[string]any{
		"paths": []string{"Traceability"},
	}))

	if code != 409 || result != "conflict" {
		t.Fatalf("delete of a root with a child = %d %q (%s), want a 409 conflict", code, msg, result)
	}
	if !strings.Contains(msg, "Events") {
		t.Fatalf("refusal = %q, want it to name the child that is in the way", msg)
	}
	if f.records["colca/v1/_SystemElement/n-edge1/Traceability"] == nil {
		t.Fatal("the refused delete removed the record anyway")
	}
}

func TestElementDeleteStillRetiresAParentWithItsChildrenInOneCommand(t *testing.T) {
	// The denominator. A cascade delete sends the parent and every descendant
	// in one command, and that has to keep working — a check that refused it
	// would make every subtree undeletable, which is worse than the bug.
	f, c := rootWithChild(t)

	code, msg, result := c.Execute(asHuman, "_CmdConfigure", "element/delete", body(t, map[string]any{
		"paths": []string{"Events", "Traceability"},
	}))

	if code != 200 {
		t.Fatalf("retiring both together = %d %q (%s), want 200", code, msg, result)
	}
	for _, topic := range []string{
		"colca/v1/_SystemElement/n-edge1/Traceability",
		"colca/v1/_SystemElement/n-edge1/Events",
	} {
		if f.records[topic] != nil {
			t.Fatalf("%s survived the delete", topic)
		}
	}
}

func TestElementDeleteStillRetiresAnElementNothingClaims(t *testing.T) {
	// The other denominator: the new check must not make an ordinary delete
	// refuse. An element with no children, by path or by parent, still goes.
	f, c := rootWithChild(t)

	code, msg, _ := c.Execute(asHuman, "_CmdConfigure", "element/delete", body(t, map[string]any{
		"paths": []string{"Events"},
	}))

	if code != 200 {
		t.Fatalf("delete of a childless element = %d %q, want 200", code, msg)
	}
	if f.records["colca/v1/_SystemElement/n-edge1/Events"] != nil {
		t.Fatal("the record survived a successful delete")
	}
}

func TestElementDeleteNamesEachChildOnce(t *testing.T) {
	// A child that is BOTH under the path and names the parent must not be
	// reported twice; the refusal an operator reads should list what is in the
	// way, not the same thing twice.
	f := newStore("n-edge1")
	c := NewConfigExec(f, nil, nil, nil, nil, nil)
	c.Execute(asHuman, "_CmdConfigure", "element/upsert", elementBody(t,
		element("line1", "01HLINE1", "Linie 1"),
		parentedElement("line1/m6", "01HM6", "Maschine 6", "01HLINE1"),
	))

	_, msg, _ := c.Execute(asHuman, "_CmdConfigure", "element/delete", body(t, map[string]any{
		"paths": []string{"line1"},
	}))

	if strings.Count(msg, "line1/m6") != 1 {
		t.Fatalf("refusal = %q, want the child named exactly once", msg)
	}
}
