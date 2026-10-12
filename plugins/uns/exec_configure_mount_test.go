package uns

import (
	"strings"
	"testing"
)

// The rule of child_mount.go at the other door: _CmdConfigure writes records
// by path, so nothing but this rule keeps a model file applied at a parent
// from standing in a child node's subtree. Every verb that writes a positioned
// record is covered here, each with the same write beside the mount applied.

// mountedConfig is a node n-site1 holding the elements `beans` and
// `packaging`, with node n-beans attached at `beans` and a machine bound to
// `packaging`.
func mountedConfig(t *testing.T) (*fakeStore, *ConfigExec) {
	t.Helper()
	f := newStore("n-site1")
	seedEditEntity(t, f, "_SystemElement", "beans", map[string]any{"id": "el-beans", "name": "Beans"})
	seedEditEntity(t, f, "_SystemElement", "packaging", map[string]any{"id": "el-packaging", "name": "Packaging"})
	seedNodeAttachment(t, f, "n-beans", "el-beans")
	// A machine bound to an element owns no positions: only a node does.
	seedEditEntity(t, f, "_EnrolledIdentity", "_colca/identities/plc-1", map[string]any{
		"ulid": "plc-1", "pubkey": strings.Repeat("cd", 32), "kind": "external", "element": "el-packaging",
	})
	c := NewConfigExec(f, newRegistryFake(), fakeNamespace{}, newFakeBlobs(testSHA), counterIDs(), nil)
	return f, c
}

// refusedBelowTheMount fails unless a command was refused with code, named
// the mount and the node attached there, and wrote nothing.
func refusedBelowTheMount(t *testing.T, f *fakeStore, what, code string, before uint64, status int, msg, result string) {
	t.Helper()
	if status != 422 || result != "invalid" || !strings.HasPrefix(msg, code+": ") {
		t.Fatalf("%s = %d %q (%s), want 422 %s", what, status, msg, result, code)
	}
	if !strings.Contains(msg, "n-beans") || !strings.Contains(msg, "below beans,") {
		t.Fatalf("%s: the refusal must name the mount and the node attached there: %q", what, msg)
	}
	if f.offset != before {
		t.Fatalf("%s: a refused command wrote %d record(s)", what, f.offset-before)
	}
}

func signalEntry(path, id string) map[string]any {
	return map[string]any{"path": path, "signal": map[string]any{
		"id": id, "name": path[strings.LastIndex(path, "/")+1:], "data_tag": nil,
	}}
}

func signalsBody(t *testing.T, entries ...map[string]any) []byte {
	t.Helper()
	return body(t, map[string]any{"signals": entries})
}

func TestConfigureElementUpsertRefusesBelowAChildNodesMount(t *testing.T) {
	f, c := mountedConfig(t)

	before := f.offset
	code, msg, result := c.Execute(asHuman, "_CmdConfigure", "element/upsert", elementBody(t,
		parentedElement("beans/roaster", "el-roaster", "Roaster", "el-beans"),
	))
	refusedBelowTheMount(t, f, "create below the mount", "foreign_position", before, code, msg, result)

	// A set is one transition: one entry below the mount refuses all of it.
	code, msg, result = c.Execute(asHuman, "_CmdConfigure", "element/upsert", elementBody(t,
		element("shipping", "el-shipping", "Shipping"),
		parentedElement("beans/roaster/drum", "el-drum", "Drum", "el-roaster"),
	))
	refusedBelowTheMount(t, f, "a set reaching below the mount", "foreign_position", before, code, msg, result)

	// Beside the mount, above it, and the mount element itself: all applied.
	code, msg, _ = c.Execute(asHuman, "_CmdConfigure", "element/upsert", elementBody(t,
		parentedElement("packaging/wrapper", "el-wrapper", "Wrapper", "el-packaging"),
		element("beansilo", "el-silo", "Bean silo"),
		element("beans", "el-beans", "Bean processing"),
	))
	if code != 200 {
		t.Fatalf("upsert beside the mount and of the mount element = %d %q, want 200", code, msg)
	}
	got := elementsUnder(f, "n-site1")
	if got["packaging/wrapper"] != "el-wrapper" || got["beansilo"] != "el-silo" {
		t.Fatalf("elements = %+v, want the ones beside the mount written", got)
	}
	if _, below := got["beans/roaster"]; below {
		t.Fatalf("elements = %+v, a refused element stands below the mount", got)
	}
}

// An upsert that names an element at another position moves it. A move to
// below the mount answers as _CmdEdit's placement does.
func TestConfigureElementUpsertRefusesAMoveBelowAChildNodesMount(t *testing.T) {
	f, c := mountedConfig(t)
	seedEditEntity(t, f, "_SystemElement", "packaging/asset", map[string]any{
		"id": "el-asset", "name": "Asset", "parent_id": "el-packaging",
	})

	before := f.offset
	code, msg, result := c.Execute(asHuman, "_CmdConfigure", "element/upsert", elementBody(t,
		parentedElement("beans/asset", "el-asset", "Asset", "el-beans"),
	))
	refusedBelowTheMount(t, f, "move below the mount", "cross_node_move", before, code, msg, result)
	if got := elementsUnder(f, "n-site1"); got["packaging/asset"] != "el-asset" {
		t.Fatalf("elements = %+v, the refused move retired the element's position", got)
	}

	// The same move to an element no node is attached at is applied.
	code, msg, _ = c.Execute(asHuman, "_CmdConfigure", "element/upsert", elementBody(t,
		element("asset", "el-asset", "Asset"),
	))
	if code != 200 {
		t.Fatalf("move beside the mount = %d %q, want 200", code, msg)
	}
	if got := elementsUnder(f, "n-site1"); got["asset"] != "el-asset" || got["packaging/asset"] != "" {
		t.Fatalf("elements = %+v, want el-asset moved to asset", got)
	}
}

func TestConfigureSignalUpsertRefusesBelowAChildNodesMount(t *testing.T) {
	f, c := mountedConfig(t)
	seedEditEntity(t, f, "_Signal", "packaging/speed", map[string]any{
		"id": "sig-speed", "name": "speed", "system_element_id": "el-packaging",
	})

	before := f.offset
	code, msg, result := c.Execute(asHuman, "_CmdConfigure", "signal/upsert",
		signalsBody(t, signalEntry("beans/temp", "sig-temp")))
	refusedBelowTheMount(t, f, "signal on the mount element", "foreign_position", before, code, msg, result)

	code, msg, result = c.Execute(asHuman, "_CmdConfigure", "signal/upsert",
		signalsBody(t, signalEntry("beans/roaster/temp", "sig-temp")))
	refusedBelowTheMount(t, f, "signal deeper below the mount", "foreign_position", before, code, msg, result)

	code, msg, result = c.Execute(asHuman, "_CmdConfigure", "signal/upsert",
		signalsBody(t, signalEntry("beans/speed", "sig-speed")))
	refusedBelowTheMount(t, f, "signal moved below the mount", "cross_node_move", before, code, msg, result)

	code, msg, _ = c.Execute(asHuman, "_CmdConfigure", "signal/upsert", signalsBody(t,
		signalEntry("packaging/temp", "sig-temp"), signalEntry("packaging/rate", "sig-speed")))
	if code != 200 {
		t.Fatalf("signals beside the mount = %d %q, want 200", code, msg)
	}
	got := signalsAt(c)
	if got["packaging/temp"].ID != "sig-temp" || got["packaging/rate"].ID != "sig-speed" || len(got) != 2 {
		t.Fatalf("signals = %+v, want the two beside the mount and none below it", got)
	}
}

func TestConfigureConstantUpsertRefusesBelowAChildNodesMount(t *testing.T) {
	f, c := mountedConfig(t)
	seedEditEntity(t, f, "_Constant", "packaging/target", map[string]any{
		"id": "c-target", "name": "target", "data_type": "int64", "value": 5,
	})

	before := f.offset
	code, msg, result := c.Execute(asHuman, "_CmdConfigure", "constant/upsert",
		constantBody(t, constant("beans/limit", "c-limit", "int64", 7)))
	refusedBelowTheMount(t, f, "constant below the mount", "foreign_position", before, code, msg, result)

	code, msg, result = c.Execute(asHuman, "_CmdConfigure", "constant/upsert",
		constantBody(t, constant("beans/target", "c-target", "int64", 5)))
	refusedBelowTheMount(t, f, "constant moved below the mount", "cross_node_move", before, code, msg, result)

	code, msg, _ = c.Execute(asHuman, "_CmdConfigure", "constant/upsert",
		constantBody(t, constant("packaging/limit", "c-limit", "int64", 7)))
	if code != 200 {
		t.Fatalf("constant beside the mount = %d %q, want 200", code, msg)
	}
	if _, ok := f.KVGet("colca/v1/_Constant/n-site1/packaging/limit"); !ok {
		t.Fatal("the constant beside the mount was not written")
	}
	if _, ok := f.KVGet("colca/v1/_Constant/n-site1/beans/limit"); ok {
		t.Fatal("a refused constant stands below the mount")
	}
}

// A resource stands on an element, at {element path}/{resource id}. The mount
// element is this node's own record, so a document on it is this node's; a
// document on anything below it stands in the child's subtree.
func TestConfigureResourceUpsertRefusesOnAnElementBelowAChildNodesMount(t *testing.T) {
	f, c := mountedConfig(t)

	before := f.offset
	code, msg, result := c.Execute(asHuman, "_CmdConfigure", "resource/upsert",
		resourceBody("beans/roaster/r1", "r1", testSHA))
	refusedBelowTheMount(t, f, "resource on an element below the mount", "foreign_position", before, code, msg, result)

	for _, path := range []string{"beans/r1", "packaging/r2"} {
		id := path[strings.LastIndex(path, "/")+1:]
		code, msg, _ = c.Execute(asHuman, "_CmdConfigure", "resource/upsert", resourceBody(path, id, testSHA))
		if code != 200 {
			t.Fatalf("resource at %s = %d %q, want 200", path, code, msg)
		}
		if _, ok := f.KVGet("colca/v1/_Resource/n-site1/" + path); !ok {
			t.Fatalf("the resource at %s was not written", path)
		}
	}
	if _, ok := f.KVGet("colca/v1/_Resource/n-site1/beans/roaster/r1"); ok {
		t.Fatal("a refused resource stands below the mount")
	}
}

// signal/autobind mints signals under the connector's element, or under the
// path the command names. Neither may be below a child's mount.
func TestConfigureAutobindRefusesBelowAChildNodesMount(t *testing.T) {
	f, c := mountedConfig(t)
	place(t, c, "el-packaging", "packaging")
	bindEntry(t, c, "01JCONN", "opcua", "el-packaging")
	publishCatalogue(t, c, "colca/v1/_DataTags/n-site1/packaging/opcua", tags("t1"))

	before := f.offset
	code, msg, result := c.Execute(asHuman, "_CmdConfigure", "signal/autobind",
		body(t, map[string]any{"connector": "01JCONN", "under": "beans"}))
	refusedBelowTheMount(t, f, "autobind under the mount element", "foreign_position", before, code, msg, result)
	if got := signalsAt(c); len(got) != 0 {
		t.Fatalf("signals = %+v, a refused autobind minted one", got)
	}

	code, msg, _ = c.Execute(asHuman, "_CmdConfigure", "signal/autobind", body(t, map[string]any{"connector": "01JCONN"}))
	if code != 200 {
		t.Fatalf("autobind beside the mount = %d %q, want 200", code, msg)
	}
	if got := signalsAt(c); len(got) != 1 || got["packaging/tag-t1"].DataTag != "t1" {
		t.Fatalf("signals = %+v, want the tag bound at packaging/tag-t1", got)
	}
}

// element/author walks a path and creates what is missing along it, for an
// identity enrolled at that path.
func TestConfigureElementAuthorRefusesBelowAChildNodesMount(t *testing.T) {
	f, c := mountedConfig(t)

	before := f.offset
	code, msg, result := c.Execute(asHuman, "_CmdConfigure", "element/author", body(t, map[string]any{"path": "beans/roaster"}))
	refusedBelowTheMount(t, f, "author below the mount", "foreign_position", before, code, msg, result)

	code, msg, _ = c.Execute(asHuman, "_CmdConfigure", "element/author", body(t, map[string]any{"path": "packaging/wrapper"}))
	if code != 200 {
		t.Fatalf("author beside the mount = %d %q, want 200", code, msg)
	}
	if got := elementsUnder(f, "n-site1"); got["packaging/wrapper"] == "" {
		t.Fatalf("elements = %+v, want packaging/wrapper authored", got)
	}
	// The mount element itself resolves: it is this node's record.
	code, msg, _ = c.Execute(asHuman, "_CmdConfigure", "element/author", body(t, map[string]any{"path": "beans"}))
	if code != 200 || msg != "el-beans" {
		t.Fatalf("author of the mount element = %d %q, want 200 el-beans", code, msg)
	}
}

// Records that already stand below a mount -- written before the refusal
// existed -- can be retired there: the delete verbs and a move out only write
// tombstones below the mount. Rewriting one in place stays refused.
func TestConfigureRetiresRecordsAlreadyBelowAChildNodesMount(t *testing.T) {
	f, c := mountedConfig(t)
	seedEditEntity(t, f, "_SystemElement", "beans/stray", map[string]any{
		"id": "el-stray", "name": "Stray", "parent_id": "el-beans",
	})
	seedEditEntity(t, f, "_SystemElement", "beans/gone", map[string]any{
		"id": "el-gone", "name": "Gone", "parent_id": "el-beans",
	})
	seedEditEntity(t, f, "_Signal", "beans/stray/sig", map[string]any{
		"id": "sig-stray", "name": "sig", "system_element_id": "el-stray",
	})
	seedEditEntity(t, f, "_Signal", "beans/gone/sig", map[string]any{
		"id": "sig-gone", "name": "sig", "system_element_id": "el-gone",
	})
	seedEditEntity(t, f, "_Constant", "beans/gone/limit", map[string]any{
		"id": "c-gone", "name": "limit", "data_type": "int64", "value": 1,
	})
	if _, err := f.seed("colca/v1/_Resource/n-site1/beans/gone/r1", []byte(`{"id":"r1","system_element_id":"el-gone",`+
		`"filename":"manual.pdf","content_type":"application/pdf","size_bytes":12,"sha256":"`+testSHA+`"}`)); err != nil {
		t.Fatal(err)
	}

	before := f.offset
	code, msg, result := c.Execute(asHuman, "_CmdConfigure", "element/upsert", elementBody(t,
		parentedElement("beans/stray", "el-stray", "Still stray", "el-beans"),
	))
	refusedBelowTheMount(t, f, "rename in place below the mount", "foreign_position", before, code, msg, result)

	// A move out: the records beside the mount, tombstones below it.
	code, msg, _ = c.Execute(asHuman, "_CmdConfigure", "element/upsert", elementBody(t,
		parentedElement("packaging/stray", "el-stray", "Stray", "el-packaging"),
	))
	if code != 200 {
		t.Fatalf("element moved out of the mount = %d %q, want 200", code, msg)
	}
	code, msg, _ = c.Execute(asHuman, "_CmdConfigure", "signal/upsert",
		signalsBody(t, signalEntry("packaging/stray/sig", "sig-stray")))
	if code != 200 {
		t.Fatalf("signal moved out of the mount = %d %q, want 200", code, msg)
	}

	for _, del := range []struct{ verb, path string }{
		{"signal/delete", "beans/gone/sig"},
		{"constant/delete", "beans/gone/limit"},
		{"resource/delete", "beans/gone/r1"},
		{"element/delete", "beans/gone"},
	} {
		code, msg, _ := c.Execute(asHuman, "_CmdConfigure", del.verb, body(t, map[string]any{"paths": []string{del.path}}))
		if code != 200 {
			t.Fatalf("%s of %s below the mount = %d %q, want 200", del.verb, del.path, code, msg)
		}
	}

	if got := elementsUnder(f, "n-site1"); got["packaging/stray"] != "el-stray" || len(got) != 3 {
		t.Fatalf("elements = %+v, want beans, packaging and packaging/stray", got)
	}
	if got := signalsAt(c); got["packaging/stray/sig"].ID != "sig-stray" || len(got) != 1 {
		t.Fatalf("signals = %+v, want only packaging/stray/sig", got)
	}
	for _, contract := range []string{"_Constant", "_Resource"} {
		if left := f.KVScan(contract, "n-site1"); len(left) != 0 {
			t.Fatalf("%s left below the mount: %+v", contract, left)
		}
	}
}

// Definitions and inventory entities have an id for an address, not a
// position; a mount does not concern them.
func TestConfigureUnpositionedRecordsIgnoreChildMounts(t *testing.T) {
	f, c := mountedConfig(t)

	code, msg, _ := c.Execute(asHuman, "_CmdConfigure", "definition/upsert", body(t, map[string]any{
		"definitions": []map[string]any{{
			"contract": "_SemanticTag", "definition": map[string]any{"id": "beans", "name": "beans"},
		}},
	}))
	if code != 200 {
		t.Fatalf("definition/upsert = %d %q, want 200", code, msg)
	}
	code, msg, _ = c.Execute(asHuman, "_CmdConfigure", "entity/upsert", body(t, map[string]any{
		"entities": []map[string]any{{
			"contract": "_ExternalReference", "entity": map[string]any{"id": "beans"},
		}},
	}))
	if code != 200 {
		t.Fatalf("entity/upsert = %d %q, want 200", code, msg)
	}
	if _, ok := f.KVGet("colca/v1/_ExternalReference/n-site1/_colca/external-references/beans"); !ok {
		t.Fatal("the inventory entity was not written")
	}
}

// The lifecycle trigger binds a new connector's catalogue without a command.
// It writes through the same commit, so a connector enrolled on the element a
// child node is attached at gets no signals minted into the child's subtree.
func TestAutobindTriggerMintsNothingBelowAChildNodesMount(t *testing.T) {
	f := newStore("n-site1")
	seedEditEntity(t, f, "_SystemElement", "beans", map[string]any{"id": "el-beans", "name": "Beans"})
	seedEditEntity(t, f, "_SystemElement", "packaging", map[string]any{"id": "el-packaging", "name": "Packaging"})
	seedNodeAttachment(t, f, "n-beans", "el-beans")
	c := NewConfigExec(f, newRegistryFake(), fakeNamespace{}, nil, counterIDs(),
		map[string]string{"autobind": "on_new_connector"})
	place(t, c, "el-beans", "beans")
	place(t, c, "el-packaging", "packaging")
	bindEntry(t, c, "01JINSIDE", "inside", "el-beans")
	bindEntry(t, c, "01JBESIDE", "beside", "el-packaging")

	c.Observe("_DataTags", "colca/v1/_DataTags/n-site1/beans/inside", mustJSON(map[string]any{"data_tags": tags("t1")}))
	c.Observe("_DataTags", "colca/v1/_DataTags/n-site1/packaging/beside", mustJSON(map[string]any{"data_tags": tags("t2")}))

	got := signalsAt(c)
	if len(got) != 1 || got["packaging/tag-t2"].DataTag != "t2" {
		t.Fatalf("signals = %+v, want only the connector beside the mount bound", got)
	}
}
