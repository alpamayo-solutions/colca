package uns

import (
	"encoding/json"
	"strings"
	"testing"
)

// declareSignal authors a signal at path the way a bootstrap manifest does:
// with its own id and everything a declaration knows, but no data_tag — the
// tag is minted by the connector at discovery, so a declaration cannot name it.
func declareSignal(t *testing.T, c *ConfigExec, path, id, element string, extra map[string]any) {
	t.Helper()
	signal := map[string]any{"id": id, "name": path[strings.LastIndex(path, "/")+1:], "data_tag": nil}
	if element != "" {
		signal["system_element_id"] = element
	}
	for k, v := range extra {
		signal[k] = v
	}
	code, msg, _ := c.Execute(asHuman, "_CmdConfigure", "signal/upsert", body(t, map[string]any{
		"signals": []map[string]any{{"path": path, "signal": signal}},
	}))
	if code != 200 {
		t.Fatalf("declareSignal(%s) = %d %q", path, code, msg)
	}
}

// signalRecordAt is the raw record at a local path, so a test can see every
// field the declaration authored, not only the binding fields boundSignal reads.
func signalRecordAt(t *testing.T, c *ConfigExec, path string) map[string]any {
	t.Helper()
	raw, ok := c.store.KVGet(c.signalTopic(path))
	if !ok {
		t.Fatalf("no signal at %s; have %+v", path, signalsAt(c))
	}
	var record map[string]any
	if err := json.Unmarshal(raw, &record); err != nil {
		t.Fatalf("signal at %s is not a record: %v", path, err)
	}
	return record
}

// A bootstrap declares what a signal IS before any connector has published;
// the catalogue says where its value comes from. They meet at the path: the
// declared record gains the binding and keeps everything it declared, and no
// `<name>-2` appears beside it.
func TestAutobindBindsADeclaredSignalInsteadOfShadowingIt(t *testing.T) {
	c := newConfigExec(t)
	place(t, c, "01HLINE1", "line1")
	bindEntry(t, c, "01JCONN", "opcua-1", "01HLINE1")
	declareSignal(t, c, "line1/tag-t1", "01SDECLARED", "01HLINE1", map[string]any{
		"unit": "°C", "precision": 1, "description": "Drum temperature",
	})
	publishCatalogue(t, c, "colca/v1/_DataTags/n1/line1/opcua-1", tags("t1", "t2"))

	code, msg, _ := c.Execute(asHuman, "_CmdConfigure", "signal/autobind", body(t, map[string]any{"connector": "01JCONN"}))
	if code != 200 || !strings.Contains(msg, `"created":2`) {
		t.Fatalf("autobind = %d %q, want 2 created (one bound in place, one minted)", code, msg)
	}

	got := signalsAt(c)
	if _, shadow := got["line1/tag-t1-2"]; shadow {
		t.Fatalf("the declared signal was shadowed by a sibling: %+v", got)
	}
	bound := got["line1/tag-t1"]
	if bound.ID != "01SDECLARED" || bound.DataTag != "t1" {
		t.Fatalf("line1/tag-t1 = %+v, want the DECLARED record (01SDECLARED) bound to t1", bound)
	}
	record := signalRecordAt(t, c, "line1/tag-t1")
	if record["unit"] != "°C" || record["description"] != "Drum temperature" {
		t.Fatalf("binding lost what the declaration authored: %+v", record)
	}
	if record["data_type"] != "float" {
		t.Fatalf("a declaration without a type takes the tag's: %+v", record)
	}
	if _, minted := got["line1/tag-t2"]; !minted {
		t.Fatalf("the undeclared tag was not minted a signal: %+v", got)
	}
}

// The declaration owns the type when it states one: a catalogue cannot
// widen a signal an operator declared as an integer into a float.
func TestADeclaredTypeOutranksTheTagsType(t *testing.T) {
	c := newConfigExec(t)
	place(t, c, "01HLINE1", "line1")
	bindEntry(t, c, "01JCONN", "opcua-1", "01HLINE1")
	declareSignal(t, c, "line1/tag-t1", "01SDECLARED", "01HLINE1", map[string]any{"data_type": "int"})
	publishCatalogue(t, c, "colca/v1/_DataTags/n1/line1/opcua-1", tags("t1"))

	if code, msg, _ := c.Execute(asHuman, "_CmdConfigure", "signal/autobind", body(t, map[string]any{"connector": "01JCONN"})); code != 200 {
		t.Fatalf("autobind = %d %q", code, msg)
	}
	if got := signalRecordAt(t, c, "line1/tag-t1")["data_type"]; got != "int" {
		t.Fatalf("data_type = %v, want the declared int", got)
	}
}

// A tag's meta.unit becomes the minted signal's unit — the SDK's own
// publish(path, value, unit=...) reaches the tree (SDK design §3, gap 2).
func TestATagsUnitIsCopiedOntoTheMintedSignal(t *testing.T) {
	c := newConfigExec(t)
	place(t, c, "01HLINE1", "line1")
	bindEntry(t, c, "01JCONN", "opcua-1", "01HLINE1")
	publishCatalogue(t, c, "colca/v1/_DataTags/n1/line1/opcua-1", []map[string]any{
		{"id": "t1", "name": "temp", "data_type": "float", "meta": map[string]any{"unit": "°C"}},
	})

	if code, msg, _ := c.Execute(asHuman, "_CmdConfigure", "signal/autobind", body(t, map[string]any{"connector": "01JCONN"})); code != 200 {
		t.Fatalf("autobind = %d %q", code, msg)
	}
	if got := signalRecordAt(t, c, "line1/temp")["unit"]; got != "°C" {
		t.Fatalf("unit = %v, want the tag's °C", got)
	}
}

// A predeclared signal with no unit of its own takes the tag's — the same
// "the declaration owns what it states, the tag fills the rest" rule
// TestAutobindBindsADeclaredSignalInsteadOfShadowingIt already pins for
// data_type.
func TestATagsUnitFillsAPredeclaredSignalWithNoUnit(t *testing.T) {
	c := newConfigExec(t)
	place(t, c, "01HLINE1", "line1")
	bindEntry(t, c, "01JCONN", "opcua-1", "01HLINE1")
	declareSignal(t, c, "line1/tag-t1", "01SDECLARED", "01HLINE1", nil)
	publishCatalogue(t, c, "colca/v1/_DataTags/n1/line1/opcua-1", []map[string]any{
		{"id": "t1", "name": "tag-t1", "data_type": "float", "meta": map[string]any{"unit": "bar"}},
	})

	if code, msg, _ := c.Execute(asHuman, "_CmdConfigure", "signal/autobind", body(t, map[string]any{"connector": "01JCONN"})); code != 200 {
		t.Fatalf("autobind = %d %q", code, msg)
	}
	if got := signalRecordAt(t, c, "line1/tag-t1")["unit"]; got != "bar" {
		t.Fatalf("unit = %v, want the tag's bar filled onto the undeclared signal", got)
	}
}

// A declared unit outranks the tag's — an operator's own declaration must
// never be overwritten by a republish, the same rule the type already gets.
func TestADeclaredUnitOutranksTheTagsUnit(t *testing.T) {
	c := newConfigExec(t)
	place(t, c, "01HLINE1", "line1")
	bindEntry(t, c, "01JCONN", "opcua-1", "01HLINE1")
	declareSignal(t, c, "line1/tag-t1", "01SDECLARED", "01HLINE1", map[string]any{"unit": "bar"})
	publishCatalogue(t, c, "colca/v1/_DataTags/n1/line1/opcua-1", []map[string]any{
		{"id": "t1", "name": "tag-t1", "data_type": "float", "meta": map[string]any{"unit": "psi"}},
	})

	if code, msg, _ := c.Execute(asHuman, "_CmdConfigure", "signal/autobind", body(t, map[string]any{"connector": "01JCONN"})); code != 200 {
		t.Fatalf("autobind = %d %q", code, msg)
	}
	if got := signalRecordAt(t, c, "line1/tag-t1")["unit"]; got != "bar" {
		t.Fatalf("unit = %v, want the operator's declared bar to outrank the tag's psi", got)
	}
}

// A signal that already holds a DIFFERENT tag is not "declared and waiting";
// it is a collision, and gets the sibling it always got.
func TestAutobindStillSidestepsASignalBoundToAnotherTag(t *testing.T) {
	c := newConfigExec(t)
	place(t, c, "01HLINE1", "line1")
	bindEntry(t, c, "01JCONN", "opcua-1", "01HLINE1")
	declareSignal(t, c, "line1/tag-t1", "01SOTHER", "01HLINE1", map[string]any{"data_tag": "t-elsewhere"})
	publishCatalogue(t, c, "colca/v1/_DataTags/n1/line1/opcua-1", tags("t1"))

	if code, msg, _ := c.Execute(asHuman, "_CmdConfigure", "signal/autobind", body(t, map[string]any{"connector": "01JCONN"})); code != 200 {
		t.Fatalf("autobind = %d %q", code, msg)
	}
	got := signalsAt(c)
	if got["line1/tag-t1"].DataTag != "t-elsewhere" {
		t.Fatalf("an existing binding was overwritten: %+v", got)
	}
	if got["line1/tag-t1-2"].DataTag != "t1" {
		t.Fatalf("the colliding tag got no sibling: %+v", got)
	}
}

// Binding in place is as idempotent as minting: the second run finds the
// tag held and changes nothing.
func TestBindingADeclaredSignalIsIdempotent(t *testing.T) {
	c := newConfigExec(t)
	place(t, c, "01HLINE1", "line1")
	bindEntry(t, c, "01JCONN", "opcua-1", "01HLINE1")
	declareSignal(t, c, "line1/tag-t1", "01SDECLARED", "01HLINE1", nil)
	publishCatalogue(t, c, "colca/v1/_DataTags/n1/line1/opcua-1", tags("t1"))

	c.Execute(asHuman, "_CmdConfigure", "signal/autobind", body(t, map[string]any{"connector": "01JCONN"}))
	code, msg, _ := c.Execute(asHuman, "_CmdConfigure", "signal/autobind", body(t, map[string]any{"connector": "01JCONN"}))
	if code != 200 || !strings.Contains(msg, `"created":0,"skipped":1`) {
		t.Fatalf("second autobind = %d %q, want nothing created", code, msg)
	}
	if got := signalsAt(c); len(got) != 1 {
		t.Fatalf("signals = %+v, want exactly the declared one", got)
	}
}

// The lifecycle trigger binds a declared signal exactly as the verb does —
// this is the path a generated node actually takes: bootstrap first, then
// the connector starts and publishes.
func TestNewConnectorBindsDeclaredSignalsOnArrival(t *testing.T) {
	c := newTriggerConfigExec(t)
	place(t, c, "01HLINE1", "line1")
	bindEntry(t, c, "01JCONN", "opcua-1", "01HLINE1")
	declareSignal(t, c, "line1/tag-t1", "01SDECLARED", "01HLINE1", map[string]any{"unit": "bar"})

	c.Observe("_DataTags", "colca/v1/_DataTags/n1/line1/opcua-1", mustJSON(map[string]any{"data_tags": tags("t1")}))

	got := signalsAt(c)
	if len(got) != 1 || got["line1/tag-t1"].ID != "01SDECLARED" || got["line1/tag-t1"].DataTag != "t1" {
		t.Fatalf("signals = %+v, want only the declared record, bound to t1", got)
	}
}

// One unplaced participant computing for several machines names, per tag,
// the element its output belongs under. The signal lands there — under that
// element, bound to it — not at the participant's own mount.
func TestATagNamingItsElementIsPlacedThere(t *testing.T) {
	c := newConfigExec(t)
	bindEntry(t, c, "01JDATAOPS", "dataops", "")
	placeElement(t, c, "line1", "01HLINE1")
	placeElement(t, c, "line1/m6", "01HM6")
	placeElement(t, c, "line1/m7", "01HM7")
	publishCatalogue(t, c, "colca/v1/_DataTags/n1/dataops", []map[string]any{
		{"id": "t-m6", "name": "oee", "data_type": "float", "meta": map[string]any{"element": "line1/m6"}},
		{"id": "t-m7", "name": "oee", "data_type": "float", "meta": map[string]any{"element": "line1/m7"}},
		{"id": "t-root", "name": "site_oee", "data_type": "float"},
	})

	code, msg, _ := c.Execute(asHuman, "_CmdConfigure", "signal/autobind", body(t, map[string]any{"connector": "01JDATAOPS"}))
	if code != 200 || !strings.Contains(msg, `"created":3`) {
		t.Fatalf("autobind = %d %q, want 3 created", code, msg)
	}
	got := signalsAt(c)
	if got["line1/m6/oee"].Element != "01HM6" || got["line1/m6/oee"].DataTag != "t-m6" {
		t.Fatalf("line1/m6/oee = %+v, want bound to element 01HM6 and tag t-m6", got["line1/m6/oee"])
	}
	if got["line1/m7/oee"].Element != "01HM7" || got["line1/m7/oee"].DataTag != "t-m7" {
		t.Fatalf("line1/m7/oee = %+v, want bound to element 01HM7 and tag t-m7", got["line1/m7/oee"])
	}
	if got["site_oee"].DataTag != "t-root" {
		t.Fatalf("a tag naming no element still lands at the mount: %+v", got)
	}
	if _, stray := got["oee"]; stray {
		t.Fatalf("an element-naming tag landed at the mount too: %+v", got)
	}
}

// Naming an element the node does not hold leaves the tag unbound — reported,
// never misplaced at the mount, where it would be a second `oee` for the
// wrong machine.
// A tag naming an element this node does not hold yet used to leave the tag
// unbound forever (nothing re-triggers autobind once the catalogue stops
// growing). It now AUTHORS the missing path and binds — the publisher's own
// path becomes tree structure (SDK design §3 rule 2), through the identical
// element-authoring code a local service's declared mount uses.
func TestATagNamingAMissingElementAuthorsItAndBinds(t *testing.T) {
	c := newConfigExec(t)
	bindEntry(t, c, "01JDATAOPS", "dataops", "")
	publishCatalogue(t, c, "colca/v1/_DataTags/n1/dataops", []map[string]any{
		{"id": "t-m6", "name": "oee", "data_type": "float", "meta": map[string]any{"element": "line1/nowhere"}},
	})

	code, msg, _ := c.Execute(asHuman, "_CmdConfigure", "signal/autobind", body(t, map[string]any{"connector": "01JDATAOPS"}))
	if code != 200 || !strings.Contains(msg, `"created":1`) {
		t.Fatalf("autobind = %d %q, want 1 created", code, msg)
	}
	s, ok := signalsAt(c)["line1/nowhere/oee"]
	if !ok {
		t.Fatalf("signals = %+v, want one bound at line1/nowhere/oee", signalsAt(c))
	}
	if s.DataTag != "t-m6" {
		t.Fatalf("signal at line1/nowhere/oee bound to tag %q, want t-m6", s.DataTag)
	}
	elements := elementsUnder(c.store.(*fakeStore), "n1")
	if _, ok := elements["line1"]; !ok {
		t.Fatalf("elements = %+v, want line1 authored along the way", elements)
	}
	if _, ok := elements["line1/nowhere"]; !ok {
		t.Fatalf("elements = %+v, want line1/nowhere authored as the tag's own element", elements)
	}
	if s.Element != elements["line1/nowhere"] {
		t.Fatalf("signal.system_element_id = %q, want the authored line1/nowhere element %q",
			s.Element, elements["line1/nowhere"])
	}
}

// A catalogue can grow after its first publish — an OPC UA connector announces
// its synthetic tags before the browse finishes and the full set afterwards.
// The tags a later publish adds are bound too; the ones already bound are
// left exactly as they were.
func TestACatalogueThatGrowsBindsItsNewTagsOnArrival(t *testing.T) {
	c := newTriggerConfigExec(t)
	place(t, c, "01HLINE1", "line1")
	bindEntry(t, c, "01JCONN", "opcua-1", "01HLINE1")
	declareSignal(t, c, "line1/tag-t2", "01SDECLARED", "01HLINE1", nil)

	c.Observe("_DataTags", "colca/v1/_DataTags/n1/line1/opcua-1", mustJSON(map[string]any{"data_tags": tags("t1")}))
	first := signalsAt(c)["line1/tag-t1"]
	c.Observe("_DataTags", "colca/v1/_DataTags/n1/line1/opcua-1", mustJSON(map[string]any{"data_tags": tags("t1", "t2")}))

	got := signalsAt(c)
	if len(got) != 2 {
		t.Fatalf("signals = %+v, want the first tag's signal and the declared second", got)
	}
	if got["line1/tag-t1"] != first {
		t.Fatalf("the already-bound signal changed on the republish: %+v -> %+v", first, got["line1/tag-t1"])
	}
	if got["line1/tag-t2"].ID != "01SDECLARED" || got["line1/tag-t2"].DataTag != "t2" {
		t.Fatalf("the tag the second publish added was not bound to its declared signal: %+v", got["line1/tag-t2"])
	}
}

// A node authors where it sits, learned from the ancestry its parent teaches.
// Re-applying a generated bootstrap — which states no position, because a
// deployment file cannot know one — must not take that away: the position
// hook does not fire again (the ancestry did not change), so a cleared
// position stays cleared, and every signal on the node then resolves to the
// wrong owner.
func TestAnUpsertOfANodesOwnRecordKeepsTheLearnedPosition(t *testing.T) {
	c := newConfigExec(t)
	learn := func(element string) {
		body := mustJSON(map[string]any{"entities": []map[string]any{{
			"contract": "_Node",
			"entity":   map[string]any{"id": "n1", "name": "edge1", "root_system_element_id": element},
		}}})
		if code, msg, _ := c.Execute(asHuman, "_CmdConfigure", "entity/upsert", body); code != 200 {
			t.Fatalf("entity/upsert = %d %q", code, msg)
		}
	}
	position := func() string {
		raw, ok := c.store.KVGet("colca/v1/_Node/n1/_colca/nodes/n1")
		if !ok {
			t.Fatal("the node has no record of itself")
		}
		var held struct {
			Root string `json:"root_system_element_id"`
			Name string `json:"name"`
		}
		if json.Unmarshal(raw, &held) != nil {
			t.Fatalf("unreadable node record: %s", raw)
		}
		if held.Name == "" {
			t.Fatalf("the upsert lost the record's other fields: %s", raw)
		}
		return held.Root
	}

	learn("01HAREA")
	// A bootstrap re-applied after enrollment: everything the deployment
	// knows, and null where the position is.
	if code, msg, _ := c.Execute(asHuman, "_CmdConfigure", "entity/upsert", mustJSON(map[string]any{
		"entities": []map[string]any{{
			"contract": "_Node",
			"entity":   map[string]any{"id": "n1", "name": "edge1", "root_system_element_id": nil},
		}},
	})); code != 200 {
		t.Fatalf("entity/upsert = %d %q", code, msg)
	}
	if got := position(); got != "01HAREA" {
		t.Fatalf("position = %q after a bootstrap that states none, want the learned 01HAREA", got)
	}

	// A writer that DOES state one still wins — this is how the node's own
	// position hook re-places it after a move.
	learn("01HOTHER")
	if got := position(); got != "01HOTHER" {
		t.Fatalf("position = %q, want the re-taught 01HOTHER", got)
	}
}

// Another node's record is not this node's position to defend: only the
// record whose id IS this node's carries the rule.
func TestAnUpsertOfAnotherNodesRecordIsUntouched(t *testing.T) {
	c := newConfigExec(t)
	body := mustJSON(map[string]any{"entities": []map[string]any{{
		"contract": "_Node",
		"entity":   map[string]any{"id": "n-other", "name": "edge2", "root_system_element_id": nil},
	}}})
	if code, msg, _ := c.Execute(asHuman, "_CmdConfigure", "entity/upsert", body); code != 200 {
		t.Fatalf("entity/upsert = %d %q", code, msg)
	}
	raw, ok := c.store.KVGet("colca/v1/_Node/n1/_colca/nodes/n-other")
	if !ok {
		t.Fatalf("no record written; have %v", c.store.KVScan("_Node", "n1"))
	}
	var held map[string]any
	if json.Unmarshal(raw, &held) != nil || held["root_system_element_id"] != nil {
		t.Fatalf("another node's record was rewritten: %s", raw)
	}
}

// A bootstrap runs on every reconcile-up and re-declares what it already
// declared. The declaration owns what a signal IS; the binding (`data_tag`,
// `is_published`, the learned `data_type`) is runtime state the catalogue
// lifecycle earned. A re-declaration that silently replaced the record whole
// unbound every declared signal of a running node — the demo plant's OEE
// outputs went dark on every `demo up`, with no republish left to rebind them.
func TestReDeclaringABoundSignalKeepsItsBinding(t *testing.T) {
	c := newConfigExec(t)
	place(t, c, "01HLINE1", "line1")
	bindEntry(t, c, "01JCONN", "opcua-1", "01HLINE1")
	declareSignal(t, c, "line1/tag-t1", "01SDECLARED", "01HLINE1", map[string]any{
		"unit": "°C", "description": "Drum temperature",
	})
	publishCatalogue(t, c, "colca/v1/_DataTags/n1/line1/opcua-1", tags("t1"))
	if code, msg, _ := c.Execute(asHuman, "_CmdConfigure", "signal/autobind", body(t, map[string]any{"connector": "01JCONN"})); code != 200 {
		t.Fatalf("autobind = %d %q", code, msg)
	}
	bound := signalRecordAt(t, c, "line1/tag-t1")
	if bound["data_tag"] != "t1" {
		t.Fatalf("precondition: signal not bound after autobind: %+v", bound)
	}

	// The same declaration again — the exact record a bootstrap manifest
	// carries, `data_tag` explicitly null (projection_bootstrap emits it so).
	declareSignal(t, c, "line1/tag-t1", "01SDECLARED", "01HLINE1", map[string]any{
		"unit": "°C", "description": "Drum temperature",
	})

	after := signalRecordAt(t, c, "line1/tag-t1")
	if after["data_tag"] != "t1" {
		t.Fatalf("re-declaration unbound the signal: data_tag=%v (want t1): %+v", after["data_tag"], after)
	}
	if after["is_published"] != true {
		t.Fatalf("re-declaration dropped is_published: %+v", after)
	}
	if after["unit"] != "°C" || after["id"] != "01SDECLARED" {
		t.Fatalf("the declared facts must still be the declaration's: %+v", after)
	}
}

// The preserve rule must not swallow a caller that MEANS to set the binding:
// an upsert naming a non-empty data_tag still wins over the stored one.
func TestAnUpsertNamingATagStillSetsIt(t *testing.T) {
	c := newConfigExec(t)
	place(t, c, "01HLINE1", "line1")
	declareSignal(t, c, "line1/tag-t1", "01SDECLARED", "01HLINE1", nil)
	code, msg, _ := c.Execute(asHuman, "_CmdConfigure", "signal/upsert", body(t, map[string]any{
		"signals": []map[string]any{{"path": "line1/tag-t1", "signal": map[string]any{
			"id": "01SDECLARED", "name": "tag-t1", "data_tag": "t9", "is_published": false,
		}}},
	}))
	if code != 200 {
		t.Fatalf("upsert = %d %q", code, msg)
	}
	after := signalRecordAt(t, c, "line1/tag-t1")
	if after["data_tag"] != "t9" || after["is_published"] != false {
		t.Fatalf("an explicit binding in the upsert must win: %+v", after)
	}
}
