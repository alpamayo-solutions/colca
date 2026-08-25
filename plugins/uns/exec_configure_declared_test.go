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
	code, msg, _ := c.Execute("_CmdConfigure", "signal/upsert", body(t, map[string]any{
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

	code, msg, _ := c.Execute("_CmdConfigure", "signal/autobind", body(t, map[string]any{"connector": "01JCONN"}))
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

	if code, msg, _ := c.Execute("_CmdConfigure", "signal/autobind", body(t, map[string]any{"connector": "01JCONN"})); code != 200 {
		t.Fatalf("autobind = %d %q", code, msg)
	}
	if got := signalRecordAt(t, c, "line1/tag-t1")["data_type"]; got != "int" {
		t.Fatalf("data_type = %v, want the declared int", got)
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

	if code, msg, _ := c.Execute("_CmdConfigure", "signal/autobind", body(t, map[string]any{"connector": "01JCONN"})); code != 200 {
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

	c.Execute("_CmdConfigure", "signal/autobind", body(t, map[string]any{"connector": "01JCONN"}))
	code, msg, _ := c.Execute("_CmdConfigure", "signal/autobind", body(t, map[string]any{"connector": "01JCONN"}))
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

	code, msg, _ := c.Execute("_CmdConfigure", "signal/autobind", body(t, map[string]any{"connector": "01JDATAOPS"}))
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
func TestATagNamingAMissingElementStaysUnbound(t *testing.T) {
	c := newConfigExec(t)
	bindEntry(t, c, "01JDATAOPS", "dataops", "")
	publishCatalogue(t, c, "colca/v1/_DataTags/n1/dataops", []map[string]any{
		{"id": "t-m6", "name": "oee", "data_type": "float", "meta": map[string]any{"element": "line1/nowhere"}},
	})

	code, msg, _ := c.Execute("_CmdConfigure", "signal/autobind", body(t, map[string]any{"connector": "01JDATAOPS"}))
	if code != 200 || !strings.Contains(msg, `"unplaced":1`) {
		t.Fatalf("autobind = %d %q, want 0 created and 1 unplaced", code, msg)
	}
	if got := signalsAt(c); len(got) != 0 {
		t.Fatalf("signals = %+v, want none", got)
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
