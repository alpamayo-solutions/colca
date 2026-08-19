package uns

import (
	"encoding/json"
	"sort"
	"strings"
	"testing"
)

// fakeStore is an in-memory EntityStore: the plugin's whole view of the node.
type fakeStore struct {
	node    string
	records map[string][]byte // topic → payload (nil = tombstoned)
	fail    map[string]string // topic → error to return from Publish
}

func newStore(node string) *fakeStore {
	return &fakeStore{node: node, records: map[string][]byte{}, fail: map[string]string{}}
}

func (f *fakeStore) NodeID() string { return f.node }

func (f *fakeStore) KVGet(topic string) ([]byte, bool) {
	p, ok := f.records[topic]
	return p, ok && p != nil
}

func (f *fakeStore) KVScan(contract, nodeID string) []KVRecord {
	var out []KVRecord
	for topic, payload := range f.records {
		if payload == nil {
			continue
		}
		p, err := Parse(topic)
		if err != nil || p.Contract != contract || p.NodeID != nodeID {
			continue
		}
		out = append(out, KVRecord{Topic: topic, Path: p.Path, NodeID: p.NodeID, Payload: payload})
	}
	return out
}

func (f *fakeStore) KVScanAll(contract string) []KVRecord {
	var out []KVRecord
	for topic, payload := range f.records {
		if payload == nil {
			continue
		}
		p, err := Parse(topic)
		if err != nil || p.Contract != contract {
			continue
		}
		out = append(out, KVRecord{Topic: topic, Path: p.Path, NodeID: p.NodeID, Payload: payload})
	}
	return out
}

func (f *fakeStore) Publish(topic string, payload []byte) error {
	if msg, bad := f.fail[topic]; bad {
		return errString(msg)
	}
	if len(payload) == 0 {
		f.records[topic] = nil
		return nil
	}
	f.records[topic] = payload
	return nil
}

type errString string

func (e errString) Error() string { return string(e) }

func body(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func withCatalogue(f *fakeStore, connector string, tags ...map[string]any) {
	f.records["colca/v1/_DataTags/"+connector+"/"+connector+"/catalogue"] = mustJSON(map[string]any{
		"connector": connector, "data_tags": tags,
	})
}

func tag(id, name string) map[string]any {
	return map[string]any{"id": id, "name": name, "data_type": "float"}
}

func mustJSON(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}

func signalsUnder(f *fakeStore, node string) map[string]boundSignal {
	out := map[string]boundSignal{}
	for _, rec := range f.KVScan("_Signal", node) {
		var s boundSignal
		if json.Unmarshal(rec.Payload, &s) == nil {
			out[rec.Path] = s
		}
	}
	return out
}

// ── upsert ────────────────────────────────────────────────────────────────

func TestUpsertWritesEachSignalAtItsPath(t *testing.T) {
	f := newStore("n-edge1")
	c := NewConfigExec(f, nil, nil)

	code, msg, _ := c.Execute("_CmdConfigure", "signal/upsert", body(t, map[string]any{
		"signals": []any{
			map[string]any{"path": "line1/m6/temp", "signal": map[string]any{"id": "s1", "name": "temp"}},
			map[string]any{"path": "line1/m6/speed", "signal": map[string]any{"id": "s2", "name": "speed"}},
		},
	}))

	if code != 200 {
		t.Fatalf("upsert = %d %q, want 200", code, msg)
	}
	got := signalsUnder(f, "n-edge1")
	if len(got) != 2 || got["line1/m6/temp"].ID != "s1" || got["line1/m6/speed"].ID != "s2" {
		t.Fatalf("stored signals = %+v", got)
	}
}

func TestUpsertRejectsEntriesItCannotPlace(t *testing.T) {
	f := newStore("n-edge1")
	c := NewConfigExec(f, nil, nil)

	for _, tc := range []struct {
		name string
		body any
	}{
		{"no signals", map[string]any{"signals": []any{}}},
		{"no path", map[string]any{"signals": []any{map[string]any{"signal": map[string]any{"id": "s"}}}}},
		{"no signal", map[string]any{"signals": []any{map[string]any{"path": "a/b"}}}},
	} {
		if code, _, _ := c.Execute("_CmdConfigure", "signal/upsert", body(t, tc.body)); code != 422 {
			t.Errorf("%s: code = %d, want 422", tc.name, code)
		}
	}
	if code, _, _ := c.Execute("_CmdConfigure", "signal/upsert", []byte("{")); code != 422 {
		t.Errorf("unreadable payload: code = %d, want 422", code)
	}
}

// The door validates what the node writes: a signal the bundle rejects comes
// back as an invalid command, naming the entry.
func TestUpsertSurfacesADoorRejection(t *testing.T) {
	f := newStore("n-edge1")
	f.fail["colca/v1/_Signal/n-edge1/line1/bad"] = "validation: missing required field name"
	c := NewConfigExec(f, nil, nil)

	code, msg, _ := c.Execute("_CmdConfigure", "signal/upsert", body(t, map[string]any{
		"signals": []any{map[string]any{"path": "line1/bad", "signal": map[string]any{"id": "s"}}},
	}))

	if code != 422 || !strings.Contains(msg, "line1/bad") || !strings.Contains(msg, "missing required field") {
		t.Fatalf("code %d msg %q — want 422 naming the entry and the cause", code, msg)
	}
}

// ── delete ────────────────────────────────────────────────────────────────

func TestDeleteTombstonesTheRecord(t *testing.T) {
	f := newStore("n-edge1")
	f.records["colca/v1/_Signal/n-edge1/line1/temp"] = mustJSON(map[string]any{"id": "s1", "name": "temp"})
	c := NewConfigExec(f, nil, nil)

	code, _, _ := c.Execute("_CmdConfigure", "signal/delete", body(t, map[string]any{
		"paths": []string{"line1/temp"},
	}))

	if code != 200 {
		t.Fatalf("delete = %d, want 200", code)
	}
	if payload := f.records["colca/v1/_Signal/n-edge1/line1/temp"]; payload != nil {
		t.Fatalf("record still present: %s — a retired path is tombstoned, not blanked", payload)
	}
}

func TestDeleteOfAnAbsentSignalIs404(t *testing.T) {
	f := newStore("n-edge1")
	c := NewConfigExec(f, nil, nil)

	code, msg, _ := c.Execute("_CmdConfigure", "signal/delete", body(t, map[string]any{
		"paths": []string{"line1/nothing"},
	}))

	if code != 404 || !strings.Contains(msg, "line1/nothing") {
		t.Fatalf("code %d msg %q — want 404 naming the path", code, msg)
	}
}

// ── autobind ──────────────────────────────────────────────────────────────

func TestAutobindBindsEveryTagOfAFreshCatalogue(t *testing.T) {
	f := newStore("n-edge1")
	withCatalogue(f, "opcua-1", tag("ns=2;s=Temp", "Temp"), tag("ns=2;s=Speed", "Speed"))
	c := NewConfigExec(f, nil, nil)

	code, msg, _ := c.Execute("_CmdConfigure", "signal/autobind", body(t, map[string]any{
		"connector": "opcua-1",
	}))

	if code != 200 || !strings.Contains(msg, `"created":2`) {
		t.Fatalf("autobind = %d %q, want 200 with 2 created", code, msg)
	}
	got := signalsUnder(f, "n-edge1")
	if len(got) != 2 {
		t.Fatalf("signals = %+v, want 2", got)
	}
	if got["opcua-1/Temp"].TagID != "ns=2;s=Temp" || got["opcua-1/Temp"].Connector != "opcua-1" {
		t.Fatalf("binding not recorded: %+v", got["opcua-1/Temp"])
	}
}

// The property that lets a person, a replayed command and a lifecycle trigger
// all issue this verb without coordinating.
func TestAutobindIsIdempotent(t *testing.T) {
	f := newStore("n-edge1")
	withCatalogue(f, "opcua-1", tag("ns=2;s=Temp", "Temp"), tag("ns=2;s=Speed", "Speed"))
	c := NewConfigExec(f, nil, nil)

	c.Execute("_CmdConfigure", "signal/autobind", body(t, map[string]any{"connector": "opcua-1"}))
	before := signalsUnder(f, "n-edge1")

	code, msg, _ := c.Execute("_CmdConfigure", "signal/autobind", body(t, map[string]any{"connector": "opcua-1"}))

	if code != 200 || !strings.Contains(msg, `"created":0`) || !strings.Contains(msg, `"skipped":2`) {
		t.Fatalf("second run = %d %q, want 0 created / 2 skipped", code, msg)
	}
	if after := signalsUnder(f, "n-edge1"); len(after) != len(before) {
		t.Fatalf("second run changed the model: %d → %d signals", len(before), len(after))
	}
}

// Autobind fills gaps; it never touches a binding someone curated.
func TestAutobindNeverOverwritesAnExistingBinding(t *testing.T) {
	f := newStore("n-edge1")
	withCatalogue(f, "opcua-1", tag("ns=2;s=Temp", "Temp"), tag("ns=2;s=Speed", "Speed"))
	f.records["colca/v1/_Signal/n-edge1/line1/m6/temperature"] = mustJSON(map[string]any{
		"id": "curated", "name": "temperature", "connector": "opcua-1", "tag_id": "ns=2;s=Temp",
		"is_published": true, "precision": 2,
	})
	c := NewConfigExec(f, nil, nil)

	code, msg, _ := c.Execute("_CmdConfigure", "signal/autobind", body(t, map[string]any{"connector": "opcua-1"}))

	if code != 200 || !strings.Contains(msg, `"created":1`) || !strings.Contains(msg, `"skipped":1`) {
		t.Fatalf("autobind = %d %q, want 1 created / 1 skipped", code, msg)
	}
	curated := f.records["colca/v1/_Signal/n-edge1/line1/m6/temperature"]
	var kept map[string]any
	if err := json.Unmarshal(curated, &kept); err != nil {
		t.Fatal(err)
	}
	if kept["id"] != "curated" || kept["precision"] == nil {
		t.Fatalf("curated binding was rewritten: %s", curated)
	}
}

func TestAutobindWithoutACatalogueIsAConflict(t *testing.T) {
	f := newStore("n-edge1")
	c := NewConfigExec(f, nil, nil)

	code, msg, result := c.Execute("_CmdConfigure", "signal/autobind", body(t, map[string]any{
		"connector": "opcua-1",
	}))

	// A retry after the connector publishes will succeed, so this is a conflict
	// with the current state and not a malformed request.
	if code != 409 || result != "conflict" || !strings.Contains(msg, "opcua-1") {
		t.Fatalf("code %d result %q msg %q — want 409/conflict naming the connector", code, result, msg)
	}
}

func TestAutobindPlacesSignalsUnderTheGivenPath(t *testing.T) {
	f := newStore("n-edge1")
	withCatalogue(f, "opcua-1", tag("t1", "Temp"))
	c := NewConfigExec(f, nil, nil)

	c.Execute("_CmdConfigure", "signal/autobind", body(t, map[string]any{
		"connector": "opcua-1", "under": "line1/m6",
	}))

	if _, ok := signalsUnder(f, "n-edge1")["line1/m6/Temp"]; !ok {
		t.Fatalf("signals = %+v, want one under line1/m6", signalsUnder(f, "n-edge1"))
	}
}

// Browse names are not topic segments: separators and wildcards cannot survive,
// and two tags may still collide afterwards. Nothing may be silently dropped.
func TestAutobindMakesTagNamesAddressable(t *testing.T) {
	f := newStore("n-edge1")
	withCatalogue(f, "opcua-1",
		tag("t1", "Line 1/Temp"),
		tag("t2", "Line 1#Temp"),
		tag("t3", "+"),
	)
	c := NewConfigExec(f, nil, nil)

	code, msg, _ := c.Execute("_CmdConfigure", "signal/autobind", body(t, map[string]any{"connector": "opcua-1"}))
	if code != 200 || !strings.Contains(msg, `"created":3`) {
		t.Fatalf("autobind = %d %q, want 3 created", code, msg)
	}

	got := signalsUnder(f, "n-edge1")
	if len(got) != 3 {
		t.Fatalf("3 tags produced %d signals — a collision swallowed one: %+v", len(got), got)
	}
	for path := range got {
		leaf := strings.TrimPrefix(path, "opcua-1/")
		if strings.ContainsAny(leaf, "/+# ") {
			t.Fatalf("path %q is not one addressable segment", path)
		}
	}
	// The original name is not lost, only the addressing form changed.
	for _, rec := range f.KVScan("_Signal", "n-edge1") {
		var s struct {
			Metadata map[string]any `json:"metadata"`
		}
		if json.Unmarshal(rec.Payload, &s) == nil && s.Metadata["tag_name"] == nil {
			t.Fatalf("%s lost its raw tag name", rec.Path)
		}
	}
}

// ── lifecycle trigger ─────────────────────────────────────────────────────

func TestNewConnectorIsBoundOnArrivalWhenEnabled(t *testing.T) {
	f := newStore("n-edge1")
	c := NewConfigExec(f, nil, map[string]string{"autobind": "on_new_connector"})
	withCatalogue(f, "opcua-1", tag("t1", "Temp"))

	c.Observe("_DataTags", "colca/v1/_DataTags/opcua-1/opcua-1/catalogue", f.records["colca/v1/_DataTags/opcua-1/opcua-1/catalogue"])

	if got := signalsUnder(f, "n-edge1"); len(got) != 1 {
		t.Fatalf("signals = %+v, want the catalogue bound on arrival", got)
	}
}

func TestNothingHappensOnArrivalWhenDisabled(t *testing.T) {
	f := newStore("n-edge1")
	c := NewConfigExec(f, nil, nil)
	withCatalogue(f, "opcua-1", tag("t1", "Temp"))

	c.Observe("_DataTags", "colca/v1/_DataTags/opcua-1/opcua-1/catalogue", f.records["colca/v1/_DataTags/opcua-1/opcua-1/catalogue"])

	if got := signalsUnder(f, "n-edge1"); len(got) != 0 {
		t.Fatalf("signals = %+v — binding must be opt-in", got)
	}
}

// A connector republishing its catalogue is not a new connector: the trigger
// must not re-bind tags a person has since deleted on purpose.
func TestARepublishedCatalogueIsNotANewConnector(t *testing.T) {
	f := newStore("n-edge1")
	c := NewConfigExec(f, nil, map[string]string{"autobind": "on_new_connector"})
	withCatalogue(f, "opcua-1", tag("t1", "Temp"), tag("t2", "Speed"))
	topic := "colca/v1/_DataTags/opcua-1/opcua-1/catalogue"

	c.Observe("_DataTags", topic, f.records[topic])
	// The operator deletes one binding deliberately.
	c.Execute("_CmdConfigure", "signal/delete", body(t, map[string]any{"paths": []string{"opcua-1/Speed"}}))
	c.Observe("_DataTags", topic, f.records[topic])

	if _, revived := signalsUnder(f, "n-edge1")["opcua-1/Speed"]; revived {
		t.Fatal("a republished catalogue revived a deliberately deleted binding")
	}
}

func TestObserveIgnoresEverythingElse(t *testing.T) {
	f := newStore("n-edge1")
	c := NewConfigExec(f, nil, map[string]string{"autobind": "on_new_connector"})
	withCatalogue(f, "opcua-1", tag("t1", "Temp"))

	c.Observe("_Metric", "colca/v1/_Metric/opcua-1/opcua-1/temp", []byte(`{"value":1}`))
	c.Observe("_DataTags", "colca/v1/_DataTags/opcua-1/opcua-1/catalogue", nil) // retired catalogue

	if got := signalsUnder(f, "n-edge1"); len(got) != 0 {
		t.Fatalf("signals = %+v, want none", got)
	}
}

func TestConfigExecClaimsOnlyItsContract(t *testing.T) {
	c := NewConfigExec(newStore("n-edge1"), nil, nil)
	if !c.Handles("_CmdConfigure") {
		t.Error("must claim _CmdConfigure")
	}
	for _, other := range []string{"_CmdAdmin", "_CmdParam", "_CmdMaintain", "_Metric"} {
		if c.Handles(other) {
			t.Errorf("must not claim %s", other)
		}
	}
}

func TestUnknownConfigureVerbIsAnswered(t *testing.T) {
	c := NewConfigExec(newStore("n-edge1"), nil, nil)
	code, msg, _ := c.Execute("_CmdConfigure", "signal/rebuild", body(t, map[string]any{}))
	if code != 422 || !strings.Contains(msg, "signal/rebuild") {
		t.Fatalf("code %d msg %q — a command aimed at the node deserves an answer", code, msg)
	}
}

// ── elements ──────────────────────────────────────────────────────────────

func elementBody(t *testing.T, entries ...map[string]any) []byte {
	t.Helper()
	return body(t, map[string]any{"elements": entries})
}

func element(path, id, name string) map[string]any {
	return map[string]any{"path": path, "element": map[string]any{"id": id, "name": name}}
}

func elementsUnder(f *fakeStore, node string) map[string]string {
	out := map[string]string{}
	for _, rec := range f.KVScan("_SystemElement", node) {
		var e struct {
			ID string `json:"id"`
		}
		if json.Unmarshal(rec.Payload, &e) == nil {
			out[rec.Path] = e.ID
		}
	}
	return out
}

func TestElementUpsertWritesEachElementAtItsPath(t *testing.T) {
	f := newStore("n-edge1")
	c := NewConfigExec(f, nil, nil)

	code, msg, _ := c.Execute("_CmdConfigure", "element/upsert", elementBody(t,
		element("line1", "01HLINE1", "Linie 1"),
		element("line1/m6", "01HM6", "Maschine 6"),
	))

	if code != 200 {
		t.Fatalf("upsert = %d %q, want 200", code, msg)
	}
	got := elementsUnder(f, "n-edge1")
	if got["line1"] != "01HLINE1" || got["line1/m6"] != "01HM6" {
		t.Fatalf("stored elements = %+v", got)
	}
}

// Two elements at one path cannot both be addressed, so the second is a
// conflict with the current state rather than a malformed request.
func TestElementUpsertRefusesACollidingSibling(t *testing.T) {
	f := newStore("n-edge1")
	c := NewConfigExec(f, nil, nil)
	c.Execute("_CmdConfigure", "element/upsert", elementBody(t, element("line1", "01HLINE1", "Linie 1")))

	code, msg, result := c.Execute("_CmdConfigure", "element/upsert",
		elementBody(t, element("line1", "01HOTHER", "Linie 1")))

	if code != 409 || result != "conflict" || !strings.Contains(msg, "line1") {
		t.Fatalf("code %d result %q msg %q — want 409 naming the path", code, result, msg)
	}
	if got := elementsUnder(f, "n-edge1")["line1"]; got != "01HLINE1" {
		t.Fatalf("the colliding write landed anyway: %s", got)
	}
}

// Re-upserting the SAME element at its own path is how a rename or an edit
// arrives — it must not be mistaken for a collision.
func TestElementUpsertOfTheSameElementIsNotACollision(t *testing.T) {
	f := newStore("n-edge1")
	c := NewConfigExec(f, nil, nil)
	c.Execute("_CmdConfigure", "element/upsert", elementBody(t, element("line1", "01HLINE1", "Linie 1")))

	code, msg, _ := c.Execute("_CmdConfigure", "element/upsert",
		elementBody(t, element("line1", "01HLINE1", "Linie 1 (Ost)")))

	if code != 200 {
		t.Fatalf("re-upsert = %d %q, want 200", code, msg)
	}
}

func TestElementUpsertRejectsWhatCannotBeAddressed(t *testing.T) {
	f := newStore("n-edge1")
	c := NewConfigExec(f, nil, nil)

	for _, tc := range []struct {
		name  string
		entry map[string]any
	}{
		{"no path", map[string]any{"element": map[string]any{"id": "x", "name": "X"}}},
		{"no element", map[string]any{"path": "line1"}},
		{"no id", map[string]any{"path": "line1", "element": map[string]any{"name": "X"}}},
	} {
		if code, _, _ := c.Execute("_CmdConfigure", "element/upsert", elementBody(t, tc.entry)); code != 422 {
			t.Errorf("%s: code = %d, want 422", tc.name, code)
		}
	}
}

func TestElementDeleteTombstonesTheRecord(t *testing.T) {
	f := newStore("n-edge1")
	c := NewConfigExec(f, nil, nil)
	c.Execute("_CmdConfigure", "element/upsert", elementBody(t, element("line1", "01HLINE1", "Linie 1")))

	code, _, _ := c.Execute("_CmdConfigure", "element/delete", body(t, map[string]any{
		"paths": []string{"line1"},
	}))

	if code != 200 {
		t.Fatalf("delete = %d, want 200", code)
	}
	if payload := f.records["colca/v1/_SystemElement/n-edge1/line1"]; payload != nil {
		t.Fatalf("record still present: %s", payload)
	}
}

// Deleting an element that still holds children would strand them: their paths
// keep working while the position above them is gone, and any grant naming the
// parent goes inert.
func TestElementDeleteRefusesWhileChildrenRemain(t *testing.T) {
	f := newStore("n-edge1")
	c := NewConfigExec(f, nil, nil)
	c.Execute("_CmdConfigure", "element/upsert", elementBody(t,
		element("line1", "01HLINE1", "Linie 1"),
		element("line1/m6", "01HM6", "Maschine 6"),
	))

	code, msg, result := c.Execute("_CmdConfigure", "element/delete", body(t, map[string]any{
		"paths": []string{"line1"},
	}))

	if code != 409 || result != "conflict" || !strings.Contains(msg, "line1/m6") {
		t.Fatalf("code %d result %q msg %q — want 409 naming what still binds", code, result, msg)
	}
	if f.records["colca/v1/_SystemElement/n-edge1/line1"] == nil {
		t.Fatal("the refused delete removed the record anyway")
	}
}

// bindings is a stub registry: which identities stand on which element.
type bindings map[string][]string

func (b bindings) BoundTo(elementID string) []string { return b[elementID] }

// Retiring a position an identity binds to would leave that identity able to
// authenticate with nowhere to write — its mount resolves through this very
// element. The refusal names who is in the way.
func TestElementDeleteRefusesWhileAnIdentityBindsToIt(t *testing.T) {
	f := newStore("n-edge1")
	c := NewConfigExec(f, bindings{"01HM6": {"m6-connector"}}, nil)
	c.Execute("_CmdConfigure", "element/upsert", elementBody(t, element("line1/m6", "01HM6", "Maschine 6")))

	code, msg, result := c.Execute("_CmdConfigure", "element/delete", body(t, map[string]any{
		"paths": []string{"line1/m6"},
	}))

	if code != 409 || result != "conflict" || !strings.Contains(msg, "m6-connector") {
		t.Fatalf("code %d result %q msg %q — want 409 naming the bound identity", code, result, msg)
	}
	if f.records["colca/v1/_SystemElement/n-edge1/line1/m6"] == nil {
		t.Fatal("the refused delete removed the record anyway")
	}
}

// The same position with nothing standing on it retires normally — the guard
// must gate on an actual binding, not on the presence of a registry.
func TestElementDeleteProceedsWhenNothingBindsToIt(t *testing.T) {
	f := newStore("n-edge1")
	c := NewConfigExec(f, bindings{"01HOTHER": {"someone-else"}}, nil)
	c.Execute("_CmdConfigure", "element/upsert", elementBody(t, element("line1/m6", "01HM6", "Maschine 6")))

	if code, msg, _ := c.Execute("_CmdConfigure", "element/delete", body(t, map[string]any{
		"paths": []string{"line1/m6"},
	})); code != 200 {
		t.Fatalf("delete = %d (%s), want 200", code, msg)
	}
}

func TestElementDeleteOfAnAbsentElementIs404(t *testing.T) {
	f := newStore("n-edge1")
	c := NewConfigExec(f, nil, nil)

	code, msg, _ := c.Execute("_CmdConfigure", "element/delete", body(t, map[string]any{
		"paths": []string{"nothing"},
	}))

	if code != 404 || !strings.Contains(msg, "nothing") {
		t.Fatalf("code %d msg %q — want 404 naming the path", code, msg)
	}
}

// definitionBody builds a definition/upsert body.
func definitionBody(t *testing.T, contract string, defs ...map[string]any) []byte {
	t.Helper()
	refs := make([]map[string]any, 0, len(defs))
	for _, d := range defs {
		refs = append(refs, map[string]any{"contract": contract, "definition": d})
	}
	return body(t, map[string]any{"definitions": refs})
}

func keysOf(f *fakeStore) []string {
	var out []string
	for k, v := range f.records {
		if v != nil {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}

// A definition is filed under its own id, at no position at all — its topic
// carries the authoring node and the id and nothing else (design §3).
func TestDefinitionUpsertFilesUnderTheIdWithNoPosition(t *testing.T) {
	f := newStore("n-global")
	c := NewConfigExec(f, nil, nil)

	code, msg, _ := c.Execute("_CmdConfigure", "definition/upsert", definitionBody(t, "_Group",
		map[string]any{"id": "01HGRP-OPS", "name": "Ops", "grants": []string{"read:01HLINE1/#"}}))

	if code != 200 {
		t.Fatalf("upsert = %d (%s), want 200", code, msg)
	}
	if f.records["colca/v1/_Group/n-global/01HGRP-OPS"] == nil {
		t.Fatalf("definition not written at its id; store holds %v", keysOf(f))
	}
}

func TestDefinitionUpsertRefusesWhatItCannotAddress(t *testing.T) {
	cases := []struct {
		name string
		body []byte
		want string
	}{
		{"no id", definitionBody(t, "_Group", map[string]any{"name": "Ops"}), "id"},
		{"id is a path", definitionBody(t, "_Group",
			map[string]any{"id": "line1/ops", "name": "Ops"}), "position"},
		{"id has a wildcard", definitionBody(t, "_Group",
			map[string]any{"id": "ops#", "name": "Ops"}), "position"},
	}
	for _, tc := range cases {
		f := newStore("n-global")
		c := NewConfigExec(f, nil, nil)
		code, msg, result := c.Execute("_CmdConfigure", "definition/upsert", tc.body)
		if code != 422 || result != "invalid" {
			t.Errorf("%s: code %d result %q, want 422/invalid", tc.name, code, result)
		}
		if !strings.Contains(msg, tc.want) {
			t.Errorf("%s: message %q must explain the defect (%q)", tc.name, msg, tc.want)
		}
	}
}

// This door authors definitions. An element or a metric arriving here is a
// caller at the wrong door, and saying so beats filing the record somewhere odd.
func TestDefinitionUpsertRefusesAContractThatIsNotADefinition(t *testing.T) {
	f := newStore("n-global")
	c := NewConfigExec(f, nil, nil)

	for _, contract := range []string{"_SystemElement", "_Metric", "_CmdParam", ""} {
		code, msg, _ := c.Execute("_CmdConfigure", "definition/upsert", definitionBody(t, contract,
			map[string]any{"id": "01HX", "name": "x"}))
		if code != 422 {
			t.Errorf("%s: code %d, want 422 (%s)", contract, code, msg)
		}
	}
	if got := keysOf(f); len(got) != 0 {
		t.Fatalf("a refused definition must write nothing: %v", got)
	}
}

func TestDefinitionDeleteTombstonesAndReportsAbsence(t *testing.T) {
	f := newStore("n-global")
	c := NewConfigExec(f, nil, nil)
	c.Execute("_CmdConfigure", "definition/upsert", definitionBody(t, "_Group",
		map[string]any{"id": "01HGRP-OPS", "name": "Ops"}))

	del := body(t, map[string]any{"definitions": []map[string]any{
		{"contract": "_Group", "id": "01HGRP-OPS"}}})
	if code, msg, _ := c.Execute("_CmdConfigure", "definition/delete", del); code != 200 {
		t.Fatalf("delete = %d (%s), want 200", code, msg)
	}
	if payload := f.records["colca/v1/_Group/n-global/01HGRP-OPS"]; payload != nil {
		t.Fatalf("definition still present: %s", payload)
	}
	// Retracting what is not there is a 404 naming it, not a silent success.
	code, msg, _ := c.Execute("_CmdConfigure", "definition/delete", del)
	if code != 404 || !strings.Contains(msg, "01HGRP-OPS") {
		t.Fatalf("second delete = %d (%s), want 404 naming the id", code, msg)
	}
}

// A group carries grant strings, and a malformed one must die at the door: the
// definition is about to descend to every node below, and each of them would
// otherwise drop the bad grant and log it for as long as the definition exists.
func TestDefinitionUpsertRefusesAGroupWithAMalformedGrant(t *testing.T) {
	f := newStore("n-global")
	c := NewConfigExec(f, nil, nil)

	code, msg, result := c.Execute("_CmdConfigure", "definition/upsert", definitionBody(t, "_Group",
		map[string]any{"id": "01HGRP-OPS", "name": "Ops",
			"grants": []string{"read:01HLINE1/#", "cmd:01HLINE1/#"}}))

	if code != 422 || result != "invalid" {
		t.Fatalf("code %d result %q, want 422/invalid", code, result)
	}
	if !strings.Contains(msg, "cmd:") {
		t.Fatalf("message %q must name the offending grant", msg)
	}
	if got := keysOf(f); len(got) != 0 {
		t.Fatalf("a refused definition must write nothing: %v", got)
	}
}

// A path-shaped zone inside a group is the same mistake as anywhere else, and
// it is refused at the same door.
func TestDefinitionUpsertRefusesAGroupGrantNamingAPath(t *testing.T) {
	f := newStore("n-global")
	c := NewConfigExec(f, nil, nil)

	code, msg, _ := c.Execute("_CmdConfigure", "definition/upsert", definitionBody(t, "_Group",
		map[string]any{"id": "01HGRP-OPS", "name": "Ops", "grants": []string{"read:site1/edge1/#"}}))

	if code != 422 || !strings.Contains(msg, "element") {
		t.Fatalf("code %d msg %q — want 422 explaining that a grant names an element", code, msg)
	}
}
