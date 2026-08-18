package uns

import (
	"encoding/json"
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
	c := NewConfigExec(f, nil)

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
	c := NewConfigExec(f, nil)

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
	c := NewConfigExec(f, nil)

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
	c := NewConfigExec(f, nil)

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
	c := NewConfigExec(f, nil)

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
	c := NewConfigExec(f, nil)

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
	c := NewConfigExec(f, nil)

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
	c := NewConfigExec(f, nil)

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
	c := NewConfigExec(f, nil)

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
	c := NewConfigExec(f, nil)

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
	c := NewConfigExec(f, nil)

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
	c := NewConfigExec(f, map[string]string{"autobind": "on_new_connector"})
	withCatalogue(f, "opcua-1", tag("t1", "Temp"))

	c.Observe("_DataTags", "colca/v1/_DataTags/opcua-1/opcua-1/catalogue", f.records["colca/v1/_DataTags/opcua-1/opcua-1/catalogue"])

	if got := signalsUnder(f, "n-edge1"); len(got) != 1 {
		t.Fatalf("signals = %+v, want the catalogue bound on arrival", got)
	}
}

func TestNothingHappensOnArrivalWhenDisabled(t *testing.T) {
	f := newStore("n-edge1")
	c := NewConfigExec(f, nil)
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
	c := NewConfigExec(f, map[string]string{"autobind": "on_new_connector"})
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
	c := NewConfigExec(f, map[string]string{"autobind": "on_new_connector"})
	withCatalogue(f, "opcua-1", tag("t1", "Temp"))

	c.Observe("_Metric", "colca/v1/_Metric/opcua-1/opcua-1/temp", []byte(`{"value":1}`))
	c.Observe("_DataTags", "colca/v1/_DataTags/opcua-1/opcua-1/catalogue", nil) // retired catalogue

	if got := signalsUnder(f, "n-edge1"); len(got) != 0 {
		t.Fatalf("signals = %+v, want none", got)
	}
}

func TestConfigExecClaimsOnlyItsContract(t *testing.T) {
	c := NewConfigExec(newStore("n-edge1"), nil)
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
	c := NewConfigExec(newStore("n-edge1"), nil)
	code, msg, _ := c.Execute("_CmdConfigure", "signal/rebuild", body(t, map[string]any{}))
	if code != 422 || !strings.Contains(msg, "signal/rebuild") {
		t.Fatalf("code %d msg %q — a command aimed at the node deserves an answer", code, msg)
	}
}
