package uns

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"testing"
)

// fakeStore is an in-memory EntityStore: the plugin's whole view of the node.
type fakeStore struct {
	node    string
	records map[string][]byte // topic → payload (nil = tombstoned)
	fail    map[string]string // topic → error to return from Publish
	offset  uint64
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

func (f *fakeStore) Publish(topic string, payload []byte) (StateWrite, error) {
	if msg, bad := f.fail[topic]; bad {
		return StateWrite{}, errString(msg)
	}
	parsed, _ := Parse(topic)
	f.offset++
	write := StateWrite{
		Stream: StreamFor(ClassOf(parsed.Contract)),
		Offset: f.offset,
		Topic:  topic,
	}
	if len(payload) == 0 {
		f.records[topic] = nil
		return write, nil
	}
	f.records[topic] = payload
	return write, nil
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
	c := NewConfigExec(f, nil, nil, nil, nil)

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
	c := NewConfigExec(f, nil, nil, nil, nil)

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
	c := NewConfigExec(f, nil, nil, nil, nil)

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
	c := NewConfigExec(f, nil, nil, nil, nil)

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
	c := NewConfigExec(f, nil, nil, nil, nil)

	code, msg, _ := c.Execute("_CmdConfigure", "signal/delete", body(t, map[string]any{
		"paths": []string{"line1/nothing"},
	}))

	if code != 404 || !strings.Contains(msg, "line1/nothing") {
		t.Fatalf("code %d msg %q — want 404 naming the path", code, msg)
	}
}

// ── autobind ──────────────────────────────────────────────────────────────

// entryFake is one registry entry as the autobind tests need it: who it is
// (its name) and where it is bound (its element, "" for unplaced).
type entryFake struct {
	name, element string
}

// registryFake is a stand-in for the registry: which identity is which
// (EntryOf), and the full list of what's enrolled (Entries, for the lifecycle
// trigger's legitimacy check) — BoundTo is unused here, kept only to satisfy
// Bindings.
type registryFake struct {
	entries map[string]entryFake
}

func newRegistryFake() *registryFake { return &registryFake{entries: map[string]entryFake{}} }

func (r *registryFake) EntryOf(ulid string) (string, string, bool) {
	e, ok := r.entries[ulid]
	return e.name, e.element, ok
}

func (r *registryFake) Entries() []EntryRef {
	out := make([]EntryRef, 0, len(r.entries))
	for ulid, e := range r.entries {
		out = append(out, EntryRef{ULID: ulid, Name: e.name, Element: e.element})
	}
	return out
}

func (r *registryFake) BoundTo(string) []string { return nil }

// fakeNamespace is a stand-in for the node's element index: element id → its
// local path.
type fakeNamespace map[string]string

func (n fakeNamespace) PathOf(elementID string) (string, bool) {
	p, ok := n[elementID]
	return p, ok
}

// counterIDs is a deterministic stand-in for the production ULID minter — a
// counter, not a special-cased production path, so autobind's tests can
// assert on ids without pulling a real ULID library into this package.
func counterIDs() func() string {
	n := 0
	return func() string {
		n++
		return fmt.Sprintf("sig-%d", n)
	}
}

// newConfigExec builds an executor over a fake store, a fake registry and a
// fake namespace — everything autobind needs to COMPUTE a catalogue's topic
// without touching a real tree.
func newConfigExec(t *testing.T) *ConfigExec {
	t.Helper()
	return NewConfigExec(newStore("n1"), newRegistryFake(), fakeNamespace{}, counterIDs(), nil)
}

// newTriggerConfigExec is newConfigExec with the lifecycle trigger enabled.
func newTriggerConfigExec(t *testing.T) *ConfigExec {
	t.Helper()
	return NewConfigExec(newStore("n1"), newRegistryFake(), fakeNamespace{}, counterIDs(),
		map[string]string{"autobind": "on_new_connector"})
}

// place records that elementID sits at path, so PathOf resolves it exactly as
// the real element index would once the element is authored there.
func place(t *testing.T, c *ConfigExec, elementID, path string) {
	t.Helper()
	ns, ok := c.elements.(fakeNamespace)
	if !ok {
		t.Fatalf("place: %T is not a fakeNamespace", c.elements)
	}
	ns[elementID] = path
}

// bindEntry enrolls ulid as an identity named name, bound to element — the
// registry's answer EntryOf(ulid) will give from here on.
func bindEntry(t *testing.T, c *ConfigExec, ulid, name, element string) {
	t.Helper()
	reg, ok := c.bound.(*registryFake)
	if !ok {
		t.Fatalf("bindEntry: %T is not a registryFake", c.bound)
	}
	reg.entries[ulid] = entryFake{name: name, element: element}
}

// publishCatalogue writes a catalogue record directly at topic, bypassing
// autobind entirely — this is what lets the forgery test prove a record
// published anywhere but the computed topic is never read.
func publishCatalogue(t *testing.T, c *ConfigExec, topic string, tags []map[string]any) {
	t.Helper()
	f, ok := c.store.(*fakeStore)
	if !ok {
		t.Fatalf("publishCatalogue: %T is not a fakeStore", c.store)
	}
	if _, err := f.Publish(topic, mustJSON(map[string]any{"data_tags": tags})); err != nil {
		t.Fatal(err)
	}
}

// tags builds a minimal catalogue tag list, one tag per id given.
func tags(ids ...string) []map[string]any {
	out := make([]map[string]any, len(ids))
	for i, id := range ids {
		out[i] = map[string]any{"id": id, "name": "tag-" + id, "data_type": "float"}
	}
	return out
}

// tagsWithIDs is tags, named at call sites where the point is specifically
// that the tag carries its OWN identity — a ULID the connector minted — and
// not a source address (design §6).
func tagsWithIDs(ids ...string) []map[string]any { return tags(ids...) }

// signalsAt is every _Signal record this executor's node holds, by path.
func signalsAt(c *ConfigExec) map[string]boundSignal {
	out := map[string]boundSignal{}
	for _, rec := range c.store.KVScan("_Signal", c.store.NodeID()) {
		var s boundSignal
		if json.Unmarshal(rec.Payload, &s) == nil {
			out[rec.Path] = s
		}
	}
	return out
}

// boundTagIDs is the set of tag ids bound to a signal at this node, sorted.
func boundTagIDs(t *testing.T, c *ConfigExec, connector string) []string {
	t.Helper()
	var out []string
	for _, s := range signalsAt(c) {
		if s.DataTag != "" {
			out = append(out, s.DataTag)
		}
	}
	sort.Strings(out)
	return out
}

// oneSignal returns the sole _Signal record this node holds, failing the test
// if there is not exactly one.
func oneSignal(t *testing.T, c *ConfigExec) boundSignal {
	t.Helper()
	got := signalsAt(c)
	if len(got) != 1 {
		t.Fatalf("signals = %+v, want exactly 1", got)
	}
	for _, s := range got {
		return s
	}
	panic("unreachable")
}

// signalByID finds the _Signal record with the given id.
func signalByID(t *testing.T, c *ConfigExec, id string) boundSignal {
	t.Helper()
	for _, s := range signalsAt(c) {
		if s.ID == id {
			return s
		}
	}
	t.Fatalf("no signal with id %q", id)
	return boundSignal{}
}

// rebind points an already-bound signal at a different tag, the way a person
// curating the model would — through the same signal/upsert door, at the
// signal's own path, keeping its id.
func rebind(t *testing.T, c *ConfigExec, signalID, newTag string) {
	t.Helper()
	for _, rec := range c.store.KVScan("_Signal", c.store.NodeID()) {
		var s boundSignal
		if json.Unmarshal(rec.Payload, &s) != nil || s.ID != signalID {
			continue
		}
		s.DataTag = newTag
		code, msg, _ := c.Execute("_CmdConfigure", "signal/upsert", body(t, map[string]any{
			"signals": []any{map[string]any{"path": rec.Path, "signal": s}},
		}))
		if code != 200 {
			t.Fatalf("rebind: upsert = %d %s", code, msg)
		}
		return
	}
	t.Fatalf("rebind: no signal with id %q", signalID)
}

// The catalogue is found by COMPUTING its topic from the connector's entry, so a
// record published anywhere else is never read — including one that mimics the
// connector's name at a different path.
//
// This is the test a scan-based lookup fails under mutation, and it fails
// NONDETERMINISTICALLY: which of the two same-named records a scan finds
// depends on map iteration order, so a reintroduced scan passes some runs and
// fails others. That flakiness is not a property of this test — it is the
// reverted design's own bug (a search can match more than one record)
// surfacing exactly where it should.
func TestAutobindReadsOnlyTheComputedCatalogueTopic(t *testing.T) {
	c := newConfigExec(t)
	place(t, c, "el-press3", "line1/press3")
	bindEntry(t, c, "01JCONN", "opcua-press", "el-press3")

	publishCatalogue(t, c, "colca/v1/_DataTags/n1/line1/press3/opcua-press", tags("t1"))
	// A forgery: right name, wrong place. Nothing may read it.
	publishCatalogue(t, c, "colca/v1/_DataTags/n1/junk/opcua-press", tags("t9"))

	code, msg, _ := c.Execute("_CmdConfigure", "signal/autobind", []byte(`{"connector":"01JCONN"}`))
	if code != 200 {
		t.Fatalf("autobind = %d %s", code, msg)
	}
	got := boundTagIDs(t, c, "01JCONN")
	if len(got) != 1 || got[0] != "t1" {
		t.Fatalf("bound %v; want only t1 — a record outside the computed topic was read", got)
	}
}

func TestAutobindRefusesAConnectorThisNodeDoesNotHold(t *testing.T) {
	c := newConfigExec(t)
	code, _, _ := c.Execute("_CmdConfigure", "signal/autobind", []byte(`{"connector":"01JNOBODY"}`))
	if code != 404 {
		t.Fatalf("autobind for an unknown connector = %d; want 404 — a parent must not "+
			"guess at a connector only its child holds", code)
	}
}

// An entry bound to an element this node cannot resolve must not silently
// read as unplaced (bound to the node itself, mount ""): PathOf fails closed
// on an unresolvable element, and autobind must too, or it would compute the
// wrong catalogue topic instead of refusing.
func TestAutobindRefusesAConnectorBoundToAnUnresolvableElement(t *testing.T) {
	c := newConfigExec(t)
	// el-ghost is never placed, so PathOf(el-ghost) fails.
	bindEntry(t, c, "01JCONN", "opcua-press", "el-ghost")
	// Planted at the topic a silent fallback-to-unplaced would compute
	// (mount ""): if the unresolvable element were mistaken for "unplaced"
	// instead of refused, autobind would find this and succeed with 200
	// instead of refusing — so a wrong implementation cannot pass by
	// accident of there being no catalogue to read either way.
	publishCatalogue(t, c, "colca/v1/_DataTags/n1/opcua-press", tags("t1"))

	code, msg, result := c.Execute("_CmdConfigure", "signal/autobind", []byte(`{"connector":"01JCONN"}`))
	if code != 409 || result != "conflict" || !strings.Contains(msg, "opcua-press") {
		t.Fatalf("code %d result %q msg %q — want 409/conflict naming the connector", code, result, msg)
	}
}

func TestASignalPointsAtTheTagsIdentityAndKeepsItsOwn(t *testing.T) {
	c := newConfigExec(t)
	place(t, c, "el-press3", "line1/press3")
	bindEntry(t, c, "01JCONN", "opcua-press", "el-press3")
	// The catalogue's tags carry their own ULIDs, minted by the connector.
	publishCatalogue(t, c, "colca/v1/_DataTags/n1/line1/press3/opcua-press", tagsWithIDs("01JTAG1"))
	c.Execute("_CmdConfigure", "signal/autobind", []byte(`{"connector":"01JCONN"}`))

	s := oneSignal(t, c)
	if s.DataTag != "01JTAG1" {
		t.Fatalf("signal.data_tag = %q; want the tag's ULID", s.DataTag)
	}
	if s.ID == "" || strings.Contains(s.ID, "01JTAG1") || strings.Contains(s.ID, "01JCONN") {
		t.Fatalf("signal.id = %q; a signal's identity must be its own, not composed "+
			"from what it is bound to — every Metric carries signal_id", s.ID)
	}
}

// Rebinding — the only mechanism for it is signal/upsert with the same id and
// a different data_tag — must leave the id untouched. Every Metric carries
// signal_id, so an id that moved on rebind would orphan that measurement
// point's whole history under the old one. (Composed-id minting itself is
// TestASignalPointsAtTheTagsIdentityAndKeepsItsOwn's claim, not this one: this
// test pins that upsert preserves whatever id it is given, which is the
// property that actually protects history across a rebind.)
func TestRebindingASignalPreservesItsID(t *testing.T) {
	c := newConfigExec(t)
	place(t, c, "el-press3", "line1/press3")
	bindEntry(t, c, "01JCONN", "opcua-press", "el-press3")
	publishCatalogue(t, c, "colca/v1/_DataTags/n1/line1/press3/opcua-press", tagsWithIDs("01JTAG1"))
	c.Execute("_CmdConfigure", "signal/autobind", []byte(`{"connector":"01JCONN"}`))
	before := oneSignal(t, c)

	rebind(t, c, before.ID, "01JTAG2")

	after := signalByID(t, c, before.ID)
	if after.ID != before.ID {
		t.Fatalf("rebinding changed the signal id %q → %q; every Metric ever published "+
			"for this point references the old one", before.ID, after.ID)
	}
	if after.DataTag != "01JTAG2" {
		t.Fatalf("signal.data_tag = %q; want the new tag", after.DataTag)
	}
}

// The property that lets a person, a replayed command and a lifecycle trigger
// all issue this verb without coordinating.
func TestAutobindIsIdempotent(t *testing.T) {
	c := newConfigExec(t)
	place(t, c, "el-1", "line1/m6")
	bindEntry(t, c, "01JCONN", "opcua-1", "el-1")
	publishCatalogue(t, c, "colca/v1/_DataTags/n1/line1/m6/opcua-1", tags("t1", "t2"))

	c.Execute("_CmdConfigure", "signal/autobind", body(t, map[string]any{"connector": "01JCONN"}))
	before := signalsAt(c)

	code, msg, _ := c.Execute("_CmdConfigure", "signal/autobind", body(t, map[string]any{"connector": "01JCONN"}))

	if code != 200 || !strings.Contains(msg, `"created":0`) || !strings.Contains(msg, `"skipped":2`) {
		t.Fatalf("second run = %d %q, want 0 created / 2 skipped", code, msg)
	}
	if after := signalsAt(c); len(after) != len(before) {
		t.Fatalf("second run changed the model: %d → %d signals", len(before), len(after))
	}
}

// Autobind fills gaps; it never touches a binding someone curated.
func TestAutobindNeverOverwritesAnExistingBinding(t *testing.T) {
	c := newConfigExec(t)
	place(t, c, "el-1", "line1/m6")
	bindEntry(t, c, "01JCONN", "opcua-1", "el-1")
	publishCatalogue(t, c, "colca/v1/_DataTags/n1/line1/m6/opcua-1", tags("t1", "t2"))
	f := c.store.(*fakeStore)
	f.records["colca/v1/_Signal/n1/line1/m6/temperature"] = mustJSON(map[string]any{
		"id": "curated", "name": "temperature", "data_tag": "t1",
		"is_published": true, "precision": 2,
	})

	code, msg, _ := c.Execute("_CmdConfigure", "signal/autobind", body(t, map[string]any{"connector": "01JCONN"}))

	if code != 200 || !strings.Contains(msg, `"created":1`) || !strings.Contains(msg, `"skipped":1`) {
		t.Fatalf("autobind = %d %q, want 1 created / 1 skipped", code, msg)
	}
	curated := f.records["colca/v1/_Signal/n1/line1/m6/temperature"]
	var kept map[string]any
	if err := json.Unmarshal(curated, &kept); err != nil {
		t.Fatal(err)
	}
	if kept["id"] != "curated" || kept["precision"] == nil {
		t.Fatalf("curated binding was rewritten: %s", curated)
	}
}

func TestAutobindWithoutACatalogueIsAConflict(t *testing.T) {
	c := newConfigExec(t)
	bindEntry(t, c, "01JCONN", "opcua-1", "")

	code, msg, result := c.Execute("_CmdConfigure", "signal/autobind", body(t, map[string]any{
		"connector": "01JCONN",
	}))

	// A retry after the connector publishes will succeed, so this is a conflict
	// with the current state and not a malformed request.
	if code != 409 || result != "conflict" || !strings.Contains(msg, "opcua-1") {
		t.Fatalf("code %d result %q msg %q — want 409/conflict naming the connector", code, result, msg)
	}
}

func TestAutobindPlacesSignalsUnderTheGivenPath(t *testing.T) {
	c := newConfigExec(t)
	bindEntry(t, c, "01JCONN", "opcua-1", "")
	publishCatalogue(t, c, "colca/v1/_DataTags/n1/opcua-1", tags("t1"))

	c.Execute("_CmdConfigure", "signal/autobind", body(t, map[string]any{
		"connector": "01JCONN", "under": "line1/m6",
	}))

	if _, ok := signalsAt(c)["line1/m6/tag-t1"]; !ok {
		t.Fatalf("signals = %+v, want one under line1/m6", signalsAt(c))
	}
}

// Browse names are not topic segments: separators and wildcards cannot survive,
// and two tags may still collide afterwards. Nothing may be silently dropped.
func TestAutobindMakesTagNamesAddressable(t *testing.T) {
	c := newConfigExec(t)
	bindEntry(t, c, "01JCONN", "opcua-1", "")
	publishCatalogue(t, c, "colca/v1/_DataTags/n1/opcua-1", []map[string]any{
		{"id": "t1", "name": "Line 1/Temp", "data_type": "float"},
		{"id": "t2", "name": "Line 1#Temp", "data_type": "float"},
		{"id": "t3", "name": "+", "data_type": "float"},
	})

	code, msg, _ := c.Execute("_CmdConfigure", "signal/autobind", body(t, map[string]any{"connector": "01JCONN"}))
	if code != 200 || !strings.Contains(msg, `"created":3`) {
		t.Fatalf("autobind = %d %q, want 3 created", code, msg)
	}

	got := signalsAt(c)
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
	for _, rec := range c.store.KVScan("_Signal", c.store.NodeID()) {
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
	c := newTriggerConfigExec(t)
	bindEntry(t, c, "01JCONN", "opcua-1", "")
	topic := "colca/v1/_DataTags/n1/opcua-1"
	payload := mustJSON(map[string]any{"data_tags": tags("t1")})

	c.Observe("_DataTags", topic, payload)

	if got := signalsAt(c); len(got) != 1 {
		t.Fatalf("signals = %+v, want the catalogue bound on arrival", got)
	}
}

// A record shaped exactly like a real catalogue — even one naming real tag
// ids — triggers nothing if it arrives at a path no enrolled entry's computed
// topic matches. Read scope lets any service see another connector's tag ids,
// so the shape and the ids prove nothing; only an entry's own identity
// computing to this path does.
func TestObserveIgnoresARecordAtAPathNoEntryOwns(t *testing.T) {
	c := newTriggerConfigExec(t)
	bindEntry(t, c, "01JCONN", "opcua-press", "")

	forged := "colca/v1/_DataTags/n1/junk/opcua-press"
	payload := mustJSON(map[string]any{"data_tags": tagsWithIDs("01JTAG1")})
	c.Observe("_DataTags", forged, payload)

	if got := signalsAt(c); len(got) != 0 {
		t.Fatalf("signals = %+v, want none — the trigger fired on a path no entry owns", got)
	}
}

// entryOwns compares the FULL topic, not just the path: a record whose path
// coincidentally matches a local entry's computed path but arrived under a
// DIFFERENT node id — the shape a child's record carries once mount-inserted
// at a parent — must not be treated as that local entry's own catalogue.
func TestObserveIgnoresARecordAtTheRightPathButAnotherNode(t *testing.T) {
	c := newTriggerConfigExec(t)
	bindEntry(t, c, "01JCONN", "opcua-1", "")

	// Same path this entry's own topic would compute to (mount "", name
	// "opcua-1"), but filed under a different node id.
	wrongNode := "colca/v1/_DataTags/n-other/opcua-1"
	payload := mustJSON(map[string]any{"data_tags": tagsWithIDs("01JTAG1")})
	c.Observe("_DataTags", wrongNode, payload)

	if got := signalsAt(c); len(got) != 0 {
		t.Fatalf("signals = %+v, want none — entryOwns matched on path alone, ignoring the node", got)
	}
}

func TestNothingHappensOnArrivalWhenDisabled(t *testing.T) {
	c := newConfigExec(t)
	topic := "colca/v1/_DataTags/n1/opcua-1"
	payload := mustJSON(map[string]any{"data_tags": tags("t1")})

	c.Observe("_DataTags", topic, payload)

	if got := signalsAt(c); len(got) != 0 {
		t.Fatalf("signals = %+v — binding must be opt-in", got)
	}
}

// A connector republishing its catalogue is not a new connector: the trigger
// must not re-bind tags a person has since deleted on purpose.
func TestARepublishedCatalogueIsNotANewConnector(t *testing.T) {
	c := newTriggerConfigExec(t)
	bindEntry(t, c, "01JCONN", "opcua-1", "")
	topic := "colca/v1/_DataTags/n1/opcua-1"
	payload := mustJSON(map[string]any{"data_tags": tags("t1", "t2")})

	c.Observe("_DataTags", topic, payload)
	// The operator deletes one binding deliberately.
	var deletedPath string
	for path, s := range signalsAt(c) {
		if s.DataTag == "t2" {
			deletedPath = path
		}
	}
	if deletedPath == "" {
		t.Fatal("setup: no signal bound to t2")
	}
	c.Execute("_CmdConfigure", "signal/delete", body(t, map[string]any{"paths": []string{deletedPath}}))
	c.Observe("_DataTags", topic, payload)

	for _, s := range signalsAt(c) {
		if s.DataTag == "t2" {
			t.Fatal("a republished catalogue revived a deliberately deleted binding")
		}
	}
}

func TestObserveIgnoresEverythingElse(t *testing.T) {
	c := newTriggerConfigExec(t)
	topic := "colca/v1/_DataTags/n1/opcua-1"

	c.Observe("_Metric", "colca/v1/_Metric/n1/opcua-1/temp", []byte(`{"value":1}`))
	c.Observe("_DataTags", topic, nil) // retired catalogue

	if got := signalsAt(c); len(got) != 0 {
		t.Fatalf("signals = %+v, want none", got)
	}
}

func TestConfigExecClaimsOnlyItsContract(t *testing.T) {
	c := NewConfigExec(newStore("n-edge1"), nil, nil, nil, nil)
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
	c := NewConfigExec(newStore("n-edge1"), nil, nil, nil, nil)
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
	c := NewConfigExec(f, nil, nil, nil, nil)

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
	c := NewConfigExec(f, nil, nil, nil, nil)
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
	c := NewConfigExec(f, nil, nil, nil, nil)
	c.Execute("_CmdConfigure", "element/upsert", elementBody(t, element("line1", "01HLINE1", "Linie 1")))

	code, msg, _ := c.Execute("_CmdConfigure", "element/upsert",
		elementBody(t, element("line1", "01HLINE1", "Linie 1 (Ost)")))

	if code != 200 {
		t.Fatalf("re-upsert = %d %q, want 200", code, msg)
	}
}

func TestElementUpsertRejectsWhatCannotBeAddressed(t *testing.T) {
	f := newStore("n-edge1")
	c := NewConfigExec(f, nil, nil, nil, nil)

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
	c := NewConfigExec(f, nil, nil, nil, nil)
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
	c := NewConfigExec(f, nil, nil, nil, nil)
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

// bindings is a stub registry: which identities stand on which element. It
// satisfies Bindings without carrying entries — the element-delete guard
// tests below only exercise BoundTo.
type bindings map[string][]string

func (b bindings) BoundTo(elementID string) []string     { return b[elementID] }
func (b bindings) EntryOf(string) (string, string, bool) { return "", "", false }
func (b bindings) Entries() []EntryRef                   { return nil }

// Retiring a position an identity binds to would leave that identity able to
// authenticate with nowhere to write — its mount resolves through this very
// element. The refusal names who is in the way.
func TestElementDeleteRefusesWhileAnIdentityBindsToIt(t *testing.T) {
	f := newStore("n-edge1")
	c := NewConfigExec(f, bindings{"01HM6": {"m6-connector"}}, nil, nil, nil)
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
	c := NewConfigExec(f, bindings{"01HOTHER": {"someone-else"}}, nil, nil, nil)
	c.Execute("_CmdConfigure", "element/upsert", elementBody(t, element("line1/m6", "01HM6", "Maschine 6")))

	if code, msg, _ := c.Execute("_CmdConfigure", "element/delete", body(t, map[string]any{
		"paths": []string{"line1/m6"},
	})); code != 200 {
		t.Fatalf("delete = %d (%s), want 200", code, msg)
	}
}

func TestElementDeleteOfAnAbsentElementIs404(t *testing.T) {
	f := newStore("n-edge1")
	c := NewConfigExec(f, nil, nil, nil, nil)

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
	c := NewConfigExec(f, nil, nil, nil, nil)

	code, msg, _ := c.Execute("_CmdConfigure", "definition/upsert", definitionBody(t, "_Group",
		map[string]any{"id": "01HGRP-OPS", "name": "Ops", "grants": []string{"read:01HLINE1/#"}}))

	if code != 200 {
		t.Fatalf("upsert = %d (%s), want 200", code, msg)
	}
	if f.records["colca/v1/_Group/n-global/01HGRP-OPS"] == nil {
		t.Fatalf("definition not written at its id; store holds %v", keysOf(f))
	}
}

func TestPlatformEntityCommandDerivesReservedPathAndReportsStateWrite(t *testing.T) {
	f := newStore("n-local")
	c := NewConfigExec(f, nil, nil, nil, nil)
	payload := body(t, map[string]any{"entities": []map[string]any{{
		"contract": "_Node",
		"entity": map[string]any{
			"id": "n-local", "name": "line-1", "root_system_element_id": "root",
		},
	}}})

	code, msg, _, writes := c.ExecuteWithWrites("_CmdConfigure", "entity/upsert", payload)
	if code != 200 {
		t.Fatalf("entity/upsert = %d (%s), want 200", code, msg)
	}
	wantTopic := "colca/v1/_Node/n-local/_colca/nodes/n-local"
	if f.records[wantTopic] == nil {
		t.Fatalf("node not written at reserved inventory path: %v", keysOf(f))
	}
	if len(writes) != 1 || writes[0].Stream != "entities" || writes[0].Offset != 1 || writes[0].Topic != wantTopic {
		t.Fatalf("state writes = %+v", writes)
	}
}

func TestPlatformEntityCommandRefusesObservedServiceState(t *testing.T) {
	f := newStore("n-local")
	c := NewConfigExec(f, nil, nil, nil, nil)
	payload := body(t, map[string]any{"entities": []map[string]any{{
		"contract": "_ServiceDetails",
		"entity":   map[string]any{"id": "svc-1", "name": "api"},
	}}})

	code, msg, _, writes := c.ExecuteWithWrites("_CmdConfigure", "entity/upsert", payload)
	if code != 422 || !strings.Contains(msg, "own writer") || len(writes) != 0 {
		t.Fatalf("observed service = %d %q writes=%+v, want refused", code, msg, writes)
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
		c := NewConfigExec(f, nil, nil, nil, nil)
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
	c := NewConfigExec(f, nil, nil, nil, nil)

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
	c := NewConfigExec(f, nil, nil, nil, nil)
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
	c := NewConfigExec(f, nil, nil, nil, nil)

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
	c := NewConfigExec(f, nil, nil, nil, nil)

	code, msg, _ := c.Execute("_CmdConfigure", "definition/upsert", definitionBody(t, "_Group",
		map[string]any{"id": "01HGRP-OPS", "name": "Ops", "grants": []string{"read:site1/edge1/#"}}))

	if code != 422 || !strings.Contains(msg, "element") {
		t.Fatalf("code %d msg %q — want 422 explaining that a grant names an element", code, msg)
	}
}
