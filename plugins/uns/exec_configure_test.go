package uns

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"
)

// fakeStore is an in-memory EntityStore: the plugin's whole view of the node.
type fakeStore struct {
	node          string
	records       map[string][]byte // topic → payload (nil = tombstoned)
	recordOffsets map[string]uint64 // topic → current retained version
	fail          map[string]string // topic → error to return from Publish
	offset        uint64
	batchCalls    int
	// eventCalls counts PublishEvent calls, the path annotation records take.
	eventCalls int
}

func newStore(node string) *fakeStore {
	return &fakeStore{
		node: node, records: map[string][]byte{}, recordOffsets: map[string]uint64{}, fail: map[string]string{},
	}
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
		out = append(out, KVRecord{
			Topic: topic, Path: p.Path, NodeID: p.NodeID, Payload: payload, Offset: f.recordOffsets[topic],
		})
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
		out = append(out, KVRecord{
			Topic: topic, Path: p.Path, NodeID: p.NodeID, Payload: payload, Offset: f.recordOffsets[topic],
		})
	}
	return out
}

// put applies one record the way the store does once its batch is accepted.
// It is not part of EntityStore; tests seed through seed().
func (f *fakeStore) put(topic string, payload []byte) (StateWrite, error) {
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
		delete(f.recordOffsets, topic)
		return write, nil
	}
	f.records[topic] = payload
	f.recordOffsets[topic] = write.Offset
	return write, nil
}

// PublishBatch mirrors the engine's commit: every record is validated before
// any is applied, and a refusal names the refused record.
func (f *fakeStore) PublishBatch(records []StateRecord) ([]StateWrite, error) {
	for i, record := range records {
		if msg, bad := f.fail[record.Topic]; bad {
			return nil, errString(fmt.Sprintf("state batch record %d (%s): %s", i, record.Topic, msg))
		}
	}
	f.batchCalls++
	writes := make([]StateWrite, 0, len(records))
	for _, record := range records {
		write, err := f.put(record.Topic, record.Payload)
		if err != nil {
			return nil, err
		}
		writes = append(writes, write)
	}
	return writes, nil
}

// PublishEvent mirrors the engine's event door: one record, never written to
// f.records, because events are never KV-projected. A KVGet miss after it is
// the real behaviour, not a quirk of the fake.
func (f *fakeStore) PublishEvent(record StateRecord) (StateWrite, error) {
	if msg, bad := f.fail[record.Topic]; bad {
		return StateWrite{}, errString(msg)
	}
	parsed, err := Parse(record.Topic)
	if err != nil {
		return StateWrite{}, err
	}
	f.offset++
	f.eventCalls++
	return StateWrite{Stream: StreamFor(ClassOf(parsed.Contract)), Offset: f.offset, Topic: record.Topic}, nil
}

// seed stores one record as setup and returns its coordinates. It bypasses
// PublishBatch so tests that count commits do not count fixtures.
func (f *fakeStore) seed(topic string, payload []byte) (StateWrite, error) {
	return f.put(topic, payload)
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
	c := NewConfigExec(f, nil, nil, nil, nil, nil)

	code, msg, _ := c.Execute(asHuman, "_CmdConfigure", "signal/upsert", body(t, map[string]any{
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
	c := NewConfigExec(f, nil, nil, nil, nil, nil)

	for _, tc := range []struct {
		name string
		body any
	}{
		{"no signals", map[string]any{"signals": []any{}}},
		{"no path", map[string]any{"signals": []any{map[string]any{"signal": map[string]any{"id": "s"}}}}},
		{"no signal", map[string]any{"signals": []any{map[string]any{"path": "a/b"}}}},
	} {
		if code, _, _ := c.Execute(asHuman, "_CmdConfigure", "signal/upsert", body(t, tc.body)); code != 422 {
			t.Errorf("%s: code = %d, want 422", tc.name, code)
		}
	}
	if code, _, _ := c.Execute(asHuman, "_CmdConfigure", "signal/upsert", []byte("{")); code != 422 {
		t.Errorf("unreadable payload: code = %d, want 422", code)
	}
}

// The door validates what the node writes: a signal the bundle rejects comes
// back as an invalid command, naming the entry.
func TestUpsertSurfacesADoorRejection(t *testing.T) {
	f := newStore("n-edge1")
	f.fail["colca/v1/_Signal/n-edge1/line1/bad"] = "validation: missing required field name"
	c := NewConfigExec(f, nil, nil, nil, nil, nil)

	code, msg, _ := c.Execute(asHuman, "_CmdConfigure", "signal/upsert", body(t, map[string]any{
		"signals": []any{map[string]any{"path": "line1/bad", "signal": map[string]any{"id": "s"}}},
	}))

	if code != 422 || !strings.Contains(msg, "line1/bad") || !strings.Contains(msg, "missing required field") {
		t.Fatalf("code %d msg %q — want 422 naming the entry and the cause", code, msg)
	}
}

// A command is one state transition: if the door refuses one record, nothing
// from the command is committed.
func TestARefusedRecordLeavesTheWholeCommandUncommitted(t *testing.T) {
	f := newStore("n-edge1")
	// The door refuses the third signal; the 422 below proves the seed took
	// effect.
	f.fail["colca/v1/_Signal/n-edge1/line1/third"] = "validation: missing required field name"
	c := NewConfigExec(f, nil, nil, nil, nil, nil)
	command := body(t, map[string]any{
		"signals": []any{
			map[string]any{"path": "line1/first", "signal": map[string]any{"id": "s1", "name": "first"}},
			map[string]any{"path": "line1/second", "signal": map[string]any{"id": "s2", "name": "second"}},
			map[string]any{"path": "line1/third", "signal": map[string]any{"id": "s3", "name": "third"}},
		},
	})

	code, msg, result, writes := c.ExecuteWithWrites(asHuman, "_CmdConfigure", "signal/upsert", command)

	if code != 422 || result != "invalid" {
		t.Fatalf("code %d result %q msg %q — want 422/invalid; the seeded refusal did not bite", code, result, msg)
	}
	if !strings.Contains(msg, "line1/third") || !strings.Contains(msg, "missing required field") {
		t.Errorf("msg %q — want the refused entry and the cause named", msg)
	}
	if len(writes) != 0 {
		t.Errorf("reported writes = %+v, want none: nothing was committed", writes)
	}
	if got := signalsUnder(f, "n-edge1"); len(got) != 0 {
		t.Fatalf("the refused command left %d signal(s) in KV: %+v — the two records "+
			"before the refused one must not survive it", len(got), got)
	}
	if f.offset != 0 {
		t.Fatalf("the refused command took %d stream position(s); want 0", f.offset)
	}
}

// An accepted command commits as one batch, not one batch per record.
func TestAnAcceptedCommandCommitsEveryRecordInOneTransition(t *testing.T) {
	f := newStore("n-edge1")
	c := NewConfigExec(f, nil, nil, nil, nil, nil)

	code, msg, _, writes := c.ExecuteWithWrites(asHuman, "_CmdConfigure", "signal/upsert", body(t, map[string]any{
		"signals": []any{
			map[string]any{"path": "line1/first", "signal": map[string]any{"id": "s1", "name": "first"}},
			map[string]any{"path": "line1/second", "signal": map[string]any{"id": "s2", "name": "second"}},
			map[string]any{"path": "line1/third", "signal": map[string]any{"id": "s3", "name": "third"}},
		},
	}))

	if code != 200 {
		t.Fatalf("upsert = %d %q, want 200", code, msg)
	}
	if f.batchCalls != 1 {
		t.Fatalf("the command committed in %d transitions, want 1", f.batchCalls)
	}
	if len(writes) != 3 {
		t.Fatalf("reported writes = %+v, want one per record", writes)
	}
	if got := signalsUnder(f, "n-edge1"); len(got) != 3 {
		t.Fatalf("stored signals = %+v, want all three", got)
	}
}

// A refusal part-way through autobind leaves the connector entirely unbound.
func TestARefusedAutobindBindsNothing(t *testing.T) {
	c := newConfigExec(t)
	f, ok := c.store.(*fakeStore)
	if !ok {
		t.Fatalf("store is %T", c.store)
	}
	place(t, c, "el-press3", "line1/press3")
	bindEntry(t, c, "01JCONN", "opcua-press", "el-press3")
	// The catalogue's third tag is the one whose signal the door refuses.
	f.fail["colca/v1/_Signal/n1/line1/press3/tag-01JTAG3"] = "validation: unknown data_type"
	publishCatalogue(t, c, "colca/v1/_DataTags/n1/line1/press3/opcua-press",
		tags("01JTAG1", "01JTAG2", "01JTAG3"))

	code, msg, _ := c.Execute(asHuman, "_CmdConfigure", "signal/autobind", []byte(`{"connector":"01JCONN"}`))

	if code != 422 {
		t.Fatalf("autobind = %d %q, want 422 — the seeded refusal did not bite", code, msg)
	}
	// Check the specific refusal: any other failure would also give a
	// 422.
	if !strings.Contains(msg, "unknown data_type") || !strings.Contains(msg, "tag-01JTAG3") {
		t.Fatalf("autobind refusal = %q, want it to name the seeded refusal (unknown data_type) "+
			"on tag-01JTAG3", msg)
	}
	if got := signalsAt(c); len(got) != 0 {
		t.Fatalf("the refused autobind bound %d tag(s): %+v — a half-bound connector "+
			"is exactly what committing per record produced", len(got), got)
	}
}

// ── delete ────────────────────────────────────────────────────────────────

func TestDeleteTombstonesTheRecord(t *testing.T) {
	f := newStore("n-edge1")
	f.records["colca/v1/_Signal/n-edge1/line1/temp"] = mustJSON(map[string]any{"id": "s1", "name": "temp"})
	c := NewConfigExec(f, nil, nil, nil, nil, nil)

	code, _, _ := c.Execute(asHuman, "_CmdConfigure", "signal/delete", body(t, map[string]any{
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
	c := NewConfigExec(f, nil, nil, nil, nil, nil)

	code, msg, _ := c.Execute(asHuman, "_CmdConfigure", "signal/delete", body(t, map[string]any{
		"paths": []string{"line1/nothing"},
	}))

	if code != 404 || !strings.Contains(msg, "line1/nothing") {
		t.Fatalf("code %d msg %q — want 404 naming the path", code, msg)
	}
}

// ── autobind ──────────────────────────────────────────────────────────────

// entryFake is a registry entry as the autobind tests need it: its name and
// its element ("" for unplaced).
type entryFake struct {
	name, element string
}

// registryFake stands in for the registry: EntryOf and Entries. BoundTo only
// satisfies Bindings.
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

// fakeNamespace maps element ids to local paths.
type fakeNamespace map[string]string

func (n fakeNamespace) PathOf(elementID string) (string, bool) {
	p, ok := n[elementID]
	return p, ok
}

// counterIDs returns a counter in place of the ULID minter, so tests can
// assert on ids.
func counterIDs() func() string {
	n := 0
	return func() string {
		n++
		return fmt.Sprintf("sig-%d", n)
	}
}

// newConfigExec builds an executor over a fake store, registry and namespace.
func newConfigExec(t *testing.T) *ConfigExec {
	t.Helper()
	return NewConfigExec(newStore("n1"), newRegistryFake(), fakeNamespace{}, nil, counterIDs(), nil)
}

// newTriggerConfigExec is newConfigExec with the lifecycle trigger enabled.
func newTriggerConfigExec(t *testing.T) *ConfigExec {
	t.Helper()
	return NewConfigExec(newStore("n1"), newRegistryFake(), fakeNamespace{}, nil, counterIDs(),
		map[string]string{"autobind": "on_new_connector"})
}

// place records that elementID sits at path, as the element index would.
func place(t *testing.T, c *ConfigExec, elementID, path string) {
	t.Helper()
	ns, ok := c.elements.(fakeNamespace)
	if !ok {
		t.Fatalf("place: %T is not a fakeNamespace", c.elements)
	}
	ns[elementID] = path
}

// bindEntry enrolls ulid as an identity named name, bound to element.
func bindEntry(t *testing.T, c *ConfigExec, ulid, name, element string) {
	t.Helper()
	reg, ok := c.bound.(*registryFake)
	if !ok {
		t.Fatalf("bindEntry: %T is not a registryFake", c.bound)
	}
	reg.entries[ulid] = entryFake{name: name, element: element}
}

// publishCatalogue writes a catalogue record directly at topic, bypassing
// autobind, so tests can plant records at the wrong topic.
func publishCatalogue(t *testing.T, c *ConfigExec, topic string, tags []map[string]any) {
	t.Helper()
	f, ok := c.store.(*fakeStore)
	if !ok {
		t.Fatalf("publishCatalogue: %T is not a fakeStore", c.store)
	}
	if _, err := f.seed(topic, mustJSON(map[string]any{"data_tags": tags})); err != nil {
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

// tagsWithIDs is tags, used where the point is that each tag carries its own
// ULID rather than a source address.
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

// rebind points a bound signal at a different tag through signal/upsert,
// keeping its id and path.
func rebind(t *testing.T, c *ConfigExec, signalID, newTag string) {
	t.Helper()
	for _, rec := range c.store.KVScan("_Signal", c.store.NodeID()) {
		var s boundSignal
		if json.Unmarshal(rec.Payload, &s) != nil || s.ID != signalID {
			continue
		}
		s.DataTag = newTag
		code, msg, _ := c.Execute(asHuman, "_CmdConfigure", "signal/upsert", body(t, map[string]any{
			"signals": []any{map[string]any{"path": rec.Path, "signal": s}},
		}))
		if code != 200 {
			t.Fatalf("rebind: upsert = %d %s", code, msg)
		}
		return
	}
	t.Fatalf("rebind: no signal with id %q", signalID)
}

// The catalogue is found by computing its topic from the connector's entry,
// so a record elsewhere is never read, even one that copies the connector's
// name at another path. A scan-based lookup would fail this test, though not
// on every run: which record it finds depends on map order.
func TestAutobindReadsOnlyTheComputedCatalogueTopic(t *testing.T) {
	c := newConfigExec(t)
	place(t, c, "el-press3", "line1/press3")
	bindEntry(t, c, "01JCONN", "opcua-press", "el-press3")

	publishCatalogue(t, c, "colca/v1/_DataTags/n1/line1/press3/opcua-press", tags("t1"))
	// A forgery: right name, wrong place. Nothing may read it.
	publishCatalogue(t, c, "colca/v1/_DataTags/n1/junk/opcua-press", tags("t9"))

	code, msg, _ := c.Execute(asHuman, "_CmdConfigure", "signal/autobind", []byte(`{"connector":"01JCONN"}`))
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
	code, _, _ := c.Execute(asHuman, "_CmdConfigure", "signal/autobind", []byte(`{"connector":"01JNOBODY"}`))
	if code != 404 {
		t.Fatalf("autobind for an unknown connector = %d; want 404 — a parent must not "+
			"guess at a connector only its child holds", code)
	}
}

// An entry bound to an element this node cannot resolve must not read as
// unplaced; autobind refuses instead of computing the wrong catalogue topic.
func TestAutobindRefusesAConnectorBoundToAnUnresolvableElement(t *testing.T) {
	c := newConfigExec(t)
	// el-ghost is never placed, so PathOf(el-ghost) fails.
	bindEntry(t, c, "01JCONN", "opcua-press", "el-ghost")
	// Planted where a fallback to "unplaced" would look (mount ""), so a
	// wrong implementation would find it and return 200.
	publishCatalogue(t, c, "colca/v1/_DataTags/n1/opcua-press", tags("t1"))

	code, msg, result := c.Execute(asHuman, "_CmdConfigure", "signal/autobind", []byte(`{"connector":"01JCONN"}`))
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
	c.Execute(asHuman, "_CmdConfigure", "signal/autobind", []byte(`{"connector":"01JCONN"}`))

	s := oneSignal(t, c)
	if s.DataTag != "01JTAG1" {
		t.Fatalf("signal.data_tag = %q; want the tag's ULID", s.DataTag)
	}
	if s.ID == "" || strings.Contains(s.ID, "01JTAG1") || strings.Contains(s.ID, "01JCONN") {
		t.Fatalf("signal.id = %q; a signal's identity must be its own, not composed "+
			"from what it is bound to — every Metric carries signal_id", s.ID)
	}
}

// A tag's meta.element names a path this node does not fully hold: missing
// segments are created one at a time and existing ones reused, the same walk
// a local service's mount uses. "m" already exists; "m/a" and "m/a/b" do not.
func TestATagsMetaElementAuthorsMissingSegmentsAndReusesExisting(t *testing.T) {
	c := newConfigExec(t)
	place(t, c, "01HMOUNT", "m")
	placeElement(t, c, "m", "01HMOUNT")
	bindEntry(t, c, "01JCONN", "svc", "01HMOUNT")
	publishCatalogue(t, c, "colca/v1/_DataTags/n1/m/svc", []map[string]any{
		{"id": "t1", "name": "temperature", "data_type": "float", "meta": map[string]any{"element": "m/a/b"}},
	})

	code, msg, _ := c.Execute(asHuman, "_CmdConfigure", "signal/autobind", []byte(`{"connector":"01JCONN"}`))
	if code != 200 {
		t.Fatalf("autobind = %d %q", code, msg)
	}

	s, ok := signalsAt(c)["m/a/b/temperature"]
	if !ok {
		t.Fatalf("signals = %+v, want one bound at m/a/b/temperature", signalsAt(c))
	}
	if s.DataTag != "t1" {
		t.Fatalf("signal at m/a/b/temperature bound to tag %q, want t1", s.DataTag)
	}

	elements := elementsUnder(c.store.(*fakeStore), "n1")
	if elements["m"] != "01HMOUNT" {
		t.Fatalf(`elements["m"] = %q, want the pre-existing 01HMOUNT reused, not duplicated`, elements["m"])
	}
	aID, ok := elements["m/a"]
	if !ok {
		t.Fatalf("elements = %+v, want m/a authored along the way", elements)
	}
	bID, ok := elements["m/a/b"]
	if !ok {
		t.Fatalf("elements = %+v, want m/a/b authored as the tag's own element", elements)
	}
	if aID == bID || aID == "01HMOUNT" || bID == "01HMOUNT" {
		t.Fatalf("authored elements must each have their own identity: m=%q m/a=%q m/a/b=%q",
			elements["m"], aID, bID)
	}
	if s.Element != bID {
		t.Fatalf("signal.system_element_id = %q, want the authored m/a/b element %q", s.Element, bID)
	}
}

// Rebinding through signal/upsert with the same id and a new data_tag keeps
// the id; metrics carry signal_id, so a new id would orphan their history.
func TestRebindingASignalPreservesItsID(t *testing.T) {
	c := newConfigExec(t)
	place(t, c, "el-press3", "line1/press3")
	bindEntry(t, c, "01JCONN", "opcua-press", "el-press3")
	publishCatalogue(t, c, "colca/v1/_DataTags/n1/line1/press3/opcua-press", tagsWithIDs("01JTAG1"))
	c.Execute(asHuman, "_CmdConfigure", "signal/autobind", []byte(`{"connector":"01JCONN"}`))
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

// Idempotency is what lets a person, a replay and the lifecycle trigger all
// run this verb.
func TestAutobindIsIdempotent(t *testing.T) {
	c := newConfigExec(t)
	place(t, c, "el-1", "line1/m6")
	bindEntry(t, c, "01JCONN", "opcua-1", "el-1")
	publishCatalogue(t, c, "colca/v1/_DataTags/n1/line1/m6/opcua-1", tags("t1", "t2"))

	c.Execute(asHuman, "_CmdConfigure", "signal/autobind", body(t, map[string]any{"connector": "01JCONN"}))
	before := signalsAt(c)

	code, msg, _ := c.Execute(asHuman, "_CmdConfigure", "signal/autobind", body(t, map[string]any{"connector": "01JCONN"}))

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

	code, msg, _ := c.Execute(asHuman, "_CmdConfigure", "signal/autobind", body(t, map[string]any{"connector": "01JCONN"}))

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

	code, msg, result := c.Execute(asHuman, "_CmdConfigure", "signal/autobind", body(t, map[string]any{
		"connector": "01JCONN",
	}))

	// A retry after the connector publishes succeeds, so this is a
	// conflict, not a malformed request.
	if code != 409 || result != "conflict" || !strings.Contains(msg, "opcua-1") {
		t.Fatalf("code %d result %q msg %q — want 409/conflict naming the connector", code, result, msg)
	}
}

func TestAutobindPlacesSignalsUnderTheGivenPath(t *testing.T) {
	c := newConfigExec(t)
	bindEntry(t, c, "01JCONN", "opcua-1", "")
	placeElement(t, c, "line1/m6", "01HM6")
	publishCatalogue(t, c, "colca/v1/_DataTags/n1/opcua-1", tags("t1"))

	c.Execute(asHuman, "_CmdConfigure", "signal/autobind", body(t, map[string]any{
		"connector": "01JCONN", "under": "line1/m6",
	}))

	got, ok := signalsAt(c)["line1/m6/tag-t1"]
	if !ok {
		t.Fatalf("signals = %+v, want one under line1/m6", signalsAt(c))
	}
	// The path says where, the binding says to what; both must agree.
	if got.Element != "01HM6" {
		t.Fatalf("signal at line1/m6 is bound to %q, want the element at that path (01HM6)", got.Element)
	}
}

// A path with no element is not a place for a signal. It is a conflict:
// author the element and the same command succeeds.
func TestAutobindRefusesAPathNoElementOccupies(t *testing.T) {
	c := newConfigExec(t)
	bindEntry(t, c, "01JCONN", "opcua-1", "")
	publishCatalogue(t, c, "colca/v1/_DataTags/n1/opcua-1", tags("t1"))

	code, msg, result := c.Execute(asHuman, "_CmdConfigure", "signal/autobind", body(t, map[string]any{
		"connector": "01JCONN", "under": "line1/nowhere",
	}))

	if code != 409 || result != "conflict" || !strings.Contains(msg, "line1/nowhere") {
		t.Fatalf("code %d result %q msg %q — want 409/conflict naming the path", code, result, msg)
	}
	if got := signalsAt(c); len(got) != 0 {
		t.Fatalf("signals = %+v, want none — a refused autobind bound something anyway", got)
	}
}

// By default a connector's signals sit directly under the element the
// connector is bound to. The connector name is not a path segment.
func TestAutobindBindsSignalsToTheConnectorsElement(t *testing.T) {
	c := newConfigExec(t)
	place(t, c, "01HLINE1", "line1")
	bindEntry(t, c, "01JCONN", "opcua-1", "01HLINE1")
	publishCatalogue(t, c, "colca/v1/_DataTags/n1/line1/opcua-1", tags("t1", "t2"))

	code, msg, _ := c.Execute(asHuman, "_CmdConfigure", "signal/autobind", body(t, map[string]any{"connector": "01JCONN"}))
	if code != 200 {
		t.Fatalf("autobind = %d %q", code, msg)
	}

	got := signalsAt(c)
	for _, path := range []string{"line1/tag-t1", "line1/tag-t2"} {
		s, ok := got[path]
		if !ok {
			t.Fatalf("signals = %+v, want one at %s — directly under the mount, no connector segment", got, path)
		}
		if s.Element != "01HLINE1" {
			t.Fatalf("%s is bound to %q, want the connector's element 01HLINE1", path, s.Element)
		}
	}
	if _, stray := got["line1/opcua-1/tag-t1"]; stray {
		t.Fatalf("a signal landed under the connector's name, which is not an element: %+v", got)
	}
}

// The lifecycle trigger binds exactly as the verb does: to the owning
// connector's element, under its mount.
func TestNewConnectorIsBoundToItsElementOnArrival(t *testing.T) {
	c := newTriggerConfigExec(t)
	place(t, c, "01HLINE1", "line1")
	bindEntry(t, c, "01JCONN", "opcua-1", "01HLINE1")

	c.Observe("_DataTags", "colca/v1/_DataTags/n1/line1/opcua-1", mustJSON(map[string]any{"data_tags": tags("t1")}))

	got, ok := signalsAt(c)["line1/tag-t1"]
	if !ok || got.Element != "01HLINE1" {
		t.Fatalf("signals = %+v, want line1/tag-t1 bound to 01HLINE1", signalsAt(c))
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

	code, msg, _ := c.Execute(asHuman, "_CmdConfigure", "signal/autobind", body(t, map[string]any{"connector": "01JCONN"}))
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
	// The raw name is still reachable: each signal names its catalogue
	// entry, which carries it.
	bound := map[string]bool{}
	for _, rec := range c.store.KVScan("_Signal", c.store.NodeID()) {
		var s struct {
			DataTag  string         `json:"data_tag"`
			Metadata map[string]any `json:"metadata"`
		}
		if json.Unmarshal(rec.Payload, &s) != nil {
			t.Fatalf("%s is not a signal record", rec.Path)
		}
		if s.DataTag == "" {
			t.Fatalf("%s reaches no catalogue entry, so its raw name is unreachable", rec.Path)
		}
		if _, carried := s.Metadata["tag_name"]; carried {
			t.Fatalf("%s copies the raw tag name into metadata — the binding already reaches it, "+
				"and a metadata key names a metadata TYPE, which `tag_name` is not", rec.Path)
		}
		bound[s.DataTag] = true
	}
	for _, id := range []string{"t1", "t2", "t3"} {
		if !bound[id] {
			t.Fatalf("tag %s was left unbound, so its name is reachable from no signal", id)
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

// A record shaped like a catalogue, even with real tag ids, triggers nothing
// at a path no enrolled entry computes to.
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

// entryOwns compares the full topic: a matching path under a different node
// id, as a child's record looks at the parent, is not this entry's catalogue.
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
	c.Execute(asHuman, "_CmdConfigure", "signal/delete", body(t, map[string]any{"paths": []string{deletedPath}}))
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
	c := NewConfigExec(newStore("n-edge1"), nil, nil, nil, nil, nil)
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
	c := NewConfigExec(newStore("n-edge1"), nil, nil, nil, nil, nil)
	code, msg, _ := c.Execute(asHuman, "_CmdConfigure", "signal/rebuild", body(t, map[string]any{}))
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

// placeElement upserts one element at path, so a test exercising something
// standing on it (a resource, a binding) has somewhere real to stand.
func placeElement(t *testing.T, exec *ConfigExec, path, id string) {
	t.Helper()
	if code, msg, _ := exec.Execute(asHuman, "_CmdConfigure", "element/upsert", elementBody(t, element(path, id, path))); code != 200 {
		t.Fatalf("placeElement(%s, %s) failed: %d %s", path, id, code, msg)
	}
}

func TestElementUpsertWritesEachElementAtItsPath(t *testing.T) {
	f := newStore("n-edge1")
	c := NewConfigExec(f, nil, nil, nil, nil, nil)

	code, msg, _ := c.Execute(asHuman, "_CmdConfigure", "element/upsert", elementBody(t,
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
	c := NewConfigExec(f, nil, nil, nil, nil, nil)
	c.Execute(asHuman, "_CmdConfigure", "element/upsert", elementBody(t, element("line1", "01HLINE1", "Linie 1")))

	code, msg, result := c.Execute(asHuman, "_CmdConfigure", "element/upsert",
		elementBody(t, element("line1", "01HOTHER", "Linie 1")))

	if code != 409 || result != "conflict" || !strings.Contains(msg, "line1") {
		t.Fatalf("code %d result %q msg %q — want 409 naming the path", code, result, msg)
	}
	if got := elementsUnder(f, "n-edge1")["line1"]; got != "01HLINE1" {
		t.Fatalf("the colliding write landed anyway: %s", got)
	}
}

// Re-upserting the same element at its own path is an edit, not a
// collision.
func TestElementUpsertOfTheSameElementIsNotACollision(t *testing.T) {
	f := newStore("n-edge1")
	c := NewConfigExec(f, nil, nil, nil, nil, nil)
	c.Execute(asHuman, "_CmdConfigure", "element/upsert", elementBody(t, element("line1", "01HLINE1", "Linie 1")))

	code, msg, _ := c.Execute(asHuman, "_CmdConfigure", "element/upsert",
		elementBody(t, element("line1", "01HLINE1", "Linie 1 (Ost)")))

	if code != 200 {
		t.Fatalf("re-upsert = %d %q, want 200", code, msg)
	}
}

func TestElementUpsertRejectsWhatCannotBeAddressed(t *testing.T) {
	f := newStore("n-edge1")
	c := NewConfigExec(f, nil, nil, nil, nil, nil)

	for _, tc := range []struct {
		name  string
		entry map[string]any
	}{
		{"no path", map[string]any{"element": map[string]any{"id": "x", "name": "X"}}},
		{"no element", map[string]any{"path": "line1"}},
		{"no id", map[string]any{"path": "line1", "element": map[string]any{"name": "X"}}},
	} {
		if code, _, _ := c.Execute(asHuman, "_CmdConfigure", "element/upsert", elementBody(t, tc.entry)); code != 422 {
			t.Errorf("%s: code = %d, want 422", tc.name, code)
		}
	}
}

func TestElementDeleteTombstonesTheRecord(t *testing.T) {
	f := newStore("n-edge1")
	c := NewConfigExec(f, nil, nil, nil, nil, nil)
	c.Execute(asHuman, "_CmdConfigure", "element/upsert", elementBody(t, element("line1", "01HLINE1", "Linie 1")))

	code, _, _ := c.Execute(asHuman, "_CmdConfigure", "element/delete", body(t, map[string]any{
		"paths": []string{"line1"},
	}))

	if code != 200 {
		t.Fatalf("delete = %d, want 200", code)
	}
	if payload := f.records["colca/v1/_SystemElement/n-edge1/line1"]; payload != nil {
		t.Fatalf("record still present: %s", payload)
	}
}

// Deleting an element that still has children would strand them.
func TestElementDeleteRefusesWhileChildrenRemain(t *testing.T) {
	f := newStore("n-edge1")
	c := NewConfigExec(f, nil, nil, nil, nil, nil)
	c.Execute(asHuman, "_CmdConfigure", "element/upsert", elementBody(t,
		element("line1", "01HLINE1", "Linie 1"),
		element("line1/m6", "01HM6", "Maschine 6"),
	))

	code, msg, result := c.Execute(asHuman, "_CmdConfigure", "element/delete", body(t, map[string]any{
		"paths": []string{"line1"},
	}))

	if code != 409 || result != "conflict" || !strings.Contains(msg, "line1/m6") {
		t.Fatalf("code %d result %q msg %q — want 409 naming what still binds", code, result, msg)
	}
	if f.records["colca/v1/_SystemElement/n-edge1/line1"] == nil {
		t.Fatal("the refused delete removed the record anyway")
	}
}

// bindings is a stub registry mapping elements to the identities on them;
// the element-delete tests only use BoundTo.
type bindings map[string][]string

func (b bindings) BoundTo(elementID string) []string     { return b[elementID] }
func (b bindings) EntryOf(string) (string, string, bool) { return "", "", false }
func (b bindings) Entries() []EntryRef                   { return nil }

// Retiring a position an identity binds to would leave it nowhere to write;
// the refusal names who is in the way.
func TestElementDeleteRefusesWhileAnIdentityBindsToIt(t *testing.T) {
	f := newStore("n-edge1")
	c := NewConfigExec(f, bindings{"01HM6": {"m6-connector"}}, nil, nil, nil, nil)
	c.Execute(asHuman, "_CmdConfigure", "element/upsert", elementBody(t, element("line1/m6", "01HM6", "Maschine 6")))

	code, msg, result := c.Execute(asHuman, "_CmdConfigure", "element/delete", body(t, map[string]any{
		"paths": []string{"line1/m6"},
	}))

	if code != 409 || result != "conflict" || !strings.Contains(msg, "m6-connector") {
		t.Fatalf("code %d result %q msg %q — want 409 naming the bound identity", code, result, msg)
	}
	if f.records["colca/v1/_SystemElement/n-edge1/line1/m6"] == nil {
		t.Fatal("the refused delete removed the record anyway")
	}
}

// With nothing bound to it, the position retires normally.
func TestElementDeleteProceedsWhenNothingBindsToIt(t *testing.T) {
	f := newStore("n-edge1")
	c := NewConfigExec(f, bindings{"01HOTHER": {"someone-else"}}, nil, nil, nil, nil)
	c.Execute(asHuman, "_CmdConfigure", "element/upsert", elementBody(t, element("line1/m6", "01HM6", "Maschine 6")))

	if code, msg, _ := c.Execute(asHuman, "_CmdConfigure", "element/delete", body(t, map[string]any{
		"paths": []string{"line1/m6"},
	})); code != 200 {
		t.Fatalf("delete = %d (%s), want 200", code, msg)
	}
}

func TestElementDeleteOfAnAbsentElementIs404(t *testing.T) {
	f := newStore("n-edge1")
	c := NewConfigExec(f, nil, nil, nil, nil, nil)

	code, msg, _ := c.Execute(asHuman, "_CmdConfigure", "element/delete", body(t, map[string]any{
		"paths": []string{"nothing"},
	}))

	if code != 404 || !strings.Contains(msg, "nothing") {
		t.Fatalf("code %d msg %q — want 404 naming the path", code, msg)
	}
}

func TestElementDeleteRefusesWhileAResourceIsAttached(t *testing.T) {
	store := newStore("n1")
	blobs := newFakeBlobs(testSHA)
	exec := NewConfigExec(store, nil, nil, blobs, nil, nil)
	placeElement(t, exec, "press3", "el1") // existing helper in this file
	if code, msg, _, _ := exec.ExecuteWithWrites(asHuman, "_CmdConfigure", "resource/upsert",
		resourceBody("press3/r1", "r1", testSHA)); code != 200 {
		t.Fatalf("setup upsert failed: %d %s", code, msg)
	}

	code, msg, result, _ := exec.ExecuteWithWrites(asHuman, "_CmdConfigure", "element/delete",
		[]byte(`{"paths":["press3"]}`))
	if code != 409 {
		t.Fatalf("code = %d (%s), want 409 — an orphaned resource pins a blob alive forever", code, msg)
	}
	if result != "conflict" {
		t.Fatalf("result = %q, want conflict", result)
	}
	if !strings.Contains(msg, "press3/r1") {
		t.Fatalf("the message must name what blocks: %q", msg)
	}

	// Denominator: with the resource retracted, the same delete succeeds.
	if code, msg, _, _ := exec.ExecuteWithWrites(asHuman, "_CmdConfigure", "resource/delete",
		[]byte(`{"paths":["press3/r1"]}`)); code != 200 {
		t.Fatalf("resource/delete failed: %d %s", code, msg)
	}
	if code, msg, _, _ := exec.ExecuteWithWrites(asHuman, "_CmdConfigure", "element/delete",
		[]byte(`{"paths":["press3"]}`)); code != 200 {
		t.Fatalf("element/delete = %d (%s), want 200 once nothing is attached", code, msg)
	}
}

// ── resources ─────────────────────────────────────────────────────────────

type fakeBlobs struct {
	held    map[string]bool
	pulled  []string
	pullErr error
}

func newFakeBlobs(have ...string) *fakeBlobs {
	f := &fakeBlobs{held: map[string]bool{}}
	for _, sha := range have {
		f.held[sha] = true
	}
	return f
}

func (f *fakeBlobs) Has(sha string) bool { return f.held[sha] }

func (f *fakeBlobs) Pull(sha string) error {
	f.pulled = append(f.pulled, sha)
	if f.pullErr != nil {
		return f.pullErr
	}
	f.held[sha] = true
	return nil
}

const testSHA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func resourceBody(path, id, sha string) []byte {
	return []byte(`{"resources":[{"path":"` + path + `","resource":{` +
		`"id":"` + id + `","system_element_id":"el1","filename":"manual.pdf",` +
		`"content_type":"application/pdf","size_bytes":12,"sha256":"` + sha + `"}}]}`)
}

func TestResourceUpsertWritesWhenTheBlobIsHeld(t *testing.T) {
	store := newStore("n1")
	blobs := newFakeBlobs(testSHA)
	exec := NewConfigExec(store, nil, nil, blobs, nil, nil)

	code, msg, result, writes := exec.ExecuteWithWrites(asHuman, "_CmdConfigure", "resource/upsert",
		resourceBody("press3/r1", "r1", testSHA))
	if code != 200 {
		t.Fatalf("code = %d (%s), want 200", code, msg)
	}
	if result != "ok" {
		t.Fatalf("result = %q, want ok", result)
	}
	if len(writes) != 1 {
		t.Fatalf("writes = %d, want 1", len(writes))
	}
	if got := writes[0].Topic; got != "colca/v1/_Resource/n1/press3/r1" {
		t.Fatalf("topic = %q", got)
	}
	if len(blobs.pulled) != 0 {
		t.Fatalf("must not pull a blob it already holds, pulled %v", blobs.pulled)
	}
}

func TestResourceUpsertPullsAMissingBlobBeforeWriting(t *testing.T) {
	store := newStore("n1")
	blobs := newFakeBlobs() // holds nothing
	exec := NewConfigExec(store, nil, nil, blobs, nil, nil)

	code, msg, _, writes := exec.ExecuteWithWrites(asHuman, "_CmdConfigure", "resource/upsert",
		resourceBody("press3/r1", "r1", testSHA))
	if code != 200 {
		t.Fatalf("code = %d (%s), want 200 — a provisioning command must pull first", code, msg)
	}
	if len(writes) != 1 {
		t.Fatalf("writes = %d, want 1", len(writes))
	}
	if len(blobs.pulled) != 1 || blobs.pulled[0] != testSHA {
		t.Fatalf("pulled = %v, want exactly [%s]", blobs.pulled, testSHA)
	}
}

func TestResourceUpsertRefusesWhenTheBlobCannotBePulled(t *testing.T) {
	store := newStore("n1")
	blobs := newFakeBlobs()
	blobs.pullErr = errors.New("no parent")
	exec := NewConfigExec(store, nil, nil, blobs, nil, nil)

	code, msg, result, writes := exec.ExecuteWithWrites(asHuman, "_CmdConfigure", "resource/upsert",
		resourceBody("press3/r1", "r1", testSHA))
	if code != 422 {
		t.Fatalf("code = %d, want 422", code)
	}
	if result != "blob_unreachable" {
		t.Fatalf("result = %q, want blob_unreachable — a failed pull is its own outcome, "+
			"not a malformed command", result)
	}
	if !strings.HasPrefix(msg, "blob_unreachable: ") {
		t.Fatalf("the message must lead with its machine-readable code, the way every other "+
			"coded refusal here does, so a caller can act on it without matching prose: %q", msg)
	}
	if !strings.Contains(msg, testSHA) {
		t.Fatalf("the message must name the digest so an operator can act on it: %q", msg)
	}
	if len(writes) != 0 {
		t.Fatalf("an entity must never be authored pointing at bytes the node lacks; wrote %d", len(writes))
	}

	// A malformed command also answers 422 but must not claim a pull
	// failure, or blob_unreachable would mean nothing.
	badCode, badMsg, badResult, badWrites := exec.ExecuteWithWrites(asHuman, "_CmdConfigure", "resource/upsert",
		[]byte(`{"resources":[]}`))
	if badCode != 422 || len(badWrites) != 0 {
		t.Fatalf("a malformed command must still be refused: code = %d, writes = %d", badCode, len(badWrites))
	}
	if badResult == "blob_unreachable" || strings.Contains(badMsg, "blob_unreachable") {
		t.Fatalf("a malformed command must not be reported as a pull failure: %q / %q", badResult, badMsg)
	}

	// Once the blob is reachable, the same executor writes.
	blobs.pullErr = nil
	if code, _, result, writes := exec.ExecuteWithWrites(asHuman, "_CmdConfigure", "resource/upsert",
		resourceBody("press3/r1", "r1", testSHA)); code != 200 || len(writes) != 1 || result != "ok" {
		t.Fatal("the refusal above proves nothing if this path cannot write at all")
	}
}

func TestResourceDeleteTombstones(t *testing.T) {
	store := newStore("n1")
	blobs := newFakeBlobs(testSHA)
	exec := NewConfigExec(store, nil, nil, blobs, nil, nil)
	if code, msg, _, _ := exec.ExecuteWithWrites(asHuman, "_CmdConfigure", "resource/upsert",
		resourceBody("press3/r1", "r1", testSHA)); code != 200 {
		t.Fatalf("setup upsert failed: %d %s", code, msg)
	}

	code, msg, _, writes := exec.ExecuteWithWrites(asHuman, "_CmdConfigure", "resource/delete",
		[]byte(`{"paths":["press3/r1"]}`))
	if code != 200 {
		t.Fatalf("code = %d (%s), want 200", code, msg)
	}
	if len(writes) != 1 {
		t.Fatalf("writes = %d, want 1", len(writes))
	}
	if _, ok := store.KVGet("colca/v1/_Resource/n1/press3/r1"); ok {
		t.Fatal("the record must be gone after a tombstone")
	}
}

func TestResourceDeleteReportsAMissingPath(t *testing.T) {
	store := newStore("n1")
	exec := NewConfigExec(store, nil, nil, newFakeBlobs(), nil, nil)
	if code, _, _, _ := exec.ExecuteWithWrites(asHuman, "_CmdConfigure", "resource/delete",
		[]byte(`{"paths":["press3/absent"]}`)); code != 404 {
		t.Fatalf("code = %d, want 404", code)
	}
}

// ── constants ─────────────────────────────────────────────────────────────

func constant(path, id, dataType string, value any) map[string]any {
	return map[string]any{
		"path": path,
		"constant": map[string]any{
			"id": id, "name": path[strings.LastIndex(path, "/")+1:],
			"data_type": dataType, "value": value,
		},
	}
}

func constantBody(t *testing.T, entries ...map[string]any) []byte {
	t.Helper()
	return body(t, map[string]any{"constants": entries})
}

func constantsUnder(f *fakeStore, node string) map[string]placedConstant {
	out := map[string]placedConstant{}
	for _, rec := range f.KVScan("_Constant", node) {
		var value placedConstant
		if json.Unmarshal(rec.Payload, &value) == nil {
			out[rec.Path] = value
		}
	}
	return out
}

func TestConstantUpsertWritesTypedValuesAtTheirPaths(t *testing.T) {
	f := newStore("n-edge1")
	c := NewConfigExec(f, nil, nil, nil, nil, nil)

	code, msg, _ := c.Execute(asHuman, "_CmdConfigure", "constant/upsert", constantBody(t,
		constant("line1/m6/target-speed", "01HINT", "int64", 18000),
		constant("line1/m6/enabled", "01HBOOL", "boolean", true),
		constant("line1/m6/recipe", "01HJSON", "json", map[string]any{"sku": "A-42"}),
	))

	if code != 200 {
		t.Fatalf("constant upsert = %d %q, want 200", code, msg)
	}
	got := constantsUnder(f, "n-edge1")
	if len(got) != 3 || got["line1/m6/target-speed"].ID != "01HINT" {
		t.Fatalf("stored constants = %+v", got)
	}
}

func TestConstantUpsertValidatesTheWholeBatchBeforeWriting(t *testing.T) {
	f := newStore("n-edge1")
	c := NewConfigExec(f, nil, nil, nil, nil, nil)

	code, msg, _ := c.Execute(asHuman, "_CmdConfigure", "constant/upsert", constantBody(t,
		constant("line1/m6/valid", "01HVALID", "string", "ready"),
		constant("line1/m6/invalid", "01HINVALID", "int64", 1.5),
	))

	if code != 422 || !strings.Contains(msg, "entry 1") {
		t.Fatalf("constant upsert = %d %q, want 422 naming entry 1", code, msg)
	}
	if got := constantsUnder(f, "n-edge1"); len(got) != 0 {
		t.Fatalf("a rejected batch wrote partial state: %+v", got)
	}
}

func TestConstantUpsertRejectsEveryInvalidTypeAndPath(t *testing.T) {
	cases := []struct {
		name     string
		entry    map[string]any
		wantCode int
	}{
		{"no path", constant("", "01H", "string", "x"), 422},
		{"non canonical path", constant("line1//speed", "01H", "string", "x"), 422},
		{"wildcard path", constant("line1/+/speed", "01H", "string", "x"), 422},
		{"float64", constant("c", "01H", "float64", "1.2"), 422},
		{"int64", constant("c", "01H", "int64", 1.2), 422},
		{"boolean", constant("c", "01H", "boolean", "true"), 422},
		{"boolean null", constant("c", "01H", "boolean", nil), 422},
		{"string", constant("c", "01H", "string", 42), 422},
		{"string null", constant("c", "01H", "string", nil), 422},
		{"datetime", constant("c", "01H", "datetime", "20 August 2026"), 422},
		{"unknown type", constant("c", "01H", "decimal", 1), 422},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newStore("n-edge1")
			c := NewConfigExec(f, nil, nil, nil, nil, nil)
			code, _, _ := c.Execute(asHuman, "_CmdConfigure", "constant/upsert", constantBody(t, tc.entry))
			if code != tc.wantCode {
				t.Fatalf("code = %d, want %d", code, tc.wantCode)
			}
		})
	}
}

func TestConstantUpsertRefusesAPathOwnedByAnotherConstant(t *testing.T) {
	f := newStore("n-edge1")
	c := NewConfigExec(f, nil, nil, nil, nil, nil)
	c.Execute(asHuman, "_CmdConfigure", "constant/upsert", constantBody(t,
		constant("line1/m6/target-speed", "01HOLD", "int64", 18000),
	))

	code, msg, result := c.Execute(asHuman, "_CmdConfigure", "constant/upsert", constantBody(t,
		constant("line1/m6/target-speed", "01HOTHER", "int64", 19000),
	))

	if code != 409 || result != "conflict" || !strings.Contains(msg, "01HOLD") {
		t.Fatalf("code %d result %q msg %q, want collision naming 01HOLD", code, result, msg)
	}
	if got := constantsUnder(f, "n-edge1")["line1/m6/target-speed"].ID; got != "01HOLD" {
		t.Fatalf("colliding write replaced %q", got)
	}
}

// An element id becomes a grant zone once grantsync registers it, and "#"
// would turn any grant on the element into the whole tree. The last row
// checks that an ordinary id still writes.
func TestElementUpsertRefusesAnIdThatIsNotAnIdentity(t *testing.T) {
	for _, tc := range []struct {
		name     string
		id       string
		wantCode int
	}{
		{"the whole-namespace wildcard", "#", 422},
		{"a single-level wildcard", "+", 422},
		{"a path", "site1/spare", 422},
		{"a grant separator", "site1:spare", 422},
		{"an ordinary ulid", "01HSPARE", 200},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newStore("n-edge1")
			c := NewConfigExec(f, nil, nil, nil, nil, nil)

			code, msg, _ := c.Execute(asHuman, "_CmdConfigure", "element/upsert",
				elementBody(t, element("site1/spare", tc.id, "Spare")))

			if code != tc.wantCode {
				t.Fatalf("element id %q = %d %q, want %d", tc.id, code, msg, tc.wantCode)
			}
			_, written := f.KVGet("colca/v1/_SystemElement/n-edge1/site1/spare")
			if written != (tc.wantCode == 200) {
				t.Fatalf("element id %q: written = %v, want %v", tc.id, written, tc.wantCode == 200)
			}
		})
	}
}

// A position holds one entity and an entity sits at one position. Every
// upsert verb must refuse the same id at a second path. Each case first
// re-upserts at the same path, which must stay a 200.
func TestUpsertRefusesOneIdentityAtTwoPositions(t *testing.T) {
	cases := []struct {
		name         string
		verb         string
		first        func(t *testing.T) []byte
		sameID       func(t *testing.T) []byte // same id, same path: an update
		secondPath   func(t *testing.T) []byte // same id, different path
		oneCommand   func(t *testing.T) []byte // both positions in one command
		heldAt       string
		strandedPath string
		contract     string
	}{
		{
			name: "element",
			verb: "element/upsert",
			first: func(t *testing.T) []byte {
				return elementBody(t, element("a/x", "01HDUP", "X"))
			},
			sameID: func(t *testing.T) []byte {
				return elementBody(t, element("a/x", "01HDUP", "X renamed"))
			},
			secondPath: func(t *testing.T) []byte {
				return elementBody(t, element("b/x", "01HDUP", "X"))
			},
			oneCommand: func(t *testing.T) []byte {
				return elementBody(t, element("c/x", "01HFRESH", "X"), element("d/x", "01HFRESH", "X"))
			},
			heldAt:       "a/x",
			strandedPath: "b/x",
			contract:     "_SystemElement",
		},
		{
			name: "signal",
			verb: "signal/upsert",
			first: func(t *testing.T) []byte {
				return body(t, map[string]any{"signals": []any{
					map[string]any{"path": "a/temp", "signal": map[string]any{"id": "01HDUP", "name": "temp"}},
				}})
			},
			sameID: func(t *testing.T) []byte {
				return body(t, map[string]any{"signals": []any{
					map[string]any{"path": "a/temp", "signal": map[string]any{"id": "01HDUP", "name": "temperature"}},
				}})
			},
			secondPath: func(t *testing.T) []byte {
				return body(t, map[string]any{"signals": []any{
					map[string]any{"path": "b/temp", "signal": map[string]any{"id": "01HDUP", "name": "temp"}},
				}})
			},
			oneCommand: func(t *testing.T) []byte {
				return body(t, map[string]any{"signals": []any{
					map[string]any{"path": "c/temp", "signal": map[string]any{"id": "01HFRESH", "name": "temp"}},
					map[string]any{"path": "d/temp", "signal": map[string]any{"id": "01HFRESH", "name": "temp"}},
				}})
			},
			heldAt:       "a/temp",
			strandedPath: "b/temp",
			contract:     "_Signal",
		},
		{
			name: "constant",
			verb: "constant/upsert",
			first: func(t *testing.T) []byte {
				return constantBody(t, constant("a/speed", "01HDUP", "int64", 1))
			},
			sameID: func(t *testing.T) []byte {
				return constantBody(t, constant("a/speed", "01HDUP", "int64", 2))
			},
			secondPath: func(t *testing.T) []byte {
				return constantBody(t, constant("b/speed", "01HDUP", "int64", 1))
			},
			oneCommand: func(t *testing.T) []byte {
				return constantBody(t,
					constant("c/speed", "01HFRESH", "int64", 1),
					constant("d/speed", "01HFRESH", "int64", 1),
				)
			},
			heldAt:       "a/speed",
			strandedPath: "b/speed",
			contract:     "_Constant",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newStore("n-edge1")
			c := NewConfigExec(f, nil, nil, nil, nil, nil)

			if code, msg, _ := c.Execute(asHuman, "_CmdConfigure", tc.verb, tc.first(t)); code != 200 {
				t.Fatalf("first upsert = %d %q, want 200", code, msg)
			}
			if code, msg, _ := c.Execute(asHuman, "_CmdConfigure", tc.verb, tc.sameID(t)); code != 200 {
				t.Fatalf("re-upsert at the same path = %d %q, want 200 — the guard refuses updates", code, msg)
			}

			code, msg, result := c.Execute(asHuman, "_CmdConfigure", tc.verb, tc.secondPath(t))
			if code != 409 || result != "conflict" || !strings.Contains(msg, tc.heldAt) {
				t.Fatalf("second position = %d %q result %q, want 409 naming %s", code, msg, result, tc.heldAt)
			}
			if _, ok := f.KVGet("colca/v1/" + tc.contract + "/n-edge1/" + tc.strandedPath); ok {
				t.Fatalf("the refused upsert wrote %s anyway", tc.strandedPath)
			}

			// Two positions for one id in one command: the store shows neither
			// yet, so only the claim map can catch it.
			before := f.offset
			code, msg, result = c.Execute(asHuman, "_CmdConfigure", tc.verb, tc.oneCommand(t))
			if code != 409 || result != "conflict" {
				t.Fatalf("one command, two positions = %d %q result %q, want 409", code, msg, result)
			}
			if f.offset != before {
				t.Fatalf("the refused batch wrote %d records", f.offset-before)
			}
		})
	}
}

func TestConstantDeleteValidatesAllPathsThenWritesTombstones(t *testing.T) {
	f := newStore("n-edge1")
	c := NewConfigExec(f, nil, nil, nil, nil, nil)
	c.Execute(asHuman, "_CmdConfigure", "constant/upsert", constantBody(t,
		constant("line1/m6/a", "01HA", "string", "a"),
		constant("line1/m6/b", "01HB", "string", "b"),
	))

	code, msg, _ := c.Execute(asHuman, "_CmdConfigure", "constant/delete", body(t, map[string]any{
		"paths": []string{"line1/m6/a", "missing"},
	}))
	if code != 404 || !strings.Contains(msg, "missing") {
		t.Fatalf("delete = %d %q, want 404 naming missing", code, msg)
	}
	if got := constantsUnder(f, "n-edge1"); len(got) != 2 {
		t.Fatalf("rejected delete wrote partial tombstones: %+v", got)
	}

	code, msg, _ = c.Execute(asHuman, "_CmdConfigure", "constant/delete", body(t, map[string]any{
		"paths": []string{"line1/m6/a", "line1/m6/b"},
	}))
	if code != 200 {
		t.Fatalf("delete = %d %q, want 200", code, msg)
	}
	if got := constantsUnder(f, "n-edge1"); len(got) != 0 {
		t.Fatalf("constants survived tombstones: %+v", got)
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

// A definition is filed under its own id with no position: its topic has
// only the authoring node and the id.
func TestDefinitionUpsertFilesUnderTheIdWithNoPosition(t *testing.T) {
	f := newStore("n-global")
	c := NewConfigExec(f, nil, nil, nil, nil, nil)

	code, msg, _ := c.Execute(asHuman, "_CmdConfigure", "definition/upsert", definitionBody(t, "_Group",
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
	c := NewConfigExec(f, nil, nil, nil, nil, nil)
	payload := body(t, map[string]any{"entities": []map[string]any{{
		"contract": "_Node",
		"entity": map[string]any{
			"id": "n-local", "name": "line-1", "root_system_element_id": "root",
		},
	}}})

	code, msg, _, writes := c.ExecuteWithWrites(asHuman, "_CmdConfigure", "entity/upsert", payload)
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
	c := NewConfigExec(f, nil, nil, nil, nil, nil)
	payload := body(t, map[string]any{"entities": []map[string]any{{
		"contract": "_ServiceDetails",
		"entity":   map[string]any{"id": "svc-1", "name": "api"},
	}}})

	code, msg, _, writes := c.ExecuteWithWrites(asHuman, "_CmdConfigure", "entity/upsert", payload)
	if code != 422 || !strings.Contains(msg, "own writer") || len(writes) != 0 {
		t.Fatalf("observed service = %d %q writes=%+v, want refused", code, msg, writes)
	}
}

func TestAlarmNotificationConfigCommandWritesOneNodeScopedSnapshot(t *testing.T) {
	f := newStore("n-leaf")
	c := NewConfigExec(f, nil, nil, nil, nil, nil)
	entity := map[string]any{
		"id": "alarm-notification-config", "schema_version": 2,
		"target_node_id": "n-leaf", "revision_id": "revision-1", "issued_at": 1,
		"alarms": []any{}, "channels": []any{}, "recipients": []any{},
		"policies": []any{}, "policy_targets": []any{},
	}
	payload := body(t, map[string]any{"entities": []map[string]any{{
		"contract": "_AlarmNotificationConfig", "entity": entity,
	}}})

	code, msg, _, writes := c.ExecuteWithWrites(asHuman, "_CmdConfigure", "entity/upsert", payload)
	if code != 200 {
		t.Fatalf("entity/upsert = %d (%s), want 200", code, msg)
	}
	wantTopic := "colca/v1/_AlarmNotificationConfig/n-leaf/_colca/alarm-notification-config/alarm-notification-config"
	if f.records[wantTopic] == nil {
		t.Fatalf("alarm config not written at reserved node path: %v", keysOf(f))
	}
	if len(writes) != 1 || writes[0].Stream != "entities" || writes[0].Topic != wantTopic {
		t.Fatalf("state writes = %+v", writes)
	}
}

func TestAlarmNotificationConfigCommandRejectsWrongTargetOrIdentity(t *testing.T) {
	tests := []struct {
		name   string
		id     string
		target string
	}{
		{name: "wrong target", id: "alarm-notification-config", target: "n-other"},
		{name: "missing target", id: "alarm-notification-config", target: ""},
		{name: "wrong id", id: "another-config", target: "n-leaf"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f := newStore("n-leaf")
			c := NewConfigExec(f, nil, nil, nil, nil, nil)
			payload := body(t, map[string]any{"entities": []map[string]any{{
				"contract": "_AlarmNotificationConfig",
				"entity":   map[string]any{"id": test.id, "target_node_id": test.target},
			}}})
			code, _, _, writes := c.ExecuteWithWrites(asHuman, "_CmdConfigure", "entity/upsert", payload)
			if code != 422 || len(writes) != 0 || len(keysOf(f)) != 0 {
				t.Fatalf("upsert = %d writes=%+v records=%v, want atomic refusal", code, writes, keysOf(f))
			}
		})
	}
}

func TestAlarmNotificationConfigDeleteTombstonesTheReservedSnapshot(t *testing.T) {
	f := newStore("n-leaf")
	c := NewConfigExec(f, nil, nil, nil, nil, nil)
	upsert := body(t, map[string]any{"entities": []map[string]any{{
		"contract": "_AlarmNotificationConfig",
		"entity": map[string]any{
			"id": "alarm-notification-config", "target_node_id": "n-leaf",
		},
	}}})
	if code, msg, _ := c.Execute(asHuman, "_CmdConfigure", "entity/upsert", upsert); code != 200 {
		t.Fatalf("seed config = %d (%s)", code, msg)
	}
	remove := body(t, map[string]any{"entities": []map[string]any{{
		"contract": "_AlarmNotificationConfig", "id": "alarm-notification-config",
	}}})
	if code, msg, _ := c.Execute(asHuman, "_CmdConfigure", "entity/delete", remove); code != 200 {
		t.Fatalf("delete config = %d (%s)", code, msg)
	}
	topic := "colca/v1/_AlarmNotificationConfig/n-leaf/_colca/alarm-notification-config/alarm-notification-config"
	if f.records[topic] != nil {
		t.Fatalf("config survived tombstone: %s", f.records[topic])
	}
}

func TestPlatformEntityCommandRefusesNotificationConfigStatus(t *testing.T) {
	f := newStore("n-local")
	c := NewConfigExec(f, nil, nil, nil, nil, nil)
	payload := body(t, map[string]any{"entities": []map[string]any{{
		"contract": "_NotificationConfigStatus",
		"entity":   map[string]any{"id": "status-1", "status": "applied"},
	}}})

	code, msg, _, writes := c.ExecuteWithWrites(asHuman, "_CmdConfigure", "entity/upsert", payload)
	if code != 422 || !strings.Contains(msg, "observed state") || len(writes) != 0 {
		t.Fatalf("notification status = %d %q writes=%+v, want refused", code, msg, writes)
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
		c := NewConfigExec(f, nil, nil, nil, nil, nil)
		code, msg, result := c.Execute(asHuman, "_CmdConfigure", "definition/upsert", tc.body)
		if code != 422 || result != "invalid" {
			t.Errorf("%s: code %d result %q, want 422/invalid", tc.name, code, result)
		}
		if !strings.Contains(msg, tc.want) {
			t.Errorf("%s: message %q must explain the defect (%q)", tc.name, msg, tc.want)
		}
	}
}

// An element or a metric sent to the definition door is refused.
func TestDefinitionUpsertRefusesAContractThatIsNotADefinition(t *testing.T) {
	f := newStore("n-global")
	c := NewConfigExec(f, nil, nil, nil, nil, nil)

	for _, contract := range []string{"_SystemElement", "_Metric", "_CmdParam", ""} {
		code, msg, _ := c.Execute(asHuman, "_CmdConfigure", "definition/upsert", definitionBody(t, contract,
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
	c := NewConfigExec(f, nil, nil, nil, nil, nil)
	c.Execute(asHuman, "_CmdConfigure", "definition/upsert", definitionBody(t, "_Group",
		map[string]any{"id": "01HGRP-OPS", "name": "Ops"}))

	del := body(t, map[string]any{"definitions": []map[string]any{
		{"contract": "_Group", "id": "01HGRP-OPS"}}})
	if code, msg, _ := c.Execute(asHuman, "_CmdConfigure", "definition/delete", del); code != 200 {
		t.Fatalf("delete = %d (%s), want 200", code, msg)
	}
	if payload := f.records["colca/v1/_Group/n-global/01HGRP-OPS"]; payload != nil {
		t.Fatalf("definition still present: %s", payload)
	}
	// Retracting what is not there is a 404 naming it, not a silent success.
	code, msg, _ := c.Execute(asHuman, "_CmdConfigure", "definition/delete", del)
	if code != 404 || !strings.Contains(msg, "01HGRP-OPS") {
		t.Fatalf("second delete = %d (%s), want 404 naming the id", code, msg)
	}
}

// A malformed grant in a group is refused before the definition descends
// to every node below.
func TestDefinitionUpsertRefusesAGroupWithAMalformedGrant(t *testing.T) {
	f := newStore("n-global")
	c := NewConfigExec(f, nil, nil, nil, nil, nil)

	code, msg, result := c.Execute(asHuman, "_CmdConfigure", "definition/upsert", definitionBody(t, "_Group",
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
	c := NewConfigExec(f, nil, nil, nil, nil, nil)

	code, msg, _ := c.Execute(asHuman, "_CmdConfigure", "definition/upsert", definitionBody(t, "_Group",
		map[string]any{"id": "01HGRP-OPS", "name": "Ops", "grants": []string{"read:site1/edge1/#"}}))

	if code != 422 || !strings.Contains(msg, "element") {
		t.Fatalf("code %d msg %q — want 422 explaining that a grant names an element", code, msg)
	}
}
