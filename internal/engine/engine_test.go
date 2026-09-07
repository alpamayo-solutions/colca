package engine

import (
	"bytes"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/metrics"
	"github.com/alpamayo-solutions/colca/internal/metrics/metricstest"
	"github.com/alpamayo-solutions/colca/internal/store"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// fakeIDs is a test Mounts: ulid → entry. The engine asks Authorize for the
// write-scope answer now (auth §5, writeZones) — there is no separate mount
// to fake.
type fakeIDs struct {
	entries  map[string]*uns.Entry
	draining []string // mounts DrainingMount treats as under an active drain
	routes   []string // mounts RoutesUnder treats as an enrolled child node's
}

func (f fakeIDs) Get(ulid string) (*uns.Entry, bool) { e, ok := f.entries[ulid]; return e, ok }

// DrainingMount and RoutesUnder both defer to uns.UnderMount, the same
// predicate registry.Manager uses — the boundary rule is the domain's, and a
// fake that spelled it out again could drift from the thing it stands in for.
func (f fakeIDs) DrainingMount(path string) bool { return coversAny(f.draining, path) }

func (f fakeIDs) RoutesUnder(path string) bool { return coversAny(f.routes, path) }

func coversAny(mounts []string, path string) bool {
	for _, mount := range mounts {
		if uns.UnderMount(path, mount) {
			return true
		}
	}
	return false
}

// testIDs' "m1" carries an explicit write: grant over its own zone — a
// machine gets no implicit write, unlike a local service (auth §5, table
// §3), so the fixture states that grant the same way a real deployment would.
// "hmi" carries a cmd grant only, which is exactly what the no-write-scope
// tests need: an entry that authenticates and reads but may not write.
func testIDs() fakeIDs {
	return fakeIDs{
		entries: map[string]*uns.Entry{
			"m1":  {ULID: "m1", Kind: uns.KindExternal, Element: "el-m1", Grants: []string{"write:el-m1/#"}},
			"hmi": {ULID: "hmi", Kind: uns.KindExternal, Element: "el-hmi", Grants: []string{"cmd:el-m1/#:param"}},
		},
	}
}

// testIDsWithDraining is testIDs plus mount "m1" under an active move-drain —
// the ClassCmd admission check (engine.go) must reject any new command
// addressed under it regardless of the caller's own grants.
func testIDsWithDraining() fakeIDs {
	f := testIDs()
	f.draining = []string{"m1"}
	return f
}

func newEngine(t *testing.T) *Engine {
	t.Helper()
	return newEngineWithIDs(t, testIDs())
}

func newEngineWithIDs(t *testing.T, ids fakeIDs) *Engine {
	t.Helper()
	s, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	cfg := &config.Config{ULID: "n-edge1"}
	e := New(s, cfg, ids, nil, nil, nil) // nils = no local MQTT delivery, no metrics, no clock in unit tests
	placeTestElements(t, e)
	return e
}

// placeTestElements gives the fixture node the elements its identities bind to
// and its grants name. Grants resolve through the element index (id-grants
// design §4), so a fixture without them would deny everything for the right
// reason and prove nothing.
func placeTestElements(t *testing.T, e *Engine) {
	t.Helper()
	for _, el := range []struct{ id, path string }{{"el-m1", "m1"}, {"el-hmi", "hmi"}} {
		topic := "colca/v1/_SystemElement/" + e.NodeID() + "/" + el.path
		if _, err := e.IngestAdmin(topic, []byte(`{"id":"`+el.id+`","name":"`+el.path+`"}`)); err != nil {
			t.Fatalf("place element %s at %s: %v", el.id, el.path, err)
		}
	}
}

// delivery is one call of engine.LocalDeliver, recorded verbatim.
type delivery struct {
	Topic   string
	Payload string
	Retain  bool
}

// recorder is a fake LocalDeliver. It is called from the ingest goroutine, so
// it locks (the -race detector otherwise catches the read in got()).
type recorder struct {
	mu   sync.Mutex
	seen []delivery
}

func (r *recorder) deliver(topic string, payload []byte, retain bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.seen = append(r.seen, delivery{Topic: topic, Payload: string(payload), Retain: retain})
}

// reset drops what the fixture's own setup delivered, so a test sees only the
// traffic it produced itself.
func (r *recorder) reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.seen = nil
}

func (r *recorder) got() []delivery {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]delivery(nil), r.seen...)
}

// newRecordingEngine is newEngine plus a recording LocalDeliver.
func newRecordingEngine(t *testing.T) (*Engine, *recorder) {
	t.Helper()
	s, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	cfg := &config.Config{ULID: "n-edge1"}
	rec := &recorder{}
	e := New(s, cfg, testIDs(), rec.deliver, nil, nil)
	placeTestElements(t, e)
	rec.reset() // the fixture's own placements are setup, not traffic under test
	return e, rec
}

// The mount rewrite is gone: level 4 is the node's own ULID for every
// publisher, and the path a client publishes is stored EXACTLY as sent — "m1"
// writes at "m1/temp" because that is where its write:el-m1/# grant covers,
// not because the engine inserted anything.
func TestClientPublishNoRewriteAndKV(t *testing.T) {
	e := newEngine(t)
	res, err := e.IngestClient("m1", "colca/v1/_Metric/n-edge1/m1/temp", []byte(`{"v":7}`))
	if err != nil {
		t.Fatal(err)
	}
	if res.Stream != "metrics" || res.Offset != 1 {
		t.Fatalf("%+v", res)
	}
	recs, _, _ := e.Store().Read("metrics", 1, 10, nil)
	if recs[0].Topic != "colca/v1/_Metric/n-edge1/m1/temp" {
		t.Fatalf("stored topic %q, want the published path unchanged — there is no mount rewrite any more", recs[0].Topic)
	}
	kv := mustKVScan(t, e.Store(), "m1/temp")
	if len(kv) != 1 || kv[0].NodeID != "n-edge1" {
		t.Fatalf("kv: %+v, want NodeID n-edge1 (the node, not the publishing client)", kv)
	}
}

func TestEntityStorePublishBatchCommitsValidatedEntityStateAtomically(t *testing.T) {
	e, delivered := newRecordingEngine(t)
	before := e.Store().NextOffset("entities")
	records := []uns.StateRecord{
		{
			Topic:   "colca/v1/_Signal/n-edge1/line1/temp",
			Payload: []byte(`{"id":"sig-temp","name":"Temperature"}`),
		},
		{
			Topic:   "colca/v1/_Signal/n-edge1/line1/speed",
			Payload: []byte(`{"id":"sig-speed","name":"Speed"}`),
		},
	}

	writes, err := e.EntityStore().PublishBatch(records)
	if err != nil {
		t.Fatal(err)
	}
	if len(writes) != 2 {
		t.Fatalf("writes = %+v, want 2", writes)
	}
	for i, write := range writes {
		if write.Stream != "entities" || write.Offset != before+uint64(i) || write.Topic != records[i].Topic {
			t.Fatalf("write %d = %+v, want entities/%d/%s", i, write, before+uint64(i), records[i].Topic)
		}
	}
	if got := e.Store().NextOffset("entities"); got != before+2 {
		t.Fatalf("next entities offset = %d, want %d", got, before+2)
	}
	if got := mustKVScan(t, e.Store(), "line1/"); len(got) != 2 {
		t.Fatalf("KV rows = %+v, want both records", got)
	}
	if got := delivered.got(); len(got) != 2 || !got[0].Retain || !got[1].Retain {
		t.Fatalf("retained deliveries = %+v, want both records after commit", got)
	}
}

func TestEntityStorePublishBatchRejectsLateInvalidRecordWithoutWrites(t *testing.T) {
	e, delivered := newRecordingEngine(t)
	before := e.Store().NextOffset("entities")

	_, err := e.EntityStore().PublishBatch([]uns.StateRecord{
		{
			Topic:   "colca/v1/_Signal/n-edge1/line1/temp",
			Payload: []byte(`{"id":"sig-temp","name":"Temperature"}`),
		},
		{
			Topic:   "colca/v1/_Signal/n-edge1/line1/speed",
			Payload: []byte(`{"name":"missing required id"}`),
		},
	})
	if err == nil {
		t.Fatal("PublishBatch accepted a schema-invalid second record")
	}
	if got := e.Store().NextOffset("entities"); got != before {
		t.Fatalf("rejected batch advanced next offset to %d, want %d", got, before)
	}
	if got := mustKVScan(t, e.Store(), "line1/"); len(got) != 0 {
		t.Fatalf("rejected batch left KV state: %+v", got)
	}
	if got := delivered.got(); len(got) != 0 {
		t.Fatalf("rejected batch reached local bus: %+v", got)
	}
}

// Definitions are authored through the same commit boundary as entities and
// land on their own stream. They have to: `definition/upsert` files several
// definitions in one command, and a command is one transition whatever it
// files.
func TestEntityStorePublishBatchCommitsDefinitionsOnTheirOwnStream(t *testing.T) {
	e, _ := newRecordingEngine(t)
	entitiesBefore := e.Store().NextOffset("entities")
	before := e.Store().NextOffset("definitions")

	writes, err := e.EntityStore().PublishBatch([]uns.StateRecord{
		{Topic: "colca/v1/_Group/n-edge1/operators", Payload: []byte(`{"id":"operators"}`)},
		{Topic: "colca/v1/_Group/n-edge1/maintainers", Payload: []byte(`{"id":"maintainers"}`)},
	})
	if err != nil {
		t.Fatal(err)
	}
	for i, write := range writes {
		if write.Stream != "definitions" || write.Offset != before+uint64(i) {
			t.Fatalf("write %d = %+v, want definitions/%d", i, write, before+uint64(i))
		}
	}
	if got := e.Store().NextOffset("entities"); got != entitiesBefore {
		t.Fatalf("a definition batch advanced the entities stream to %d, want %d", got, entitiesBefore)
	}
}

// One batch is one Pebble batch on one stream, so records for two streams could
// only be committed as two — which is the half-applied outcome this path exists
// to prevent. No verb mixes them; this refusal is what keeps that true.
func TestEntityStorePublishBatchRefusesRecordsFromTwoStreams(t *testing.T) {
	e, delivered := newRecordingEngine(t)
	entitiesBefore := e.Store().NextOffset("entities")
	definitionsBefore := e.Store().NextOffset("definitions")

	_, err := e.EntityStore().PublishBatch([]uns.StateRecord{
		{Topic: "colca/v1/_Signal/n-edge1/line1/temp", Payload: []byte(`{"id":"sig-temp","name":"Temperature"}`)},
		{Topic: "colca/v1/_Group/n-edge1/operators", Payload: []byte(`{"id":"operators"}`)},
	})

	if err == nil {
		t.Fatal("PublishBatch accepted a batch spanning the entities and definitions streams")
	}
	// Pin the two-stream refusal by name, not merely that SOME error came
	// back: the stream check runs before validateContract today, so a
	// reordering that let a different rule refuse first (or a validation
	// bug on the _Group payload) would still make err != nil while no
	// longer testing the two-stream rule this test claims to pin.
	if !strings.Contains(err.Error(), `belongs to stream "definitions", not "entities"`) {
		t.Fatalf("PublishBatch error = %q, want it to name the two-stream refusal", err)
	}
	if got := e.Store().NextOffset("entities"); got != entitiesBefore {
		t.Fatalf("entities advanced to %d, want %d", got, entitiesBefore)
	}
	if got := e.Store().NextOffset("definitions"); got != definitionsBefore {
		t.Fatalf("definitions advanced to %d, want %d", got, definitionsBefore)
	}
	if got := delivered.got(); len(got) != 0 {
		t.Fatalf("refused batch reached local bus: %+v", got)
	}
}

// The whole point of routing ConfigExec through the atomic port, proven against
// the real store and the real contract floor rather than a fake: a configure
// command whose late record fails validation leaves the node exactly as it was.
func TestAConfigureCommandCommitsNothingWhenALateRecordFailsValidation(t *testing.T) {
	e, delivered := newRecordingEngine(t)
	domain := uns.NewConfigExec(e.EntityStore(), nil, nil, nil, nil, nil)

	// Precondition, asserted rather than assumed: this command shape is
	// accepted, so the refusal below is the late payload's doing and not the
	// fixture quietly rejecting everything.
	if code, msg, _ := domain.Execute(uns.CommandContext{}, "_CmdConfigure", "signal/upsert", []byte(`{"signals":[
		{"path":"line1/temp","signal":{"id":"sig-temp","name":"Temperature"}},
		{"path":"line1/speed","signal":{"id":"sig-speed","name":"Speed"}}]}`)); code != 200 {
		t.Fatalf("valid signal/upsert = %d %q, want 200", code, msg)
	}
	offsetBefore := e.Store().NextOffset("entities")
	delivered.reset()

	// Second record has no id, which the floor requires of every data-model
	// record. The first is valid and, before the executor committed as one
	// transition, would already have been written by the time it was refused.
	code, msg, result := domain.Execute(uns.CommandContext{}, "_CmdConfigure", "signal/upsert", []byte(`{"signals":[
		{"path":"line1/press","signal":{"id":"sig-press","name":"Press"}},
		{"path":"line1/broken","signal":{"name":"no id"}}]}`))

	if code != 422 || result != "invalid" {
		t.Fatalf("code %d result %q msg %q — want 422/invalid", code, result, msg)
	}
	if !strings.Contains(msg, "line1/broken") {
		t.Errorf("msg %q — want the refused record named", msg)
	}
	if got := e.Store().NextOffset("entities"); got != offsetBefore {
		t.Fatalf("the refused command took stream positions (%d → %d)", offsetBefore, got)
	}
	for _, path := range []string{"line1/press", "line1/broken"} {
		if got := mustKVScan(t, e.Store(), path); len(got) != 0 {
			t.Fatalf("the refused command left %s in KV: %+v — the valid first record "+
				"must not survive the refusal of the second", path, got)
		}
	}
	if got := delivered.got(); len(got) != 0 {
		t.Fatalf("the refused command reached the local bus: %+v", got)
	}
}

// Level 4 must be THIS node's own ULID for every publisher — not the
// client's identity, which no longer appears in the topic at all (auth §2).
func TestClientLevel4MustBeThisNode(t *testing.T) {
	e := newEngine(t)
	_, err := e.IngestClient("m1", "colca/v1/_Metric/OTHER/temp", []byte(`{"v":1}`))
	if err == nil || ReasonOf(err) != metrics.ReasonNodeID {
		t.Fatalf("level-4 rule not enforced: %v (reason %q)", err, ReasonOf(err))
	}
	_, err = e.IngestClient("m1", "colca/v1/_CmdParam/m1/x", []byte(`{"correlation_id":"c","expires_at":1}`))
	if err == nil {
		t.Fatal("an ungranted client must not publish commands")
	}
}

// metricPayload is a valid _Metric body, shared by the write-rule tests below
// (task-7/8, local-service-trust design §2/§5) — none of them care about the
// payload's content, only about whether the publish is admitted.
var metricPayload = []byte(`{"v":1}`)

// mustKVScan is KVScan with the error handled the only way a test fixture
// can: fail loud (resources design §8).
func mustKVScan(t *testing.T, st *store.Store, prefix string) []store.KVEntry {
	t.Helper()
	entries, err := st.KVScan(prefix)
	if err != nil {
		t.Fatal(err)
	}
	return entries
}

// newTestEngine builds a fresh engine identified by nodeULID, whose OWN
// element is "el-root" (a one-step ancestry: this node IS el-root), and
// enrolls one KindLocal service "01JSVC" bound to that same element — i.e.
// bound to the node itself. writeZones resolves el-root's zone through
// Scope.Reaches (it is the node's own element) to "#": the local service may
// write anywhere on the node, exactly what "bound to the node itself" means
// (design §2, §3).
func newTestEngine(t *testing.T, nodeULID string) *Engine {
	t.Helper()
	s, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	cfg := &config.Config{ULID: nodeULID}
	ids := fakeIDs{entries: map[string]*uns.Entry{
		"01JSVC": {ULID: "01JSVC", Kind: uns.KindLocal, Name: "svc", Element: "el-root"},
	}}
	e := New(s, cfg, ids, nil, nil, nil)
	e.SetAncestry(uns.Ancestry{{Element: "el-root", Name: nodeULID}})
	return e
}

// newTestEngineScoped is newTestEngine, but the local service "01JSVC" binds
// to elementID placed at path instead of the node's own root — its write
// scope narrows to that subtree (writeZones, auth §5.2).
func newTestEngineScoped(t *testing.T, nodeULID, elementID, path string) *Engine {
	t.Helper()
	s, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	cfg := &config.Config{ULID: nodeULID}
	ids := fakeIDs{entries: map[string]*uns.Entry{
		"01JSVC": {ULID: "01JSVC", Kind: uns.KindLocal, Name: "svc", Element: elementID},
	}}
	e := New(s, cfg, ids, nil, nil, nil)
	topic := "colca/v1/_SystemElement/" + nodeULID + "/" + path
	if _, err := e.IngestAdmin(topic, []byte(`{"id":"`+elementID+`","name":"`+path+`"}`)); err != nil {
		t.Fatalf("place element %s at %s: %v", elementID, path, err)
	}
	return e
}

// lastTopic reads back the topic of the most recently persisted metrics-
// stream record — asserting what the engine actually STORED, not what a
// Result claims, since the whole point of this task is that the stored topic
// is the published one, unchanged.
func lastTopic(t *testing.T, e *Engine) string {
	t.Helper()
	next := e.Store().NextOffset("metrics")
	if next < 2 {
		t.Fatalf("lastTopic: metrics stream is empty (next offset %d)", next)
	}
	recs, _, err := e.Store().Read("metrics", next-1, 1, nil)
	if err != nil || len(recs) != 1 {
		t.Fatalf("lastTopic: read metrics offset %d: recs=%v err=%v", next-1, recs, err)
	}
	return recs[0].Topic
}

// assertRejectReason fails the test unless err is an engine rejection
// carrying exactly the given metrics reason. Takes the error itself (via
// ReasonOf, engine.go's typed accessor) rather than the engine — nothing
// about "why was the last publish rejected" is state the *Engine holds.
func assertRejectReason(t *testing.T, err error, want string) {
	t.Helper()
	if got := ReasonOf(err); got != want {
		t.Fatalf("reject reason = %q, want %q (err: %v)", got, want, err)
	}
}

// A local service publishes inside its own scope: no rewrite, level 4 is the
// node's own ULID (local-service-trust design §2, §5, task-8 brief).
func TestAClientPublishesUnderTheNodesULID(t *testing.T) {
	e := newTestEngine(t, "n1")
	res, err := e.IngestClient("01JSVC", "colca/v1/_Metric/n1/line1/temp", metricPayload)
	if err != nil {
		t.Fatalf("IngestClient: %v", err)
	}
	if !res.Persisted {
		t.Fatal("a local service's publish inside its scope was not persisted")
	}
	if got := lastTopic(t, e); got != "colca/v1/_Metric/n1/line1/temp" {
		t.Fatalf("stored topic %q; want the path exactly as published — there is no rewrite any more", got)
	}
}

func TestOnlyLocalServicePublishesCanonicalAuditEvent(t *testing.T) {
	e := newTestEngine(t, "n1")
	payload := []byte(`{"event_id":"evt-1","source":"api","action":"authorize","outcome":"denied","actor_kind":"human","occurred_at":1}`)
	topic := "colca/v1/_AuditEvent/n1/_colca/audit/evt-1"
	res, err := e.IngestClient("01JSVC", topic, payload)
	if err != nil {
		t.Fatalf("local audit publish: %v", err)
	}
	if !res.Persisted || res.Stream != "audit" {
		t.Fatalf("audit result = %+v", res)
	}
	if kv := mustKVScan(t, e.Store(), ""); len(kv) != 0 {
		t.Fatalf("audit event reached KV: %+v", kv)
	}

	if _, err := e.IngestClient("01JSVC", "colca/v1/_AuditEvent/n1/_colca/audit/other", payload); err == nil {
		t.Fatal("topic event id may not disagree with the payload")
	}
	if _, err := e.IngestAdmin(topic, payload); err == nil {
		t.Fatal("admin-token door must not publish _AuditEvent")
	}

	machine := newEngine(t)
	if _, err := machine.IngestClient("m1", "colca/v1/_AuditEvent/n-edge1/_colca/audit/evt-1", payload); err == nil {
		t.Fatal("external machine door must not publish _AuditEvent")
	}
}

func TestLocalConfigureAuthorityFollowsPlacement(t *testing.T) {
	payload := []byte(`{"correlation_id":"c-local","expires_at":9999999999999}`)
	topic := "colca/v1/_CmdConfigure/n-edge1/definition/upsert"

	unplacedIDs := fakeIDs{entries: map[string]*uns.Entry{
		"svc-api": {ULID: "svc-api", Kind: uns.KindLocal},
	}}
	unplaced := newEngineWithIDs(t, unplacedIDs)
	unplaced.SetExecutor(&recordingExec{contract: "_CmdConfigure"})
	res, err := unplaced.IngestClient("svc-api", topic, payload)
	if err != nil {
		t.Fatalf("unplaced local configure: %v", err)
	}
	if res.Command == nil {
		t.Fatal("local configure executed without returning its command outcome")
	}
	if _, err := unplaced.IngestClient(
		"svc-api", "colca/v1/_CmdAdmin/n-edge1/enroll", payload,
	); err == nil {
		t.Fatal("unplaced local service gained implicit _CmdAdmin")
	}

	placedIDs := fakeIDs{entries: map[string]*uns.Entry{
		"svc-ui": {ULID: "svc-ui", Kind: uns.KindLocal, Element: "el-ui"},
	}}
	placed := newEngineWithIDs(t, placedIDs)
	if _, err := placed.IngestAdmin(
		"colca/v1/_SystemElement/n-edge1/ui", []byte(`{"id":"el-ui","name":"ui"}`),
	); err != nil {
		t.Fatal(err)
	}
	if _, err := placed.IngestClient("svc-ui", topic, payload); err == nil {
		t.Fatal("placed local service configured without a cmd grant")
	}
	placedIDs.entries["svc-ui"].Grants = []string{"cmd:el-ui/#:configure"}
	if _, err := placed.IngestClient(
		"svc-ui", "colca/v1/_CmdConfigure/n-edge1/ui/signal/upsert", payload,
	); err != nil {
		t.Fatalf("placed local configure with covering grant: %v", err)
	}
}

// Level 4 must name THIS node, whoever the publisher is.
func TestLevel4MustBeThisNode(t *testing.T) {
	e := newTestEngine(t, "n1")
	_, err := e.IngestClient("01JSVC", "colca/v1/_Metric/other-node/line1/temp", metricPayload)
	if err == nil {
		t.Fatal("a client filed a record under another node's ULID")
	}
	assertRejectReason(t, err, metrics.ReasonNodeID)
}

// A service scoped to one subtree cannot write outside it — the level-3 proof
// of this task's write rule (task-7/8 brief; un-quarantines
// tests/node/test_node_contract.py::test_a_publish_outside_the_write_scope_is_rejected).
func TestAPublishOutsideTheWriteScopeIsRejected(t *testing.T) {
	e := newTestEngineScoped(t, "n1", "el-press3", "line1/press3")

	// Inside the granted element's subtree: admitted. Asserted first so a
	// broken placement (el-press3 never actually resolving to line1/press3)
	// cannot make the rejection below pass for the wrong reason — a scope
	// that grants nothing rejects everything too.
	res, err := e.IngestClient("01JSVC", "colca/v1/_Metric/n1/line1/press3/leaf", metricPayload)
	if err != nil || !res.Persisted {
		t.Fatalf("a publish inside the granted write scope was rejected: %v", err)
	}

	// Outside it: denied, even though grammar/level-4 are otherwise fine.
	_, err = e.IngestClient("01JSVC", "colca/v1/_Metric/n1/line1/press4/temp", metricPayload)
	if err == nil {
		t.Fatal("a scoped service wrote outside its subtree")
	}
	assertRejectReason(t, err, metrics.ReasonWriteDenied)
}

func TestProjectedEntityAuthorsCannotImpersonateNodesOrServices(t *testing.T) {
	e := newEngine(t)

	if _, err := e.IngestClient("m1", "colca/v1/_ServiceDetails/n-edge1/m1/_service", []byte(
		`{"id":"m1","name":"opcua","service_type":"connector","colca_node_id":"other"}`)); err == nil {
		t.Fatal("a service must not claim a different Colca node")
	}
	if _, err := e.IngestClient("m1", "colca/v1/_ServiceDetails/n-edge1/m1/_service", []byte(
		`{"id":"other","name":"opcua","service_type":"connector","colca_node_id":"n-edge1"}`)); err == nil {
		t.Fatal("a service record id must equal its authenticated identity")
	}
	if _, err := e.IngestClient("m1", "colca/v1/_ServiceDetails/n-edge1/m1/_service", []byte(
		`{"id":"m1","name":"opcua","service_type":"connector","colca_node_id":"n-edge1"}`)); err != nil {
		t.Fatalf("the service's own registration must be accepted: %v", err)
	}

	if _, err := e.IngestClient("m1", "colca/v1/_Node/n-edge1/_colca/nodes/m1", []byte(
		`{"id":"m1","name":"fake","root_system_element_id":"el-m1"}`)); err == nil {
		t.Fatal("a machine door must not author a Colca node")
	}
	if _, err := e.IngestAdmin("colca/v1/_Node/other/_colca/nodes/other", []byte(
		`{"id":"other","name":"fake","root_system_element_id":"el-m1"}`)); err == nil {
		t.Fatal("the local admin door must not author another Colca node")
	}
	if _, err := e.IngestAdmin("colca/v1/_Node/n-edge1/_colca/nodes/n-edge1", []byte(
		`{"id":"n-edge1","name":"edge1","root_system_element_id":"el-m1"}`)); err != nil {
		t.Fatalf("the node must be able to author its own Colca record: %v", err)
	}
}

func TestDefinitionsAreAuthoredByTheLocalNodeOnly(t *testing.T) {
	e := newEngine(t)
	payload := []byte(`{"id":"meta-1","name":"work_order","data_type":"string"}`)

	if _, err := e.IngestClient("m1", "colca/v1/_MetadataType/n-edge1/meta-1", payload); err == nil {
		t.Fatal("a machine door must not author a global definition")
	}
	if _, err := e.IngestAdmin("colca/v1/_MetadataType/other/meta-1", payload); err == nil {
		t.Fatal("the local admin door must not impersonate another definition author")
	}
	if _, err := e.IngestAdmin("colca/v1/_MetadataType/n-edge1/meta-1", payload); err != nil {
		t.Fatalf("the node must be able to author its own definition: %v", err)
	}
}

// Registry entries enter through the enrollment door ONLY (auth §3): _EnrolledIdentity
// is rejected at both ordinary ingest doors, no matter who sends it.
func TestEnrolledIdentityRejectedAtOrdinaryDoors(t *testing.T) {
	e := newEngine(t)
	before := e.Store().NextOffset("entities") // the fixture's own element placements
	if _, err := e.IngestClient("m1", "colca/v1/_EnrolledIdentity/n-edge1/_colca/identities/m1", []byte(`{"ulid":"m1"}`)); err == nil {
		t.Fatal("client _EnrolledIdentity publish must be rejected")
	}
	if _, err := e.IngestAdmin("colca/v1/_EnrolledIdentity/n-edge1/_colca/identities/x", []byte(`{"ulid":"x"}`)); err == nil {
		t.Fatal("admin _EnrolledIdentity publish must be rejected")
	}
	if got := e.Store().NextOffset("entities"); got != before {
		t.Fatalf("rejected _EnrolledIdentity must not be persisted: entities %d → %d", before, got)
	}
	// Replication is NOT an ordinary door: a child's already-enrolled fact
	// rides upward like any entity (rejecting it would hole the stream).
	recs := []store.ReplRecord{{ChildOffset: 1, Topic: "colca/v1/_EnrolledIdentity/child1/edge1/_colca/identities/m9",
		Payload: []byte(`{"ulid":"m9"}`), TS: 1, KVPath: "edge1/_colca/identities/m9", KVNode: "child1"}}
	if _, _, err := e.IngestReplicated("child1", "entities", recs); err != nil {
		t.Fatalf("replicated _EnrolledIdentity must be accepted: %v", err)
	}
	if kv := mustKVScan(t, e.Store(), "edge1/_colca/identities/m9"); len(kv) != 1 {
		t.Fatalf("replicated _EnrolledIdentity must project into KV: %v", kv)
	}
}

// Time-sync design §2.2/§4: _TimeSync is ephemeral and node-local-publish-only
// — unlike _EnrolledIdentity, it is rejected at EVERY ingest door including
// replication, since a well-behaved child's own store can never legitimately
// contain one (its own engine already rejects it before persistence). Both
// the canonical (no-path) and a padded topic shape must be rejected with the
// SAME dedicated reason, not the generic "grammar" reason Parse's 4-segment
// relaxation would otherwise produce.
func TestTimeSyncRejectedAtEveryIngestDoor(t *testing.T) {
	s, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	cfg := &config.Config{ULID: "n-edge1"}
	m := metrics.New(s, config.Retention{}, nil)
	e := New(s, cfg, testIDs(), nil, m, nil)

	const rejectedLine = `colca_rejected_publishes_total{reason="time_sync"}`
	if v := metricstest.Value(t, m, rejectedLine); v != 0 {
		t.Fatalf("%s = %v before any attempt, want 0", rejectedLine, v)
	}

	for _, topic := range []string{"colca/v1/_TimeSync/n-edge1", "colca/v1/_TimeSync/n-edge1/extra"} {
		if _, err := e.IngestClient("m1", topic, []byte(`{"now_ms":1}`)); err == nil || !strings.Contains(err.Error(), "_TimeSync") {
			t.Fatalf("client publish of %s must be rejected with a _TimeSync-specific error, got %v", topic, err)
		}
		if _, err := e.IngestAdmin(topic, []byte(`{"now_ms":1}`)); err == nil || !strings.Contains(err.Error(), "_TimeSync") {
			t.Fatalf("admin publish of %s must be rejected with a _TimeSync-specific error, got %v", topic, err)
		}
		// The human door too (merge composition, human-authz × time-sync):
		// rejected with the dedicated time_sync reason, not human_write —
		// and no grant, however wide, changes that.
		if _, err := e.IngestHuman(humanEntry(t, "read:#", "cmd:#:admin", "admin:#"), topic, []byte(`{"now_ms":1}`)); err == nil || !strings.Contains(err.Error(), "_TimeSync") {
			t.Fatalf("human publish of %s must be rejected with a _TimeSync-specific error, got %v", topic, err)
		}
	}
	for _, stream := range []string{"metrics", "entities", "commands", "definitions", "audit"} {
		if off := e.Store().NextOffset(stream); off != 1 {
			t.Fatalf("stream %s next offset = %d, want 1 (rejected _TimeSync must never persist)", stream, off)
		}
	}
	if v := metricstest.Value(t, m, rejectedLine); v != 6 {
		t.Fatalf("%s = %v after 6 rejected attempts (2 topic shapes x client+admin+human), want 6", rejectedLine, v)
	}

	// Replication: a forged child offset carrying a _TimeSync record is
	// dropped before it reaches the store; sibling records in the same batch
	// still apply, and the surviving higher offset still advances the hwm.
	recs := []store.ReplRecord{
		{ChildOffset: 1, Topic: "colca/v1/_Metric/m1/child1/m1/a", Payload: []byte(`{"v":1}`), TS: 1, KVPath: "child1/m1/a", KVNode: "m1"},
		{ChildOffset: 2, Topic: "colca/v1/_TimeSync/n-child", Payload: []byte(`{"now_ms":1}`), TS: 1},
		{ChildOffset: 3, Topic: "colca/v1/_Metric/m1/child1/m1/b", Payload: []byte(`{"v":2}`), TS: 2, KVPath: "child1/m1/b", KVNode: "m1"},
	}
	applied, hwm, err := e.IngestReplicated("n-child", "metrics", recs)
	if err != nil {
		t.Fatal(err)
	}
	if applied != 2 {
		t.Fatalf("applied = %d, want 2 (the _TimeSync record must be dropped, never persisted)", applied)
	}
	if hwm != 3 {
		t.Fatalf("hwm = %d, want 3", hwm)
	}
	if v := metricstest.Value(t, m, rejectedLine); v != 7 {
		t.Fatalf("%s = %v after the replicated _TimeSync attempt, want 7", rejectedLine, v)
	}
}

// Dropping a forged _TimeSync record from a replicated
// batch must not falsely trip the §6.4 gap-jump detector — that log/metric
// means genuine, investigatable child-side data loss, and dropping an
// ephemeral _TimeSync record lost nothing. A gap only PARTIALLY explained by
// a dropped _TimeSync record (a real offset is also genuinely missing) must
// still log, unchanged — the fix removes the false positive, not real
// detection.
func TestTimeSyncDropDoesNotFalsePositiveGapJump(t *testing.T) {
	const marker = "replication offset jump"

	t.Run("gap fully explained by the dropped record is silent", func(t *testing.T) {
		s, err := store.Open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { s.Close() })
		buf := captureLogs(t)
		m := metrics.New(s, config.Retention{}, nil)
		e := New(s, &config.Config{ULID: "n-parent"}, testIDs(), nil, m, nil)

		recs := []store.ReplRecord{
			{ChildOffset: 1, Topic: "colca/v1/_Metric/m1/child1/m1/a", Payload: []byte(`{"v":1}`), TS: 1, KVPath: "child1/m1/a", KVNode: "m1"},
			{ChildOffset: 2, Topic: "colca/v1/_TimeSync/n-child", Payload: []byte(`{"now_ms":1}`), TS: 1},
			{ChildOffset: 3, Topic: "colca/v1/_Metric/m1/child1/m1/b", Payload: []byte(`{"v":2}`), TS: 2, KVPath: "child1/m1/b", KVNode: "m1"},
		}
		applied, hwm, err := e.IngestReplicated("n-child", "metrics", recs)
		if err != nil {
			t.Fatal(err)
		}
		if applied != 2 || hwm != 3 {
			t.Fatalf("applied=%d hwm=%d, want 2/3", applied, hwm)
		}
		if strings.Contains(buf.String(), marker) {
			t.Fatalf("gap fully explained by a dropped _TimeSync record must not log a jump:\n%s", buf.String())
		}
		if body := scrapeBody(t, m); strings.Contains(body, `colca_repl_gap_applied_total{child="n-child",stream="metrics"}`) {
			t.Fatalf("gap fully explained by a dropped _TimeSync record must not touch colca_repl_gap_applied_total:\n%s", body)
		}
	})

	t.Run("a genuinely missing offset alongside a dropped record still logs", func(t *testing.T) {
		s, err := store.Open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { s.Close() })
		buf := captureLogs(t)
		m := metrics.New(s, config.Retention{}, nil)
		e := New(s, &config.Config{ULID: "n-parent"}, testIDs(), nil, m, nil)

		// Offset 2 is the forged _TimeSync (dropped, explained); offset 3 is
		// ALSO simply absent from the batch — a real, separate loss — so the
		// gap from 1 to 4 is only partially explained by the drop.
		recs := []store.ReplRecord{
			{ChildOffset: 1, Topic: "colca/v1/_Metric/m1/child1/m1/a", Payload: []byte(`{"v":1}`), TS: 1, KVPath: "child1/m1/a", KVNode: "m1"},
			{ChildOffset: 2, Topic: "colca/v1/_TimeSync/n-child", Payload: []byte(`{"now_ms":1}`), TS: 1},
			{ChildOffset: 4, Topic: "colca/v1/_Metric/m1/child1/m1/c", Payload: []byte(`{"v":3}`), TS: 3, KVPath: "child1/m1/c", KVNode: "m1"},
		}
		applied, hwm, err := e.IngestReplicated("n-child", "metrics", recs)
		if err != nil {
			t.Fatal(err)
		}
		if applied != 2 || hwm != 4 {
			t.Fatalf("applied=%d hwm=%d, want 2/4", applied, hwm)
		}
		if !strings.Contains(buf.String(), marker) || !strings.Contains(buf.String(), "have=1") || !strings.Contains(buf.String(), "got=4") {
			t.Fatalf("a partially-explained gap (real offset 3 also missing) must still log:\n%s", buf.String())
		}
		if body := scrapeBody(t, m); !strings.Contains(body, `colca_repl_gap_applied_total{child="n-child",stream="metrics"} 1`) {
			t.Fatalf("a partially-explained gap must still increment colca_repl_gap_applied_total:\n%s", body)
		}
	})
}

// A client with a covering cmd grant may publish commands of the granted
// class into the granted zone — absolute node-local paths, no mount rewrite,
// no level-4 identity rule (auth §5.3 ActCmd).
func TestClientCmdGrants(t *testing.T) {
	e := newEngine(t)
	payload := []byte(`{"correlation_id":"c","expires_at":99999999999}`)
	res, err := e.IngestClient("hmi", "colca/v1/_CmdParam/m1/m1/set-speed", payload)
	if err != nil {
		t.Fatalf("granted cmd rejected: %v", err)
	}
	if res.Stream != "commands" {
		t.Fatalf("%+v", res)
	}
	recs, _, _ := e.Store().Read("commands", res.Offset, 1, nil)
	if recs[0].Topic != "colca/v1/_CmdParam/m1/m1/set-speed" {
		t.Fatalf("cmd publish must not be rewritten: %s", recs[0].Topic)
	}
	// Class outside the grant.
	if _, err := e.IngestClient("hmi", "colca/v1/_CmdMaintain/m1/m1/calibrate", payload); err == nil {
		t.Fatal("ungranted cmd class must be rejected")
	}
	// Zone outside the grant.
	if _, err := e.IngestClient("hmi", "colca/v1/_CmdParam/x/other/set", payload); err == nil {
		t.Fatal("cmd outside the granted zone must be rejected")
	}
	// Invalid payload still rejected even with a grant.
	if _, err := e.IngestClient("hmi", "colca/v1/_CmdParam/m1/m1/set", []byte(`{}`)); err == nil {
		t.Fatal("cmd payload validation must still apply")
	}
}

// humanEntry builds the ephemeral token entry the doors hand to IngestHuman.
func humanEntry(t *testing.T, grants ...string) *uns.Entry {
	t.Helper()
	e, err := uns.TokenEntry("kc-sub-anna", grants)
	if err != nil {
		t.Fatal(err)
	}
	return e
}

// Humans command and nothing else (World-2 rule, human-authz §5.2).
func TestIngestHumanCommandsOnly(t *testing.T) {
	e := newEngine(t)
	payload := []byte(`{"correlation_id":"c","expires_at":99999999999999}`)

	// Granted command persists to the commands stream, no rewrite.
	res, err := e.IngestHuman(humanEntry(t, "cmd:el-m1/#:param"), "colca/v1/_CmdParam/m1/m1/set-speed", payload)
	if err != nil {
		t.Fatalf("granted human cmd rejected: %v", err)
	}
	if res.Stream != "commands" {
		t.Fatalf("%+v", res)
	}
	recs, _, _ := e.Store().Read("commands", res.Offset, 1, nil)
	if recs[0].Topic != "colca/v1/_CmdParam/m1/m1/set-speed" {
		t.Fatalf("human cmd must not be rewritten: %s", recs[0].Topic)
	}

	// Class outside the grant → cmd_denied.
	if _, err := e.IngestHuman(humanEntry(t, "cmd:el-m1/#:param"), "colca/v1/_CmdMaintain/m1/m1/cal", payload); err == nil {
		t.Fatal("ungranted class must be rejected")
	}
	// No grant at all → cmd_denied.
	if _, err := e.IngestHuman(humanEntry(t), "colca/v1/_CmdParam/m1/m1/x", payload); err == nil {
		t.Fatal("grantless human cmd must be rejected")
	}

	// Data / entity / ack: rejected regardless of grants — there IS no grant
	// that allows a human to write state.
	before := map[string]uint64{}
	for _, s := range []string{"metrics", "entities", "commands", "audit"} {
		before[s] = e.Store().NextOffset(s)
	}
	wide := humanEntry(t, "read:#", "cmd:#:admin", "admin:#")
	for _, topic := range []string{
		"colca/v1/_Metric/m1/m1/temp",
		"colca/v1/_Signal/m1/m1/cfg",
		"colca/v1/_Ack/m1/m1/set-speed",
	} {
		if _, err := e.IngestHuman(wide, topic, []byte(`{"v":1,"ulid":"x","correlation_id":"c","result_code":1}`)); err == nil {
			t.Fatalf("human write of %s must be rejected", topic)
		}
	}
	// _EnrolledIdentity: enrollment-door rule wins over the human_write rule.
	if _, err := e.IngestHuman(wide, "colca/v1/_EnrolledIdentity/n-edge1/_colca/identities/x", []byte(`{"ulid":"x"}`)); err == nil {
		t.Fatal("human _EnrolledIdentity publish must be rejected")
	}
	// Invalid payload on a granted command still validates.
	if _, err := e.IngestHuman(humanEntry(t, "cmd:el-m1/#:param"), "colca/v1/_CmdParam/m1/m1/x", []byte(`{}`)); err == nil {
		t.Fatal("cmd payload validation must still apply")
	}
	for _, s := range []string{"metrics", "entities"} {
		if got := e.Store().NextOffset(s); got != before[s] {
			t.Fatalf("rejected human writes persisted to %s: %d → %d", s, before[s], got)
		}
	}
}

// Move-drain design §3.2 item 2: a mount under an active drain rejects new
// ClassCmd publishes at admission — client (grant notwithstanding) and admin
// alike — with reason "draining", not the ordinary "cmd_denied". A command
// outside the draining mount is unaffected.
func TestClassCmdRejectedUnderDrainingMount(t *testing.T) {
	s, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	cfg := &config.Config{ULID: "n-edge1"}
	m := metrics.New(s, config.Retention{}, nil)
	e := New(s, cfg, testIDsWithDraining(), nil, m, nil)
	placeTestElements(t, e)

	const rejectedLine = `colca_rejected_publishes_total{reason="draining"}`
	if v := metricstest.Value(t, m, rejectedLine); v != 0 {
		t.Fatalf("%s = %v before any attempt, want 0", rejectedLine, v)
	}

	payload := []byte(`{"correlation_id":"c","expires_at":99999999999}`)
	// A client with a covering grant still gets rejected — draining outranks
	// the grant check (engine.go checks it first).
	if _, err := e.IngestClient("hmi", "colca/v1/_CmdParam/m1/m1/set-speed", payload); err == nil || !strings.Contains(err.Error(), "draining") {
		t.Fatalf("client cmd under a draining mount must be rejected mentioning 'draining', got %v", err)
	}
	// The admin token is not exempt either.
	if _, err := e.IngestAdmin("colca/v1/_CmdParam/m1/m1/set-speed", payload); err == nil || !strings.Contains(err.Error(), "draining") {
		t.Fatalf("admin cmd under a draining mount must be rejected mentioning 'draining', got %v", err)
	}
	// Nor is the parent->child relay door: in a
	// multi-hop tree, a command authored ABOVE this node's own parent —
	// where this node's own draining child is invisible — relays down and
	// arrives here exactly like any other downlinked command. Without this
	// check it would land straight in the draining mount, the "chasing a
	// moving tail" failure the admission gate exists to prevent.
	if _, err := e.IngestDownlink("colca/v1/_CmdParam/m1/m1/set-speed", payload, 4711); err == nil || !strings.Contains(err.Error(), "draining") {
		t.Fatalf("downlinked cmd under a draining mount must be rejected mentioning 'draining', got %v", err)
	}
	if e.Store().NextOffset("commands") != 1 {
		t.Fatal("rejected commands must not be persisted")
	}
	if v := metricstest.Value(t, m, rejectedLine); v != 3 {
		t.Fatalf("%s = %v after 3 rejected attempts (client+admin+downlink), want 3", rejectedLine, v)
	}

	// A command outside the draining mount is unaffected, on all three doors.
	if _, err := e.IngestAdmin("colca/v1/_CmdParam/hmi/hmi/ping", payload); err != nil {
		t.Fatalf("cmd outside the draining mount must still be admitted (admin): %v", err)
	}
	if _, err := e.IngestDownlink("colca/v1/_CmdParam/hmi/hmi/ping", payload, 4712); err != nil {
		t.Fatalf("cmd outside the draining mount must still be admitted (downlink): %v", err)
	}

	// The human door is a door too ("at every door", move-drain design §3.2
	// item 2): a human's covering cmd grant does not exempt them from the
	// draining admission gate.
	if _, err := e.IngestHuman(humanEntry(t, "cmd:el-m1/#:param"), "colca/v1/_CmdParam/m1/m1/set-speed", payload); err == nil || !strings.Contains(err.Error(), "draining") {
		t.Fatalf("human cmd under a draining mount must be rejected mentioning 'draining', got %v", err)
	}
	if v := metricstest.Value(t, m, rejectedLine); v != 4 {
		t.Fatalf("%s = %v after 4 rejected attempts (client+admin+downlink+human), want 4", rejectedLine, v)
	}
	if _, err := e.IngestHuman(humanEntry(t, "cmd:el-hmi/#:param"), "colca/v1/_CmdParam/hmi/hmi/ping", payload); err != nil {
		t.Fatalf("cmd outside the draining mount must still be admitted (human): %v", err)
	}
}

func TestValidationReject(t *testing.T) {
	e := newEngine(t)
	_, err := e.IngestClient("m1", "colca/v1/_Metric/n-edge1/m1/temp", []byte(`{"v":"bad"}`))
	if err == nil || ReasonOf(err) != metrics.ReasonValidation {
		t.Fatalf("invalid payload must be rejected with reason validation, got %v (reason %q)", err, ReasonOf(err))
	}
	if e.Store().NextOffset("metrics") != 1 {
		t.Fatal("rejected payload must not be persisted")
	}
}

func TestAdminPublishCmdNoRewrite(t *testing.T) {
	e := newEngine(t)
	res, err := e.IngestAdmin("colca/v1/_CmdParam/m1/m1/set-speed", []byte(`{"correlation_id":"c1","expires_at":99999999999}`))
	if err != nil {
		t.Fatal(err)
	}
	if res.Stream != "commands" {
		t.Fatalf("%+v", res)
	}
	recs, _, _ := e.Store().Read("commands", 1, 10, nil)
	if recs[0].Topic != "colca/v1/_CmdParam/m1/m1/set-speed" {
		t.Fatal("admin publish must not be rewritten")
	}
}

func TestNonUnsIgnored(t *testing.T) {
	e := newEngine(t)
	res, err := e.IngestClient("m1", "other/random/topic", []byte("x"))
	if err != nil {
		t.Fatal("non-UNS must pass through without error")
	}
	if res.Persisted {
		t.Fatal("non-UNS must not be persisted")
	}
}

// The bus mirrors the STORE, so what a subscriber sees is exactly the topic
// the client published — there is no rewrite any more — and a metric is
// state, so it is retained.
func TestIngestClientDeliversCanonicalTopicRetained(t *testing.T) {
	e, rec := newRecordingEngine(t)
	if _, err := e.IngestClient("m1", "colca/v1/_Metric/n-edge1/m1/temp", []byte(`{"v":7}`)); err != nil {
		t.Fatal(err)
	}
	got := rec.got()
	if len(got) != 1 {
		t.Fatalf("want exactly one delivery, got %d: %+v", len(got), got)
	}
	want := delivery{Topic: "colca/v1/_Metric/n-edge1/m1/temp", Payload: `{"v":7}`, Retain: true}
	if got[0] != want {
		t.Fatalf("delivery = %+v, want %+v", got[0], want)
	}
}

// A command is an event: retaining it would re-deliver a stale command to every
// new subscriber.
func TestIngestAdminDeliversCommandUnretained(t *testing.T) {
	e, rec := newRecordingEngine(t)
	topic := "colca/v1/_CmdParam/m1/m1/set-speed"
	if _, err := e.IngestAdmin(topic, []byte(`{"correlation_id":"c1","expires_at":99999999999}`)); err != nil {
		t.Fatal(err)
	}
	got := rec.got()
	if len(got) != 1 {
		t.Fatalf("want exactly one delivery, got %d: %+v", len(got), got)
	}
	if got[0].Topic != topic {
		t.Fatalf("topic = %q, want %q", got[0].Topic, topic)
	}
	if got[0].Retain {
		t.Fatal("_CmdParam must NOT be retained — commands are events, not state")
	}
}

// A _StreamGap marker is an event (design §6.4: "no KV projection, not
// retained"), exactly like a command/ack — it must never hit the local
// broker's retained set. Pins the real retainFor function directly (not just
// the class enum in plugins/uns): mutating retainFor to also cover ClassGap
// turns this red.
func TestRetainForExcludesStreamGap(t *testing.T) {
	if retainFor(uns.ClassGap) {
		t.Fatal("retainFor(ClassGap) must be false — _StreamGap is an event, not state")
	}
	// Sanity: the two classes that ARE retained still are, so the assertion
	// above is actually exercising the gate, not a vacuously-false function.
	if !retainFor(uns.ClassData) || !retainFor(uns.ClassEntity) {
		t.Fatal("retainFor must still retain data/entity classes")
	}
}

// A downlinked command is delivered exactly once: persistTS mirrors it, and
// IngestDownlink must not publish a second copy on top of that.
func TestIngestDownlinkDeliversOnce(t *testing.T) {
	e, rec := newRecordingEngine(t)
	topic := "colca/v1/_CmdParam/m1/m1/set-speed"
	if _, err := e.IngestDownlink(topic, []byte(`{"correlation_id":"c1","expires_at":99999999999}`), 4711); err != nil {
		t.Fatal(err)
	}
	got := rec.got()
	if len(got) != 1 {
		t.Fatalf("downlink must deliver exactly once, got %d: %+v", len(got), got)
	}
	if got[0].Topic != topic || got[0].Retain {
		t.Fatalf("delivery = %+v", got[0])
	}
}

// The bus must never show something the store rejected.
func TestRejectedPublishDeliversNothing(t *testing.T) {
	e, rec := newRecordingEngine(t)
	if _, err := e.IngestClient("m1", "colca/v1/_Metric/n-edge1/m1/temp", []byte(`{"v":"bad"}`)); err == nil {
		t.Fatal("invalid payload must be rejected")
	}
	if _, err := e.IngestClient("m1", "colca/v1/_Metric/OTHER/temp", []byte(`{"v":1}`)); err == nil {
		t.Fatal("wrong level-4 must be rejected")
	}
	if got := rec.got(); len(got) != 0 {
		t.Fatalf("a rejected publish must deliver nothing, got %+v", got)
	}
}

// An identity with no write grant covering the topic — "hmi" holds only a
// cmd grant — may connect and subscribe, but the engine refuses everything it
// publishes. The rejected metric never reaches the bus; the security audit
// event does, unretained (auth §5, writeZones).
func TestClientWithNoWriteScopeMayNotPublish(t *testing.T) {
	e, rec := newRecordingEngine(t)
	_, err := e.IngestClient("hmi", "colca/v1/_Metric/n-edge1/hmi/temp", []byte(`{"v":1}`))
	if err == nil || ReasonOf(err) != metrics.ReasonWriteDenied {
		t.Fatalf("publish with no write scope must be rejected with reason write_denied, got %v (reason %q)", err, ReasonOf(err))
	}
	if e.Store().NextOffset("metrics") != 1 {
		t.Fatal("publish with no write scope must not be persisted")
	}
	if got := rec.got(); len(got) != 1 || !strings.Contains(got[0].Topic, "/_AuditEvent/") || got[0].Retain {
		t.Fatalf("publish denial must deliver only one unretained audit event, got %+v", got)
	}
}

// Replication is the fourth write path and must mirror too — but only records
// that were actually applied. Pushing the same batch twice delivers nothing the
// second time, exactly like the high-water-mark dedupe in the store.
func TestIngestReplicatedDeliversOnlyNewRecords(t *testing.T) {
	e, rec := newRecordingEngine(t)
	batch := []store.ReplRecord{
		{ChildOffset: 1, Topic: "colca/v1/_Metric/m1/edge1/m1/a", Payload: []byte(`{"v":1}`), TS: 1, KVPath: "edge1/m1/a", KVNode: "m1"},
		{ChildOffset: 2, Topic: "colca/v1/_Metric/m1/edge1/m1/b", Payload: []byte(`{"v":2}`), TS: 2, KVPath: "edge1/m1/b", KVNode: "m1"},
	}
	applied, hwm, err := e.IngestReplicated("n-edge1", "metrics", batch)
	if err != nil {
		t.Fatal(err)
	}
	if applied != 2 || hwm != 2 {
		t.Fatalf("applied %d hwm %d, want 2/2", applied, hwm)
	}
	got := rec.got()
	if len(got) != 2 {
		t.Fatalf("want 2 deliveries, got %d: %+v", len(got), got)
	}
	for i, want := range []delivery{
		{Topic: "colca/v1/_Metric/m1/edge1/m1/a", Payload: `{"v":1}`, Retain: true},
		{Topic: "colca/v1/_Metric/m1/edge1/m1/b", Payload: `{"v":2}`, Retain: true},
	} {
		if got[i] != want {
			t.Fatalf("delivery %d = %+v, want %+v", i, got[i], want)
		}
	}

	// same batch again → fully deduplicated → nothing new on the bus
	applied, hwm, err = e.IngestReplicated("n-edge1", "metrics", batch)
	if err != nil {
		t.Fatal(err)
	}
	if applied != 0 || hwm != 2 {
		t.Fatalf("second apply: applied %d hwm %d, want 0/2", applied, hwm)
	}
	if len(rec.got()) != 2 {
		t.Fatalf("deduplicated records must not be delivered again: %+v", rec.got())
	}

	// an _Ack replicating up is an event, so it is mirrored unretained
	ack := []store.ReplRecord{{ChildOffset: 1, Topic: "colca/v1/_Ack/m1/edge1/m1/set-speed", Payload: []byte(`{"correlation_id":"c","result_code":200}`), TS: 3}}
	if _, _, err := e.IngestReplicated("n-edge1", "commands", ack); err != nil {
		t.Fatal(err)
	}
	last := rec.got()[2]
	if last.Topic != "colca/v1/_Ack/m1/edge1/m1/set-speed" || last.Retain {
		t.Fatalf("_Ack delivery = %+v, want the ack topic unretained", last)
	}
}

// A record whose topic does not parse is logged and skipped — never a panic,
// and never a reason to fail the apply that already happened.
func TestIngestReplicatedSkipsUnparseableTopic(t *testing.T) {
	e, rec := newRecordingEngine(t)
	batch := []store.ReplRecord{
		{ChildOffset: 1, Topic: "garbage", Payload: []byte(`{"v":1}`), TS: 1},
		{ChildOffset: 2, Topic: "colca/v1/_Metric/m1/edge1/m1/b", Payload: []byte(`{"v":2}`), TS: 2, KVPath: "edge1/m1/b", KVNode: "m1"},
	}
	applied, _, err := e.IngestReplicated("n-edge1", "metrics", batch)
	if err != nil {
		t.Fatal(err)
	}
	if applied != 2 {
		t.Fatalf("both records must still be applied, got %d", applied)
	}
	got := rec.got()
	if len(got) != 1 || got[0].Topic != "colca/v1/_Metric/m1/edge1/m1/b" {
		t.Fatalf("only the parseable record may be mirrored: %+v", got)
	}
}

// captureLogs routes slog.Default through a buffer for the duration of the
// test, returning the buffer. Engines constructed AFTER the call log into it.
func captureLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return &buf
}

func newCapturedEngine(t *testing.T) (*Engine, *bytes.Buffer) {
	t.Helper()
	buf := captureLogs(t)
	s, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return New(s, &config.Config{ULID: "n-parent"}, testIDs(), nil, nil, nil), buf
}

func replBatchAt(topic string, offsets ...uint64) []store.ReplRecord {
	out := make([]store.ReplRecord, len(offsets))
	for i, o := range offsets {
		out[i] = store.ReplRecord{ChildOffset: o, Topic: topic, Payload: []byte(`{"v":1}`), TS: int64(o)}
	}
	return out
}

// Spec §6.4 second net: child stream offsets are gapless and the uplink reads
// them contiguously, so an applied ChildOffset above HWM+1 is a gap — the
// parent logs it to catch a child that failed to emit its _StreamGap marker.
// Detection only; the batch is still applied unchanged.
func TestIngestReplicatedLogsOffsetJumps(t *testing.T) {
	const marker = "replication offset jump"
	metric := "colca/v1/_Metric/m1/child1/m1/t"

	t.Run("contiguous offsets are silent", func(t *testing.T) {
		e, buf := newCapturedEngine(t)
		if _, _, err := e.IngestReplicated("n-child", "metrics", replBatchAt(metric, 1, 2, 3)); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(buf.String(), marker) {
			t.Fatalf("contiguous batch logged a jump:\n%s", buf.String())
		}
	})

	t.Run("jump across batches is logged and applied", func(t *testing.T) {
		e, buf := newCapturedEngine(t)
		if _, _, err := e.IngestReplicated("n-child", "metrics", replBatchAt(metric, 1, 2)); err != nil {
			t.Fatal(err)
		}
		applied, hwm, err := e.IngestReplicated("n-child", "metrics", replBatchAt(metric, 5, 6))
		if err != nil || applied != 2 || hwm != 6 {
			t.Fatalf("apply after jump: %d %d %v — detection must never block the apply", applied, hwm, err)
		}
		if !strings.Contains(buf.String(), marker) || !strings.Contains(buf.String(), "have=2") || !strings.Contains(buf.String(), "got=5") {
			t.Fatalf("jump 2→5 not logged:\n%s", buf.String())
		}
	})

	t.Run("jump inside a batch is logged", func(t *testing.T) {
		e, buf := newCapturedEngine(t)
		if _, _, err := e.IngestReplicated("n-child", "metrics", replBatchAt(metric, 1, 2, 7)); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(buf.String(), marker) || !strings.Contains(buf.String(), "got=7") {
			t.Fatalf("in-batch jump 2→7 not logged:\n%s", buf.String())
		}
	})

	t.Run("first contact past offset 1 is a gap", func(t *testing.T) {
		// The child pruned before ever replicating: the parent genuinely
		// misses [1..3] — the fresh-cursor twin of the §6.1 [delta].
		e, buf := newCapturedEngine(t)
		if _, _, err := e.IngestReplicated("n-child", "audit", replBatchAt("colca/v1/_AuditEvent/n-child/child1/_colca/audit/e1", 4, 5)); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(buf.String(), marker) || !strings.Contains(buf.String(), "have=0") || !strings.Contains(buf.String(), "got=4") {
			t.Fatalf("first-contact jump not logged:\n%s", buf.String())
		}
	})

	t.Run("filtered uplinks are exempt", func(t *testing.T) {
		// The commands uplink carries only _Ack + _StreamGap, and the entities
		// uplink keeps the node-private Edit receipt home, so child-offset
		// holes on either are the filter working, not data loss. The domain
		// answers which streams (uns.UplinkCarriesEveryRecord); a hand-kept
		// `stream == "commands"` here would have reported every Edit
		// command executed at a child as a gap at its parent.
		for stream, topic := range map[string]string{
			"commands": "colca/v1/_Ack/m1/child1/m1/go",
			"entities": "colca/v1/_SystemElement/m1/child1/m1/a",
		} {
			e, buf := newCapturedEngine(t)
			if _, _, err := e.IngestReplicated("n-child", stream, replBatchAt(topic, 3, 9)); err != nil {
				t.Fatal(err)
			}
			if strings.Contains(buf.String(), marker) {
				t.Fatalf("filtered %s stream must not report offset jumps:\n%s", stream, buf.String())
			}
		}
	})
}

// Retention design §7.1: an empty payload on a KV-projecting class is the
// tombstone. The engine appends the record as history (correct class/stream,
// validation's field checks bypassed by the §7 rule), deletes the KV key in the
// same batch, and mirrors the empty payload retained — the retained-clear —
// onto the bus. Exercised for both KV classes: data (client path, no rewrite)
// and entity (admin path, no rewrite).
func TestEmptyPayloadTombstonesKVAndDeliversRetainedClear(t *testing.T) {
	e, rec := newRecordingEngine(t)

	// Data class through the client path.
	if _, err := e.IngestClient("m1", "colca/v1/_Metric/n-edge1/m1/temp", []byte(`{"v":7}`)); err != nil {
		t.Fatal(err)
	}
	res, err := e.IngestClient("m1", "colca/v1/_Metric/n-edge1/m1/temp", nil)
	if err != nil {
		t.Fatalf("empty payload on _Metric must be accepted as a tombstone: %v", err)
	}
	if !res.Persisted || res.Stream != "metrics" || res.Offset != 2 {
		t.Fatalf("tombstone result = %+v, want persisted metrics offset 2", res)
	}
	if got := mustKVScan(t, e.Store(), "m1/temp"); len(got) != 0 {
		t.Fatalf("tombstone did not retire the KV key: %+v", got)
	}
	recs, _, _ := e.Store().Read("metrics", 1, 10, nil)
	if len(recs) != 2 || len(recs[1].Payload) != 0 || recs[1].Topic != "colca/v1/_Metric/n-edge1/m1/temp" {
		t.Fatalf("tombstone record = %+v, want empty payload under the topic published, unchanged", recs)
	}

	// Entity class through the admin path.
	if _, err := e.IngestAdmin("colca/v1/_Signal/m1/m1/sig-a", []byte(`{"id":"sig-a"}`)); err != nil {
		t.Fatal(err)
	}
	res, err = e.IngestAdmin("colca/v1/_Signal/m1/m1/sig-a", nil)
	if err != nil {
		t.Fatalf("empty payload on _Signal must be accepted as a tombstone: %v", err)
	}
	if res.Stream != "entities" {
		t.Fatalf("entity tombstone stream = %q, want entities", res.Stream)
	}
	if got := mustKVScan(t, e.Store(), "m1/sig-a"); len(got) != 0 {
		t.Fatalf("entity tombstone did not retire the KV key: %+v", got)
	}

	// Delivery: each tombstone is mirrored as an empty payload with retain=true
	// — the MQTT retained-clear — under the stored topic.
	got := rec.got()
	if len(got) != 4 {
		t.Fatalf("want 4 deliveries (2 sets + 2 clears), got %d: %+v", len(got), got)
	}
	if want := (delivery{Topic: "colca/v1/_Metric/n-edge1/m1/temp", Payload: "", Retain: true}); got[1] != want {
		t.Fatalf("metric clear delivery = %+v, want %+v", got[1], want)
	}
	if want := (delivery{Topic: "colca/v1/_Signal/m1/m1/sig-a", Payload: "", Retain: true}); got[3] != want {
		t.Fatalf("entity clear delivery = %+v, want %+v", got[3], want)
	}
}

// Retention design §7.3: for non-KV classes an empty payload was never a valid
// value and deletion is not meaningful — commands and acks with empty payloads
// are rejected, nothing is persisted, nothing reaches the bus.
func TestEmptyPayloadRejectedForNonKVClasses(t *testing.T) {
	e, rec := newRecordingEngine(t)
	if _, err := e.IngestAdmin("colca/v1/_CmdParam/m1/m1/set-speed", nil); err == nil {
		t.Fatal("empty _CmdParam payload must be rejected — commands cannot be tombstoned")
	}
	if _, err := e.IngestClient("m1", "colca/v1/_Ack/n-edge1/set-speed", nil); err == nil || ReasonOf(err) != metrics.ReasonValidation {
		t.Fatalf("empty _Ack payload must be rejected with reason validation — acks cannot be tombstoned, got %v (reason %q)", err, ReasonOf(err))
	}
	if e.Store().NextOffset("commands") != 1 {
		t.Fatal("rejected empty payloads must not be persisted")
	}
	if got := rec.got(); len(got) != 0 {
		t.Fatalf("rejected empty payloads must deliver nothing, got %+v", got)
	}
}

// The level-4-is-this-node rule already gates tombstones: an empty payload is
// a publish like any other, so a client cannot retire another NODE's path.
func TestClientCannotTombstoneForeignPath(t *testing.T) {
	e, rec := newRecordingEngine(t)
	// A path owned by node OTHER, seeded as replicated state would be.
	if _, _, err := e.Store().Append("metrics", []store.Record{
		{Topic: "colca/v1/_Metric/OTHER/x/temp", Payload: []byte(`{"v":1}`), TS: 1, KVPath: "x/temp", KVNode: "OTHER"},
	}); err != nil {
		t.Fatal(err)
	}
	_, err := e.IngestClient("m1", "colca/v1/_Metric/OTHER/x/temp", nil)
	if err == nil || ReasonOf(err) != metrics.ReasonNodeID {
		t.Fatalf("foreign tombstone must fail the level-4 rule, got: %v (reason %q)", err, ReasonOf(err))
	}
	if got := mustKVScan(t, e.Store(), "x/temp"); len(got) != 1 {
		t.Fatalf("foreign KV entry must survive the rejected tombstone: %+v", got)
	}
	if e.Store().NextOffset("metrics") != 2 {
		t.Fatal("rejected tombstone must not be persisted")
	}
	if got := rec.got(); len(got) != 0 {
		t.Fatalf("rejected tombstone must deliver nothing, got %+v", got)
	}
}

// Retention design §7.1: the tombstone replicates upward like any record and
// retires the path at the ancestor the same way — KV key deleted by
// ApplyReplicated, retained message cleared by the empty-payload mirror.
func TestIngestReplicatedTombstoneRetiresKVAndClearsRetained(t *testing.T) {
	e, rec := newRecordingEngine(t)
	set := []store.ReplRecord{{ChildOffset: 1, Topic: "colca/v1/_Metric/m1/edge1/m1/a", Payload: []byte(`{"v":1}`), TS: 1, KVPath: "edge1/m1/a", KVNode: "m1"}}
	if _, _, err := e.IngestReplicated("n-edge1", "metrics", set); err != nil {
		t.Fatal(err)
	}
	if len(mustKVScan(t, e.Store(), "edge1/m1/a")) != 1 {
		t.Fatal("setup: replicated KV entry missing")
	}

	tomb := []store.ReplRecord{{ChildOffset: 2, Topic: "colca/v1/_Metric/m1/edge1/m1/a", TS: 2, KVPath: "edge1/m1/a", KVNode: "m1", Delete: true}}
	applied, hwm, err := e.IngestReplicated("n-edge1", "metrics", tomb)
	if err != nil {
		t.Fatal(err)
	}
	if applied != 1 || hwm != 2 {
		t.Fatalf("tombstone apply: %d/%d, want 1/2", applied, hwm)
	}
	if got := mustKVScan(t, e.Store(), "edge1/m1/a"); len(got) != 0 {
		t.Fatalf("replicated tombstone did not retire the parent KV: %+v", got)
	}
	got := rec.got()
	if len(got) != 2 {
		t.Fatalf("want 2 deliveries, got %d: %+v", len(got), got)
	}
	if want := (delivery{Topic: "colca/v1/_Metric/m1/edge1/m1/a", Payload: "", Retain: true}); got[1] != want {
		t.Fatalf("parent clear delivery = %+v, want %+v (empty payload, retained)", got[1], want)
	}
}

// IngestRefresh (retention spec §6.5 [delta]) is the pruner's guarded refresh
// entry: full admin semantics when the KV guard holds — append, KV upsert, bus
// mirror with retain — and a TOTAL skip when it does not: no record, no
// delivery, nil error. Non-KV classes and empty payloads (a refresh must never
// smuggle a tombstone) are rejected outright.
func TestIngestRefreshGuardAndSkipSemantics(t *testing.T) {
	e, rec := newRecordingEngine(t)
	topic := "colca/v1/_SystemElement/n-edge1/line1/press"
	// Offsets are relative to whatever the fixture already wrote (its own
	// element placements), so the test states the CAS position it means rather
	// than assuming an empty stream.
	seeded, err := e.IngestAdmin(topic, []byte(`{"id":"P1"}`))
	if err != nil {
		t.Fatal(err)
	}

	// Guard holds: applied with full delivery semantics.
	res, applied, err := e.IngestRefresh(topic, []byte(`{"id":"P1"}`), seeded.Offset)
	if err != nil || !applied || !res.Persisted || res.Offset != seeded.Offset+1 {
		t.Fatalf("refresh = (%+v, %v, %v), want applied at offset %d", res, applied, err, seeded.Offset+1)
	}
	got := rec.got()
	if len(got) != 2 || got[1].Topic != topic || !got[1].Retain {
		t.Fatalf("applied refresh must mirror retained onto the bus: %+v", got)
	}

	// Tombstone the path, then refresh against the stale snapshot: total skip.
	if _, err := e.IngestAdmin(topic, nil); err != nil { // tombstone, offset 3
		t.Fatal(err)
	}
	before := e.Store().NextOffset("entities")
	deliveries := len(rec.got())
	res, applied, err = e.IngestRefresh(topic, []byte(`{"id":"P1"}`), seeded.Offset+1)
	if err != nil || applied || res.Persisted {
		t.Fatalf("stale refresh = (%+v, %v, %v), want a clean skip", res, applied, err)
	}
	if e.Store().NextOffset("entities") != before {
		t.Fatal("skipped refresh appended a record")
	}
	if len(rec.got()) != deliveries {
		t.Fatal("skipped refresh delivered to the bus")
	}
	if kv := mustKVScan(t, e.Store(), "line1/press"); len(kv) != 0 {
		t.Fatalf("skipped refresh resurrected the tombstoned path: %+v", kv)
	}

	// Guard is meaningless outside KV classes; empty payloads are not refreshes.
	if _, _, err := e.IngestRefresh("colca/v1/_Ack/n-edge1/line1/x", []byte(`{"correlation_id":"c","result_code":0}`), 1); err == nil {
		t.Fatal("refresh of a non-KV class must be rejected")
	}
	if _, _, err := e.IngestRefresh(topic, nil, 2); err == nil {
		t.Fatal("empty refresh payload must be rejected — a refresh cannot tombstone")
	}
}
