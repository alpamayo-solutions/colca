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

// fakeIDs is a test Mounts: ulid → entry.
type fakeIDs struct {
	entries  map[string]*uns.Entry
	draining []string // mounts DrainingMount treats as under an active drain
}

func (f fakeIDs) MountOf(ulid string) (string, bool) {
	e, ok := f.entries[ulid]
	if !ok || e.Mount == "" {
		return "", false
	}
	return e.Mount, true
}
func (f fakeIDs) Get(ulid string) (*uns.Entry, bool) { e, ok := f.entries[ulid]; return e, ok }

// DrainingMount mirrors registry.Manager.DrainingMount's own boundary rule
// (path-separator, not string-prefix) against the test-configured set of
// draining mounts.
func (f fakeIDs) DrainingMount(path string) bool {
	for _, mount := range f.draining {
		if strings.HasPrefix(path, mount+"/") {
			return true
		}
	}
	return false
}

func testIDs() fakeIDs {
	return fakeIDs{entries: map[string]*uns.Entry{
		"m1":       {ULID: "m1", Kind: uns.KindMachine, Mount: "m1"},
		"observer": {ULID: "observer", Kind: uns.KindMachine},
		"hmi":      {ULID: "hmi", Kind: uns.KindMachine, Mount: "hmi", Grants: []string{"cmd:m1/#:param"}},
	}}
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
	return New(s, cfg, ids, nil, nil, nil) // nils = no local MQTT delivery, no metrics, no clock in unit tests
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

func (r *recorder) got() []delivery {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]delivery(nil), r.seen...)
}

// newRecordingEngine is newEngine plus a recording LocalDeliver, and a second
// mount-less "observer" client (read-only, may not publish).
func newRecordingEngine(t *testing.T) (*Engine, *recorder) {
	t.Helper()
	s, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	cfg := &config.Config{ULID: "n-edge1"}
	rec := &recorder{}
	return New(s, cfg, testIDs(), rec.deliver, nil, nil), rec
}

func TestClientPublishMountAndKV(t *testing.T) {
	e := newEngine(t)
	res, err := e.IngestClient("m1", "colca/v1/_Metric/m1/temp", []byte(`{"v":7}`))
	if err != nil {
		t.Fatal(err)
	}
	if res.Stream != "metrics" || res.Offset != 1 {
		t.Fatalf("%+v", res)
	}
	recs, _, _ := e.Store().Read("metrics", 1, 10, nil)
	if recs[0].Topic != "colca/v1/_Metric/m1/m1/temp" {
		t.Fatalf("mount rewrite failed: %s", recs[0].Topic)
	}
	kv := e.Store().KVScan("m1/temp")
	if len(kv) != 1 || kv[0].NodeID != "m1" {
		t.Fatalf("kv: %+v", kv)
	}
}

func TestClientIdentityRule(t *testing.T) {
	e := newEngine(t)
	_, err := e.IngestClient("m1", "colca/v1/_Metric/OTHER/temp", []byte(`{"v":1}`))
	if err == nil || !strings.Contains(err.Error(), "identity") {
		t.Fatalf("level-4 rule not enforced: %v", err)
	}
	_, err = e.IngestClient("m1", "colca/v1/_CmdParam/m1/x", []byte(`{"correlation_id":"c","expires_at":1}`))
	if err == nil {
		t.Fatal("clients must not publish commands")
	}
}

// Registry entries enter through the enrollment door ONLY (auth §3): _EdgeNode
// is rejected at both ordinary ingest doors, no matter who sends it.
func TestEdgeNodeRejectedAtOrdinaryDoors(t *testing.T) {
	e := newEngine(t)
	if _, err := e.IngestClient("m1", "colca/v1/_EdgeNode/m1/somewhere", []byte(`{"ulid":"m1"}`)); err == nil {
		t.Fatal("client _EdgeNode publish must be rejected")
	}
	if _, err := e.IngestAdmin("colca/v1/_EdgeNode/x/somewhere", []byte(`{"ulid":"x"}`)); err == nil {
		t.Fatal("admin _EdgeNode publish must be rejected")
	}
	if e.Store().NextOffset("entities") != 1 {
		t.Fatal("rejected _EdgeNode must not be persisted")
	}
	// Replication is NOT an ordinary door: a child's already-enrolled fact
	// rides upward like any entity (rejecting it would hole the stream).
	recs := []store.ReplRecord{{ChildOffset: 1, Topic: "colca/v1/_EdgeNode/m9/edge1/z/m9",
		Payload: []byte(`{"ulid":"m9"}`), TS: 1, KVPath: "edge1/z/m9", KVNode: "m9"}}
	if _, _, err := e.IngestReplicated("child1", "entities", recs); err != nil {
		t.Fatalf("replicated _EdgeNode must be accepted: %v", err)
	}
	if kv := e.Store().KVScan("edge1/z/m9"); len(kv) != 1 {
		t.Fatalf("replicated _EdgeNode must project into KV: %v", kv)
	}
}

// Time-sync design §2.2/§4: _TimeSync is ephemeral and node-local-publish-only
// — unlike _EdgeNode, it is rejected at EVERY ingest door including
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
	}
	for _, stream := range []string{"metrics", "entities", "commands"} {
		if off := e.Store().NextOffset(stream); off != 1 {
			t.Fatalf("stream %s next offset = %d, want 1 (rejected _TimeSync must never persist)", stream, off)
		}
	}
	if v := metricstest.Value(t, m, rejectedLine); v != 4 {
		t.Fatalf("%s = %v after 4 rejected attempts (2 topic shapes x client+admin), want 4", rejectedLine, v)
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
	if v := metricstest.Value(t, m, rejectedLine); v != 5 {
		t.Fatalf("%s = %v after the replicated _TimeSync attempt, want 5", rejectedLine, v)
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
	if e.Store().NextOffset("commands") != 1 {
		t.Fatal("rejected commands must not be persisted")
	}
	if v := metricstest.Value(t, m, rejectedLine); v != 2 {
		t.Fatalf("%s = %v after 2 rejected attempts, want 2", rejectedLine, v)
	}

	// A command outside the draining mount is unaffected.
	if _, err := e.IngestAdmin("colca/v1/_CmdParam/hmi/hmi/ping", payload); err != nil {
		t.Fatalf("cmd outside the draining mount must still be admitted: %v", err)
	}
}

func TestValidationReject(t *testing.T) {
	e := newEngine(t)
	_, err := e.IngestClient("m1", "colca/v1/_Metric/m1/temp", []byte(`{"v":"bad"}`))
	if err == nil {
		t.Fatal("invalid payload must be rejected")
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

// The bus mirrors the STORE, so what a subscriber sees is the canonical,
// mount-rewritten topic — never the raw topic the machine published — and a
// metric is state, so it is retained.
func TestIngestClientDeliversCanonicalTopicRetained(t *testing.T) {
	e, rec := newRecordingEngine(t)
	if _, err := e.IngestClient("m1", "colca/v1/_Metric/m1/temp", []byte(`{"v":7}`)); err != nil {
		t.Fatal(err)
	}
	got := rec.got()
	if len(got) != 1 {
		t.Fatalf("want exactly one delivery, got %d: %+v", len(got), got)
	}
	want := delivery{Topic: "colca/v1/_Metric/m1/m1/temp", Payload: `{"v":7}`, Retain: true}
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
	if _, err := e.IngestClient("m1", "colca/v1/_Metric/m1/temp", []byte(`{"v":"bad"}`)); err == nil {
		t.Fatal("invalid payload must be rejected")
	}
	if _, err := e.IngestClient("m1", "colca/v1/_Metric/OTHER/temp", []byte(`{"v":1}`)); err == nil {
		t.Fatal("identity violation must be rejected")
	}
	if got := rec.got(); len(got) != 0 {
		t.Fatalf("a rejected publish must deliver nothing, got %+v", got)
	}
}

// A mount-less client is a read-only observer: it may connect and subscribe,
// but the engine refuses everything it publishes, and nothing reaches the bus.
func TestObserverClientMayNotPublish(t *testing.T) {
	e, rec := newRecordingEngine(t)
	_, err := e.IngestClient("observer", "colca/v1/_Metric/observer/temp", []byte(`{"v":1}`))
	if err == nil || !strings.Contains(err.Error(), "no mount registered") {
		t.Fatalf("observer publish must be rejected with 'no mount registered', got %v", err)
	}
	if e.Store().NextOffset("metrics") != 1 {
		t.Fatal("observer publish must not be persisted")
	}
	if got := rec.got(); len(got) != 0 {
		t.Fatalf("observer publish must deliver nothing, got %+v", got)
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
		if _, _, err := e.IngestReplicated("n-child", "entities", replBatchAt("colca/v1/_SystemElement/m1/child1/m1/a", 4, 5)); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(buf.String(), marker) || !strings.Contains(buf.String(), "have=0") || !strings.Contains(buf.String(), "got=4") {
			t.Fatalf("first-contact jump not logged:\n%s", buf.String())
		}
	})

	t.Run("commands stream is exempt", func(t *testing.T) {
		// The commands uplink is filtered (_Ack + _StreamGap only), so
		// child-offset holes there are the filter working, not data loss.
		e, buf := newCapturedEngine(t)
		if _, _, err := e.IngestReplicated("n-child", "commands", replBatchAt("colca/v1/_Ack/m1/child1/m1/go", 3, 9)); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(buf.String(), marker) {
			t.Fatalf("filtered commands stream must not report offset jumps:\n%s", buf.String())
		}
	})
}

// Retention design §7.1: an empty payload on a KV-projecting class is the
// tombstone. The engine appends the record as history (correct class/stream,
// validation's field checks bypassed by the §7 rule), deletes the KV key in the
// same batch, and mirrors the empty payload retained — the retained-clear —
// onto the bus. Exercised for both KV classes: data (client path, mount
// rewrite) and entity (admin path, no rewrite).
func TestEmptyPayloadTombstonesKVAndDeliversRetainedClear(t *testing.T) {
	e, rec := newRecordingEngine(t)

	// Data class through the client path.
	if _, err := e.IngestClient("m1", "colca/v1/_Metric/m1/temp", []byte(`{"v":7}`)); err != nil {
		t.Fatal(err)
	}
	res, err := e.IngestClient("m1", "colca/v1/_Metric/m1/temp", nil)
	if err != nil {
		t.Fatalf("empty payload on _Metric must be accepted as a tombstone: %v", err)
	}
	if !res.Persisted || res.Stream != "metrics" || res.Offset != 2 {
		t.Fatalf("tombstone result = %+v, want persisted metrics offset 2", res)
	}
	if got := e.Store().KVScan("m1/temp"); len(got) != 0 {
		t.Fatalf("tombstone did not retire the KV key: %+v", got)
	}
	recs, _, _ := e.Store().Read("metrics", 1, 10, nil)
	if len(recs) != 2 || len(recs[1].Payload) != 0 || recs[1].Topic != "colca/v1/_Metric/m1/m1/temp" {
		t.Fatalf("tombstone record = %+v, want empty payload under the canonical topic", recs)
	}

	// Entity class through the admin path.
	if _, err := e.IngestAdmin("colca/v1/_Signal/m1/m1/sig-a", []byte(`{"ulid":"sig-a"}`)); err != nil {
		t.Fatal(err)
	}
	res, err = e.IngestAdmin("colca/v1/_Signal/m1/m1/sig-a", nil)
	if err != nil {
		t.Fatalf("empty payload on _Signal must be accepted as a tombstone: %v", err)
	}
	if res.Stream != "entities" {
		t.Fatalf("entity tombstone stream = %q, want entities", res.Stream)
	}
	if got := e.Store().KVScan("m1/sig-a"); len(got) != 0 {
		t.Fatalf("entity tombstone did not retire the KV key: %+v", got)
	}

	// Delivery: each tombstone is mirrored as an empty payload with retain=true
	// — the MQTT retained-clear — under the stored topic.
	got := rec.got()
	if len(got) != 4 {
		t.Fatalf("want 4 deliveries (2 sets + 2 clears), got %d: %+v", len(got), got)
	}
	if want := (delivery{Topic: "colca/v1/_Metric/m1/m1/temp", Payload: "", Retain: true}); got[1] != want {
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
	if _, err := e.IngestClient("m1", "colca/v1/_Ack/m1/set-speed", nil); err == nil {
		t.Fatal("empty _Ack payload must be rejected — acks cannot be tombstoned")
	}
	if e.Store().NextOffset("commands") != 1 {
		t.Fatal("rejected empty payloads must not be persisted")
	}
	if got := rec.got(); len(got) != 0 {
		t.Fatalf("rejected empty payloads must deliver nothing, got %+v", got)
	}
}

// The level-4 identity rule already gates tombstones: an empty payload is a
// publish like any other, so a client cannot retire another node's path.
func TestClientCannotTombstoneForeignPath(t *testing.T) {
	e, rec := newRecordingEngine(t)
	// A path owned by node OTHER, seeded as replicated state would be.
	if _, _, err := e.Store().Append("metrics", []store.Record{
		{Topic: "colca/v1/_Metric/OTHER/x/temp", Payload: []byte(`{"v":1}`), TS: 1, KVPath: "x/temp", KVNode: "OTHER"},
	}); err != nil {
		t.Fatal(err)
	}
	_, err := e.IngestClient("m1", "colca/v1/_Metric/OTHER/x/temp", nil)
	if err == nil || !strings.Contains(err.Error(), "identity") {
		t.Fatalf("foreign tombstone must fail the identity rule, got: %v", err)
	}
	if got := e.Store().KVScan("x/temp"); len(got) != 1 {
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
	if len(e.Store().KVScan("edge1/m1/a")) != 1 {
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
	if got := e.Store().KVScan("edge1/m1/a"); len(got) != 0 {
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
	if _, err := e.IngestAdmin(topic, []byte(`{"ulid":"P1"}`)); err != nil { // entities offset 1
		t.Fatal(err)
	}

	// Guard holds: applied with full delivery semantics.
	res, applied, err := e.IngestRefresh(topic, []byte(`{"ulid":"P1"}`), 1)
	if err != nil || !applied || !res.Persisted || res.Offset != 2 {
		t.Fatalf("refresh = (%+v, %v, %v), want applied at offset 2", res, applied, err)
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
	res, applied, err = e.IngestRefresh(topic, []byte(`{"ulid":"P1"}`), 2)
	if err != nil || applied || res.Persisted {
		t.Fatalf("stale refresh = (%+v, %v, %v), want a clean skip", res, applied, err)
	}
	if e.Store().NextOffset("entities") != before {
		t.Fatal("skipped refresh appended a record")
	}
	if len(rec.got()) != deliveries {
		t.Fatal("skipped refresh delivered to the bus")
	}
	if kv := e.Store().KVScan("line1/press"); len(kv) != 0 {
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
