package engine

import (
	"bytes"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/store"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

func newEngine(t *testing.T) *Engine {
	t.Helper()
	s, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	cfg := &config.Config{ULID: "n-edge1", Clients: []config.Client{{ULID: "m1", Token: "tok", Mount: "m1"}}}
	return New(s, cfg, nil, nil) // nil, nil = no local MQTT delivery, no metrics in unit tests
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
	cfg := &config.Config{ULID: "n-edge1", Clients: []config.Client{
		{ULID: "m1", Token: "tok", Mount: "m1"},
		{ULID: "observer", Token: "observer-secret"},
	}}
	rec := &recorder{}
	return New(s, cfg, rec.deliver, nil), rec
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
	return New(s, &config.Config{ULID: "n-parent"}, nil, nil), buf
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
