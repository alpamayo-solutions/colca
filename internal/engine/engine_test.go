package engine

import (
	"strings"
	"sync"
	"testing"

	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/store"
)

func newEngine(t *testing.T) *Engine {
	t.Helper()
	s, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	cfg := &config.Config{ULID: "n-edge1", Clients: []config.Client{{ULID: "m1", Token: "tok", Mount: "m1"}}}
	return New(s, cfg, nil) // nil = no local MQTT delivery in unit tests
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
	return New(s, cfg, rec.deliver), rec
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
