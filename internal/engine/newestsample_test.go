package engine

import (
	"testing"

	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/store"
)

// An older sample reaches subscribers but not as the retained message: the
// bus keeps the newest sample as current, like the KV entry.
func TestOlderSampleIsDeliveredWithoutTheRetainedFlag(t *testing.T) {
	e, rec := newRecordingEngine(t)
	const topic = "colca/v1/_Metric/n-edge1/m1/temp"
	newer := `{"v":2,"timestamp":1700000200}`
	for _, p := range []string{newer, `{"v":1,"timestamp":1700000100}`, `{"v":3,"timestamp":1700000200}`} {
		if _, err := e.IngestClient("m1", topic, []byte(p)); err != nil {
			t.Fatal(err)
		}
	}
	got := rec.got()
	if len(got) != 3 {
		t.Fatalf("deliveries %+v, want 3", got)
	}
	want := []bool{true, false, true}
	for i, d := range got {
		if d.Retain != want[i] {
			t.Fatalf("delivery %d (%s) retain=%v, want %v", i, d.Payload, d.Retain, want[i])
		}
	}
	kv, ok, err := e.Store().KVGet("m1/temp", "n-edge1", topic)
	if err != nil || !ok || string(kv.Payload) != `{"v":3,"timestamp":1700000200}` {
		t.Fatalf("KV %s ok=%v err=%v, want the same-timestamp correction", kv.Payload, ok, err)
	}
}

func TestReplicatedOlderSampleIsNotRetainedAtTheParent(t *testing.T) {
	s, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	cfg := &config.Config{ULID: "n-hub"}
	cfg.Bus.RetainChildMetrics = true
	rec := &recorder{}
	e := New(s, cfg, testIDs(), rec.deliver, nil, nil)
	const topic = "colca/v1/_Metric/n-edge1/edge1/m1/temp"
	r := func(off uint64, p string) store.ReplRecord {
		return store.ReplRecord{ChildOffset: off, Topic: topic, Payload: []byte(p), TS: 1_800_000_000_000, KVPath: "edge1/m1/temp", KVNode: "n-edge1"}
	}
	if _, _, err := e.IngestReplicated("n-edge1", "metrics", []store.ReplRecord{
		r(1, `{"v":2,"timestamp":1700000200}`), r(2, `{"v":1,"timestamp":1700000100}`),
	}); err != nil {
		t.Fatal(err)
	}
	got := rec.got()
	if len(got) != 2 || !got[0].Retain || got[1].Retain {
		t.Fatalf("deliveries %+v, want the newer retained and the older not", got)
	}
}
