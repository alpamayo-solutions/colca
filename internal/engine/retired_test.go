package engine

import (
	"strings"
	"testing"

	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/metrics"
	"github.com/alpamayo-solutions/colca/internal/metrics/metricstest"
	"github.com/alpamayo-solutions/colca/internal/store"
)

// An older child still replicates _ClockProgress markers from its metrics
// stream. The parent stores none of them, applies the samples around them, and
// acknowledges the whole batch, a marker at its end included, so the lane moves
// on. Each marker counts as a rejected publish; the skipped offsets are not a gap.
func TestReplicatedRecordsOfRetiredContractsAreSkipped(t *testing.T) {
	s, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	logs := captureLogs(t)
	m := metrics.New(s, config.Retention{}, nil)
	e := New(s, &config.Config{ULID: "n-parent"}, testIDs(), nil, m, nil)

	const rejectedLine = `colca_rejected_publishes_total{reason="validation"}`
	recs := []store.ReplRecord{
		{ChildOffset: 1, Topic: "colca/v1/_Metric/m1/child1/m1/a", Payload: []byte(`{"v":1}`), TS: 1, KVPath: "child1/m1/a", KVNode: "m1"},
		{ChildOffset: 2, Topic: "colca/v1/_ClockProgress/n-child/child1/historian", Payload: []byte(`{"run_id":"r","processed_at":1,"ready":true,"observed_at":1}`), TS: 1, KVPath: "child1/historian", KVNode: "n-child"},
		{ChildOffset: 3, Topic: "colca/v1/_Metric/m1/child1/m1/b", Payload: []byte(`{"v":2}`), TS: 2, KVPath: "child1/m1/b", KVNode: "m1"},
		{ChildOffset: 4, Topic: "colca/v1/_ClockProgress/n-child/child1/historian", Payload: []byte(`{"run_id":"r","processed_at":2,"ready":true,"observed_at":2}`), TS: 2, KVPath: "child1/historian", KVNode: "n-child"},
	}
	applied, hwm, err := e.IngestReplicated("n-child", "metrics", recs)
	if err != nil {
		t.Fatal(err)
	}
	if applied != 2 || hwm != 4 {
		t.Fatalf("applied=%d hwm=%d, want 2 samples applied and the HWM past the last marker (4)", applied, hwm)
	}
	if got := e.Store().NextOffset("metrics"); got != 3 {
		t.Fatalf("metrics next offset %d, want 3: only the two samples are stored", got)
	}
	entries, err := e.Store().KVScan("")
	if err != nil {
		t.Fatal(err)
	}
	for _, en := range entries {
		if strings.Contains(en.Topic, "_ClockProgress") {
			t.Fatalf("retired record reached KV: %s", en.Topic)
		}
	}
	if v := metricstest.Value(t, m, rejectedLine); v != 2 {
		t.Fatalf("%s = %v, want 2", rejectedLine, v)
	}
	if strings.Contains(logs.String(), "replication offset jump") {
		t.Fatalf("skipped markers reported as a gap:\n%s", logs.String())
	}

	// A resend of the same batch is a replay: nothing new, no error.
	if applied, hwm, err := e.IngestReplicated("n-child", "metrics", recs); err != nil || applied != 0 || hwm != 4 {
		t.Fatalf("replay: applied=%d hwm=%d err=%v, want 0/4/nil", applied, hwm, err)
	}
}
