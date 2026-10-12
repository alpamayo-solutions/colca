package repl

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/alpamayo-solutions/colca/internal/clock"
	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/hostclock"
	"github.com/alpamayo-solutions/colca/internal/metrics"
	"github.com/alpamayo-solutions/colca/internal/metrics/metricstest"
	"github.com/alpamayo-solutions/colca/internal/store"
)

func fixedClock(isRoot bool, at time.Time) *clock.Clock {
	return clock.NewWithHost(isRoot, func() time.Time { return at }, func() hostclock.Status { return hostclock.Status{} })
}

// Both replication responses carry now_ms from the answering node's own wall
// clock. A node in the middle of the tree answers with its own clock too, not
// with one corrected towards its parent: each node measures against the node it
// talks to. Fixed clocks make the check exact.
func TestReplicationResponsesCarryTheAnsweringNodesOwnClock(t *testing.T) {
	dir := t.TempDir()
	parentID := mustIdentity(t, filepath.Join(dir, "p.key"))
	childID := mustIdentity(t, filepath.Join(dir, "c.key"))

	const parentMS = int64(2_000_000_000_000)
	// The answering node is itself a child that measured a skew of a minute to
	// its own parent.
	clk := fixedClock(false, time.UnixMilli(parentMS))

	ps := mustStore(t, filepath.Join(dir, "pdata"))
	pcfg := &config.Config{ULID: "n-parent", Repl: config.Endpoint{Addr: "127.0.0.1:0"}}
	preg, peng := nodeParts(t, ps, pcfg, nil, nil, clk, childSpec{"n-child", childID.PublicHex(), "child1"})
	peng.ObserveParentClock(parentMS + 60_000)
	srv, addr := startServer(t, pcfg, peng, parentID, preg)
	defer srv.Stop()

	cl := mustClient(t, addr, parentID.PublicHex(), childID)

	// A queued command means the downlink poll returns at once instead of riding
	// out the long poll.
	mustIngestAdmin(t, peng, "colca/v1/_CmdParam/m1/child1/m1/go", `{"correlation_id":"c1","expires_at":99999999999}`)

	res, err := cl.downlink(t.Context(), 1, 1, 10, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if res.NowMS != parentMS {
		t.Fatalf("/downlink now_ms = %d, want %d: the answering node's own wall clock, uncorrected", res.NowMS, parentMS)
	}

	_, nowMS, err := cl.replicate(t.Context(), envelope{stream: "metrics"}, []store.ReplRecord{
		{ChildOffset: 1, Topic: "colca/v1/_Metric/m1/m1/temp", Payload: []byte(`{"v":1}`), TS: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	if nowMS != parentMS {
		t.Fatalf("/replicate now_ms = %d, want %d: the answering node's own wall clock, uncorrected", nowMS, parentMS)
	}
}

// A child whose clock is 20s behind its parent's measures that from the
// parent's now_ms on a /downlink response and exports it. Its own clock is not
// moved: the node keeps deciding on the host's time. All clocks are fixed, so
// once a poll was answered the values are exact; waitFor only waits for the
// HTTP round trip.
func TestAChildMeasuresItsSkewFromTheParentsNowMSAndKeepsItsOwnClock(t *testing.T) {
	dir := t.TempDir()
	parentAt := time.UnixMilli(3_000_000_000_000)
	childAt := parentAt.Add(-20 * time.Second)

	parentID := mustIdentity(t, filepath.Join(dir, "p.key"))
	childID := mustIdentity(t, filepath.Join(dir, "c.key"))
	ps := mustStore(t, filepath.Join(dir, "pdata"))
	pcfg := &config.Config{ULID: "n-parent", Repl: config.Endpoint{Addr: "127.0.0.1:0"}}
	preg, peng := nodeParts(t, ps, pcfg, nil, nil, fixedClock(true, parentAt), childSpec{"n-child", childID.PublicHex(), "child1"})
	srv, addr := startServer(t, pcfg, peng, parentID, preg)
	defer srv.Stop()
	// A command under the child's mount so its first poll returns at once. The
	// child attaches in front of it: the filler only makes the position seedable.
	mustIngestAdmin(t, peng, "colca/v1/_CmdParam/m1/child1/m1/filler", `{"correlation_id":"c0","expires_at":99999999999999}`)
	childFrom := ps.NextOffset("commands")
	mustIngestAdmin(t, peng, "colca/v1/_CmdParam/m1/child1/m1/go", `{"correlation_id":"c1","expires_at":99999999999999}`)

	cs := mustStore(t, filepath.Join(dir, "cdata"))
	cclk := fixedClock(false, childAt)
	cm := metrics.New(cs, config.Retention{}, cclk)
	_, ceng := nodeParts(t, cs, &config.Config{ULID: "n-child"}, nil, cm, cclk)

	const skewLine = "colca_clock_parent_skew_seconds"
	if v, ok := metricstest.Lookup(t, cm, skewLine); ok {
		t.Fatalf("%s = %v before the first response from the parent, want the series absent", skewLine, v)
	}

	cl := mustClient(t, addr, parentID.PublicHex(), childID)
	attachAt(t, cs, cl, childFrom)
	stop, done := make(chan struct{}), make(chan struct{})
	go func() { defer close(done); RunDownlink(cl, ceng, cm, stop) }()
	t.Cleanup(func() { close(stop); waitForClosed(t, "RunDownlink to stop", done, 5*time.Second) })

	waitFor(t, "the child to measure its skew to the parent", 5*time.Second, func() bool {
		_, ok := metricstest.Lookup(t, cm, skewLine)
		return ok
	})
	if v := metricstest.Value(t, cm, skewLine); v != 20 {
		t.Fatalf("%s = %v, want 20: the parent's clock is 20s ahead of the child's", skewLine, v)
	}
	if v := metricstest.Value(t, cm, "colca_clock_parent_sample_age_seconds"); v != 0 {
		t.Fatalf("colca_clock_parent_sample_age_seconds = %v on a fixed clock, want 0", v)
	}
	if got := ceng.Now(); !got.Equal(childAt) {
		t.Fatalf("the child's clock = %v after the sample, want its own wall clock %v: the skew is measured, never applied", got, childAt)
	}
}

// A node that only pushes, with no downlink poll answered yet, measures its
// skew all the same: every /replicate response carries the parent's clock.
func TestAChildMeasuresItsSkewFromAReplicateResponse(t *testing.T) {
	dir := t.TempDir()
	parentAt := time.UnixMilli(3_000_000_000_000)
	childAt := parentAt.Add(3 * time.Second) // the child's clock runs ahead

	parentID := mustIdentity(t, filepath.Join(dir, "p.key"))
	childID := mustIdentity(t, filepath.Join(dir, "c.key"))
	ps := mustStore(t, filepath.Join(dir, "pdata"))
	pcfg := &config.Config{ULID: "n-parent", Repl: config.Endpoint{Addr: "127.0.0.1:0"}}
	preg, peng := nodeParts(t, ps, pcfg, nil, nil, fixedClock(true, parentAt), childSpec{"n-child", childID.PublicHex(), "child1"})
	srv, addr := startServer(t, pcfg, peng, parentID, preg)
	defer srv.Stop()

	cs := mustStore(t, filepath.Join(dir, "cdata"))
	cclk := fixedClock(false, childAt)
	cm := metrics.New(cs, config.Retention{}, cclk)
	_, ceng := nodeParts(t, cs, &config.Config{ULID: "n-child"}, nil, cm, cclk)
	payload := []byte(`{"event_id":"evt-1","source":"api","action":"authorize","outcome":"denied","actor_kind":"human","occurred_at":1}`)
	if _, _, err := cs.Append("audit", []store.Record{{
		Topic: "colca/v1/_AuditEvent/n-child/_colca/audit/evt-1", Payload: payload, TS: 1, WrittenBy: "api",
	}}); err != nil {
		t.Fatal(err)
	}

	cl := mustClient(t, addr, parentID.PublicHex(), childID)
	stop, done := make(chan struct{}), make(chan struct{})
	go func() { defer close(done); RunUplink(cl, ceng, nil, cm, stop) }()
	t.Cleanup(func() { close(stop); waitForClosed(t, "RunUplink to stop", done, 5*time.Second) })

	const skewLine = "colca_clock_parent_skew_seconds"
	waitFor(t, "the child to measure its skew to the parent", 5*time.Second, func() bool {
		_, ok := metricstest.Lookup(t, cm, skewLine)
		return ok
	})
	if v := metricstest.Value(t, cm, skewLine); v != -3 {
		t.Fatalf("%s = %v, want -3: the parent's clock is 3s behind the child's", skewLine, v)
	}
}
