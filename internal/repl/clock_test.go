package repl

import (
	"net/http"
	"path/filepath"
	"strings"
	"sync/atomic"
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
	peng.ObserveParentClock(clock.ParentSample{NowMS: parentMS + 60_000, Sent: time.UnixMilli(parentMS), Received: time.UnixMilli(parentMS)})
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
	if res.Clock.NowMS != parentMS {
		t.Fatalf("/downlink now_ms = %d, want %d: the answering node's own wall clock, uncorrected", res.Clock.NowMS, parentMS)
	}

	_, pc, err := cl.replicate(t.Context(), envelope{stream: "metrics"}, []store.ReplRecord{
		{ChildOffset: 1, Topic: "colca/v1/_Metric/m1/m1/temp", Payload: []byte(`{"v":1}`), TS: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	if pc.NowMS != parentMS {
		t.Fatalf("/replicate now_ms = %d, want %d: the answering node's own wall clock, uncorrected", pc.NowMS, parentMS)
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

// sharedWall is one wall clock for two nodes, as two hosts synchronized to the
// same source have. Tests move it by hand.
type sharedWall struct{ ms atomic.Int64 }

func (w *sharedWall) now() time.Time          { return time.UnixMilli(w.ms.Load()) }
func (w *sharedWall) advance(d time.Duration) { w.ms.Add(d.Milliseconds()) }
func (w *sharedWall) clock(isRoot bool) *clock.Clock {
	return clock.NewWithHost(isRoot, w.now, func() hostclock.Status { return hostclock.Status{} })
}

// stalledLink delays the first /downlink response that carries a command: the
// shared clock moves on by stall between the parent writing the response and
// the child reading it, as when the link drops packets for a while and the TCP
// session survives.
type stalledLink struct {
	next    http.RoundTripper
	wall    *sharedWall
	stall   time.Duration
	stalled atomic.Bool
}

func (l *stalledLink) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := l.next.RoundTrip(req)
	if err == nil && strings.HasSuffix(req.URL.Path, "/downlink") && req.URL.Query().Get("hello") == "" &&
		req.URL.Query().Get("confirm") == "" && l.stalled.CompareAndSwap(false, true) {
		l.wall.advance(l.stall)
	}
	return resp, err
}

// Parent and child are on one clock. The response that hands the child a
// command sits on a stalled link for 6s. The child stores the command and
// measures no skew from that response: read against the receive time alone it
// would be 6s and an alarm.
func TestAResponseStalledOnTheLinkIsNotMeasuredAsSkew(t *testing.T) {
	dir := t.TempDir()
	wall := &sharedWall{}
	wall.ms.Store(3_000_000_000_000)

	parentID := mustIdentity(t, filepath.Join(dir, "p.key"))
	childID := mustIdentity(t, filepath.Join(dir, "c.key"))
	ps := mustStore(t, filepath.Join(dir, "pdata"))
	pcfg := &config.Config{ULID: "n-parent", Repl: config.Endpoint{Addr: "127.0.0.1:0"}}
	preg, peng := nodeParts(t, ps, pcfg, nil, nil, wall.clock(true), childSpec{"n-child", childID.PublicHex(), "child1"})
	srv, addr := startServer(t, pcfg, peng, parentID, preg)
	defer srv.Stop()
	mustIngestAdmin(t, peng, "colca/v1/_CmdParam/m1/child1/m1/filler", `{"correlation_id":"c0","expires_at":99999999999999}`)
	childFrom := ps.NextOffset("commands")
	mustIngestAdmin(t, peng, "colca/v1/_CmdParam/m1/child1/m1/go", `{"correlation_id":"c1","expires_at":99999999999999}`)

	cs := mustStore(t, filepath.Join(dir, "cdata"))
	cclk := wall.clock(false)
	cm := metrics.New(cs, config.Retention{}, cclk)
	_, ceng := nodeParts(t, cs, &config.Config{ULID: "n-child"}, nil, cm, cclk)

	cl := mustClient(t, addr, parentID.PublicHex(), childID)
	link := &stalledLink{next: cl.http.Transport, wall: wall, stall: 6 * time.Second}
	cl.http.Transport = link
	attachAt(t, cs, cl, childFrom)
	stop, done := make(chan struct{}), make(chan struct{})
	go func() { defer close(done); RunDownlink(cl, ceng, cm, stop) }()
	t.Cleanup(func() { close(stop); waitForClosed(t, "RunDownlink to stop", done, 5*time.Second) })

	const discarded = "colca_clock_parent_samples_discarded_total"
	waitFor(t, "the child to take the command from the stalled response", 5*time.Second, func() bool {
		return cs.NextOffset("commands") > 1 && metricstest.Value(t, cm, discarded) == 1
	})
	// The page is confirmed over the link as it is now, and that exchange is a
	// sample: the two clocks agree.
	waitFor(t, "the child to measure its skew from the confirming request", 5*time.Second, func() bool {
		_, ok := metricstest.Lookup(t, cm, "colca_clock_parent_skew_seconds")
		return ok
	})
	if v := metricstest.Value(t, cm, "colca_clock_parent_skew_seconds"); v != 0 {
		t.Fatalf("colca_clock_parent_skew_seconds = %v after a response that waited 6s on the link, want 0: the stall is not skew", v)
	}
	if status := ceng.ClockStatus(); status["parent_skew_ms"] != int64(0) {
		t.Fatalf("/health clock = %v, want parent_skew_ms 0", status)
	}
}

// A long poll that the parent holds is not a slow link: the response says how
// long it was held and the child takes that off the exchange. Both nodes run on
// the machine's real clock here, so the measured skew is near zero and the
// sample, although the exchange outlasted clock.MaxRoundTrip, is used.
func TestTheParentsLongPollHoldIsNotCountedAsRoundTrip(t *testing.T) {
	dir := t.TempDir()
	parentID := mustIdentity(t, filepath.Join(dir, "p.key"))
	childID := mustIdentity(t, filepath.Join(dir, "c.key"))
	ps := mustStore(t, filepath.Join(dir, "pdata"))
	pcfg := &config.Config{ULID: "n-parent", Repl: config.Endpoint{Addr: "127.0.0.1:0"}}
	onRealTime := func(isRoot bool) *clock.Clock {
		return clock.NewWithHost(isRoot, time.Now, func() hostclock.Status { return hostclock.Status{} })
	}
	preg, peng := nodeParts(t, ps, pcfg, nil, nil, onRealTime(true), childSpec{"n-child", childID.PublicHex(), "child1"})
	srv, addr := startServer(t, pcfg, peng, parentID, preg)
	defer srv.Stop()
	cl := mustClient(t, addr, parentID.PublicHex(), childID)
	hello, err := cl.hello(t.Context(), greeting{})
	if err != nil {
		t.Fatal(err)
	}

	const hold = clock.MaxRoundTrip + 300*time.Millisecond
	go func() {
		time.Sleep(hold)
		mustIngestAdmin(t, peng, "colca/v1/_CmdParam/m1/child1/m1/go", `{"correlation_id":"c1","expires_at":99999999999999}`)
	}()
	sent := time.Now()
	res, err := cl.downlink(t.Context(), hello.Head, hello.DefNext, 10, 10*time.Second)
	received := time.Now()
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Records) != 1 {
		t.Fatalf("the poll returned %d commands, want the one issued while it waited", len(res.Records))
	}
	if held := time.Duration(res.Clock.HeldMS) * time.Millisecond; held < hold-50*time.Millisecond || held > received.Sub(sent) {
		t.Fatalf("held_ms = %v for a poll the parent held about %v (exchange %v)", held, hold, received.Sub(sent))
	}
	skew, _, used := onRealTime(false).ObserveParent(res.Clock.sample(sent, received))
	if !used || skew.Abs() > clock.MaxRoundTrip/2 {
		t.Fatalf("skew = %v, used = %v, want a used sample near zero: the hold is not round trip", skew, used)
	}

	// /replicate reports its hold as well.
	_, pc, err := cl.replicate(t.Context(), envelope{stream: "metrics"}, []store.ReplRecord{
		{ChildOffset: 1, Topic: "colca/v1/_Metric/m1/m1/temp", Payload: []byte(`{"v":1}`), TS: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	if pc.NowMS == 0 || pc.HeldMS < 0 || pc.HeldMS > 5_000 {
		t.Fatalf("/replicate clock = %+v, want now_ms and a small held_ms", pc)
	}
}
