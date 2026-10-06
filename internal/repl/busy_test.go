package repl

import (
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/metrics"
	"github.com/alpamayo-solutions/colca/internal/metrics/metricstest"
	"github.com/alpamayo-solutions/colca/internal/store"
)

// A parent holding as many push bytes as its budget allows answers the next
// push 429 with Retry-After instead of taking it into memory; the push lands
// once the budget frees, and a busy parent is not a refusal.
func TestAParentOverItsPushBudgetAnswersBusyAndTakesThePushLater(t *testing.T) {
	dir := t.TempDir()
	parentID := mustIdentity(t, filepath.Join(dir, "p.key"))
	childID := mustIdentity(t, filepath.Join(dir, "c.key"))
	ps := mustStore(t, filepath.Join(dir, "pdata"))
	pcfg := &config.Config{ULID: "n-parent", Repl: config.Endpoint{Addr: "127.0.0.1:0"}}
	preg, peng := nodeParts(t, ps, pcfg, nil, nil, nil, childSpec{"n-child", childID.PublicHex(), "child1"})
	m := metrics.New(ps, config.Retention{}, nil)
	srv, addr := startServerWithMetrics(t, pcfg, peng, parentID, preg, m)
	t.Cleanup(srv.Stop)
	cl := mustClient(t, addr, parentID.PublicHex(), childID)
	rec := []store.ReplRecord{{ChildOffset: 1, Topic: "colca/v1/_Metric/n-child/m1/t", Payload: []byte(`{"v":1}`), TS: 1}}

	srv.pushBytes.used.Store(srv.pushBytes.limit) // other pushes hold the whole budget
	_, err := cl.Replicate("metrics", rec)
	var busy *replError
	if !errors.As(err, &busy) || !busy.Busy() || busy.Refused() || busy.RetryAfter != time.Second {
		t.Fatalf("push over the budget: %v, want a busy answer with Retry-After 1s that is not a refusal", err)
	}
	if got := ps.NextOffset("metrics"); got != 1 {
		t.Fatalf("a push over the budget was applied (metrics next %d)", got)
	}
	if v := metricstest.Value(t, m, `colca_http_request_limited_total{class="replication_bytes",door="repl"}`); v != 1 {
		t.Fatalf("limited counter %v, want 1", v)
	}

	srv.pushBytes.used.Store(0)
	if _, err := cl.Replicate("metrics", rec); err != nil {
		t.Fatalf("push after the budget freed: %v", err)
	}
	if got := ps.NextOffset("metrics"); got != 2 {
		t.Fatalf("metrics next %d, want 2", got)
	}
	if used := srv.pushBytes.used.Load(); used != 0 {
		t.Fatalf("%d push bytes still held after the push finished", used)
	}
}

// A child whose parent is busy waits as long as the parent asked before it
// pushes again, instead of its own short retry, and then delivers everything
// exactly once.
func TestTheUplinkWaitsOutABusyParentAndLosesNothing(t *testing.T) {
	dir := t.TempDir()
	parentID := mustIdentity(t, filepath.Join(dir, "p.key"))
	childID := mustIdentity(t, filepath.Join(dir, "c.key"))
	ps := mustStore(t, filepath.Join(dir, "pdata"))
	pcfg := &config.Config{ULID: "n-parent", Repl: config.Endpoint{Addr: "127.0.0.1:0"}}
	preg, peng := nodeParts(t, ps, pcfg, nil, nil, nil, childSpec{"n-child", childID.PublicHex(), "child1"})
	m := metrics.New(ps, config.Retention{}, nil)
	srv, addr := startServerWithMetrics(t, pcfg, peng, parentID, preg, m)
	t.Cleanup(srv.Stop)

	cs := mustStore(t, filepath.Join(dir, "cdata"))
	_, ceng := nodeParts(t, cs, &config.Config{ULID: "n-child"}, nil, nil, nil)
	for i := range 5 {
		mustIngestAdmin(t, ceng, "colca/v1/_Metric/m1/m1/temp", `{"v":`+string(rune('0'+i))+`}`)
	}
	srv.pushBytes.used.Store(srv.pushBytes.limit)
	cl := mustClient(t, addr, parentID.PublicHex(), childID)
	stop, done := make(chan struct{}), make(chan struct{})
	go func() { defer close(done); RunUplink(cl, ceng, nil, nil, stop) }()

	const busyFor = 3 * time.Second
	time.Sleep(busyFor)
	attempts := metricstest.Value(t, m, `colca_http_request_limited_total{class="replication_bytes",door="repl"}`)
	srv.pushBytes.used.Store(0)
	waitFor(t, "the held batch to land", 10*time.Second, func() bool { return ps.NextOffset("metrics") == 6 })
	time.Sleep(300 * time.Millisecond) // a second delivery would show here
	close(stop)
	waitForClosed(t, "RunUplink to stop", done, 5*time.Second)

	// Retry-After is 1 s with jitter in [0.5 s, 1.5 s): at most ~6 in 3 s at the
	// shortest wait; the 500 ms retry it replaces would make 6-7, back to back.
	if attempts < 1 || attempts > 5 {
		t.Fatalf("%v pushes while the parent was busy for %v, want 1..5", attempts, busyFor)
	}
	if got := ps.NextOffset("metrics"); got != 6 {
		t.Fatalf("metrics next %d, want 6: every record once", got)
	}
}

// While the store is behind, a push is answered 429 before its body is read,
// and taken again once the backlog clears.
func TestAParentWhoseStoreIsBehindAnswersBusy(t *testing.T) {
	dir := t.TempDir()
	parentID := mustIdentity(t, filepath.Join(dir, "p.key"))
	childID := mustIdentity(t, filepath.Join(dir, "c.key"))
	ps := mustStore(t, filepath.Join(dir, "pdata"))
	pcfg := &config.Config{ULID: "n-parent", Repl: config.Endpoint{Addr: "127.0.0.1:0"}}
	preg, peng := nodeParts(t, ps, pcfg, nil, nil, nil, childSpec{"n-child", childID.PublicHex(), "child1"})
	m := metrics.New(ps, config.Retention{}, nil)
	srv, addr := startServerWithMetrics(t, pcfg, peng, parentID, preg, m)
	t.Cleanup(srv.Stop)
	cl := mustClient(t, addr, parentID.PublicHex(), childID)
	rec := []store.ReplRecord{{ChildOffset: 1, Topic: "colca/v1/_Metric/n-child/m1/t", Payload: []byte(`{"v":1}`), TS: 1}}

	saved := maxReplicatedBacklog
	maxReplicatedBacklog = 0 // any backlog, even none, is too much
	_, err := cl.Replicate("metrics", rec)
	maxReplicatedBacklog = saved
	var busy *replError
	if !errors.As(err, &busy) || !busy.Busy() || busy.RetryAfter != time.Second {
		t.Fatalf("push while the store is behind: %v, want busy with Retry-After 1s", err)
	}
	if v := metricstest.Value(t, m, `colca_http_request_limited_total{class="replication_backlog",door="repl"}`); v != 1 {
		t.Fatalf("limited counter %v, want 1", v)
	}
	if _, err := cl.Replicate("metrics", rec); err != nil || ps.NextOffset("metrics") != 2 {
		t.Fatalf("push after the backlog cleared: %v, metrics next %d", err, ps.NextOffset("metrics"))
	}
	if ps.ReplicatedBacklog() != 0 {
		t.Fatalf("backlog %d after every push finished", ps.ReplicatedBacklog())
	}
}

// A push large enough to ask before sending its body (Expect: 100-continue)
// gets the same answers: busy while the parent is over its budget, applied
// once it is not, on the same client.
func TestALargePushAsksFirstAndIsTakenOnceTheParentHasRoom(t *testing.T) {
	dir := t.TempDir()
	parentID := mustIdentity(t, filepath.Join(dir, "p.key"))
	childID := mustIdentity(t, filepath.Join(dir, "c.key"))
	ps := mustStore(t, filepath.Join(dir, "pdata"))
	pcfg := &config.Config{ULID: "n-parent", Repl: config.Endpoint{Addr: "127.0.0.1:0"}}
	preg, peng := nodeParts(t, ps, pcfg, nil, nil, nil, childSpec{"n-child", childID.PublicHex(), "child1"})
	srv, addr := startServer(t, pcfg, peng, parentID, preg)
	t.Cleanup(srv.Stop)
	cl := mustClient(t, addr, parentID.PublicHex(), childID)
	pad := strings.Repeat("x", 500)
	recs := make([]store.ReplRecord, 200)
	for i := range recs {
		recs[i] = store.ReplRecord{ChildOffset: uint64(i + 1), Topic: "colca/v1/_Metric/n-child/m1/t",
			Payload: []byte(`{"v":1,"note":"` + pad + `"}`), TS: 1}
	}
	if body, _ := marshalReplication("", "metrics", recs); len(body) < expectContinueBytes {
		t.Fatalf("the push is %d bytes, below the %d that ask first: the test proves nothing", len(body), expectContinueBytes)
	}
	srv.pushBytes.used.Store(srv.pushBytes.limit)
	var busy *replError
	if _, err := cl.Replicate("metrics", recs); !errors.As(err, &busy) || !busy.Busy() {
		t.Fatalf("large push to a busy parent: %v, want busy", err)
	}
	srv.pushBytes.used.Store(0)
	if _, err := cl.Replicate("metrics", recs); err != nil || ps.NextOffset("metrics") != 201 {
		t.Fatalf("large push once the parent has room: %v, metrics next %d", err, ps.NextOffset("metrics"))
	}
}

// Records queued for the bus count against the push budget like pushes in
// progress; an idle budget still takes one push of any size.
func TestThePushBudgetCountsRecordsQueuedForTheBus(t *testing.T) {
	b := byteBudget{limit: 1000}
	if b.reserve(400, 700) {
		t.Fatal("a push fit although queued records and it exceed the budget")
	}
	if !b.reserve(400, 500) {
		t.Fatal("a push that fits beside the queued records was refused")
	}
	b.release(400)
	if !b.reserve(5000, 0) {
		t.Fatal("an idle budget refused an oversized push for good")
	}
}
