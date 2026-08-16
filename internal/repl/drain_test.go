package repl

import (
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/alpamayo-solutions/colca/internal/clock"
	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/engine"
	"github.com/alpamayo-solutions/colca/internal/metrics"
	"github.com/alpamayo-solutions/colca/internal/metrics/metricstest"
	"github.com/alpamayo-solutions/colca/internal/registry"
	"github.com/alpamayo-solutions/colca/internal/store"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// Full lifecycle (move-drain design §3.2 items 1-4): the child stays fully
// functional while draining (connects, fetches, acks — items 1), a queued
// command is delivered through the live connection, and the drain
// auto-revokes with outcome "delivered" once a poll observes the queue
// caught up to its own delivery-floor cursor. A fixed, non-advancing clock
// keeps "not yet expired" unambiguous throughout.
func TestDrainLifecycleDeliveredThenAutoRevoke(t *testing.T) {
	dir := t.TempDir()
	parentID := mustIdentity(t, filepath.Join(dir, "p.key"))
	childID := mustIdentity(t, filepath.Join(dir, "c.key"))
	ps := mustStore(t, filepath.Join(dir, "pdata"))
	pcfg := &config.Config{ULID: "n-parent", Repl: config.Endpoint{Addr: "127.0.0.1:0"}}
	preg := regWithChildren(t, ps, pcfg.ULID, childSpec{"n-child", childID.PublicHex(), "child1"})
	clk := clock.New(true, func() time.Time { return time.UnixMilli(1_000_000) })
	peng := engine.New(ps, pcfg, preg, nil, nil, clk)
	pm := metrics.New(ps, config.Retention{}, clk)
	preg.SetMetrics(pm) // move-drain design §3.4: colca_drains_active is registry-owned
	srv, addr := startServerWithMetrics(t, pcfg, peng, parentID, preg, pm)
	t.Cleanup(srv.Stop)
	cl := mustClient(t, addr, parentID.PublicHex(), childID)

	mustIngestAdmin(t, peng, "colca/v1/_CmdParam/m1/child1/m1/go", `{"correlation_id":"c1","expires_at":99999999999}`)

	if _, err := preg.Drain("n-child"); err != nil {
		t.Fatalf("Drain: %v", err)
	}
	if v := metricstest.Value(t, pm, `colca_drains_active`); v != 1 {
		t.Fatalf("colca_drains_active after Drain = %v, want 1", v)
	}

	// First poll: the child fetches the queued command. The identity is
	// still fully valid mid-drain (item 1) — this must succeed exactly like
	// an ordinary downlink.
	recs, next, _, err := cl.Downlink(1, 10, 5*time.Second)
	if err != nil {
		t.Fatalf("first downlink: %v", err)
	}
	if len(recs) != 1 || recs[0].Topic != "colca/v1/_CmdParam/m1/m1/go" {
		t.Fatalf("first downlink records = %+v, want the one queued command", recs)
	}
	if next != 2 {
		t.Fatalf("next = %d, want 2", next)
	}
	// Not complete yet: this poll's own CursorAck(after=1) leaves the just
	// -delivered record still counted as pending by the completion scan
	// (evaluated BEFORE this poll's records were served) — the child has not
	// yet come back reporting it consumed them.
	if _, ok := preg.Get("n-child"); !ok {
		t.Fatal("child must still be enrolled after only one poll")
	}

	// Second poll: after=2 (what the client would send next in real usage)
	// persists the delivery-floor cursor past the only command, so THIS
	// poll's completion check (which runs synchronously at the top of the
	// handler, before the long-poll wait) finds nothing pending and
	// auto-revokes. The response itself has nothing left to deliver, so it
	// legitimately rides out the server's full 20s long poll — fired in the
	// background and left for srv.Stop's cleanup to kill, same pattern as
	// TestDownlinkPollPersistsChildCursorAndClampsPrune; only the registry
	// state (which converges in milliseconds) is asserted.
	go func() {
		_, _, _, _ = cl.Downlink(next, 10, 25*time.Second) //nolint:errcheck // fire-and-forget, killed by srv.Stop
	}()
	waitFor(t, "the child to be auto-revoked once the queue caught up", 5*time.Second, func() bool {
		_, ok := preg.Get("n-child")
		return !ok
	})
	if v := metricstest.Value(t, pm, `colca_drains_active`); v != 0 {
		t.Fatalf("colca_drains_active after completion = %v, want 0", v)
	}
	if v := metricstest.Value(t, pm, `colca_drains_completed_total{outcome="delivered"}`); v != 1 {
		t.Fatalf(`colca_drains_completed_total{outcome="delivered"} = %v, want 1`, v)
	}
	if v := metricstest.Value(t, pm, `colca_drains_completed_total{outcome="expired"}`); v != 0 {
		t.Fatalf(`colca_drains_completed_total{outcome="expired"} = %v, want 0 (this was delivered, not expired)`, v)
	}
}

// Expiry-bounded completion (move-drain design §3.2 item 3, "deliver or
// expire, literally"): a child that never fetches at all still converges —
// once AuthoritativeNow passes every queued command's expires_at, the drain
// completes with outcome "expired". clkNow is mutated directly between the
// two evaluations: single test goroutine, no concurrent tick running, so no
// synchronization is needed for the closure read.
func TestDrainExpiryBoundedCompletion(t *testing.T) {
	dir := t.TempDir()
	parentID := mustIdentity(t, filepath.Join(dir, "p.key"))
	childID := mustIdentity(t, filepath.Join(dir, "c.key"))
	ps := mustStore(t, filepath.Join(dir, "pdata"))
	pcfg := &config.Config{ULID: "n-parent", Repl: config.Endpoint{Addr: "127.0.0.1:0"}}
	preg := regWithChildren(t, ps, pcfg.ULID, childSpec{"n-child", childID.PublicHex(), "child1"})

	clkNow := int64(1_000_000)
	clk := clock.New(true, func() time.Time { return time.UnixMilli(clkNow) })
	peng := engine.New(ps, pcfg, preg, nil, nil, clk)
	pm := metrics.New(ps, config.Retention{}, clk)
	preg.SetMetrics(pm) // move-drain design §3.4: colca_drains_active is registry-owned
	srv, _ := startServerWithMetrics(t, pcfg, peng, parentID, preg, pm)
	t.Cleanup(srv.Stop)

	mustIngestAdmin(t, peng, "colca/v1/_CmdParam/m1/child1/m1/go",
		`{"correlation_id":"c1","expires_at":1500000}`) // expires at clkNow+500000

	if _, err := preg.Drain("n-child"); err != nil {
		t.Fatalf("Drain: %v", err)
	}

	// The child never polls. Before expiry the tick must find the command
	// still live and NOT complete the drain.
	srv.evaluateAllDrains() // simulates one 30s tick, called directly per the pruner test precedent
	if _, ok := preg.Get("n-child"); !ok {
		t.Fatal("drain must not complete while the queued command is still live")
	}
	if v := metricstest.Value(t, pm, `colca_drain_pending_commands{child="n-child"}`); v != 1 {
		t.Fatalf(`colca_drain_pending_commands{child="n-child"} = %v, want 1 while the command is still live`, v)
	}

	// Advance authoritative time past expires_at and tick again.
	clkNow = 2_000_000
	srv.evaluateAllDrains()
	if _, ok := preg.Get("n-child"); ok {
		t.Fatal("drain must auto-revoke once the only queued command expired")
	}
	if v := metricstest.Value(t, pm, `colca_drains_completed_total{outcome="expired"}`); v != 1 {
		t.Fatalf(`colca_drains_completed_total{outcome="expired"} = %v, want 1`, v)
	}
	if v := metricstest.Value(t, pm, `colca_drains_completed_total{outcome="delivered"}`); v != 0 {
		t.Fatalf(`colca_drains_completed_total{outcome="delivered"} = %v, want 0 (this was expired, not delivered)`, v)
	}
}

// Move-drain design §3.2: "status ... persisted in the r/ entry; survives
// restart" and "the tick re-evaluates after boot". Simulates a restart by
// reopening the SAME store with a fresh registry.Manager/Engine/Server
// (mirroring node.Start's assembly order), then calling evaluateAllDrains
// once the way RunDrainTicker does before its first real tick — the
// pre-restart drain (with an already-expired command) must still converge.
func TestDrainStatusSurvivesRestartAndBootTickReEvaluates(t *testing.T) {
	dir := t.TempDir()
	parentID := mustIdentity(t, filepath.Join(dir, "p.key"))
	childID := mustIdentity(t, filepath.Join(dir, "c.key"))
	storeDir := filepath.Join(dir, "pdata")

	clkNow := int64(5_000_000) // already past the command's expiry below
	clk := clock.New(true, func() time.Time { return time.UnixMilli(clkNow) })

	func() { // first "process": drain started, never completes before "restart"
		ps, err := store.Open(storeDir)
		if err != nil {
			t.Fatalf("open store: %v", err)
		}
		pcfg := &config.Config{ULID: "n-parent", Repl: config.Endpoint{Addr: "127.0.0.1:0"}}
		preg := regWithChildren(t, ps, pcfg.ULID, childSpec{"n-child", childID.PublicHex(), "child1"})
		peng := engine.New(ps, pcfg, preg, nil, nil, clk)
		mustIngestAdmin(t, peng, "colca/v1/_CmdParam/m1/child1/m1/go", `{"correlation_id":"c1","expires_at":1000000}`)
		if _, err := preg.Drain("n-child"); err != nil {
			t.Fatalf("Drain: %v", err)
		}
		if _, ok := preg.Get("n-child"); !ok {
			t.Fatal("setup: child must still be draining before the simulated restart")
		}
		// No completion check ran in this "process" at all (unlike the real
		// node, which would have a 30s ticker) — the persisted state alone
		// carries the drain across the restart.
		ps.Close()
	}()

	// "Second process": reopen the same store, rebuild the stack fresh.
	ps2 := mustStore(t, storeDir)
	pcfg2 := &config.Config{ULID: "n-parent", Repl: config.Endpoint{Addr: "127.0.0.1:0"}}
	preg2, err := registry.New(ps2, pcfg2.ULID)
	if err != nil {
		t.Fatalf("reload registry: %v", err)
	}
	e2, ok := preg2.Get("n-child")
	if !ok || e2.Status != uns.StatusDraining {
		t.Fatalf("status did not survive the restart: %+v %v", e2, ok)
	}
	peng2 := engine.New(ps2, pcfg2, preg2, nil, nil, clk)
	pm2 := metrics.New(ps2, config.Retention{}, clk)
	preg2.SetMetrics(pm2) // move-drain design §3.4: colca_drains_active is registry-owned
	if v := metricstest.Value(t, pm2, `colca_drains_active`); v != 1 {
		t.Fatalf("colca_drains_active on the fresh Metrics instance = %v, want 1 (re-derived from the persisted status)", v)
	}
	srv2, err := NewServer(pcfg2, peng2, parentID, preg2, pm2)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	// The RunDrainTicker boot behavior, without waiting out the goroutine's
	// internal timer: its own first action is exactly this call.
	srv2.evaluateAllDrains()

	if _, ok := preg2.Get("n-child"); ok {
		t.Fatal("the boot re-evaluation must auto-revoke a drain whose only command already expired before the restart")
	}
	if v := metricstest.Value(t, pm2, `colca_drains_completed_total{outcome="expired"}`); v != 1 {
		t.Fatalf(`colca_drains_completed_total{outcome="expired"} = %v, want 1`, v)
	}
}

// Concurrency safety: a /downlink poll and the periodic tick (or two ticks)
// can race to evaluate — and complete — the same drain. registry.Revoke is
// the synchronization point (registry design), so exactly one caller must
// observe success and record the outcome; the other must see ErrNotEnrolled
// and log nothing as an error. Run under -race.
func TestDrainConcurrentEvaluationCompletesExactlyOnce(t *testing.T) {
	dir := t.TempDir()
	parentID := mustIdentity(t, filepath.Join(dir, "p.key"))
	childID := mustIdentity(t, filepath.Join(dir, "c.key"))
	ps := mustStore(t, filepath.Join(dir, "pdata"))
	pcfg := &config.Config{ULID: "n-parent", Repl: config.Endpoint{Addr: "127.0.0.1:0"}}
	preg := regWithChildren(t, ps, pcfg.ULID, childSpec{"n-child", childID.PublicHex(), "child1"})
	clk := clock.New(true, func() time.Time { return time.UnixMilli(1_000_000) })
	peng := engine.New(ps, pcfg, preg, nil, nil, clk)
	pm := metrics.New(ps, config.Retention{}, clk)
	preg.SetMetrics(pm) // move-drain design §3.4: colca_drains_active is registry-owned
	srv, _ := startServerWithMetrics(t, pcfg, peng, parentID, preg, pm)
	t.Cleanup(srv.Stop)

	// No queued commands at all: the very first evaluation on every racer
	// already sees an empty queue (outcome "delivered").
	if _, err := preg.Drain("n-child"); err != nil {
		t.Fatalf("Drain: %v", err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			srv.evaluateDrain("n-child")
		}()
	}
	wg.Wait()

	if _, ok := preg.Get("n-child"); ok {
		t.Fatal("child must be revoked after the race")
	}
	if v := metricstest.Value(t, pm, `colca_drains_completed_total{outcome="delivered"}`); v != 1 {
		t.Fatalf(`colca_drains_completed_total{outcome="delivered"} = %v, want exactly 1 despite 8 concurrent evaluators`, v)
	}
	if v := metricstest.Value(t, pm, `colca_drains_active`); v != 0 {
		t.Fatalf("colca_drains_active = %v, want 0", v)
	}
}
