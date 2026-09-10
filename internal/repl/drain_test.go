package repl

import (
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/alpamayo-solutions/colca/internal/clock"
	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/metrics"
	"github.com/alpamayo-solutions/colca/internal/metrics/metricstest"
	"github.com/alpamayo-solutions/colca/internal/store"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// Full lifecycle: the child keeps working while draining, a queued command is
// delivered over the live connection, and the drain auto-revokes with outcome
// "delivered" once a poll sees the queue caught up to the child's
// delivery-floor cursor. A fixed clock keeps every command unexpired.
func TestDrainLifecycleDeliveredThenAutoRevoke(t *testing.T) {
	dir := t.TempDir()
	parentID := mustIdentity(t, filepath.Join(dir, "p.key"))
	childID := mustIdentity(t, filepath.Join(dir, "c.key"))
	ps := mustStore(t, filepath.Join(dir, "pdata"))
	pcfg := &config.Config{ULID: "n-parent", Repl: config.Endpoint{Addr: "127.0.0.1:0"}}
	clk := clock.New(true, func() time.Time { return time.UnixMilli(1_000_000) })
	preg, peng := nodeParts(t, ps, pcfg, nil, nil, clk, childSpec{"n-child", childID.PublicHex(), "child1"})
	pm := metrics.New(ps, config.Retention{}, clk)
	preg.SetMetrics(pm) // the registry owns colca_drains_active
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

	// The first poll fetches the queued command; the identity is still valid
	// mid-drain.
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
	// Not complete yet: this poll's ack (after=1) still leaves the delivered record
	// pending, because completion was evaluated before the records were served.
	if _, ok := preg.Get("n-child"); !ok {
		t.Fatal("child must still be enrolled after only one poll")
	}

	// The second poll with after=2 moves the delivery floor past the only command,
	// so this poll's completion check, which runs before the long-poll wait, finds
	// nothing pending and auto-revokes. The response itself waits out the full long
	// poll, so it runs in the background and srv.Stop cleans it up.
	go func() {
		_, _, _, _ = cl.Downlink(next, 10, 25*time.Second) //nolint:errcheck // fire-and-forget, killed by srv.Stop
	}()

	// Wait on the last thing completion does. evaluateDrain revokes first and then
	// calls Metrics.DrainCompleted, which updates the gauge, the pending series and
	// finally the outcome counter. Seeing the counter means everything this test
	// asserts has landed.
	waitFor(t, "the drain to be recorded complete once the queue caught up", 5*time.Second, func() bool {
		return metricstest.Value(t, pm, `colca_drains_completed_total{outcome="delivered"}`) == 1
	})
	if _, ok := preg.Get("n-child"); ok {
		t.Fatal("child must be auto-revoked by the time its drain is recorded complete")
	}
	if v := metricstest.Value(t, pm, `colca_drains_active`); v != 0 {
		t.Fatalf("colca_drains_active after completion = %v, want 0", v)
	}
	if v := metricstest.Value(t, pm, `colca_drains_completed_total{outcome="expired"}`); v != 0 {
		t.Fatalf(`colca_drains_completed_total{outcome="expired"} = %v, want 0 (this was delivered, not expired)`, v)
	}
}

// A child that never fetches still converges: once AuthoritativeNow passes
// every queued command's expires_at, the drain completes with "expired".
// clkNow is changed directly; nothing else runs concurrently.
func TestDrainExpiryBoundedCompletion(t *testing.T) {
	dir := t.TempDir()
	parentID := mustIdentity(t, filepath.Join(dir, "p.key"))
	childID := mustIdentity(t, filepath.Join(dir, "c.key"))
	ps := mustStore(t, filepath.Join(dir, "pdata"))
	pcfg := &config.Config{ULID: "n-parent", Repl: config.Endpoint{Addr: "127.0.0.1:0"}}
	clkNow := int64(1_000_000)
	clk := clock.New(true, func() time.Time { return time.UnixMilli(clkNow) })
	preg, peng := nodeParts(t, ps, pcfg, nil, nil, clk, childSpec{"n-child", childID.PublicHex(), "child1"})
	pm := metrics.New(ps, config.Retention{}, clk)
	preg.SetMetrics(pm) // the registry owns colca_drains_active
	srv, _ := startServerWithMetrics(t, pcfg, peng, parentID, preg, pm)
	t.Cleanup(srv.Stop)

	mustIngestAdmin(t, peng, "colca/v1/_CmdParam/m1/child1/m1/go",
		`{"correlation_id":"c1","expires_at":1500000}`) // expires at clkNow+500000

	if _, err := preg.Drain("n-child"); err != nil {
		t.Fatalf("Drain: %v", err)
	}

	// The child never polls. Before expiry the tick finds the command live and does
	// not complete the drain.
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

// Drain status is persisted and survives a restart, and the boot tick evaluates
// it again. The test reopens the same store with a fresh registry, engine and
// server in node.Start's order and runs evaluateAllDrains once like
// RunDrainTicker; the drain with an expired command must still complete.
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
		preg, peng := nodeParts(t, ps, pcfg, nil, nil, clk, childSpec{"n-child", childID.PublicHex(), "child1"})
		mustIngestAdmin(t, peng, "colca/v1/_CmdParam/m1/child1/m1/go", `{"correlation_id":"c1","expires_at":1000000}`)
		if _, err := preg.Drain("n-child"); err != nil {
			t.Fatalf("Drain: %v", err)
		}
		if _, ok := preg.Get("n-child"); !ok {
			t.Fatal("setup: child must still be draining before the simulated restart")
		}
		// No completion check ran in this "process"; the persisted state alone carries
		// the drain across the restart.
		ps.Close()
	}()

	// "Second process": reopen the same store, rebuild the stack fresh.
	ps2 := mustStore(t, storeDir)
	pcfg2 := &config.Config{ULID: "n-parent", Repl: config.Endpoint{Addr: "127.0.0.1:0"}}
	preg2, peng2 := nodeParts(t, ps2, pcfg2, nil, nil, clk)
	e2, ok := preg2.Get("n-child")
	if !ok || e2.Status != uns.StatusDraining {
		t.Fatalf("status did not survive the restart: %+v %v", e2, ok)
	}
	// The child's placement survives too: its element record is in the store,
	// so the rebuilt namespace resolves the same mount without re-enrollment.
	if mount, ok := peng2.Elements().PathOf(e2.Element); !ok || mount != "child1" {
		t.Fatalf("mount after restart = %q %v, want child1", mount, ok)
	}
	pm2 := metrics.New(ps2, config.Retention{}, clk)
	preg2.SetMetrics(pm2) // the registry owns colca_drains_active
	if v := metricstest.Value(t, pm2, `colca_drains_active`); v != 1 {
		t.Fatalf("colca_drains_active on the fresh Metrics instance = %v, want 1 (re-derived from the persisted status)", v)
	}
	srv2, err := NewServer(pcfg2, peng2, parentID, preg2, nil, pm2)
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

// A /downlink poll and the tick (or two ticks) can race to complete the same
// drain. registry.Revoke decides: exactly one caller records the outcome, the
// other sees ErrNotEnrolled and logs no error. Run with -race.
func TestDrainConcurrentEvaluationCompletesExactlyOnce(t *testing.T) {
	dir := t.TempDir()
	parentID := mustIdentity(t, filepath.Join(dir, "p.key"))
	childID := mustIdentity(t, filepath.Join(dir, "c.key"))
	ps := mustStore(t, filepath.Join(dir, "pdata"))
	pcfg := &config.Config{ULID: "n-parent", Repl: config.Endpoint{Addr: "127.0.0.1:0"}}
	clk := clock.New(true, func() time.Time { return time.UnixMilli(1_000_000) })
	preg, peng := nodeParts(t, ps, pcfg, nil, nil, clk, childSpec{"n-child", childID.PublicHex(), "child1"})
	pm := metrics.New(ps, config.Retention{}, clk)
	preg.SetMetrics(pm) // the registry owns colca_drains_active
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

// In a tree A -> B -> C with C draining at B, a command authored above B, where
// C is invisible, must still be rejected when it relays through B's downlink
// loop into engine.IngestDownlink, as if it had been admitted at B. Otherwise it
// reaches B's commands stream and C fetches it through B's own door.
func TestDownlinkRelayRejectsCommandForDrainingGrandchildMount(t *testing.T) {
	dir := t.TempDir()
	rootID := mustIdentity(t, filepath.Join(dir, "root.key"))
	midID := mustIdentity(t, filepath.Join(dir, "mid.key"))
	leafID := mustIdentity(t, filepath.Join(dir, "leaf.key"))

	// Root A: parent of B, unaware of C.
	rs := mustStore(t, filepath.Join(dir, "rdata"))
	rcfg := &config.Config{ULID: "n-root", Repl: config.Endpoint{Addr: "127.0.0.1:0"}}
	rreg, reng := nodeParts(t, rs, rcfg, nil, nil, nil, childSpec{"n-mid", midID.PublicHex(), "mid1"})
	rsrv, raddr := startServer(t, rcfg, reng, rootID, rreg)
	t.Cleanup(rsrv.Stop)

	// Mid B: child of A, parent of C, which is draining here. A's registry knows
	// nothing of it.
	ms := mustStore(t, filepath.Join(dir, "mdata"))
	mcfg := &config.Config{ULID: "n-mid", Repl: config.Endpoint{Addr: "127.0.0.1:0"}}
	mm := metrics.New(ms, config.Retention{}, nil)
	mreg, meng := nodeParts(t, ms, mcfg, nil, mm, nil, childSpec{"n-leaf", leafID.PublicHex(), "leaf1"})
	mreg.SetMetrics(mm)
	msrv, _ := startServerWithMetrics(t, mcfg, meng, midID, mreg, mm) // no client of B's own connects in this test
	t.Cleanup(msrv.Stop)

	mcl := mustClient(t, raddr, rootID.PublicHex(), midID)
	mStop, mDone := make(chan struct{}), make(chan struct{})
	go func() { defer close(mDone); RunDownlink(mcl, meng, nil, mStop) }()
	t.Cleanup(func() { close(mStop); waitForClosed(t, "mid RunDownlink to stop", mDone, 5*time.Second) })
	// Attach B before A gets the command: a command already in A's stream at first
	// contact is never relayed, which would make this test race the handshake.
	waitForAttached(t, ms, mcl)

	if _, err := mreg.Drain("n-leaf"); err != nil {
		t.Fatalf("Drain C at B: %v", err)
	}

	// A accepts this: for A it is an ordinary command under B's mount, and A's
	// registry has no entry for C.
	mustIngestAdmin(t, reng, "colca/v1/_CmdParam/m1/mid1/leaf1/go", `{"correlation_id":"c1","expires_at":99999999999}`)

	// B's downlink loop fetches it from A, already in B's coordinates ("leaf1/go"),
	// and must reject it at engine.IngestDownlink without storing it, counted like
	// the direct-door rejections.
	const rejectedLine = `colca_rejected_publishes_total{reason="draining"}`
	waitFor(t, "B to relay and reject the command via IngestDownlink", 5*time.Second, func() bool {
		return metricstest.Value(t, mm, rejectedLine) == 1
	})
	if off := meng.Store().NextOffset("commands"); off != 1 {
		t.Fatalf("B's commands stream next offset = %d, want 1 — the relayed command must never be persisted", off)
	}
}

// A live command for a draining child, never fetched, is removed by retention
// before the drain's completion check sees it. The drain must still end, since
// the data can no longer arrive, but with outcome "gapped", never "delivered":
// the command was never fetched and acked.
func TestDrainCompletesGappedNotDeliveredWhenRetentionPrunesUndeliveredCommand(t *testing.T) {
	dir := t.TempDir()
	parentID := mustIdentity(t, filepath.Join(dir, "p.key"))
	childID := mustIdentity(t, filepath.Join(dir, "c.key"))
	ps := mustStore(t, filepath.Join(dir, "pdata"))
	pcfg := &config.Config{ULID: "n-parent", Repl: config.Endpoint{Addr: "127.0.0.1:0"}}
	clk := clock.New(true, func() time.Time { return time.UnixMilli(1_000_000) })
	preg, peng := nodeParts(t, ps, pcfg, nil, nil, clk, childSpec{"n-child", childID.PublicHex(), "child1"})
	pm := metrics.New(ps, config.Retention{}, clk)
	preg.SetMetrics(pm)
	srv, _ := startServerWithMetrics(t, pcfg, peng, parentID, preg, pm)
	t.Cleanup(srv.Stop)

	// A live command for the child's mount, queued but never fetched; the child's
	// downlink cursor is still at its default of 1.
	mustIngestAdmin(t, peng, "colca/v1/_CmdParam/m1/child1/m1/go", `{"correlation_id":"c1","expires_at":99999999999}`)

	if _, err := preg.Drain("n-child"); err != nil {
		t.Fatalf("Drain: %v", err)
	}

	// Retention removes the record before the child fetched it: prune the commands
	// stream past offset 1 directly through the store, as the pruner would.
	if n, err := ps.Prune("commands", 2, nil, nil); err != nil || n != 1 {
		t.Fatalf("setup prune: removed=%d err=%v, want 1 record removed", n, err)
	}

	srv.evaluateAllDrains() // simulates one tick, per the pruner test precedent

	if _, ok := preg.Get("n-child"); ok {
		t.Fatal("the drain must still terminate — never hang on data a gap made permanently unreachable")
	}
	if v := metricstest.Value(t, pm, `colca_drains_completed_total{outcome="gapped"}`); v != 1 {
		t.Fatalf(`colca_drains_completed_total{outcome="gapped"} = %v, want 1`, v)
	}
	if v := metricstest.Value(t, pm, `colca_drains_completed_total{outcome="delivered"}`); v != 0 {
		t.Fatalf(`colca_drains_completed_total{outcome="delivered"} = %v, want 0 — a pruned, never-fetched command must NEVER be reported as delivered`, v)
	}
	if v := metricstest.Value(t, pm, `colca_drains_completed_total{outcome="expired"}`); v != 0 {
		t.Fatalf(`colca_drains_completed_total{outcome="expired"} = %v, want 0 (this was gapped, not naturally expired)`, v)
	}
}

// A gap and surviving pending work are independent. store.Gap only shows that
// [cursor, LWM) is lost; [LWM, next) can still hold a live, undelivered command.
// The drain must keep waiting on that range as if there were no gap, and only
// once it clears does the earlier gap decide the label: gapped, not expired.
func TestDrainWaitsOnSurvivingRangeDespiteGapThenCompletesGapped(t *testing.T) {
	dir := t.TempDir()
	parentID := mustIdentity(t, filepath.Join(dir, "p.key"))
	childID := mustIdentity(t, filepath.Join(dir, "c.key"))
	ps := mustStore(t, filepath.Join(dir, "pdata"))
	pcfg := &config.Config{ULID: "n-parent", Repl: config.Endpoint{Addr: "127.0.0.1:0"}}
	clkNow := int64(1_000_000)
	clk := clock.New(true, func() time.Time { return time.UnixMilli(clkNow) })
	preg, peng := nodeParts(t, ps, pcfg, nil, nil, clk, childSpec{"n-child", childID.PublicHex(), "child1"})
	pm := metrics.New(ps, config.Retention{}, clk)
	preg.SetMetrics(pm)
	srv, _ := startServerWithMetrics(t, pcfg, peng, parentID, preg, pm)
	t.Cleanup(srv.Stop)

	// Offset 1 gets pruned. Offset 2 is a separate live command that survives the
	// prune, the one a shortcut on the gap would abandon.
	mustIngestAdmin(t, peng, "colca/v1/_CmdParam/m1/child1/m1/lost", `{"correlation_id":"c1","expires_at":99999999999}`)
	mustIngestAdmin(t, peng, "colca/v1/_CmdParam/m1/child1/m1/survives", `{"correlation_id":"c2","expires_at":1500000}`)

	if _, err := preg.Drain("n-child"); err != nil {
		t.Fatalf("Drain: %v", err)
	}

	// Prune only offset 1; offset 2 survives, unread, in [LWM, next).
	if n, err := ps.Prune("commands", 2, nil, nil); err != nil || n != 1 {
		t.Fatalf("setup prune: removed=%d err=%v, want 1 record removed", n, err)
	}

	// There is a gap (cursor 1 < LWM 2) and a live command at offset 2 (expires_at
	// 1500000, clkNow 1000000). The drain must not complete.
	srv.evaluateAllDrains()
	if _, ok := preg.Get("n-child"); !ok {
		t.Fatal("the drain must NOT complete while a live command survives past the gap in [LWM, next) — this is the round-2 regression")
	}
	if v := metricstest.Value(t, pm, `colca_drain_pending_commands{child="n-child"}`); v != 1 {
		t.Fatalf(`colca_drain_pending_commands{child="n-child"} = %v, want 1 (the surviving live command)`, v)
	}
	if v := metricstest.Value(t, pm, `colca_drains_completed_total{outcome="gapped"}`); v != 0 {
		t.Fatalf(`colca_drains_completed_total{outcome="gapped"} = %v, want 0 — must not complete yet`, v)
	}

	// Past the surviving command's expiry nothing is pending, and the earlier gap
	// decides the label: the drain lost data it cannot account for, so "gapped",
	// not "expired".
	clkNow = 2_000_000
	srv.evaluateAllDrains()
	if _, ok := preg.Get("n-child"); ok {
		t.Fatal("the drain must complete once the surviving range clears")
	}
	if v := metricstest.Value(t, pm, `colca_drains_completed_total{outcome="gapped"}`); v != 1 {
		t.Fatalf(`colca_drains_completed_total{outcome="gapped"} = %v, want 1`, v)
	}
	if v := metricstest.Value(t, pm, `colca_drains_completed_total{outcome="expired"}`); v != 0 {
		t.Fatalf(`colca_drains_completed_total{outcome="expired"} = %v, want 0 — gap outranks a surviving record's own expiry in the outcome label`, v)
	}
	if v := metricstest.Value(t, pm, `colca_drains_completed_total{outcome="delivered"}`); v != 0 {
		t.Fatalf(`colca_drains_completed_total{outcome="delivered"} = %v, want 0`, v)
	}
}
