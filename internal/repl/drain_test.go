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
	clk := clock.New(true, func() time.Time { return time.UnixMilli(1_000_000) })
	preg, peng := nodeParts(t, ps, pcfg, nil, nil, clk, childSpec{"n-child", childID.PublicHex(), "child1"})
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
	clkNow := int64(1_000_000)
	clk := clock.New(true, func() time.Time { return time.UnixMilli(clkNow) })
	preg, peng := nodeParts(t, ps, pcfg, nil, nil, clk, childSpec{"n-child", childID.PublicHex(), "child1"})
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
		preg, peng := nodeParts(t, ps, pcfg, nil, nil, clk, childSpec{"n-child", childID.PublicHex(), "child1"})
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
	preg2.SetMetrics(pm2) // move-drain design §3.4: colca_drains_active is registry-owned
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
	clk := clock.New(true, func() time.Time { return time.UnixMilli(1_000_000) })
	preg, peng := nodeParts(t, ps, pcfg, nil, nil, clk, childSpec{"n-child", childID.PublicHex(), "child1"})
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

// In a multi-hop tree (root A -> mid B -> leaf C, C
// draining at B), a command authored ABOVE B — where B's own draining child
// is invisible — must still be bounced once it relays down through B's
// downlink-poll loop into engine.IngestDownlink, exactly as if it had been
// admitted directly at B. Without the fix this reaches B's local commands
// stream (already in B-local coordinates under C's mount) and C fetches it
// normally through B's own /downlink door: the "chasing a moving tail"
// failure §3.2 item 2 exists to prevent, reachable even though the direct
// client/admin doors are covered.
func TestDownlinkRelayRejectsCommandForDrainingGrandchildMount(t *testing.T) {
	dir := t.TempDir()
	rootID := mustIdentity(t, filepath.Join(dir, "root.key"))
	midID := mustIdentity(t, filepath.Join(dir, "mid.key"))
	leafID := mustIdentity(t, filepath.Join(dir, "leaf.key"))

	// --- Root A: parent of B, has no idea C (B's own child) exists at all.
	rs := mustStore(t, filepath.Join(dir, "rdata"))
	rcfg := &config.Config{ULID: "n-root", Repl: config.Endpoint{Addr: "127.0.0.1:0"}}
	rreg, reng := nodeParts(t, rs, rcfg, nil, nil, nil, childSpec{"n-mid", midID.PublicHex(), "mid1"})
	rsrv, raddr := startServer(t, rcfg, reng, rootID, rreg)
	t.Cleanup(rsrv.Stop)

	// --- Mid B: child of A, parent of C. C is draining HERE, at B — a fact
	// with no representation anywhere in A's own registry.
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
	// B must be attached before A is given the command: a command already
	// sitting in A's stream when B first contacts it is pre-attachment and
	// never relayed (parent-scoped-cursors design §3.2), which would leave this
	// test racing the handshake.
	waitForAttached(t, ms, mcl)

	if _, err := mreg.Drain("n-leaf"); err != nil {
		t.Fatalf("Drain C at B: %v", err)
	}

	// A admits this without complaint: from A's vantage point it is an
	// ordinary command addressed somewhere under B's own mount — A's
	// DrainingMount check only ever sees A's OWN registry, which has no
	// entry for C at all.
	mustIngestAdmin(t, reng, "colca/v1/_CmdParam/m1/mid1/leaf1/go", `{"correlation_id":"c1","expires_at":99999999999}`)

	// B's downlink-poll loop fetches it from A (arriving already stripped
	// to B-local coordinates, path "leaf1/go") and must bounce it at
	// engine.IngestDownlink — never persisting it into B's own commands
	// stream, and counting it exactly like the direct-door rejections.
	const rejectedLine = `colca_rejected_publishes_total{reason="draining"}`
	waitFor(t, "B to relay and reject the command via IngestDownlink", 5*time.Second, func() bool {
		return metricstest.Value(t, mm, rejectedLine) == 1
	})
	if off := meng.Store().NextOffset("commands"); off != 1 {
		t.Fatalf("B's commands stream next offset = %d, want 1 — the relayed command must never be persisted", off)
	}
}

// A live, unexpired command addressed to a draining
// child's mount, never fetched, physically removed by retention BEFORE the
// drain's own completion predicate ever sees it — the child's own
// delivery-floor cursor never advanced past it, so this is indistinguishable
// (from the drain's viewpoint) from the staleness-override scenario: some data this child was owed is now gone. The drain
// must still terminate (never hang on data that can no longer arrive) but
// must record outcome "gapped", never "delivered" — the design's own §3.2
// definition of "delivered" is "fetched-and-acked on the downlink cursor",
// which a pruned record never was.
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

	// A live command (expires_at far in the future) for the child's mount,
	// queued but never fetched — the child's downlink cursor is still at its
	// never-acked default (1).
	mustIngestAdmin(t, peng, "colca/v1/_CmdParam/m1/child1/m1/go", `{"correlation_id":"c1","expires_at":99999999999}`)

	if _, err := preg.Drain("n-child"); err != nil {
		t.Fatalf("Drain: %v", err)
	}

	// Simulate retention (staleness-override or, just as commonly, ordinary
	// age/size pruning on a stream this never-polled cursor never
	// protected) physically removing the record before the child ever
	// fetched it: prune the commands stream past offset 1, directly through
	// the store, exactly as the pruner itself would commit it.
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

// A gap and surviving pending
// work are orthogonal facts, not alternatives. store.Gap only proves the
// PREFIX [cursor, LWM) is lost — it says nothing about [LWM, next), which
// can still hold a live, undelivered, unexpired command. The round-1 shape
// (return gapped=true and skip the scan entirely) would auto-revoke here
// while that surviving command sits unread — command abandonment, strictly
// worse than the mislabeling round-1 targeted. The drain must keep waiting
// on the surviving range exactly as if there were no gap at all, and only
// once THAT clears does the earlier gap decide the outcome label (gapped,
// not expired, even though the surviving command's own fate was expiry).
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

	// Offset 1: will be pruned (the lost prefix, same setup as the sibling
	// test above). Offset 2: a SEPARATE live command that SURVIVES the
	// prune — this is the record round-1's short-circuit would have
	// silently abandoned.
	mustIngestAdmin(t, peng, "colca/v1/_CmdParam/m1/child1/m1/lost", `{"correlation_id":"c1","expires_at":99999999999}`)
	mustIngestAdmin(t, peng, "colca/v1/_CmdParam/m1/child1/m1/survives", `{"correlation_id":"c2","expires_at":1500000}`)

	if _, err := preg.Drain("n-child"); err != nil {
		t.Fatalf("Drain: %v", err)
	}

	// Prune only offset 1 — offset 2 survives, unread, in [LWM, next).
	if n, err := ps.Prune("commands", 2, nil, nil); err != nil || n != 1 {
		t.Fatalf("setup prune: removed=%d err=%v, want 1 record removed", n, err)
	}

	// A gap exists (cursor 1 < LWM 2) AND a live, undelivered command sits
	// at offset 2 (expires_at 1500000, clkNow 1000000 — still live). The
	// drain must NOT complete: the gap does not excuse waiting on real,
	// currently-live work.
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

	// Advance time past the surviving command's expiry and re-evaluate: now
	// pending clears, and the EARLIER gap decides the outcome label — even
	// though the surviving record's own fate was expiry, the drain has lost
	// data it can never account for, so the honest label is "gapped", not
	// "expired".
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
