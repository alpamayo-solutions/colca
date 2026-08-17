package repl

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/alpamayo-solutions/colca/internal/clock"
	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/engine"
	"github.com/alpamayo-solutions/colca/internal/store"
)

// TestServerStampsNowMSFromEngineAuthoritativeNow pins the low-level wire
// contract (time-sync design §2.1/§2.3 rule 4) for both response envelopes:
// now_ms is exactly the SERVER's own AuthoritativeNow, not raw local time —
// checked here with an injected, non-advancing clock so the assertion is
// exact, not a plausibility window.
func TestServerStampsNowMSFromEngineAuthoritativeNow(t *testing.T) {
	dir := t.TempDir()
	parentID := mustIdentity(t, filepath.Join(dir, "p.key"))
	childID := mustIdentity(t, filepath.Join(dir, "c.key"))

	authorityMS := int64(2_000_000_000_000)
	clk := clock.New(true, func() time.Time { return time.UnixMilli(authorityMS) })

	ps := mustStore(t, filepath.Join(dir, "pdata"))
	pcfg := &config.Config{ULID: "n-parent", Repl: config.Endpoint{Addr: "127.0.0.1:0"}}
	preg := regWithChildren(t, ps, pcfg.ULID, childSpec{"n-child", childID.PublicHex(), "child1"})
	peng := engine.New(ps, pcfg, preg, nil, nil, clk)
	srv, addr := startServer(t, pcfg, peng, parentID, preg)
	defer srv.Stop()

	cl := mustClient(t, addr, parentID.PublicHex(), childID)

	// A queued command means the downlink poll returns immediately instead
	// of riding out the 20s long poll.
	mustIngestAdmin(t, peng, "colca/v1/_CmdParam/m1/child1/m1/go", `{"correlation_id":"c1","expires_at":99999999999}`)

	// /downlink
	_, _, _, nowMS, _, err := cl.downlink(t.Context(), 1, 10, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if nowMS != authorityMS {
		t.Fatalf("/downlink now_ms = %d, want %d (the server's AuthoritativeNow)", nowMS, authorityMS)
	}

	// /replicate
	_, nowMS, err = cl.replicate(t.Context(), "metrics", []store.ReplRecord{
		{ChildOffset: 1, Topic: "colca/v1/_Metric/m1/m1/temp", Payload: []byte(`{"v":1}`), TS: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	if nowMS != authorityMS {
		t.Fatalf("/replicate now_ms = %d, want %d (the server's AuthoritativeNow)", nowMS, authorityMS)
	}
}

// TestThreeLevelChainTelescopesToRootClock is the Go-level proof of design
// §2.1's core claim: a root R at authority time T, a mid node M whose own
// clock reads 5s behind T, and a leaf L whose own clock reads 20s behind T —
// after M syncs from R over one /downlink poll, and L syncs from M over one
// /downlink poll, L's AuthoritativeNow converges to T (the ROOT's clock),
// not to M's own uncorrected (T-5s) clock. That is telescoping: L never
// talks to R directly, only through M.
//
// Every clock here is fixed (non-advancing) and injected, so once a poll
// applies the correction the assertion holds forever — no timing flakiness.
// waitFor's poll loop exists only to wait out the one real HTTP round trip
// each level needs, not to average out any simulated clock drift.
func TestThreeLevelChainTelescopesToRootClock(t *testing.T) {
	dir := t.TempDir()
	const authorityMS = int64(3_000_000_000_000) // T
	T := time.UnixMilli(authorityMS)

	// --- Root ---------------------------------------------------------
	rootID := mustIdentity(t, filepath.Join(dir, "root.key"))
	midID := mustIdentity(t, filepath.Join(dir, "mid.key"))
	rs := mustStore(t, filepath.Join(dir, "rdata"))
	rcfg := &config.Config{ULID: "n-root", Repl: config.Endpoint{Addr: "127.0.0.1:0"}}
	rreg := regWithChildren(t, rs, rcfg.ULID, childSpec{"n-mid", midID.PublicHex(), "mid1"})
	rclk := clock.New(true, func() time.Time { return T })
	reng := engine.New(rs, rcfg, rreg, nil, nil, rclk)
	rsrv, raddr := startServer(t, rcfg, reng, rootID, rreg)
	defer rsrv.Stop()
	// A command addressed under M's mount so M's first downlink poll returns
	// immediately instead of riding out the 20s long poll.
	mustIngestAdmin(t, reng, "colca/v1/_CmdParam/m1/mid1/m1/go", `{"correlation_id":"c1","expires_at":99999999999}`)

	// --- Mid: 5s behind the authority ----------------------------------
	leafID := mustIdentity(t, filepath.Join(dir, "leaf.key"))
	ms := mustStore(t, filepath.Join(dir, "mdata"))
	mcfg := &config.Config{ULID: "n-mid", Repl: config.Endpoint{Addr: "127.0.0.1:0"}}
	mreg := regWithChildren(t, ms, mcfg.ULID, childSpec{"n-leaf", leafID.PublicHex(), "leaf1"})
	mclk := clock.New(false, func() time.Time { return T.Add(-5 * time.Second) })
	meng := engine.New(ms, mcfg, mreg, nil, nil, mclk)
	msrv, maddr := startServer(t, mcfg, meng, midID, mreg)
	defer msrv.Stop()

	mcl := mustClient(t, raddr, rootID.PublicHex(), midID)
	mStop, mDone := make(chan struct{}), make(chan struct{})
	go func() { defer close(mDone); RunDownlink(mcl, meng, nil, mStop) }()
	t.Cleanup(func() { close(mStop); waitForClosed(t, "mid RunDownlink to stop", mDone, 5*time.Second) })

	waitFor(t, "mid to sync its clock from the root", 5*time.Second, func() bool {
		return meng.AuthoritativeNow().Equal(T)
	})
	if got := meng.AuthoritativeNow(); !got.Equal(T) {
		t.Fatalf("mid AuthoritativeNow = %v, want root's clock %v", got, T)
	}

	// M now itself queues a command addressed under L's mount — M's server
	// stamps now_ms from meng.AuthoritativeNow(), which is now the corrected
	// (T) value, not M's raw (T-5s) clock.
	mustIngestAdmin(t, meng, "colca/v1/_CmdParam/m1/leaf1/m1/go", `{"correlation_id":"c2","expires_at":99999999999}`)

	// --- Leaf: 20s behind the authority (never talks to root directly) --
	lsdir := filepath.Join(dir, "ldata")
	ls := mustStore(t, lsdir)
	lcfg := &config.Config{ULID: "n-leaf"}
	lclk := clock.New(false, func() time.Time { return T.Add(-20 * time.Second) })
	leng := engine.New(ls, lcfg, regWithChildren(t, ls, lcfg.ULID), nil, nil, lclk)

	lcl := mustClient(t, maddr, midID.PublicHex(), leafID)
	lStop, lDone := make(chan struct{}), make(chan struct{})
	go func() { defer close(lDone); RunDownlink(lcl, leng, nil, lStop) }()
	t.Cleanup(func() { close(lStop); waitForClosed(t, "leaf RunDownlink to stop", lDone, 5*time.Second) })

	waitFor(t, "leaf to sync its clock from the mid (telescoped from the root)", 5*time.Second, func() bool {
		return leng.AuthoritativeNow().Equal(T)
	})
	if got := leng.AuthoritativeNow(); !got.Equal(T) {
		t.Fatalf("leaf AuthoritativeNow = %v, want the ROOT's clock %v (telescoped through mid) — "+
			"a leaf converging to mid's own uncorrected clock (%v) would mean telescoping is broken",
			got, T, T.Add(-5*time.Second))
	}
}

// TestNeverSyncedNodeServesOwnWallClockOverRepl is the repl-surface
// companion to clock.TestNeverSyncedNonRootServesOwnWallClock: a non-root
// engine that has never received a /downlink or /replicate response reports
// its own raw wall clock as AuthoritativeNow (design §2.1, "best effort").
func TestNeverSyncedNodeServesOwnWallClockOverRepl(t *testing.T) {
	raw := time.UnixMilli(4_000_000_000_000)
	clk := clock.New(false, func() time.Time { return raw })
	s := mustStore(t, t.TempDir())
	cfg := &config.Config{ULID: "n-child"}
	eng := engine.New(s, cfg, regWithChildren(t, s, cfg.ULID), nil, nil, clk)

	if got := eng.AuthoritativeNow(); !got.Equal(raw) {
		t.Fatalf("never-synced AuthoritativeNow = %v, want raw wall clock %v", got, raw)
	}
}
