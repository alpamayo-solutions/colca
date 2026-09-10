package repl

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/alpamayo-solutions/colca/internal/clock"
	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/store"
)

// TestServerStampsNowMSFromEngineAuthoritativeNow: both response envelopes carry
// now_ms from the server's AuthoritativeNow, not raw local time. A fixed clock
// makes the check exact.
func TestServerStampsNowMSFromEngineAuthoritativeNow(t *testing.T) {
	dir := t.TempDir()
	parentID := mustIdentity(t, filepath.Join(dir, "p.key"))
	childID := mustIdentity(t, filepath.Join(dir, "c.key"))

	authorityMS := int64(2_000_000_000_000)
	clk := clock.New(true, func() time.Time { return time.UnixMilli(authorityMS) })

	ps := mustStore(t, filepath.Join(dir, "pdata"))
	pcfg := &config.Config{ULID: "n-parent", Repl: config.Endpoint{Addr: "127.0.0.1:0"}}
	preg, peng := nodeParts(t, ps, pcfg, nil, nil, clk, childSpec{"n-child", childID.PublicHex(), "child1"})
	srv, addr := startServer(t, pcfg, peng, parentID, preg)
	defer srv.Stop()

	cl := mustClient(t, addr, parentID.PublicHex(), childID)

	// A queued command means the downlink poll returns immediately instead
	// of riding out the 20s long poll.
	mustIngestAdmin(t, peng, "colca/v1/_CmdParam/m1/child1/m1/go", `{"correlation_id":"c1","expires_at":99999999999}`)

	// /downlink
	res, err := cl.downlink(t.Context(), 1, 1, 10, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	nowMS := res.NowMS
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

// TestThreeLevelChainTelescopesToRootClock: root R at time T, mid M 5s behind,
// leaf L 20s behind. After M syncs from R and L from M, L's AuthoritativeNow is
// T, the root's clock, although L only talks to M. All clocks are fixed, so once
// a poll applies the correction the assertion holds; waitFor only waits for the
// HTTP round trips.
func TestThreeLevelChainTelescopesToRootClock(t *testing.T) {
	dir := t.TempDir()
	const authorityMS = int64(3_000_000_000_000) // T
	T := time.UnixMilli(authorityMS)

	// --- Root ---------------------------------------------------------
	rootID := mustIdentity(t, filepath.Join(dir, "root.key"))
	midID := mustIdentity(t, filepath.Join(dir, "mid.key"))
	rs := mustStore(t, filepath.Join(dir, "rdata"))
	rcfg := &config.Config{ULID: "n-root", Repl: config.Endpoint{Addr: "127.0.0.1:0"}}
	rclk := clock.New(true, func() time.Time { return T })
	rreg, reng := nodeParts(t, rs, rcfg, nil, nil, rclk, childSpec{"n-mid", midID.PublicHex(), "mid1"})
	rsrv, raddr := startServer(t, rcfg, reng, rootID, rreg)
	defer rsrv.Stop()
	// A command under M's mount so M's first poll returns at once. M is attached in
	// front of it: a child without a cursor adopts the head, so the filler only
	// makes M's position seedable.
	mustIngestAdmin(t, reng, "colca/v1/_CmdParam/m1/mid1/m1/filler", `{"correlation_id":"c0","expires_at":99999999999}`)
	midAt := rs.NextOffset("commands")
	mustIngestAdmin(t, reng, "colca/v1/_CmdParam/m1/mid1/m1/go", `{"correlation_id":"c1","expires_at":99999999999}`)

	// --- Mid: 5s behind the authority ----------------------------------
	leafID := mustIdentity(t, filepath.Join(dir, "leaf.key"))
	ms := mustStore(t, filepath.Join(dir, "mdata"))
	mcfg := &config.Config{ULID: "n-mid", Repl: config.Endpoint{Addr: "127.0.0.1:0"}}
	mclk := clock.New(false, func() time.Time { return T.Add(-5 * time.Second) })
	mreg, meng := nodeParts(t, ms, mcfg, nil, nil, mclk, childSpec{"n-leaf", leafID.PublicHex(), "leaf1"})
	msrv, maddr := startServer(t, mcfg, meng, midID, mreg)
	defer msrv.Stop()

	mcl := mustClient(t, raddr, rootID.PublicHex(), midID)
	attachAt(t, ms, mcl, midAt)
	mStop, mDone := make(chan struct{}), make(chan struct{})
	go func() { defer close(mDone); RunDownlink(mcl, meng, nil, mStop) }()
	t.Cleanup(func() { close(mStop); waitForClosed(t, "mid RunDownlink to stop", mDone, 5*time.Second) })

	waitFor(t, "mid to sync its clock from the root", 5*time.Second, func() bool {
		return meng.AuthoritativeNow().Equal(T)
	})
	if got := meng.AuthoritativeNow(); !got.Equal(T) {
		t.Fatalf("mid AuthoritativeNow = %v, want root's clock %v", got, T)
	}

	// M now queues a command under L's mount and stamps now_ms from its corrected
	// clock (T), not its raw T-5s. L attaches in front of that record, so its offset
	// is captured after M's own command from the root has landed; the clock is
	// applied before ingest in the same poll, so the two could land in either order.
	waitFor(t, "mid to have ingested the root's command", 5*time.Second, func() bool {
		return ms.NextOffset("commands") > 1
	})
	leafAt := ms.NextOffset("commands")
	mustIngestAdmin(t, meng, "colca/v1/_CmdParam/m1/leaf1/m1/go", `{"correlation_id":"c2","expires_at":99999999999}`)

	// --- Leaf: 20s behind the authority (never talks to root directly) --
	lsdir := filepath.Join(dir, "ldata")
	ls := mustStore(t, lsdir)
	lcfg := &config.Config{ULID: "n-leaf"}
	lclk := clock.New(false, func() time.Time { return T.Add(-20 * time.Second) })
	_, leng := nodeParts(t, ls, lcfg, nil, nil, lclk)

	lcl := mustClient(t, maddr, midID.PublicHex(), leafID)
	attachAt(t, ls, lcl, leafAt)
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

// TestNeverSyncedNodeServesOwnWallClockOverRepl: a non-root engine that never
// received a response reports its raw wall clock as AuthoritativeNow.
func TestNeverSyncedNodeServesOwnWallClockOverRepl(t *testing.T) {
	raw := time.UnixMilli(4_000_000_000_000)
	clk := clock.New(false, func() time.Time { return raw })
	s := mustStore(t, t.TempDir())
	cfg := &config.Config{ULID: "n-child"}
	_, eng := nodeParts(t, s, cfg, nil, nil, clk)

	if got := eng.AuthoritativeNow(); !got.Equal(raw) {
		t.Fatalf("never-synced AuthoritativeNow = %v, want raw wall clock %v", got, raw)
	}
}
