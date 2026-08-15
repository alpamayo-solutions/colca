package repl

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/engine"
	"github.com/alpamayo-solutions/colca/internal/metrics"
	"github.com/alpamayo-solutions/colca/internal/metrics/metricstest"
)

// scrapeMetric reads back one metric value through the shared test helper
// (metricstest.Value) — see that package's doc comment for why this goes
// through Handler() rather than a Collector/Gatherer accessor.
var scrapeMetric = metricstest.Value

// TestUplinkMetricsProgressAndFailure pins repl health as numeric progress
// the per-stream last-success gauge is 0 until the first
// successful push, advances on every later successful cycle, and stops
// advancing the moment pushes start failing — while the failure counter moves
// instead.
func TestUplinkMetricsProgressAndFailure(t *testing.T) {
	dir := t.TempDir()
	parentID := mustIdentity(t, filepath.Join(dir, "p.key"))
	childID := mustIdentity(t, filepath.Join(dir, "c.key"))

	ps := mustStore(t, filepath.Join(dir, "pdata"))
	pcfg := &config.Config{ULID: "n-parent", Repl: config.Endpoint{Addr: "127.0.0.1:0"},
		Children: []config.Child{{ULID: "n-child", Pubkey: childID.PublicHex(), Mount: "child1"}}}
	peng := engine.New(ps, pcfg, nil, nil)
	srv, addr := startServer(t, pcfg, peng, parentID)

	cs := mustStore(t, filepath.Join(dir, "cdata"))
	ccfg := &config.Config{ULID: "n-child"}
	cm := metrics.New(cs, config.Retention{})
	ceng := engine.New(cs, ccfg, nil, cm)
	mustIngestAdmin(t, ceng, "colca/v1/_Metric/m1/m1/temp", `{"v":1}`)

	cl := mustClient(t, addr, parentID.PublicHex(), childID)

	const gauge = `colca_uplink_last_success_timestamp_seconds{stream="metrics"}`
	const fails = `colca_uplink_push_failures_total{stream="metrics"}`
	if v := scrapeMetric(t, cm, gauge); v != 0 {
		t.Fatalf("%s = %v before RunUplink starts, want 0", gauge, v)
	}

	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		RunUplink(cl, ceng, cm, stop)
	}()

	waitFor(t, "the first uplink push to succeed", 5*time.Second, func() bool {
		return scrapeMetric(t, cm, gauge) > 0
	})
	v1 := scrapeMetric(t, cm, gauge)

	// A second record must produce a second, later success.
	time.Sleep(50 * time.Millisecond)
	mustIngestAdmin(t, ceng, "colca/v1/_Metric/m1/m1/temp", `{"v":2}`)
	waitFor(t, "a second uplink push to advance the gauge", 5*time.Second, func() bool {
		return scrapeMetric(t, cm, gauge) > v1
	})
	v2 := scrapeMetric(t, cm, gauge)

	// Kill the parent: the next push must fail, bump the failure counter, and
	// leave the last-success gauge exactly where it was.
	srv.Stop()
	mustIngestAdmin(t, ceng, "colca/v1/_Metric/m1/m1/temp", `{"v":3}`)
	waitFor(t, "a push failure to be counted", 5*time.Second, func() bool {
		return scrapeMetric(t, cm, fails) >= 1
	})
	if v := scrapeMetric(t, cm, gauge); v != v2 {
		t.Fatalf("%s = %v after a failed push, want unchanged %v (last-success gauge must not move on failure)", gauge, v, v2)
	}

	close(stop)
	waitForClosed(t, "RunUplink to return after stop", done, 5*time.Second)
}

// TestDownlinkMetricsProgressAndFailure is the downlink half of the same
// contract: the (unlabeled) last-success gauge advances on every successful
// fetch — including one that turns out to carry data, since the metric fires
// unconditionally on fetch success, not on payload — and stalls once the
// parent is gone, while the failure counter moves instead.
func TestDownlinkMetricsProgressAndFailure(t *testing.T) {
	dir := t.TempDir()
	parentID := mustIdentity(t, filepath.Join(dir, "p.key"))
	childID := mustIdentity(t, filepath.Join(dir, "c.key"))

	ps := mustStore(t, filepath.Join(dir, "pdata"))
	pcfg := &config.Config{ULID: "n-parent", Repl: config.Endpoint{Addr: "127.0.0.1:0"},
		Children: []config.Child{{ULID: "n-child", Pubkey: childID.PublicHex(), Mount: "child1"}}}
	peng := engine.New(ps, pcfg, nil, nil)
	srv, addr := startServer(t, pcfg, peng, parentID)
	// Seed one command so the first /downlink returns immediately instead of
	// riding the 20s empty long-poll.
	mustIngestAdmin(t, peng, "colca/v1/_CmdParam/m1/child1/m1/go", `{"correlation_id":"c1","expires_at":99999999999}`)

	cs := mustStore(t, filepath.Join(dir, "cdata"))
	ccfg := &config.Config{ULID: "n-child"}
	cm := metrics.New(cs, config.Retention{})
	ceng := engine.New(cs, ccfg, nil, cm)
	cl := mustClient(t, addr, parentID.PublicHex(), childID)

	const gauge = `colca_downlink_last_success_timestamp_seconds`
	const fails = `colca_downlink_fetch_failures_total`
	if v := scrapeMetric(t, cm, gauge); v != 0 {
		t.Fatalf("%s = %v before RunDownlink starts, want 0", gauge, v)
	}

	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		RunDownlink(cl, ceng, cm, stop)
	}()

	waitFor(t, "the first downlink fetch to succeed", 5*time.Second, func() bool {
		return scrapeMetric(t, cm, gauge) > 0
	})
	v1 := scrapeMetric(t, cm, gauge)
	if v := scrapeMetric(t, cm, fails); v != 0 {
		t.Fatalf("%s = %v after a successful fetch, want 0", fails, v)
	}

	// Kill the parent: the loop is either mid-long-poll or about to start a
	// new one; either way the next fetch must fail, bump the failure counter,
	// and leave the last-success gauge exactly where it was.
	srv.Stop()
	waitFor(t, "a fetch failure to be counted", 10*time.Second, func() bool {
		return scrapeMetric(t, cm, fails) >= 1
	})
	if v := scrapeMetric(t, cm, gauge); v != v1 {
		t.Fatalf("%s = %v after a failed fetch, want unchanged %v (last-success gauge must not move on failure)", gauge, v, v1)
	}

	close(stop)
	waitForClosed(t, "RunDownlink to return after stop", done, 5*time.Second)
}
