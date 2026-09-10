package repl

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/metrics"
	"github.com/alpamayo-solutions/colca/internal/metrics/metricstest"
)

// scrapeMetric reads one metric value through the shared test helper.
var scrapeMetric = metricstest.Value

// TestUplinkMetricsProgressAndFailure: the per-stream last-success gauge is 0
// until the first successful push, advances on each later success, and stops
// while pushes fail, when the failure counter moves instead.
func TestUplinkMetricsProgressAndFailure(t *testing.T) {
	dir := t.TempDir()
	parentID := mustIdentity(t, filepath.Join(dir, "p.key"))
	childID := mustIdentity(t, filepath.Join(dir, "c.key"))

	ps := mustStore(t, filepath.Join(dir, "pdata"))
	pcfg := &config.Config{ULID: "n-parent", Repl: config.Endpoint{Addr: "127.0.0.1:0"}}
	preg, peng := nodeParts(t, ps, pcfg, nil, nil, nil, childSpec{"n-child", childID.PublicHex(), "child1"})
	srv, addr := startServer(t, pcfg, peng, parentID, preg)

	cs := mustStore(t, filepath.Join(dir, "cdata"))
	ccfg := &config.Config{ULID: "n-child"}
	cm := metrics.New(cs, config.Retention{}, nil)
	_, ceng := nodeParts(t, cs, ccfg, nil, cm, nil)
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
		RunUplink(cl, ceng, nil, cm, stop)
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

// TestDownlinkMetricsProgressAndFailure is the downlink half: the last-success
// gauge advances on every successful fetch, with or without data, and stalls
// once the parent is gone while the failure counter moves.
func TestDownlinkMetricsProgressAndFailure(t *testing.T) {
	dir := t.TempDir()
	parentID := mustIdentity(t, filepath.Join(dir, "p.key"))
	childID := mustIdentity(t, filepath.Join(dir, "c.key"))

	ps := mustStore(t, filepath.Join(dir, "pdata"))
	pcfg := &config.Config{ULID: "n-parent", Repl: config.Endpoint{Addr: "127.0.0.1:0"}}
	preg, peng := nodeParts(t, ps, pcfg, nil, nil, nil, childSpec{"n-child", childID.PublicHex(), "child1"})
	srv, addr := startServer(t, pcfg, peng, parentID, preg)
	// Seed commands so the first /downlink returns at once. The child is attached in
	// front of the second one, so the first only makes position 2 seedable (see
	// attachAt).
	mustIngestAdmin(t, peng, "colca/v1/_CmdParam/m1/child1/m1/filler", `{"correlation_id":"c0","expires_at":99999999999}`)
	at := ps.NextOffset("commands")
	mustIngestAdmin(t, peng, "colca/v1/_CmdParam/m1/child1/m1/go", `{"correlation_id":"c1","expires_at":99999999999}`)

	cs := mustStore(t, filepath.Join(dir, "cdata"))
	ccfg := &config.Config{ULID: "n-child"}
	cm := metrics.New(cs, config.Retention{}, nil)
	_, ceng := nodeParts(t, cs, ccfg, nil, cm, nil)
	cl := mustClient(t, addr, parentID.PublicHex(), childID)
	attachAt(t, cs, cl, at)

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

	// Stop the parent. The next fetch must fail, count a failure and leave the
	// last-success gauge where it was.
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
