package engine

import (
	"testing"
	"time"

	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/metrics"
	"github.com/alpamayo-solutions/colca/internal/metrics/metricstest"
	"github.com/alpamayo-solutions/colca/internal/store"
)

func newMetricsTestEngine(t *testing.T) (*Engine, *metrics.Metrics) {
	t.Helper()
	s, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	cfg := &config.Config{ULID: "n-edge1"}
	m := metrics.New(s, config.Retention{}, nil)
	e := New(s, cfg, testIDs(), nil, m, nil)
	placeTestElements(t, e)
	return e, m
}

// TestMetricWithNoSignalIsCountedAndOncePerReminderLogged is the level-1 pin
// of SDK design §7 gap 6: a _Metric accepted on a path with no _Signal there
// is counted EVERY time (colca_metrics_unbound_total is never rate-limited —
// it must reflect the true volume) but logged only once per reminder window,
// exactly mirroring internal/repl/linkstate.go's own reminder shape.
func TestMetricWithNoSignalIsCountedAndOncePerReminderLogged(t *testing.T) {
	e, m := newMetricsTestEngine(t)

	const line = "colca_metrics_unbound_total"
	if v := metricstest.Value(t, m, line); v != 0 {
		t.Fatalf("%s = %v before any publish, want 0", line, v)
	}

	// Deny denominator: prove the query finds a bound path
	// before trusting it to report an unbound one as absent — otherwise a
	// broken KVGet lookup would pass this test by finding nothing either way.
	if _, err := e.IngestAdmin("colca/v1/_Signal/n-edge1/m1/bound", []byte(`{"id":"01SIGBOUND"}`)); err != nil {
		t.Fatalf("place bound signal: %v", err)
	}
	if _, err := e.IngestClient("m1", "colca/v1/_Metric/n-edge1/m1/bound", []byte(`{"v":1}`)); err != nil {
		t.Fatalf("publish bound metric: %v", err)
	}
	if v := metricstest.Value(t, m, line); v != 0 {
		t.Fatalf("%s = %v after a metric WITH a _Signal, want 0 (denominator check)", line, v)
	}

	if _, err := e.IngestClient("m1", "colca/v1/_Metric/n-edge1/m1/unbound", []byte(`{"v":1}`)); err != nil {
		t.Fatalf("publish unbound metric: %v", err)
	}
	if v := metricstest.Value(t, m, line); v != 1 {
		t.Fatalf("%s = %v after one unbound metric, want 1", line, v)
	}

	// A second sample on the SAME path within the reminder window still
	// counts (the metric is never throttled) — only the log line is.
	if _, err := e.IngestClient("m1", "colca/v1/_Metric/n-edge1/m1/unbound", []byte(`{"v":2}`)); err != nil {
		t.Fatalf("publish second unbound metric: %v", err)
	}
	if v := metricstest.Value(t, m, line); v != 2 {
		t.Fatalf("%s = %v after a second unbound sample on the same path, want 2 (the counter is never rate-limited)", line, v)
	}

	// A different unbound path counts independently.
	if _, err := e.IngestClient("m1", "colca/v1/_Metric/n-edge1/m1/other-unbound", []byte(`{"v":1}`)); err != nil {
		t.Fatalf("publish second unbound path: %v", err)
	}
	if v := metricstest.Value(t, m, line); v != 3 {
		t.Fatalf("%s = %v after a second distinct unbound path, want 3", line, v)
	}
}

// TestUnboundMetricLogRateLimiting pins the log throttle in isolation
// (unbound.go), independent of the engine and the counter above.
func TestUnboundMetricLogRateLimiting(t *testing.T) {
	log := newUnboundMetricLog()
	base := time.Unix(0, 0)

	if !log.shouldLog("m1/temp", base) {
		t.Fatal("the first occurrence of a path must always be logged")
	}
	if log.shouldLog("m1/temp", base.Add(time.Minute)) {
		t.Fatal("a repeat within the reminder window must not be logged again")
	}
	if !log.shouldLog("m1/temp", base.Add(unboundMetricReminder+time.Second)) {
		t.Fatal("a repeat past the reminder window must be logged again")
	}
	if !log.shouldLog("m1/other", base) {
		t.Fatal("a distinct path must be logged independently of an unrelated path's window")
	}
}

// TestMetricWithSignalOnAnotherPathStillCountsSeparately guards against a
// checkMetricBinding that answers "bound" for the wrong path (e.g. a KVGet
// that ignores the path segment) by planting the signal at a sibling path.
func TestMetricWithSignalOnAnotherPathStillCountsSeparately(t *testing.T) {
	e, m := newMetricsTestEngine(t)
	if _, err := e.IngestAdmin("colca/v1/_Signal/n-edge1/m1/sibling-bound", []byte(`{"id":"01SIGSIB"}`)); err != nil {
		t.Fatalf("place sibling signal: %v", err)
	}
	if _, err := e.IngestClient("m1", "colca/v1/_Metric/n-edge1/m1/sibling-unbound", []byte(`{"v":1}`)); err != nil {
		t.Fatalf("publish: %v", err)
	}
	const line = "colca_metrics_unbound_total"
	if v := metricstest.Value(t, m, line); v != 1 {
		t.Fatalf("%s = %v, want 1 — a signal at a sibling path must not mask this one", line, v)
	}
}
