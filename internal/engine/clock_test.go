package engine

import (
	"math"
	"strings"
	"testing"
	"time"

	"github.com/alpamayo-solutions/colca/internal/clock"
	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/metrics"
	"github.com/alpamayo-solutions/colca/internal/store"
)

// newClockEngine builds an engine on an explicit clock. Pass the same clock to
// metrics.New when the test reads clock metrics.
func newClockEngine(t *testing.T, cfg *config.Config, clk *clock.Clock, m *metrics.Metrics) *Engine {
	t.Helper()
	s, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return New(s, cfg, testIDs(), nil, m, clk)
}

// A root engine's AuthoritativeNow is its raw clock, unaffected by
// ApplyClockSample.
func TestEngineAuthoritativeNowRootIsRawWallClock(t *testing.T) {
	base := time.UnixMilli(1_700_000_000_000)
	clk := clock.New(true, func() time.Time { return base })
	e := newClockEngine(t, &config.Config{ULID: "n-root"}, clk, nil)

	if got := e.AuthoritativeNow(); !got.Equal(base) {
		t.Fatalf("root AuthoritativeNow = %v, want %v", got, base)
	}
	e.ApplyClockSample(base.UnixMilli() + 60_000)
	if got := e.AuthoritativeNow(); !got.Equal(base) {
		t.Fatalf("root AuthoritativeNow after ApplyClockSample = %v, want unchanged %v", got, base)
	}
}

// ApplyClockSample corrects AuthoritativeNow on a non-root engine.
func TestEngineApplyClockSampleCorrectsAuthoritativeNow(t *testing.T) {
	wall := time.UnixMilli(1_700_000_000_000)
	cur := wall
	clk := clock.New(false, func() time.Time { return cur })
	e := newClockEngine(t, &config.Config{ULID: "n-child"}, clk, nil)

	// Before any sample: never synced, best-effort raw wall clock.
	if got := e.AuthoritativeNow(); !got.Equal(wall) {
		t.Fatalf("never-synced AuthoritativeNow = %v, want raw wall clock %v", got, wall)
	}

	// Authority is 45s ahead of this node's clock at receipt.
	off := e.ApplyClockSample(wall.UnixMilli() + 45_000)
	if off != 45_000 {
		t.Fatalf("ApplyClockSample returned %d, want 45000", off)
	}
	cur = wall.Add(3 * time.Second) // clock advances
	want := cur.Add(45 * time.Second)
	if got := e.AuthoritativeNow(); !got.Equal(want) {
		t.Fatalf("AuthoritativeNow after sample = %v, want %v", got, want)
	}
}

// A sample beyond time_sync.drift_warn_ms logs a warning; one within it does not.
func TestEngineApplyClockSampleWarnsPastDriftThreshold(t *testing.T) {
	buf := captureLogs(t)
	wall := time.UnixMilli(0)
	clk := clock.New(false, func() time.Time { return wall })
	driftWarnMS := int64(1000)
	cfg := &config.Config{ULID: "n-child", TimeSync: config.TimeSync{DriftWarnMS: &driftWarnMS}}
	e := newClockEngine(t, cfg, clk, nil)

	e.ApplyClockSample(500) // 500ms drift, under the 1000ms threshold
	if strings.Contains(buf.String(), "clock drift exceeds warn threshold") {
		t.Fatalf("drift under threshold must not warn, got log:\n%s", buf.String())
	}

	e.ApplyClockSample(5000) // 5000ms drift, over the 1000ms threshold
	if !strings.Contains(buf.String(), "clock drift exceeds warn threshold") {
		t.Fatalf("drift over threshold must warn, got log:\n%s", buf.String())
	}
}

// colca_clock_offset_ms and colca_clock_sync_age_seconds follow a clock shared by
// engine.New and metrics.New, before and after a sample.
func TestEngineClockOffsetVisibleInMetrics(t *testing.T) {
	wall := time.UnixMilli(1_700_000_000_000)
	clk := clock.New(false, func() time.Time { return wall })
	s, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	m := metrics.New(s, config.Retention{}, clk)
	e := New(s, &config.Config{ULID: "n-child"}, testIDs(), nil, m, clk)

	if v := scrapeMetric(t, m, "colca_clock_offset_ms"); v != 0 {
		t.Fatalf("colca_clock_offset_ms before any sample = %v, want 0", v)
	}
	if v := scrapeMetric(t, m, "colca_clock_sync_age_seconds"); !math.IsInf(v, 1) {
		t.Fatalf("colca_clock_sync_age_seconds before any sample = %v, want +Inf", v)
	}

	e.ApplyClockSample(wall.UnixMilli() + 7_000)
	if v := scrapeMetric(t, m, "colca_clock_offset_ms"); v != 7000 {
		t.Fatalf("colca_clock_offset_ms after sample = %v, want 7000", v)
	}
	if v := scrapeMetric(t, m, "colca_clock_sync_age_seconds"); v != 0 {
		t.Fatalf("colca_clock_sync_age_seconds right after the sample (same injected wall clock) = %v, want 0", v)
	}
}

// The root reports zero clock age in its metrics.
func TestEngineRootClockAlwaysZeroInMetrics(t *testing.T) {
	wall := time.UnixMilli(1_700_000_000_000)
	clk := clock.New(true, func() time.Time { return wall })
	s, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	m := metrics.New(s, config.Retention{}, clk)
	e := New(s, &config.Config{ULID: "n-root"}, testIDs(), nil, m, clk)
	e.ApplyClockSample(wall.UnixMilli() + 60_000) // ignored on the root

	if v := scrapeMetric(t, m, "colca_clock_offset_ms"); v != 0 {
		t.Fatalf("colca_clock_offset_ms on root = %v, want 0", v)
	}
	if v := scrapeMetric(t, m, "colca_clock_sync_age_seconds"); v != 0 {
		t.Fatalf("colca_clock_sync_age_seconds on root = %v, want 0", v)
	}
}
