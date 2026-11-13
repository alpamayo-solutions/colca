package retention

import (
	"testing"
	"time"

	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/metrics/metricstest"
)

// colca_retention_blocked_by_cursor comes from the pruner's pass: every
// protecting cursor below the policy's own target counts, and it drops once
// they caught up. A scrape reads the value and scans nothing.
func TestThePrunersPassSetsTheBlockedCursorGauge(t *testing.T) {
	st, eng := mustParts(t)
	old := time.Now().Add(-2 * time.Hour).UnixMilli()
	appendAt(t, st, "metrics", 10, old, 1)
	if !st.CursorAck("slower", "metrics", 2) || !st.CursorAck("slow", "metrics", 3) || !st.CursorAck("fast", "metrics", 11) {
		t.Fatal("cursor acks must move")
	}
	p, m := newPrunerWithMetrics(t, st, eng, retFor("metrics", config.StreamRetention{MaxAge: config.Duration(time.Minute)}))
	blocked := func() float64 {
		return metricstest.Value(t, m, `colca_retention_blocked_by_cursor{stream="metrics"}`)
	}
	if got := blocked(); got != 0 {
		t.Fatalf("gauge = %v before any pass, want 0", got)
	}
	p.pruneStream("metrics")
	if got := blocked(); got != 2 {
		t.Fatalf("gauge = %v after a clamped pass, want 2: slower and slow both block", got)
	}
	if !st.CursorAck("slower", "metrics", 11) || !st.CursorAck("slow", "metrics", 11) {
		t.Fatal("cursor acks must move")
	}
	if got := blocked(); got != 2 {
		t.Fatalf("gauge = %v between passes, want the last pass's 2: scrapes do not scan", got)
	}
	p.pruneStream("metrics")
	if got := blocked(); got != 0 {
		t.Fatalf("gauge = %v after the cursors caught up, want 0", got)
	}
}

// The scan behind the gauge is capped like the pass's own: past the cap the
// target is a floor and only the cursors below it count.
func TestTheBlockedCursorScanIsCapped(t *testing.T) {
	st, eng := mustParts(t)
	appendAt(t, st, "metrics", 1000, time.Now().Add(-2*time.Hour).UnixMilli(), 1)
	if !st.CursorAck("below-floor", "metrics", 30) || !st.CursorAck("above-floor", "metrics", 200) {
		t.Fatal("cursor acks must move")
	}
	p, m := newPrunerWithMetrics(t, st, eng, retFor("metrics", config.StreamRetention{MaxAge: config.Duration(time.Minute)}))
	p.scanCap = 50
	p.pruneStream("metrics")
	if got := metricstest.Value(t, m, `colca_retention_blocked_by_cursor{stream="metrics"}`); got != 1 {
		t.Fatalf("gauge = %v with a cap of 50, want 1: only the cursor below the capped floor", got)
	}
}
