package retention

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/engine"
	"github.com/alpamayo-solutions/colca/internal/metrics"
	"github.com/alpamayo-solutions/colca/internal/metrics/metricstest"
	"github.com/alpamayo-solutions/colca/internal/registry"
	"github.com/alpamayo-solutions/colca/internal/store"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

const nodeULID = "n-edge1"

// scrapeMetric reads back one metric value through the shared test helper —
// see metricstest's doc comment for why this goes through Handler() rather
// than a Collector/Gatherer accessor.
var scrapeMetric = metricstest.Value

func mustParts(t *testing.T) (*store.Store, *engine.Engine) {
	t.Helper()
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	cfg := &config.Config{ULID: nodeULID}
	reg, err := registry.New(st, nodeULID)
	if err != nil {
		t.Fatal(err)
	}
	return st, engine.New(st, cfg, reg, nil, nil, nil)
}

func newPruner(t *testing.T, st *store.Store, eng *engine.Engine, ret config.Retention) *Pruner {
	t.Helper()
	return NewPruner(st, eng, ret, nil, nodeULID)
}

// newPrunerWithMetrics is newPruner for tests asserting the design §8
// counters (colca_retention_pruned_records_total, ..._prune_runs_total,
// ..._gap_records_total, ..._state_refresh_*_total).
func newPrunerWithMetrics(t *testing.T, st *store.Store, eng *engine.Engine, ret config.Retention) (*Pruner, *metrics.Metrics) {
	t.Helper()
	m := metrics.New(st, ret, nil)
	return NewPruner(st, eng, ret, m, nodeULID), m
}

// appendAt appends n records to a stream with explicit timestamps ts, ts+step,
// ts+2*step, … — the store surface, exactly how the tests seed history.
func appendAt(t *testing.T, st *store.Store, stream string, n int, ts, step int64) {
	t.Helper()
	var recs []store.Record
	for i := 0; i < n; i++ {
		recs = append(recs, store.Record{
			Topic:   fmt.Sprintf("colca/v1/_Metric/%s/plant/s%d", nodeULID, i),
			Payload: []byte(fmt.Sprintf(`{"v":%d}`, i)),
			TS:      ts + int64(i)*step,
		})
	}
	if _, _, err := st.Append(stream, recs); err != nil {
		t.Fatal(err)
	}
}

func readAll(t *testing.T, st *store.Store, stream string, from uint64) []store.StoredRecord {
	t.Helper()
	recs, _, err := st.Read(stream, from, 1000, nil)
	if err != nil {
		t.Fatal(err)
	}
	return recs
}

func retFor(stream string, pol config.StreamRetention) config.Retention {
	return config.Retention{Streams: map[string]config.StreamRetention{stream: pol}}
}

// Spec §4.1: "age: advance while record.TS < now − max_age; stop at the first
// younger record". The boundary is exact and exclusive — a record whose TS
// equals the cutoff is NOT pruned.
func TestAgePolicyPrunesToExactBoundary(t *testing.T) {
	st, eng := mustParts(t)
	base := time.Now().UnixMilli()
	appendAt(t, st, "metrics", 10, base, 1000) // offsets 1..10, TS base..base+9000
	bytesBefore := st.StreamBytes("metrics")

	ret := retFor("metrics", config.StreamRetention{MaxAge: config.Duration(time.Hour)})
	p, m := newPrunerWithMetrics(t, st, eng, ret)
	// cutoff = now − 1h = base+5000, exactly the TS of offset 6: offsets 1..5
	// (TS < cutoff) go, offset 6 (TS == cutoff) survives.
	p.now = func() time.Time { return time.UnixMilli(base + 5000).Add(time.Hour) }
	p.runOnce()

	if got := st.LWM("metrics"); got != 6 {
		t.Fatalf("LWM = %d, want exactly 6 (record with TS == cutoff must survive)", got)
	}
	recs := readAll(t, st, "metrics", 1)
	if len(recs) != 5 || recs[0].Offset != 6 {
		t.Fatalf("survivors = %d records starting at %d, want 5 starting at 6", len(recs), recs[0].Offset)
	}
	if next := st.NextOffset("metrics"); next != 11 {
		t.Fatalf("no marker expected on a plain policy prune: next = %d, want 11", next)
	}

	// design §8: one run removed exactly 5 records and the bytes the store
	// itself shed (no override on this stream, so no gap-records counter).
	if v := scrapeMetric(t, m, `colca_retention_prune_runs_total{stream="metrics"}`); v != 1 {
		t.Fatalf("prune runs = %v, want 1", v)
	}
	if v := scrapeMetric(t, m, `colca_retention_pruned_records_total{stream="metrics"}`); v != 5 {
		t.Fatalf("pruned records = %v, want 5", v)
	}
	wantBytes := float64(bytesBefore - st.StreamBytes("metrics"))
	if v := scrapeMetric(t, m, `colca_retention_pruned_bytes_total{stream="metrics"}`); v != wantBytes {
		t.Fatalf("pruned bytes = %v, want %v (bytes actually shed from the store)", v, wantBytes)
	}
	if v := scrapeMetric(t, m, `colca_retention_gap_records_total{stream="metrics"}`); v != 0 {
		t.Fatalf("gap records = %v, want 0 (no cursor override on a plain policy prune)", v)
	}

	// An idle cycle (nothing left to prune) must not tick the run counter.
	p.runOnce()
	if v := scrapeMetric(t, m, `colca_retention_prune_runs_total{stream="metrics"}`); v != 1 {
		t.Fatalf("prune runs after an idle cycle = %v, want unchanged 1", v)
	}
}

// Spec §4.1: "size: additionally advance while live_bytes − bytes_shed >
// max_bytes". The oldest records are shed until the stream is at or under the
// limit — and not one record further.
func TestSizePolicyShedsOldestUntilUnderLimit(t *testing.T) {
	st, eng := mustParts(t)
	base := time.Now().UnixMilli()
	appendAt(t, st, "metrics", 10, base, 1000)

	var sizes []uint64
	if err := st.ScanRecords("metrics", 1, 11, func(off uint64, ts int64, size uint64) bool {
		sizes = append(sizes, size)
		return true
	}); err != nil {
		t.Fatal(err)
	}
	total := st.StreamBytes("metrics")
	// Limit chosen so exactly the first 3 records must go: after shedding 2
	// the stream is still over, after 3 it is exactly at the limit.
	limit := total - sizes[0] - sizes[1] - sizes[2]

	p := newPruner(t, st, eng, retFor("metrics", config.StreamRetention{
		MaxAge:   config.Duration(100000 * time.Hour), // age never fires
		MaxBytes: config.ByteSize(limit),
	}))
	p.runOnce()

	if got := st.LWM("metrics"); got != 4 {
		t.Fatalf("LWM = %d, want exactly 4 (shed 3 oldest records, not one more)", got)
	}
	if got := st.StreamBytes("metrics"); got != limit {
		t.Fatalf("StreamBytes = %d, want %d", got, limit)
	}
}

// Spec §5.2 default: never prune past the slowest protected cursor, no matter
// how loudly both policies want to. This test is the mutation guard for the
// clamp: removing the cursor floor turns it red.
func TestCursorClampBeatsBothPolicies(t *testing.T) {
	st, eng := mustParts(t)
	old := time.Now().Add(-2 * time.Hour).UnixMilli()
	appendAt(t, st, "metrics", 10, old, 1)

	if !st.CursorAck("fast", "metrics", 7) || !st.CursorAck("slow", "metrics", 3) {
		t.Fatal("cursor acks must move")
	}
	// Both policies want the whole stream gone; no staleness window (0 = never).
	p := newPruner(t, st, eng, retFor("metrics", config.StreamRetention{
		MaxAge:   config.Duration(time.Minute),
		MaxBytes: config.ByteSize(1),
	}))
	p.runOnce()

	if got := st.LWM("metrics"); got != 3 {
		t.Fatalf("LWM = %d, want 3: the SLOWEST cursor clamps, beating age and size", got)
	}
	if next := st.NextOffset("metrics"); next != 11 {
		t.Fatalf("a cursor-clamped prune must not emit a marker: next = %d, want 11", next)
	}
}

// Spec §5.2 opt-in: a cursor stops protecting only once its last advance is
// older than ignore_cursors_after. Before the window: clamp, no marker. After:
// override with exactly ONE _StreamGap marker carrying the pruned span and the
// overridden cursor list (§6.4), appended in the prune batch itself.
func TestStalenessOverrideFiresOnlyPastWindow(t *testing.T) {
	st, eng := mustParts(t)
	old := time.Now().Add(-2 * time.Hour).UnixMilli()
	appendAt(t, st, "metrics", 10, old, 1000)
	if !st.CursorAck("lag", "metrics", 3) {
		t.Fatal("cursor ack must move")
	}
	ackTime := time.Now()

	ret := retFor("metrics", config.StreamRetention{
		MaxAge:             config.Duration(time.Minute),
		IgnoreCursorsAfter: config.Duration(time.Hour),
	})
	p, m := newPrunerWithMetrics(t, st, eng, ret)

	// Inside the window: the cursor still protects. Offsets 1..2 are pruned
	// (below the cursor), the rest is clamped. No override, no marker.
	p.now = func() time.Time { return ackTime }
	p.runOnce()
	if got := st.LWM("metrics"); got != 3 {
		t.Fatalf("inside the window: LWM = %d, want 3 (cursor clamps)", got)
	}
	if next := st.NextOffset("metrics"); next != 11 {
		t.Fatalf("inside the window: no marker allowed, next = %d, want 11", next)
	}
	// design §8: a real (clamped) prune still counts as a run, but with no
	// override there is no gap-records tick.
	if v := scrapeMetric(t, m, `colca_retention_prune_runs_total{stream="metrics"}`); v != 1 {
		t.Fatalf("prune runs after the clamped cycle = %v, want 1", v)
	}
	if v := scrapeMetric(t, m, `colca_retention_pruned_records_total{stream="metrics"}`); v != 2 {
		t.Fatalf("pruned records after the clamped cycle = %v, want 2", v)
	}
	if v := scrapeMetric(t, m, `colca_retention_gap_records_total{stream="metrics"}`); v != 0 {
		t.Fatalf("gap records after the clamped cycle = %v, want 0 (no override yet)", v)
	}

	// Past the window: the cursor is overridden.
	p.now = func() time.Time { return ackTime.Add(2 * time.Hour) }
	p.runOnce()
	if got := st.LWM("metrics"); got != 11 {
		t.Fatalf("past the window: LWM = %d, want 11 (policy bound)", got)
	}
	if next := st.NextOffset("metrics"); next != 12 {
		t.Fatalf("exactly one marker must be appended: next = %d, want 12", next)
	}
	// design §8: the second run adds 8 more pruned records (3..10) and counts
	// its own run; the override ticks gap-records exactly once — matching the
	// single _StreamGap marker just asserted above.
	if v := scrapeMetric(t, m, `colca_retention_prune_runs_total{stream="metrics"}`); v != 2 {
		t.Fatalf("prune runs after the override cycle = %v, want 2", v)
	}
	if v := scrapeMetric(t, m, `colca_retention_pruned_records_total{stream="metrics"}`); v != 10 {
		t.Fatalf("pruned records after the override cycle = %v, want 10 (2 + 8)", v)
	}
	if v := scrapeMetric(t, m, `colca_retention_gap_records_total{stream="metrics"}`); v != 1 {
		t.Fatalf("gap records after the override cycle = %v, want exactly 1", v)
	}
	recs := readAll(t, st, "metrics", 11)
	if len(recs) != 1 {
		t.Fatalf("head records = %d, want exactly the marker", len(recs))
	}
	marker := recs[0]
	wantTopic := "colca/v1/_StreamGap/" + nodeULID + "/metrics"
	if marker.Topic != wantTopic {
		t.Fatalf("marker topic = %q, want %q", marker.Topic, wantTopic)
	}
	if err := uns.Validate("_StreamGap", marker.Payload); err != nil {
		t.Fatalf("marker payload must satisfy the _StreamGap contract: %v", err)
	}
	var gp gapPayload
	if err := json.Unmarshal(marker.Payload, &gp); err != nil {
		t.Fatal(err)
	}
	if gp.Stream != "metrics" || gp.FromOffset != 3 || gp.ToOffset != 10 {
		t.Fatalf("marker span = %s [%d..%d], want metrics [3..10] (this run's pruned span)", gp.Stream, gp.FromOffset, gp.ToOffset)
	}
	if gp.FirstTS != old+2*1000 || gp.LastTS != old+9*1000 {
		t.Fatalf("marker time span = [%d..%d], want [%d..%d]", gp.FirstTS, gp.LastTS, old+2000, old+9000)
	}
	if len(gp.OverriddenCursors) != 1 || gp.OverriddenCursors[0] != "lag" {
		t.Fatalf("overridden_cursors = %v, want [lag]", gp.OverriddenCursors)
	}

	// A third cycle with nothing left to prune must not emit another marker
	// for the still-dead cursor: no prune, no override.
	p.runOnce()
	if next := st.NextOffset("metrics"); next != 12 {
		t.Fatalf("idle cycle emitted a record: next = %d, want 12", next)
	}
	if got := st.LWM("metrics"); got != 11 {
		t.Fatalf("idle cycle moved the LWM: %d, want 11", got)
	}
}

// Spec §6.5: after an overridden prune on entities, the pruner re-appends the
// current KV entry of every affected path — entity-class entries with Offset ∈
// [minOverriddenCursorPos, newLWM) — AFTER the marker, through the ordinary
// engine path. Entries already consumed (below the range) and entries still in
// the stream (at or above newLWM) are not touched.
func TestEntitiesRefreshExactlyAffectedPathsAfterMarker(t *testing.T) {
	st, eng := mustParts(t)
	seed := func(topic, payload string) {
		t.Helper()
		if _, err := eng.IngestAdmin(topic, []byte(payload)); err != nil {
			t.Fatal(err)
		}
	}
	seed("colca/v1/_SystemElement/"+nodeULID+"/line1/a", `{"id":"A1"}`) // offset 1
	seed("colca/v1/_SystemElement/"+nodeULID+"/line1/b", `{"id":"B1"}`) // offset 2
	seed("colca/v1/_Signal/"+nodeULID+"/line1/c", `{"id":"C1"}`)        // offset 3
	seed("colca/v1/_SystemElement/"+nodeULID+"/line1/a", `{"id":"A2"}`) // offset 4 — update of line1/a
	// A metric-class KV entry whose Offset (in the METRICS stream) lands
	// numerically inside the refresh range [3,5): the §6.5 entity-class
	// filter must skip it — KV offsets from different streams share nothing.
	for i := 0; i < 3; i++ {
		seed("colca/v1/_Metric/"+nodeULID+"/line1/m", fmt.Sprintf(`{"v":%d}`, i)) // metrics offsets 1..3
	}
	if !st.CursorAck("uplink", "entities", 3) { // consumed offsets 1..2
		t.Fatal("cursor ack must move")
	}

	ret := retFor("entities", config.StreamRetention{
		MaxAge:             config.Duration(time.Hour),
		IgnoreCursorsAfter: config.Duration(30 * time.Minute),
	})
	p, m := newPrunerWithMetrics(t, st, eng, ret)
	p.now = func() time.Time { return time.Now().Add(2 * time.Hour) } // records old, cursor stale
	p.runOnce()

	// design §8: exactly the 2 affected paths counted as applied refreshes;
	// nothing skipped or failed.
	if v := scrapeMetric(t, m, `colca_retention_state_refresh_records_total`); v != 2 {
		t.Fatalf("state refresh records = %v, want 2", v)
	}
	if v := scrapeMetric(t, m, `colca_retention_state_refresh_skipped_total`); v != 0 {
		t.Fatalf("state refresh skipped = %v, want 0", v)
	}
	if v := scrapeMetric(t, m, `colca_retention_state_refresh_failures_total`); v != 0 {
		t.Fatalf("state refresh failures = %v, want 0", v)
	}

	// Prune [1..4] → LWM 5, marker at 5, refresh appends after it.
	if got := st.LWM("entities"); got != 5 {
		t.Fatalf("LWM = %d, want 5", got)
	}
	recs := readAll(t, st, "entities", 5)
	// Affected KV entries: line1/a (Offset 4) and line1/c (Offset 3) — both in
	// [3,5). line1/b (Offset 2) was consumed before the override and must NOT
	// be re-appended. So: 1 marker + exactly 2 refresh records.
	if len(recs) != 3 {
		t.Fatalf("head records = %d, want 3 (marker + 2 refreshes): %+v", len(recs), recs)
	}
	if got := recs[0].Topic; got != "colca/v1/_StreamGap/"+nodeULID+"/entities" {
		t.Fatalf("first head record must be the gap marker, got %q", got)
	}
	refreshed := map[string]string{}
	for _, r := range recs[1:] {
		if r.Offset <= recs[0].Offset {
			t.Fatalf("refresh at %d does not come AFTER the marker at %d", r.Offset, recs[0].Offset)
		}
		refreshed[r.Topic] = string(r.Payload)
	}
	if got := refreshed["colca/v1/_SystemElement/"+nodeULID+"/line1/a"]; got != `{"id":"A2"}` {
		t.Fatalf("line1/a refresh = %q, want the CURRENT value {\"ulid\":\"A2\"}", got)
	}
	if got := refreshed["colca/v1/_Signal/"+nodeULID+"/line1/c"]; got != `{"id":"C1"}` {
		t.Fatalf("line1/c refresh = %q, want {\"ulid\":\"C1\"}", got)
	}

	// KV converged onto the refresh offsets; the untouched path keeps its
	// (pruned-below-LWM, provenance-only) offset.
	for _, e := range st.KVScan("") {
		switch e.Path {
		case "line1/a", "line1/c":
			if e.Offset < 6 {
				t.Fatalf("KV %s Offset = %d, want the new refresh offset (>= 6)", e.Path, e.Offset)
			}
		case "line1/b":
			if e.Offset != 2 {
				t.Fatalf("KV line1/b Offset = %d, want untouched 2", e.Offset)
			}
		case "line1/m":
			if e.Offset != 3 {
				t.Fatalf("metric KV line1/m Offset = %d, want untouched 3 — §6.5 refreshes entity-class entries only", e.Offset)
			}
		}
	}
	// The metrics stream saw no refresh traffic either.
	if next := st.NextOffset("metrics"); next != 4 {
		t.Fatalf("metrics next = %d, want 4: an entities refresh must never re-append metric-class KV entries", next)
	}
	// The completed refresh cleared its persisted obligation.
	if r, ok := st.RefreshPending("entities"); ok {
		t.Fatalf("refresh obligation %+v not cleared after full success", r)
	}
}

// Spec §6.5 exclusion: metrics are KV-projected too, but an overridden prune
// on metrics refreshes NOTHING — the marker is the only record appended. This
// test is the second mutation guard: dropping the entities-only gate (or the
// entity-class filter) turns it red.
func TestMetricsOverrideRefreshesNothing(t *testing.T) {
	st, eng := mustParts(t)
	for i, topic := range []string{"line1/temp", "line1/pres"} {
		if _, err := eng.IngestAdmin(fmt.Sprintf("colca/v1/_Metric/%s/%s", nodeULID, topic), []byte(fmt.Sprintf(`{"v":%d}`, i))); err != nil {
			t.Fatal(err)
		}
	}
	if !st.CursorAck("uplink", "metrics", 2) { // offset 2 unread → will be overridden
		t.Fatal("cursor ack must move")
	}
	p := newPruner(t, st, eng, retFor("metrics", config.StreamRetention{
		MaxAge:             config.Duration(time.Hour),
		IgnoreCursorsAfter: config.Duration(30 * time.Minute),
	}))
	p.now = func() time.Time { return time.Now().Add(2 * time.Hour) }
	p.runOnce()

	if got := st.LWM("metrics"); got != 3 {
		t.Fatalf("LWM = %d, want 3", got)
	}
	if next := st.NextOffset("metrics"); next != 4 {
		t.Fatalf("next = %d, want 4: the marker and NOTHING else — metrics KV entries must not be refreshed", next)
	}
	// The metric KV entries stay untouched, offsets pointing below the LWM
	// (provenance, not a dangling reference — spec §2).
	for _, e := range st.KVScan("line1/") {
		if e.Offset > 2 {
			t.Fatalf("metric KV %s was re-appended (Offset %d) — §6.5 refreshes entities only", e.Path, e.Offset)
		}
	}
	// A metrics override persists no refresh obligation at all.
	if r, ok := st.RefreshPending("metrics"); ok {
		t.Fatalf("metrics override persisted a refresh obligation %+v — §6.5 is entities-only", r)
	}
}

// Config contract: EffectiveInterval() == 0 is the operator's
// explicit "pruner disabled" — Run must return immediately and never touch
// the store.
func TestDisabledPrunerNeverRuns(t *testing.T) {
	st, eng := mustParts(t)
	appendAt(t, st, "metrics", 5, time.Now().Add(-48*time.Hour).UnixMilli(), 1)

	zero := config.Duration(0)
	p := newPruner(t, st, eng, config.Retention{
		Interval: &zero,
		Streams:  map[string]config.StreamRetention{"metrics": {MaxAge: config.Duration(time.Minute)}},
	})
	stop := make(chan struct{}) // never closed: Run must exit on its own
	done := make(chan struct{})
	go func() { p.Run(stop); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return immediately with interval 0 (pruner disabled)")
	}
	if got := st.LWM("metrics"); got != 1 {
		t.Fatalf("disabled pruner pruned: LWM = %d, want 1", got)
	}
}

// Run prunes on its cadence and returns promptly when stop closes — the
// node's WaitGroup discipline depends on that return.
func TestRunPrunesOnCadenceAndStopsCleanly(t *testing.T) {
	st, eng := mustParts(t)
	appendAt(t, st, "metrics", 5, time.Now().Add(-48*time.Hour).UnixMilli(), 1)

	iv := config.Duration(5 * time.Millisecond)
	p := newPruner(t, st, eng, config.Retention{
		Interval: &iv,
		Streams:  map[string]config.StreamRetention{"metrics": {MaxAge: config.Duration(time.Minute)}},
	})
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() { p.Run(stop); close(done) }()

	deadline := time.Now().Add(5 * time.Second)
	for st.LWM("metrics") != 6 {
		if time.Now().After(deadline) {
			t.Fatalf("pruner never pruned on its cadence: LWM = %d", st.LWM("metrics"))
		}
		time.Sleep(5 * time.Millisecond)
	}
	close(stop)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after stop")
	}
}

// Spec §5.2 [delta]: a cursor whose FIRST ack lands in the TOCTOU window
// between the pruner's policy evaluation and the Prune commit is never pruned
// past — the store's in-batch recheck clamps the range, and the §6.4 marker is
// rewritten to the effective span (one source of truth, shared with the
// journal). Deterministic: the beforePrune hook injects the ack exactly into
// the window.
func TestConcurrentFirstAckNeverPrunedPast(t *testing.T) {
	st, eng := mustParts(t)
	old := time.Now().Add(-2 * time.Hour).UnixMilli()
	appendAt(t, st, "metrics", 10, old, 1000)
	if !st.CursorAck("lag", "metrics", 3) { // stale, will be overridden
		t.Fatal("ack must move")
	}
	// The EFFECTIVE span [1..4] this run actually removes, computed before
	// the prune so it can still be scanned — the bytes design §8's
	// colca_retention_pruned_bytes_total must report, NOT the bytes of the
	// policy's originally intended (and larger) [1..10] span.
	var wantShedBytes uint64
	if err := st.ScanRecords("metrics", 1, 5, func(_ uint64, _ int64, size uint64) bool {
		wantShedBytes += size
		return true
	}); err != nil {
		t.Fatal(err)
	}

	ret := retFor("metrics", config.StreamRetention{
		MaxAge:             config.Duration(time.Minute),
		IgnoreCursorsAfter: config.Duration(time.Hour),
	})
	p, m := newPrunerWithMetrics(t, st, eng, ret)
	p.now = func() time.Time { return time.Now().Add(2 * time.Hour) }
	p.beforePrune = func(stream string) {
		// A consumer's first ack, racing the cycle: policy already decided to
		// prune to 11.
		if !st.CursorAck("sniper", "metrics", 5) {
			t.Error("concurrent ack must move")
		}
	}
	p.runOnce()

	// design §8: pruned_bytes must be the EFFECTIVE
	// post-shrink figure (4 records), never the pre-commit scan's larger
	// intended figure (10 records) — the store's own store.PruneSpan.Shed,
	// captured by the plan closure, is the only correct source.
	if v := scrapeMetric(t, m, `colca_retention_pruned_records_total{stream="metrics"}`); v != 4 {
		t.Fatalf("pruned records = %v, want 4 (the shrunk [1..4] span, not the intended 10)", v)
	}
	if v, want := scrapeMetric(t, m, `colca_retention_pruned_bytes_total{stream="metrics"}`), float64(wantShedBytes); v != want {
		t.Fatalf("pruned bytes = %v, want %v (bytes of the EFFECTIVE [1..4] span, not the larger intended span)", v, want)
	}

	if got := st.LWM("metrics"); got != 5 {
		t.Fatalf("LWM = %d, want 5: the concurrently acked cursor must clamp the prune", got)
	}
	recs := readAll(t, st, "metrics", 1)
	// Offsets 5..10 survive, one marker at 11.
	if len(recs) != 7 || recs[0].Offset != 5 {
		t.Fatalf("survivors = %d records from %d, want 7 from 5", len(recs), recs[0].Offset)
	}
	marker := recs[6]
	if marker.Topic != "colca/v1/_StreamGap/"+nodeULID+"/metrics" {
		t.Fatalf("head record = %q, want the gap marker", marker.Topic)
	}
	var gp gapPayload
	if err := json.Unmarshal(marker.Payload, &gp); err != nil {
		t.Fatal(err)
	}
	// The marker describes the EFFECTIVE pruned span [1..4], not the
	// requested [1..10] — same numbers as the journal entry.
	if gp.FromOffset != 1 || gp.ToOffset != 4 {
		t.Fatalf("marker span = [%d..%d], want the shrunk [1..4]", gp.FromOffset, gp.ToOffset)
	}
	if gp.FirstTS != old || gp.LastTS != old+3*1000 {
		t.Fatalf("marker time span = [%d..%d], want [%d..%d]", gp.FirstTS, gp.LastTS, old, old+3000)
	}
	if len(gp.OverriddenCursors) != 1 || gp.OverriddenCursors[0] != "lag" {
		t.Fatalf("overridden_cursors = %v, want [lag]", gp.OverriddenCursors)
	}
	j := st.PruneJournal("metrics")
	if j[len(j)-1].To != 4 {
		t.Fatalf("journal span %+v disagrees with the marker", j[len(j)-1])
	}
}

// Spec §6.5 [delta]: the refresh obligation is persisted in the prune batch
// (rp/), so a partial refresh failure — or a crash before the refresh — never
// loses it: the range stays pending, is retried each cycle, and a NEW pruner
// instance (process restart) completes it at startup. Already-refreshed paths
// drop out naturally (their KV Offset moved past the range), so retries never
// double-refresh.
func TestRefreshObligationSurvivesFailureAndRestart(t *testing.T) {
	st, eng := mustParts(t)
	seed := func(topic, payload string) {
		t.Helper()
		if _, err := eng.IngestAdmin(topic, []byte(payload)); err != nil {
			t.Fatal(err)
		}
	}
	seed("colca/v1/_SystemElement/"+nodeULID+"/line1/a", `{"id":"A1"}`) // offset 1
	seed("colca/v1/_SystemElement/"+nodeULID+"/line1/b", `{"id":"B1"}`) // offset 2
	seed("colca/v1/_Signal/"+nodeULID+"/line1/c", `{"id":"C1"}`)        // offset 3
	if !st.CursorAck("uplink", "entities", 2) {                         // offsets 2..3 unread
		t.Fatal("ack must move")
	}
	ret := retFor("entities", config.StreamRetention{
		MaxAge:             config.Duration(time.Hour),
		IgnoreCursorsAfter: config.Duration(30 * time.Minute),
	})

	// Cycle 1: override prunes [1..3]; the refresh append for line1/c fails.
	pA, mA := newPrunerWithMetrics(t, st, eng, ret)
	pA.now = func() time.Time { return time.Now().Add(2 * time.Hour) }
	realPublish := pA.publish
	pA.publish = func(topic string, payload []byte, ifKVOffset uint64) (bool, error) {
		if strings.HasSuffix(topic, "/line1/c") {
			return false, fmt.Errorf("injected refresh failure")
		}
		return realPublish(topic, payload, ifKVOffset)
	}
	pA.runOnce()

	if got := st.LWM("entities"); got != 4 {
		t.Fatalf("LWM = %d, want 4", got)
	}
	r, ok := st.RefreshPending("entities")
	if !ok || r != (store.RefreshRange{From: 2, To: 4}) {
		t.Fatalf("pending after partial failure = %+v/%v, want [2,4) kept", r, ok)
	}
	// Marker at 4, successful refresh of line1/b at 5; line1/c still owed.
	if next := st.NextOffset("entities"); next != 6 {
		t.Fatalf("entities next = %d, want 6 (marker + one successful refresh)", next)
	}
	// design §8: this is the refresh path's OWN failure counter — line1/c's
	// injected error counts here, line1/b's success counts as applied, and
	// nothing was skipped.
	if v := scrapeMetric(t, mA, `colca_retention_state_refresh_failures_total`); v != 1 {
		t.Fatalf("pA state refresh failures = %v, want 1", v)
	}
	if v := scrapeMetric(t, mA, `colca_retention_state_refresh_records_total`); v != 1 {
		t.Fatalf("pA state refresh records = %v, want 1", v)
	}
	if v := scrapeMetric(t, mA, `colca_retention_state_refresh_skipped_total`); v != 0 {
		t.Fatalf("pA state refresh skipped = %v, want 0", v)
	}

	// "Restart": a NEW pruner instance (own metrics registry too — a
	// restarted process starts every Prometheus counter at zero) on the same
	// store, healthy publish path, completes the obligation at Run startup —
	// before any tick.
	pB, mB := newPrunerWithMetrics(t, st, eng, ret)
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() { pB.Run(stop); close(done) }()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, ok := st.RefreshPending("entities"); !ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("restarted pruner never completed the pending refresh")
		}
		time.Sleep(5 * time.Millisecond)
	}
	close(stop)
	<-done

	// line1/c refreshed exactly once (offset 6); line1/b NOT re-refreshed.
	recs := readAll(t, st, "entities", 4)
	if len(recs) != 3 { // marker, b-refresh, c-refresh
		t.Fatalf("head records = %d, want 3: %+v", len(recs), recs)
	}
	if got := recs[2].Topic; got != "colca/v1/_Signal/"+nodeULID+"/line1/c" {
		t.Fatalf("retried refresh = %q, want line1/c", got)
	}
	if string(recs[2].Payload) != `{"id":"C1"}` {
		t.Fatalf("retried refresh payload = %q", recs[2].Payload)
	}
	if next := st.NextOffset("entities"); next != 7 {
		t.Fatalf("entities next = %d, want 7 — the retry must not double-refresh line1/b", next)
	}
	// design §8: on pB's own registry, the retried line1/c is exactly one
	// applied refresh — no failures, no skips. line1/b never re-enters the
	// scan (its KV Offset already moved past the range), so it does not
	// double-count here.
	if v := scrapeMetric(t, mB, `colca_retention_state_refresh_records_total`); v != 1 {
		t.Fatalf("pB state refresh records = %v, want 1", v)
	}
	if v := scrapeMetric(t, mB, `colca_retention_state_refresh_failures_total`); v != 0 {
		t.Fatalf("pB state refresh failures = %v, want 0", v)
	}

	// rp/ absent → completion is a no-op: another cycle appends nothing.
	pC := newPruner(t, st, eng, ret)
	pC.runOnce()
	if next := st.NextOffset("entities"); next != 7 {
		t.Fatalf("no-op completion appended records: next = %d, want 7", next)
	}
}

// Spec §5.1 [delta] + §5.2: a child's persisted downlink cursor
// (downlink:{child-ulid} on the parent's commands stream, written by the
// /downlink handler) is an ordinary named cursor — an offline child blocks
// the parent's commands prune until the staleness window passes, then is
// overridden with the marker naming it.
func TestStaleDownlinkChildCursorBlocksThenOverridden(t *testing.T) {
	st, eng := mustParts(t)
	old := time.Now().Add(-2 * time.Hour).UnixMilli()
	appendAt(t, st, "commands", 6, old, 1000)
	// What the /downlink handler persists when the child last polled at 3.
	if !st.CursorAck("downlink:n-child-offline", "commands", 3) {
		t.Fatal("ack must move")
	}
	pollTime := time.Now()

	p := newPruner(t, st, eng, retFor("commands", config.StreamRetention{
		MaxAge:             config.Duration(time.Minute),
		IgnoreCursorsAfter: config.Duration(time.Hour),
	}))

	// Child offline, but inside the window: its cursor clamps the prune.
	p.now = func() time.Time { return pollTime }
	p.runOnce()
	if got := st.LWM("commands"); got != 3 {
		t.Fatalf("LWM = %d, want 3: the offline child's downlink cursor must block the prune", got)
	}
	if next := st.NextOffset("commands"); next != 7 {
		t.Fatalf("clamped prune must not emit a marker: next = %d, want 7", next)
	}

	// Past the window the child stops protecting: override + marker naming it.
	p.now = func() time.Time { return pollTime.Add(2 * time.Hour) }
	p.runOnce()
	if got := st.LWM("commands"); got != 7 {
		t.Fatalf("LWM = %d, want 7 (policy bound after the override)", got)
	}
	recs := readAll(t, st, "commands", 7)
	if len(recs) != 1 || recs[0].Topic != "colca/v1/_StreamGap/"+nodeULID+"/commands" {
		t.Fatalf("head records = %+v, want exactly the commands gap marker", recs)
	}
	var gp gapPayload
	if err := json.Unmarshal(recs[0].Payload, &gp); err != nil {
		t.Fatal(err)
	}
	if len(gp.OverriddenCursors) != 1 || gp.OverriddenCursors[0] != "downlink:n-child-offline" {
		t.Fatalf("overridden_cursors = %v, want the child's downlink cursor", gp.OverriddenCursors)
	}
}

// Spec §6.5 [delta] × §7.1: a tombstone landing between the refresh's KVScan
// snapshot and that entry's publish must NOT be resurrected by the refresh.
// Deterministic: the publish seam injects the tombstone exactly into the
// window, then delegates to the real guarded publish — which must skip. The
// skip voids the obligation for that path (nothing current to refresh), so
// rp/ still clears. This test is the mutation guard for the CAS: dropping the
// offset check in AppendIfKVUnchanged turns it red.
func TestTombstoneDuringRefreshIsNotResurrected(t *testing.T) {
	st, eng := mustParts(t)
	topicA := "colca/v1/_SystemElement/" + nodeULID + "/line1/a"
	topicB := "colca/v1/_SystemElement/" + nodeULID + "/line1/b"
	for _, s := range []struct{ topic, payload string }{
		{topicA, `{"id":"A1"}`}, // entities offset 1
		{topicB, `{"id":"B1"}`}, // entities offset 2
	} {
		if _, err := eng.IngestAdmin(s.topic, []byte(s.payload)); err != nil {
			t.Fatal(err)
		}
	}
	if !st.CursorAck("uplink", "entities", 2) { // offset 2 (line1/b) unread
		t.Fatal("ack must move")
	}
	ret := retFor("entities", config.StreamRetention{
		MaxAge:             config.Duration(time.Hour),
		IgnoreCursorsAfter: config.Duration(30 * time.Minute),
	})
	p, m := newPrunerWithMetrics(t, st, eng, ret)
	p.now = func() time.Time { return time.Now().Add(2 * time.Hour) }
	realPublish := p.publish
	p.publish = func(topic string, payload []byte, ifKVOffset uint64) (bool, error) {
		if topic == topicB {
			// The race, made deterministic: the path is retired AFTER the
			// KVScan snapshot, BEFORE its refresh publish.
			if _, err := eng.IngestAdmin(topicB, nil); err != nil {
				t.Errorf("tombstone injection failed: %v", err)
			}
		}
		return realPublish(topic, payload, ifKVOffset)
	}
	p.runOnce()

	// The retired path stays GONE — no resurrection.
	if kv := st.KVScan("line1/b"); len(kv) != 0 {
		t.Fatalf("tombstoned path resurrected by the refresh: %+v", kv)
	}
	// design §8: the guard skip is a completion, not a failure — it counts
	// against the skipped family only. Nothing was applied (topicA falls
	// outside the refresh range [2,3)).
	if v := scrapeMetric(t, m, `colca_retention_state_refresh_skipped_total`); v != 1 {
		t.Fatalf("state refresh skipped = %v, want 1", v)
	}
	if v := scrapeMetric(t, m, `colca_retention_state_refresh_records_total`); v != 0 {
		t.Fatalf("state refresh records = %v, want 0", v)
	}
	if v := scrapeMetric(t, m, `colca_retention_state_refresh_failures_total`); v != 0 {
		t.Fatalf("state refresh failures = %v, want 0", v)
	}
	// Stream head: marker at 3 (prune of [1..2]), injected tombstone at 4,
	// and NO refresh record for line1/b.
	recs := readAll(t, st, "entities", 3)
	if len(recs) != 2 {
		t.Fatalf("head records = %d, want 2 (marker + tombstone): %+v", len(recs), recs)
	}
	if recs[0].Topic != "colca/v1/_StreamGap/"+nodeULID+"/entities" {
		t.Fatalf("offset 3 = %q, want the gap marker", recs[0].Topic)
	}
	if recs[1].Topic != topicB || len(recs[1].Payload) != 0 {
		t.Fatalf("offset 4 = %q (%d bytes), want the empty tombstone for line1/b", recs[1].Topic, len(recs[1].Payload))
	}
	// The skip voids the obligation: rp/ cleared, nothing left pending.
	if r, ok := st.RefreshPending("entities"); ok {
		t.Fatalf("obligation %+v not cleared — a guard skip must count as completion", r)
	}
	// The untouched sibling keeps its state.
	if kv := st.KVScan("line1/a"); len(kv) != 1 || string(kv[0].Payload) != `{"id":"A1"}` {
		t.Fatalf("sibling path damaged: %+v", kv)
	}
}


// The pruner's cycle reclaims the definitions stream too — by compaction, not
// by policy (definition-stream design §6). Without this the stream would grow
// forever however the retention config is tuned, because no policy applies to
// it at all.
func TestRunOnceCompactsTheDefinitionsStream(t *testing.T) {
	st, eng := mustParts(t)
	for _, payload := range []string{`{"id":"01HGRP-OPS","v":1}`, `{"id":"01HGRP-OPS","v":2}`} {
		if _, _, err := st.Append("definitions", []store.Record{{
			Topic: "colca/v1/_Group/" + nodeULID + "/01HGRP-OPS", Payload: []byte(payload), TS: 1,
			KVPath: "01HGRP-OPS", KVNode: nodeULID,
		}}); err != nil {
			t.Fatal(err)
		}
	}

	p := newPruner(t, st, eng, config.Retention{})
	p.runOnce()

	recs, _, err := st.Read("definitions", 1, 100, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 1 {
		t.Fatalf("definitions after a cycle = %d records, want the superseded one gone", len(recs))
	}
	if string(recs[0].Payload) != `{"id":"01HGRP-OPS","v":2}` {
		t.Fatalf("the surviving record is not the latest: %s", recs[0].Payload)
	}
}
