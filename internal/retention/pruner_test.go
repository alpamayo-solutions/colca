package retention

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/engine"
	"github.com/alpamayo-solutions/colca/internal/store"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

const nodeULID = "n-edge1"

func mustParts(t *testing.T) (*store.Store, *engine.Engine) {
	t.Helper()
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	cfg := &config.Config{ULID: nodeULID}
	return st, engine.New(st, cfg, nil, nil)
}

func newPruner(t *testing.T, st *store.Store, eng *engine.Engine, ret config.Retention) *Pruner {
	t.Helper()
	return NewPruner(st, eng, ret, nil, nodeULID)
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

	p := newPruner(t, st, eng, retFor("metrics", config.StreamRetention{MaxAge: config.Duration(time.Hour)}))
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

	p := newPruner(t, st, eng, retFor("metrics", config.StreamRetention{
		MaxAge:             config.Duration(time.Minute),
		IgnoreCursorsAfter: config.Duration(time.Hour),
	}))

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

	// Past the window: the cursor is overridden.
	p.now = func() time.Time { return ackTime.Add(2 * time.Hour) }
	p.runOnce()
	if got := st.LWM("metrics"); got != 11 {
		t.Fatalf("past the window: LWM = %d, want 11 (policy bound)", got)
	}
	if next := st.NextOffset("metrics"); next != 12 {
		t.Fatalf("exactly one marker must be appended: next = %d, want 12", next)
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
	seed("colca/v1/_SystemElement/"+nodeULID+"/line1/a", `{"ulid":"A1"}`) // offset 1
	seed("colca/v1/_SystemElement/"+nodeULID+"/line1/b", `{"ulid":"B1"}`) // offset 2
	seed("colca/v1/_Signal/"+nodeULID+"/line1/c", `{"ulid":"C1"}`)        // offset 3
	seed("colca/v1/_SystemElement/"+nodeULID+"/line1/a", `{"ulid":"A2"}`) // offset 4 — update of line1/a
	// A metric-class KV entry whose Offset (in the METRICS stream) lands
	// numerically inside the refresh range [3,5): the §6.5 entity-class
	// filter must skip it — KV offsets from different streams share nothing.
	for i := 0; i < 3; i++ {
		seed("colca/v1/_Metric/"+nodeULID+"/line1/m", fmt.Sprintf(`{"v":%d}`, i)) // metrics offsets 1..3
	}
	if !st.CursorAck("uplink", "entities", 3) { // consumed offsets 1..2
		t.Fatal("cursor ack must move")
	}

	p := newPruner(t, st, eng, retFor("entities", config.StreamRetention{
		MaxAge:             config.Duration(time.Hour),
		IgnoreCursorsAfter: config.Duration(30 * time.Minute),
	}))
	p.now = func() time.Time { return time.Now().Add(2 * time.Hour) } // records old, cursor stale
	p.runOnce()

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
	if got := refreshed["colca/v1/_SystemElement/"+nodeULID+"/line1/a"]; got != `{"ulid":"A2"}` {
		t.Fatalf("line1/a refresh = %q, want the CURRENT value {\"ulid\":\"A2\"}", got)
	}
	if got := refreshed["colca/v1/_Signal/"+nodeULID+"/line1/c"]; got != `{"ulid":"C1"}` {
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
