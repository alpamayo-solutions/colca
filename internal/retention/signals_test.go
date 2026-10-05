package retention

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/store"
)

// appendSignal appends one _Metric record per age, oldest first, for a signal
// at path, and returns their offsets.
func appendSignal(t *testing.T, st *store.Store, now time.Time, path, signalID string, ages ...time.Duration) []uint64 {
	t.Helper()
	var recs []store.Record
	for _, age := range ages {
		recs = append(recs, store.Record{
			Topic:   fmt.Sprintf("colca/v1/_Metric/%s/%s", nodeULID, path),
			Payload: []byte(fmt.Sprintf(`{"signal_id":%q,"value":%d}`, signalID, age.Milliseconds())),
			TS:      now.Add(-age).UnixMilli(),
		})
	}
	first, last, err := st.Append("metrics", recs)
	if err != nil {
		t.Fatal(err)
	}
	var out []uint64
	for off := first; off <= last; off++ {
		out = append(out, off)
	}
	return out
}

// liveOffsets lists which of offs are still in the metrics stream.
func liveOffsets(t *testing.T, st *store.Store, offs []uint64) []uint64 {
	t.Helper()
	live := map[uint64]bool{}
	for _, r := range readAll(t, st, "metrics", 1) {
		live[r.Offset] = true
	}
	var out []uint64
	for _, off := range offs {
		if live[off] {
			out = append(out, off)
		}
	}
	return out
}

func signalRet(rules ...config.SignalRetention) config.Retention {
	return retFor("metrics", config.StreamRetention{Signals: rules})
}

// A rule selected by topic filter, and one by signal id, each cut their
// signals to the window plus the value in force when it opens. A signal no
// rule selects keeps the stream's 14 days, and the LWM does not move.
func TestSignalRulesPruneSelectedSignalsAndKeepTheValueInForce(t *testing.T) {
	st, eng := mustParts(t)
	now := time.Now()
	ages := []time.Duration{5 * time.Hour, 4 * time.Hour, 3 * time.Hour, 30 * time.Minute}
	vib := appendSignal(t, st, now, "line1/vibration/x", "SIGVIB", ages...)
	byID := appendSignal(t, st, now, "line1/temp", "SIGTEMP", ages...)
	other := appendSignal(t, st, now, "line1/count", "SIGCOUNT", ages...)

	ret := signalRet(
		config.SignalRetention{Topics: []string{"colca/v1/_Metric/+/+/vibration/#"}, MaxAge: config.Duration(time.Hour)},
		config.SignalRetention{SignalIDs: []string{"SIGTEMP"}, MaxAge: config.Duration(210 * time.Minute)},
	)
	p, m := newPrunerWithMetrics(t, st, eng, ret)
	p.now = func() time.Time { return now }
	p.runOnce()

	if got := liveOffsets(t, st, vib); len(got) != 2 || got[0] != vib[2] {
		t.Fatalf("vibration keeps %v, want %v (the 3h value in force and the window)", got, vib[2:])
	}
	if got := liveOffsets(t, st, byID); len(got) != 3 || got[0] != byID[1] {
		t.Fatalf("SIGTEMP keeps %v, want %v (3.5h window: the 4h value in force onwards)", got, byID[1:])
	}
	if got := liveOffsets(t, st, other); len(got) != len(other) {
		t.Fatalf("a signal no rule selects lost records: %v of %v", got, other)
	}
	if st.LWM("metrics") != 1 {
		t.Fatalf("per-signal retention moved the LWM to %d", st.LWM("metrics"))
	}
	if got := scrapeMetric(t, m, `colca_retention_pruned_records_total{stream="metrics"}`); got != 3 {
		t.Fatalf("pruned records counter = %v, want 3", got)
	}
}

// A protecting cursor stops per-signal retention exactly as it stops the
// stream prune: nothing the consumer has not read goes.
func TestSignalRulesStopAtAProtectingCursor(t *testing.T) {
	st, eng := mustParts(t)
	now := time.Now()
	offs := appendSignal(t, st, now, "line1/vibration/x", "SIGVIB", 5*time.Hour, 4*time.Hour, 3*time.Hour, 2*time.Hour, time.Minute)
	if created, err := st.CursorSetIfAbsent("c/historian/metrics", "metrics", offs[2]); err != nil || !created {
		t.Fatalf("cursor: %v", err)
	}
	ret := signalRet(config.SignalRetention{Topics: []string{"colca/v1/_Metric/#"}, MaxAge: config.Duration(time.Hour)})
	p := newPruner(t, st, eng, ret)
	p.now = func() time.Time { return now }
	p.runOnce()

	// Below the cursor at offs[2]: offs[0] goes, offs[1] stands in as the value
	// in force among what may be touched. From the cursor on, everything stays.
	if got := liveOffsets(t, st, offs); len(got) != 4 || got[0] != offs[1] {
		t.Fatalf("keeps %v, want %v", got, offs[1:])
	}
}

// With ignore_cursors_after, a stale cursor no longer protects: per-signal
// retention passes it and leaves a _StreamGap marker naming it.
func TestSignalRulesPassAStaleCursorWithAGapMarker(t *testing.T) {
	st, eng := mustParts(t)
	now := time.Now()
	offs := appendSignal(t, st, now, "line1/vibration/x", "SIGVIB", 5*time.Hour, 4*time.Hour, time.Minute)
	if created, err := st.CursorSetIfAbsent("c/gone", "metrics", 1); err != nil || !created {
		t.Fatalf("cursor: %v", err)
	}
	pol := config.StreamRetention{
		IgnoreCursorsAfter: config.Duration(time.Minute),
		Signals:            []config.SignalRetention{{Topics: []string{"colca/v1/_Metric/#"}, MaxAge: config.Duration(time.Hour)}},
	}
	p := newPruner(t, st, eng, retFor("metrics", pol))
	p.now = func() time.Time { return now.Add(2 * time.Hour) } // the cursor's last advance is two hours old
	head := st.NextOffset("metrics")
	p.runOnce()

	// Two hours on, all three are older than the window: the newest stays as the
	// value in force.
	if got := liveOffsets(t, st, offs); len(got) != 1 || got[0] != offs[2] {
		t.Fatalf("keeps %v, want %v", got, offs[2:])
	}
	recs := readAll(t, st, "metrics", head)
	if len(recs) != 1 || !strings.Contains(recs[0].Topic, "/_StreamGap/") {
		t.Fatalf("want one _StreamGap marker at the head, got %+v", recs)
	}
	var gap gapPayload
	if err := json.Unmarshal(recs[0].Payload, &gap); err != nil {
		t.Fatal(err)
	}
	if gap.FromOffset != offs[0] || gap.ToOffset != offs[1] || len(gap.OverriddenCursors) != 1 || gap.OverriddenCursors[0] != "c/gone" {
		t.Fatalf("marker %+v, want [%d..%d] naming c/gone", gap, offs[0], offs[1])
	}
}

// A stream that grows faster than one capped policy scan per cycle is pruned
// to its policy in one cycle: the pass repeats while it removes records.
func TestCappedPolicyScanRepeatsWithinACycle(t *testing.T) {
	st, eng := mustParts(t)
	base := time.Now().UnixMilli()
	appendAt(t, st, "logs", 50, base, 10)
	ret := retFor("logs", config.StreamRetention{MaxAge: config.Duration(time.Hour)})
	p := newPruner(t, st, eng, ret)
	p.scanCap = 10
	p.now = func() time.Time { return time.UnixMilli(base).Add(2 * time.Hour) }
	p.runOnce()

	if got := st.LWM("logs"); got != 51 {
		t.Fatalf("LWM(logs) = %d after one cycle, want 51 (five capped passes)", got)
	}
}
