package store

import (
	"context"
	"strings"
	"testing"
	"time"
)

// signalRuleFor gives the listed signals a window and leaves the rest to the
// stream policy.
func signalRuleFor(windows map[string]time.Duration) SignalRule {
	return func(signalID, _ string) (time.Duration, bool) {
		d, ok := windows[signalID]
		return d, ok
	}
}

// signalOffsets lists the offsets of one signal's live records.
func signalOffsets(t *testing.T, s *Store, signalID string) []uint64 {
	t.Helper()
	recs, _, err := s.ReadSignals(context.Background(), "metrics", s.LWM("metrics"), 10_000, 0, []string{signalID}, nil)
	if err != nil {
		t.Fatal(err)
	}
	var out []uint64
	for _, r := range recs {
		out = append(out, r.Offset)
	}
	return out
}

func equalOffsets(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// appendSignalHistory writes, for each minute offset in ago, one record of
// every signal in turn, oldest first, and returns the offsets per signal.
func appendSignalHistory(t *testing.T, s *Store, now time.Time, signals []string, ago []time.Duration) map[string][]uint64 {
	t.Helper()
	var recs []Record
	at := map[string][]uint64{}
	next := s.NextOffset("metrics")
	for _, a := range ago {
		for _, sig := range signals {
			recs = append(recs, metricRecord(sig, now.Add(-a).UnixMilli()))
			at[sig] = append(at[sig], next)
			next++
		}
	}
	if _, _, err := s.Append("metrics", recs); err != nil {
		t.Fatal(err)
	}
	return at
}

// A rule removes a signal's records older than its window but keeps the newest
// of them, the value in force when the window opens. Other signals, the LWM and
// the journal stay as they were; the byte counter drops by exactly the records
// removed, and their index entries go with them.
func TestPruneSignalsKeepsTheWindowAndTheValueInForce(t *testing.T) {
	s := mustOpen(t)
	now := time.Now()
	ago := []time.Duration{3 * time.Hour, 2 * time.Hour, 90 * time.Minute, 30 * time.Minute, 10 * time.Minute}
	at := appendSignalHistory(t, s, now, []string{"fast", "kept"}, ago)
	// "quiet" stopped changing long ago: only its last value may survive.
	quiet := appendSignalHistory(t, s, now, []string{"quiet"}, []time.Duration{5 * time.Hour, 4 * time.Hour, 3 * time.Hour})["quiet"]
	bytesBefore := s.StreamBytes("metrics")

	var wantShed uint64
	for _, off := range []uint64{at["fast"][0], at["fast"][1], quiet[0], quiet[1]} {
		rec, ok, err := s.readRecordMeta("metrics", off)
		if err != nil || !ok {
			t.Fatalf("read %d: %v %v", off, ok, err)
		}
		wantShed += rec.size
	}

	rule := signalRuleFor(map[string]time.Duration{"fast": time.Hour, "quiet": time.Hour})
	res, err := s.PruneSignals("metrics", "", now, rule, s.NextOffset("metrics"), nil, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.Removed != 4 || res.Bytes != wantShed || res.Resume != "" || res.Signals != 3 {
		t.Fatalf("result %+v, want 4 removed, %d bytes, no resume, 3 signals", res, wantShed)
	}
	if got := signalOffsets(t, s, "fast"); !equalOffsets(got, at["fast"][2:]) {
		t.Fatalf("fast keeps %v, want %v (90m value in force, then the window)", got, at["fast"][2:])
	}
	if got := signalOffsets(t, s, "kept"); !equalOffsets(got, at["kept"]) {
		t.Fatalf("a signal without a rule lost records: %v, want %v", got, at["kept"])
	}
	if got := signalOffsets(t, s, "quiet"); !equalOffsets(got, quiet[2:]) {
		t.Fatalf("a signal that stopped changing keeps %v, want its last value %v", got, quiet[2:])
	}
	if got := s.StreamBytes("metrics"); got != bytesBefore-wantShed {
		t.Fatalf("StreamBytes = %d, want %d", got, bytesBefore-wantShed)
	}
	if s.LWM("metrics") != 1 || len(s.PruneJournal("metrics")) != 0 {
		t.Fatalf("per-signal pruning moved the LWM (%d) or wrote the journal", s.LWM("metrics"))
	}
	if n := countIndexEntries(t, s, "metrics"); n["fast"] != 3 || n["quiet"] != 1 || n["kept"] != 5 {
		t.Fatalf("index entries %v, want fast 3, quiet 1, kept 5", n)
	}

	// A second pass at the same time has nothing left to do.
	res, err = s.PruneSignals("metrics", "", now, rule, s.NextOffset("metrics"), nil, 0, nil)
	if err != nil || res.Removed != 0 {
		t.Fatalf("second pass removed %d (err %v), want 0", res.Removed, err)
	}
}

// Nothing at or above the clamp goes, and a cursor that is not overridden
// shrinks the delete under the mutex even when the caller's clamp was higher.
func TestPruneSignalsHonoursTheCursorFloor(t *testing.T) {
	s := mustOpen(t)
	now := time.Now()
	ago := []time.Duration{6 * time.Hour, 5 * time.Hour, 4 * time.Hour, 3 * time.Hour, 2 * time.Hour}
	at := appendSignalHistory(t, s, now, []string{"a"}, ago)["a"]
	rule := signalRuleFor(map[string]time.Duration{"a": time.Hour})

	// The caller's clamp sits at the third record: only the first may go, since
	// the second is the newest record below the clamp and stands in for the value
	// in force.
	res, err := s.PruneSignals("metrics", "", now, rule, at[2], nil, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.Removed != 1 {
		t.Fatalf("removed %d below clamp %d, want 1", res.Removed, at[2])
	}

	// A consumer acks at the third record after the caller took the floor at the
	// head: the recheck keeps everything from there.
	if !s.CursorAck("c/consumer", "metrics", at[2]) {
		t.Fatal("ack refused")
	}
	res, err = s.PruneSignals("metrics", "", now, rule, s.NextOffset("metrics"), nil, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.Removed != 1 {
		t.Fatalf("removed %d, want 1 (only the record below the cursor)", res.Removed)
	}
	if got := signalOffsets(t, s, "a"); !equalOffsets(got, at[2:]) {
		t.Fatalf("a keeps %v, want %v", got, at[2:])
	}
}

// A stale cursor the caller overrides is passed, and the batch that passes it
// carries the marker the callback builds, appended at the head.
func TestPruneSignalsPassesAnOverriddenCursorWithAMarker(t *testing.T) {
	s := mustOpen(t)
	now := time.Now()
	at := appendSignalHistory(t, s, now, []string{"a"}, []time.Duration{3 * time.Hour, 2 * time.Hour, time.Minute})["a"]
	if created, err := s.CursorSetIfAbsent("c/dead", "metrics", 1); err != nil || !created {
		t.Fatalf("cursor not created: %v", err)
	}
	head := s.NextOffset("metrics")
	var gotSpan PruneSpan
	var gotPassed []string
	marker := func(span PruneSpan, passed []string) []Record {
		gotSpan, gotPassed = span, passed
		return []Record{{Topic: "colca/v1/_StreamGap/n/metrics", Payload: []byte(`{}`), TS: now.UnixMilli()}}
	}
	rule := signalRuleFor(map[string]time.Duration{"a": time.Hour})
	res, err := s.PruneSignals("metrics", "", now, rule, head, map[string]uint64{"c/dead": 1}, 0, marker)
	if err != nil {
		t.Fatal(err)
	}
	if res.Removed != 1 || len(res.Overridden) != 1 || res.Overridden[0] != "c/dead" {
		t.Fatalf("result %+v, want one record removed past c/dead", res)
	}
	if gotSpan.From != at[0] || gotSpan.To != at[0] || len(gotPassed) != 1 {
		t.Fatalf("marker span %+v passed %v, want [%d..%d] past c/dead", gotSpan, gotPassed, at[0], at[0])
	}
	recs := readAllFrom(t, s, head)
	if len(recs) != 1 || !strings.Contains(recs[0].Topic, "_StreamGap") {
		t.Fatalf("want the marker at the head, got %+v", recs)
	}
}

// A visit budget stops at a signal boundary and names where to resume; resumed
// passes end where one unbounded pass would.
func TestPruneSignalsResumesAcrossCalls(t *testing.T) {
	s := mustOpen(t)
	now := time.Now()
	signals := []string{"s1", "s2", "s3", "s4", "s5"}
	appendSignalHistory(t, s, now, signals, []time.Duration{4 * time.Hour, 3 * time.Hour, 2 * time.Hour, time.Minute})
	rule := signalRuleFor(map[string]time.Duration{"s1": time.Hour, "s2": time.Hour, "s3": time.Hour, "s4": time.Hour, "s5": time.Hour})

	from, calls := "", 0
	var removed uint64
	for {
		res, err := s.PruneSignals("metrics", from, now, rule, s.NextOffset("metrics"), nil, 4, nil)
		if err != nil {
			t.Fatal(err)
		}
		removed += res.Removed
		calls++
		if res.Resume == "" {
			break
		}
		if res.Resume <= from {
			t.Fatalf("resume %q does not move past %q", res.Resume, from)
		}
		from = res.Resume
	}
	if calls < 2 || removed != 10 {
		t.Fatalf("%d calls removed %d, want several calls removing 2 per signal (10)", calls, removed)
	}
	for _, sig := range signals {
		if n := len(signalOffsets(t, s, sig)); n != 2 {
			t.Fatalf("%s keeps %d records, want 2 (value in force + window)", sig, n)
		}
	}
}

// The deletes and the byte counter are durable: a reopened store reads the
// same records and the same count.
func TestPruneSignalsSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	appendSignalHistory(t, s, now, []string{"a", "b"}, []time.Duration{3 * time.Hour, 2 * time.Hour, time.Minute})
	rule := signalRuleFor(map[string]time.Duration{"a": time.Hour})
	if _, err := s.PruneSignals("metrics", "", now, rule, s.NextOffset("metrics"), nil, 0, nil); err != nil {
		t.Fatal(err)
	}
	wantBytes, wantA := s.StreamBytes("metrics"), signalOffsets(t, s, "a")
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if got := s.StreamBytes("metrics"); got != wantBytes {
		t.Fatalf("StreamBytes after reopen = %d, want %d", got, wantBytes)
	}
	if got := signalOffsets(t, s, "a"); !equalOffsets(got, wantA) {
		t.Fatalf("a after reopen = %v, want %v", got, wantA)
	}
}

func readAllFrom(t *testing.T, s *Store, from uint64) []StoredRecord {
	t.Helper()
	recs, _, err := s.ReadRecords("metrics", from, 1000, nil)
	if err != nil {
		t.Fatal(err)
	}
	return recs
}

// One signal with a long expired run is worked through in several bounded
// calls that resume at the same signal, and ends with the value in force.
func TestPruneSignalsWorksThroughALongSignalInBoundedCalls(t *testing.T) {
	s := mustOpen(t)
	now := time.Now()
	var ago []time.Duration
	for i := 40; i > 0; i-- {
		ago = append(ago, time.Duration(i)*time.Hour)
	}
	ago = append(ago, time.Minute)
	at := appendSignalHistory(t, s, now, []string{"long"}, ago)["long"]
	rule := signalRuleFor(map[string]time.Duration{"long": 30 * time.Minute})

	from, calls := "", 0
	for {
		res, err := s.PruneSignals("metrics", from, now, rule, s.NextOffset("metrics"), nil, 5, nil)
		if err != nil {
			t.Fatal(err)
		}
		calls++
		if res.Resume == "" {
			break
		}
		if calls > 50 {
			t.Fatal("no progress across bounded calls")
		}
		from = res.Resume
	}
	if got := signalOffsets(t, s, "long"); !equalOffsets(got, at[len(at)-2:]) {
		t.Fatalf("long keeps %v after %d calls, want %v", got, calls, at[len(at)-2:])
	}
}

// A cursor the caller overrode at one position protects again once it has
// moved: it acked during the pass, so it is alive.
func TestPruneSignalsStopsOverridingACursorThatMoved(t *testing.T) {
	s := mustOpen(t)
	now := time.Now()
	at := appendSignalHistory(t, s, now, []string{"a"}, []time.Duration{5 * time.Hour, 4 * time.Hour, 3 * time.Hour, time.Minute})["a"]
	if created, err := s.CursorSetIfAbsent("c/slow", "metrics", 1); err != nil || !created {
		t.Fatalf("cursor: %v", err)
	}
	// The caller saw c/slow stale at 1; it acks to the second record meanwhile.
	if !s.CursorAck("c/slow", "metrics", at[1]) {
		t.Fatal("ack refused")
	}
	called := false
	marker := func(PruneSpan, []string) []Record { called = true; return nil }
	rule := signalRuleFor(map[string]time.Duration{"a": time.Hour})
	res, err := s.PruneSignals("metrics", "", now, rule, s.NextOffset("metrics"), map[string]uint64{"c/slow": 1}, 0, marker)
	if err != nil {
		t.Fatal(err)
	}
	if res.Removed != 1 || len(res.Overridden) != 0 || called {
		t.Fatalf("result %+v marker %v, want only the record below c/slow removed and no override", res, called)
	}
	if got := signalOffsets(t, s, "a"); !equalOffsets(got, at[1:]) {
		t.Fatalf("a keeps %v, want %v", got, at[1:])
	}
}

// The value in force is the record with the latest timestamp before the
// cutoff, not the one appended last.
func TestPruneSignalsKeepsTheLatestTimestampAsTheValueInForce(t *testing.T) {
	s := mustOpen(t)
	now := time.Now()
	// Appended in this order: 4h, 2h, then a late 3h sample, then a fresh one.
	at := appendSignalHistory(t, s, now, []string{"a"}, []time.Duration{4 * time.Hour, 2 * time.Hour, 3 * time.Hour, time.Minute})["a"]
	rule := signalRuleFor(map[string]time.Duration{"a": time.Hour})
	if _, err := s.PruneSignals("metrics", "", now, rule, s.NextOffset("metrics"), nil, 0, nil); err != nil {
		t.Fatal(err)
	}
	if got, want := signalOffsets(t, s, "a"), []uint64{at[1], at[3]}; !equalOffsets(got, want) {
		t.Fatalf("a keeps %v, want %v (the 2h sample stays in force, the late 3h one goes)", got, want)
	}
}
