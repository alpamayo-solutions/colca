package store

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
)

// recCost is the logical byte cost the b/{stream} accounting must charge for
// one record at a given offset: len(stream key) + len(encoded value).
func recCost(t *testing.T, stream string, off uint64, topic string, payload []byte, ts int64) uint64 {
	t.Helper()
	val, err := json.Marshal(recEnc{topic, payload, ts})
	if err != nil {
		t.Fatal(err)
	}
	return uint64(len(streamKey(stream, off)) + len(val))
}

func appendMetrics(t *testing.T, s *Store, n int, tsBase int64) {
	t.Helper()
	var recs []Record
	for i := 0; i < n; i++ {
		recs = append(recs, Record{
			Topic:   fmt.Sprintf("colca/v1/_Metric/m1/m1/t%d", i),
			Payload: []byte(fmt.Sprintf(`{"v":%d}`, i)),
			TS:      tsBase + int64(i),
		})
	}
	if _, _, err := s.Append("metrics", recs); err != nil {
		t.Fatal(err)
	}
}

// Prune removes exactly the contiguous prefix [LWM..upTo) and nothing else:
// records at/after upTo keep their offsets and payloads, KV projections
// (including ones whose producing record is pruned), cursors and HWMs are
// untouched, and NextOffset is unaffected (gapless contract).
func TestPruneDeletesExactlyThePrefix(t *testing.T) {
	s := mustOpen(t)
	recs := []Record{
		{Topic: "colca/v1/_Metric/m1/m1/a", Payload: []byte(`{"v":1}`), TS: 100, KVPath: "m1/a", KVNode: "m1"},
		{Topic: "colca/v1/_Metric/m1/m1/b", Payload: []byte(`{"v":2}`), TS: 101},
		{Topic: "colca/v1/_Metric/m1/m1/c", Payload: []byte(`{"v":3}`), TS: 102},
		{Topic: "colca/v1/_Metric/m1/m1/d", Payload: []byte(`{"v":4}`), TS: 103},
		{Topic: "colca/v1/_Metric/m1/m1/e", Payload: []byte(`{"v":5}`), TS: 104, KVPath: "m1/e", KVNode: "m1"},
	}
	if _, _, err := s.Append("metrics", recs); err != nil {
		t.Fatal(err)
	}
	if !s.CursorAck("hub", "metrics", 2) {
		t.Fatal("cursor ack should move")
	}
	if _, _, err := s.ApplyReplicated("n-child", "entities", []ReplRecord{
		{ChildOffset: 4, Topic: "colca/v1/_Entity/m1/x", Payload: []byte("1"), TS: 1},
	}); err != nil {
		t.Fatal(err)
	}

	pruned, err := s.Prune("metrics", 4, nil)
	if err != nil {
		t.Fatal(err)
	}
	if pruned != 3 {
		t.Fatalf("pruned = %d, want 3", pruned)
	}
	if got := s.LWM("metrics"); got != 4 {
		t.Fatalf("LWM = %d, want 4", got)
	}
	// NextOffset unaffected — offsets stay gapless across a prune.
	if got := s.NextOffset("metrics"); got != 6 {
		t.Fatalf("NextOffset = %d, want 6", got)
	}

	// Read from 1 (below LWM) must not error and starts at the LWM.
	got, next, err := s.Read("metrics", 1, 100, nil)
	if err != nil {
		t.Fatalf("read from below LWM must not error: %v", err)
	}
	if len(got) != 2 || got[0].Offset != 4 || got[1].Offset != 5 || next != 6 {
		t.Fatalf("surviving records wrong: %+v next=%d", got, next)
	}
	if got[0].Topic != recs[3].Topic || string(got[1].Payload) != `{"v":5}` {
		t.Fatalf("surviving record content mangled: %+v", got)
	}
	// Read from inside the pruned range behaves the same.
	got, _, err = s.Read("metrics", 2, 100, nil)
	if err != nil || len(got) != 2 || got[0].Offset != 4 {
		t.Fatalf("read from inside pruned range: %+v err=%v", got, err)
	}

	// KV projection untouched — including m1/a, whose producing record
	// (offset 1) is pruned: the entry is current state, its Offset field is
	// provenance and may point below the LWM.
	kv := map[string]KVEntry{}
	for _, e := range s.KVScan("") {
		kv[e.Path] = e
	}
	if len(kv) != 2 {
		t.Fatalf("KV entries = %d, want 2 (%v)", len(kv), kv)
	}
	if e := kv["m1/a"]; e.Offset != 1 || string(e.Payload) != `{"v":1}` {
		t.Fatalf("KV entry below LWM must survive prune: %+v", e)
	}

	// Cursors and HWMs untouched.
	if got := s.CursorGet("hub", "metrics"); got != 2 {
		t.Fatalf("cursor moved by prune: %d", got)
	}
	if got := s.HWMGet("n-child", "entities"); got != 4 {
		t.Fatalf("HWM moved by prune: %d", got)
	}

	// Journal records the pruned span and its time range.
	j := s.PruneJournal("metrics")
	if len(j) != 1 {
		t.Fatalf("journal = %+v, want one span", j)
	}
	want := PruneSpan{From: 1, To: 3, FirstTS: 100, LastTS: 102}
	if j[0] != want {
		t.Fatalf("journal span = %+v, want %+v", j[0], want)
	}
}

func TestPruneNoopAndErrorEdges(t *testing.T) {
	s := mustOpen(t)
	appendMetrics(t, s, 5, 100)

	if _, err := s.Prune("nope", 2, nil); err == nil {
		t.Fatal("unknown stream must error")
	}
	if _, err := s.Prune("metrics", 7, nil); err == nil {
		t.Fatal("pruning beyond NextOffset must error")
	}
	if pruned, err := s.Prune("metrics", 1, nil); pruned != 0 || err != nil {
		t.Fatalf("upTo == LWM must be a no-op, got %d %v", pruned, err)
	}
	if pruned, err := s.Prune("metrics", 3, nil); pruned != 2 || err != nil {
		t.Fatalf("prune to 3: %d %v", pruned, err)
	}
	if pruned, err := s.Prune("metrics", 2, nil); pruned != 0 || err != nil {
		t.Fatalf("upTo below LWM must be a no-op, got %d %v", pruned, err)
	}
	// no-op runs write no journal entries
	if j := s.PruneJournal("metrics"); len(j) != 1 {
		t.Fatalf("no-op prune must not journal: %+v", j)
	}

	// Pruning everything (upTo == NextOffset) empties the stream but keeps
	// the offset sequence: the next append continues where it left off.
	if pruned, err := s.Prune("metrics", 6, nil); pruned != 3 || err != nil {
		t.Fatalf("prune all: %d %v", pruned, err)
	}
	if got, _, err := s.Read("metrics", 1, 100, nil); err != nil || len(got) != 0 {
		t.Fatalf("stream should be empty: %+v %v", got, err)
	}
	first, last, err := s.Append("metrics", []Record{{Topic: "t", Payload: []byte("1"), TS: 1}})
	if err != nil || first != 6 || last != 6 {
		t.Fatalf("append after full prune: %d..%d %v (gapless contract broken)", first, last, err)
	}
}

// LWM, byte counter and journal are restored on Open exactly like m/ meta.
func TestRetentionStateSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	appendMetrics(t, s, 8, 100)
	if _, err := s.Prune("metrics", 5, nil); err != nil {
		t.Fatal(err)
	}
	wantLWM, wantBytes, wantJournal := s.LWM("metrics"), s.StreamBytes("metrics"), s.PruneJournal("metrics")
	s.Close()

	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if got := s2.LWM("metrics"); got != wantLWM {
		t.Fatalf("LWM after reopen = %d, want %d", got, wantLWM)
	}
	if got := s2.StreamBytes("metrics"); got != wantBytes {
		t.Fatalf("StreamBytes after reopen = %d, want %d", got, wantBytes)
	}
	if got := s2.PruneJournal("metrics"); len(got) != len(wantJournal) || got[0] != wantJournal[0] {
		t.Fatalf("journal after reopen = %+v, want %+v", got, wantJournal)
	}
	// The pruned prefix stays pruned, the survivors stay readable.
	got, _, err := s2.Read("metrics", 1, 100, nil)
	if err != nil || len(got) != 4 || got[0].Offset != 5 {
		t.Fatalf("records after reopen: %+v %v", got, err)
	}
}

// A corrupt l/ or b/ value must fail Open loudly — a corrupt LWM silently
// read as 1 would resurrect the pruned range as a phantom gap (spec §4.2).
func TestCorruptRetentionCountersFailOpen(t *testing.T) {
	for _, tc := range []struct {
		name string
		key  []byte
	}{
		{"lwm", lwmKey("metrics")},
		{"bytes", bytesKey("metrics")},
		{"journal", journalKey("metrics", 1)}, // "bad" is not valid journal JSON
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			s, err := Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			appendMetrics(t, s, 2, 100)
			if err := s.db.Set(tc.key, []byte("bad"), nil); err != nil {
				t.Fatal(err)
			}
			s.Close()
			if _, err := Open(dir); err == nil || !strings.Contains(err.Error(), "corrupt") {
				t.Fatalf("Open must fail loudly on corrupt %s: %v", tc.name, err)
			}
		})
	}
}

// b/{stream} tracks exactly len(stream key)+len(encoded value) per live
// record across Append, ApplyReplicated and Prune. KV projection keys are
// current state, not stream history, and are not counted.
func TestStreamBytesAccounting(t *testing.T) {
	s := mustOpen(t)
	if got := s.StreamBytes("metrics"); got != 0 {
		t.Fatalf("fresh stream bytes = %d", got)
	}

	recs := []Record{
		{Topic: "colca/v1/_Metric/m1/m1/a", Payload: []byte(`{"v":1}`), TS: 100, KVPath: "m1/a", KVNode: "m1"},
		{Topic: "colca/v1/_Metric/m1/m1/bb", Payload: []byte(`{"v":22}`), TS: 101},
		{Topic: "colca/v1/_Metric/m1/m1/ccc", Payload: []byte(`{"v":333}`), TS: 102},
	}
	if _, _, err := s.Append("metrics", recs); err != nil {
		t.Fatal(err)
	}
	var want uint64
	for i, r := range recs {
		want += recCost(t, "metrics", uint64(i+1), r.Topic, r.Payload, r.TS)
	}
	if got := s.StreamBytes("metrics"); got != want {
		t.Fatalf("after append: bytes = %d, want %d", got, want)
	}

	// ApplyReplicated counts only the records it actually writes (dedupe).
	repl := []ReplRecord{
		{ChildOffset: 1, Topic: "colca/v1/_Metric/m1/edge/x", Payload: []byte("1"), TS: 200},
		{ChildOffset: 2, Topic: "colca/v1/_Metric/m1/edge/y", Payload: []byte("2"), TS: 201},
	}
	if _, _, err := s.ApplyReplicated("n-edge", "metrics", repl); err != nil {
		t.Fatal(err)
	}
	for i, r := range repl {
		want += recCost(t, "metrics", uint64(4+i), r.Topic, r.Payload, r.TS)
	}
	if got := s.StreamBytes("metrics"); got != want {
		t.Fatalf("after replicate: bytes = %d, want %d", got, want)
	}
	if _, _, err := s.ApplyReplicated("n-edge", "metrics", repl); err != nil {
		t.Fatal(err)
	}
	if got := s.StreamBytes("metrics"); got != want {
		t.Fatalf("deduped replicate must not change bytes: %d, want %d", got, want)
	}

	// Prune decrements by exactly the pruned records' cost.
	if _, err := s.Prune("metrics", 3, nil); err != nil {
		t.Fatal(err)
	}
	want -= recCost(t, "metrics", 1, recs[0].Topic, recs[0].Payload, recs[0].TS)
	want -= recCost(t, "metrics", 2, recs[1].Topic, recs[1].Payload, recs[1].TS)
	if got := s.StreamBytes("metrics"); got != want {
		t.Fatalf("after prune: bytes = %d, want %d", got, want)
	}
}

// gapRecords ride in the prune batch as ordinary records with KV semantics:
// appended at the head (they survive their own prune run), counted in b/,
// meta advanced and persisted.
func TestPruneAppendsGapRecords(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	appendMetrics(t, s, 4, 100)
	gap := Record{
		Topic:   "colca/v1/_StreamGap/n1/metrics",
		Payload: []byte(`{"from":1,"to":3}`),
		TS:      500,
		KVPath:  "gap/metrics", KVNode: "n1",
	}
	bytesBefore := s.StreamBytes("metrics")
	pruned, err := s.Prune("metrics", 4, []Record{gap})
	if err != nil {
		t.Fatal(err)
	}
	if pruned != 3 {
		t.Fatalf("pruned = %d, want 3 (gap records are appended, not pruned)", pruned)
	}
	// The gap record landed at the head (old next = 5) and advanced meta.
	if got := s.NextOffset("metrics"); got != 6 {
		t.Fatalf("NextOffset = %d, want 6", got)
	}
	got, _, err := s.Read("metrics", 5, 10, nil)
	if err != nil || len(got) != 1 || got[0].Topic != gap.Topic || string(got[0].Payload) != string(gap.Payload) {
		t.Fatalf("gap record not readable at head: %+v %v", got, err)
	}
	if got[0].Offset < s.LWM("metrics") {
		t.Fatal("gap record must sit at/after the LWM so it survives its own prune run")
	}
	// KV semantics apply.
	kv := s.KVScan("gap/")
	if len(kv) != 1 || kv[0].Offset != 5 {
		t.Fatalf("gap record KV projection: %+v", kv)
	}
	// Byte accounting: minus 3 pruned, plus the gap record.
	want := bytesBefore
	for off := uint64(1); off <= 3; off++ {
		r := fmt.Sprintf("colca/v1/_Metric/m1/m1/t%d", off-1)
		want -= recCost(t, "metrics", off, r, []byte(fmt.Sprintf(`{"v":%d}`, off-1)), 100+int64(off-1))
	}
	want += recCost(t, "metrics", 5, gap.Topic, gap.Payload, gap.TS)
	if got := s.StreamBytes("metrics"); got != want {
		t.Fatalf("bytes = %d, want %d", got, want)
	}

	// meta advance is durable: reopen continues after the gap record.
	s.Close()
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if got := s2.NextOffset("metrics"); got != 6 {
		t.Fatalf("NextOffset after reopen = %d, want 6", got)
	}
}

// The journal is bounded at journalCap entries by coalescing the two oldest
// (union range, min/max ts); coverage of [1..LWM) stays a contiguous,
// complete partition.
func TestPruneJournalCoalescesAtCap(t *testing.T) {
	s := mustOpen(t)
	const runs = journalCap + 6
	appendMetrics(t, s, runs, 100) // ts of offset o = 100 + (o-1)
	for i := 1; i <= runs; i++ {
		if pruned, err := s.Prune("metrics", uint64(i+1), nil); pruned != 1 || err != nil {
			t.Fatalf("run %d: pruned=%d err=%v", i, pruned, err)
		}
	}
	j := s.PruneJournal("metrics")
	if len(j) != journalCap {
		t.Fatalf("journal length = %d, want %d", len(j), journalCap)
	}
	// Head absorbed the 6 overflow runs: [1..7] with the union time span,
	// marked Coalesced (merging an already-coalesced head stays true) so the
	// API layer can report the gap's first_ts as approximate.
	head := PruneSpan{From: 1, To: 7, FirstTS: 100, LastTS: 106, Coalesced: true}
	if j[0] != head {
		t.Fatalf("coalesced head = %+v, want %+v", j[0], head)
	}
	if j[1].Coalesced || j[len(j)-1].Coalesced {
		t.Fatalf("never-merged entries must not be marked coalesced: %+v, %+v", j[1], j[len(j)-1])
	}
	// Contiguous partition of [1..LWM).
	if j[0].From != 1 {
		t.Fatalf("journal must start at 1: %+v", j[0])
	}
	for i := 1; i < len(j); i++ {
		if j[i].From != j[i-1].To+1 {
			t.Fatalf("journal not contiguous at %d: %+v -> %+v", i, j[i-1], j[i])
		}
	}
	if last := j[len(j)-1]; last.To != s.LWM("metrics")-1 {
		t.Fatalf("journal must end at LWM-1: %+v, LWM %d", last, s.LWM("metrics"))
	}
}

// Prune interleaved with concurrent Append leaves a fully consistent store
// (run under -race): the surviving records are exactly [LWM..NextOffset),
// gapless, and b/ equals the recomputed cost of exactly those records. This
// pins the one-synced-batch discipline structurally — a torn prune (delete
// without counter/LWM update or vice versa) breaks one of these equalities.
func TestPruneConcurrentWithAppendStaysConsistent(t *testing.T) {
	s := mustOpen(t)
	const total = 400
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < total; i++ {
			if _, _, err := s.Append("metrics", []Record{{
				Topic:   fmt.Sprintf("colca/v1/_Metric/m1/m1/c%d", i),
				Payload: []byte(`{"v":1}`),
				TS:      int64(i),
			}}); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	for {
		select {
		case <-done:
			goto drained
		default:
			if _, err := s.Prune("metrics", s.NextOffset("metrics"), nil); err != nil {
				t.Fatal(err)
			}
		}
	}
drained:
	if _, err := s.Prune("metrics", s.NextOffset("metrics")-1, nil); err != nil {
		t.Fatal(err)
	}

	lwm, next := s.LWM("metrics"), s.NextOffset("metrics")
	if next != total+1 {
		t.Fatalf("NextOffset = %d, want %d (prune must never eat offsets)", next, total+1)
	}
	got, _, err := s.Read("metrics", 1, total+1, nil)
	if err != nil {
		t.Fatal(err)
	}
	if uint64(len(got)) != next-lwm {
		t.Fatalf("live records = %d, want %d (LWM %d, next %d)", len(got), next-lwm, lwm, next)
	}
	var wantBytes uint64
	for i, r := range got {
		if want := lwm + uint64(i); r.Offset != want {
			t.Fatalf("offset gap in survivors: got %d, want %d", r.Offset, want)
		}
		wantBytes += recCost(t, "metrics", r.Offset, r.Topic, r.Payload, r.TS)
	}
	if gotBytes := s.StreamBytes("metrics"); gotBytes != wantBytes {
		t.Fatalf("StreamBytes = %d, recomputed live cost = %d", gotBytes, wantBytes)
	}
	// Journal still partitions [1..LWM) contiguously.
	j := s.PruneJournal("metrics")
	if len(j) == 0 || j[0].From != 1 || j[len(j)-1].To != lwm-1 {
		t.Fatalf("journal does not cover [1..%d): %+v", lwm, j)
	}
	for i := 1; i < len(j); i++ {
		if j[i].From != j[i-1].To+1 {
			t.Fatalf("journal not contiguous: %+v -> %+v", j[i-1], j[i])
		}
	}
}

// Spec §2/§5.2: every CursorAck writes the ct/ last-advance timestamp in the
// same synced write as the cursor, and both survive restart — the staleness
// clock must not rewind when the process restarts.
func TestCursorLastAdvanceStampedAndSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	appendMetrics(t, s, 5, 100)
	before := time.Now().UnixMilli()
	if !s.CursorAck("uplink", "metrics", 4) {
		t.Fatal("ack must move")
	}
	after := time.Now().UnixMilli()

	cs := s.Cursors()
	if len(cs) != 1 || cs[0].Name != "uplink" || cs[0].Stream != "metrics" {
		// Exactly one clean cursor also pins the scan bounds: ct/ keys share
		// the first byte with c/ keys and must never surface as a bogus
		// cursor with an empty name.
		t.Fatalf("Cursors() = %+v, want exactly [uplink/metrics]", cs)
	}
	stamped := cs[0].LastAdvanceMS
	if stamped < before || stamped > after {
		t.Fatalf("LastAdvanceMS = %d, want within [%d..%d]", stamped, before, after)
	}
	// A non-advancing ack (same offset) must NOT refresh the timestamp: the
	// clock tracks advances, not polls.
	if s.CursorAck("uplink", "metrics", 4) {
		t.Fatal("non-advancing ack must report false")
	}
	if got := s.Cursors()[0].LastAdvanceMS; got != stamped {
		t.Fatalf("non-advancing ack refreshed LastAdvanceMS: %d, want %d", got, stamped)
	}
	s.Close()

	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if got := s2.Cursors()[0].LastAdvanceMS; got != stamped {
		t.Fatalf("LastAdvanceMS after reopen = %d, want %d", got, stamped)
	}
}

// Spec §5.2 upgrade case: a cursor key that predates the ct/ timestamps reads
// LastAdvanceMS 0, and CursorMarkSeen stamps it exactly once (first sighting
// counts as an advance; later sightings must not keep resetting the clock).
func TestCursorMarkSeenStampsLegacyCursorOnce(t *testing.T) {
	s := mustOpen(t)
	appendMetrics(t, s, 5, 100)
	// A pre-ct cursor: the raw c/ key without its ct/ companion — exactly
	// what a store written by an older build contains.
	if err := s.db.Set(cursorKey("legacy", "metrics"), be64(3), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	cs := s.Cursors()
	if len(cs) != 1 || cs[0].LastAdvanceMS != 0 {
		t.Fatalf("legacy cursor = %+v, want LastAdvanceMS 0", cs)
	}

	s.CursorMarkSeen("legacy", "metrics", 42_000)
	if got := s.Cursors()[0].LastAdvanceMS; got != 42_000 {
		t.Fatalf("LastAdvanceMS after first sighting = %d, want 42000", got)
	}
	// Second sighting: no-op — the staleness clock keeps running.
	s.CursorMarkSeen("legacy", "metrics", 99_000)
	if got := s.Cursors()[0].LastAdvanceMS; got != 42_000 {
		t.Fatalf("second CursorMarkSeen moved the clock: %d, want 42000", got)
	}
	// A real advance DOES refresh it.
	if !s.CursorAck("legacy", "metrics", 5) {
		t.Fatal("ack must move")
	}
	if got := s.Cursors()[0].LastAdvanceMS; got <= 42_000 {
		t.Fatalf("advance did not refresh LastAdvanceMS: %d", got)
	}
}

// ScanRecords reports each record's offset, timestamp and the exact logical
// byte cost the b/ accounting charges, honors [from, upTo) and the early-exit
// return.
func TestScanRecordsBoundsAndSizes(t *testing.T) {
	s := mustOpen(t)
	appendMetrics(t, s, 6, 100)

	type seen struct {
		off  uint64
		ts   int64
		size uint64
	}
	var got []seen
	if err := s.ScanRecords("metrics", 2, 5, func(off uint64, ts int64, size uint64) bool {
		got = append(got, seen{off, ts, size})
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[0].off != 2 || got[2].off != 4 {
		t.Fatalf("scan [2,5) = %+v, want offsets 2..4", got)
	}
	recs, _, err := s.Read("metrics", 2, 3, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i, r := range recs {
		if want := recCost(t, "metrics", r.Offset, r.Topic, r.Payload, r.TS); got[i].size != want || got[i].ts != r.TS {
			t.Fatalf("record %d: size/ts = %d/%d, want %d/%d", r.Offset, got[i].size, got[i].ts, want, r.TS)
		}
	}
	// Early exit: fn returning false stops the scan.
	calls := 0
	if err := s.ScanRecords("metrics", 1, 7, func(uint64, int64, uint64) bool {
		calls++
		return false
	}); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("early exit ignored: fn called %d times, want 1", calls)
	}
}
