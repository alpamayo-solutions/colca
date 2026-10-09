package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
)

func metricRecord(signalID string, ts int64) Record {
	return Record{
		Topic:   "colca/v1/_Metric/n/" + signalID,
		Payload: []byte(fmt.Sprintf(`{"signal_id":%q,"value":%d}`, signalID, ts)),
		TS:      ts,
	}
}

// appendSparse writes `others` records of a busy signal with a and b at the
// given positions among them, and returns the offsets a and b landed at.
func appendSparse(t *testing.T, s *Store, others int, at map[int]string) map[uint64]string {
	t.Helper()
	var recs []Record
	want := map[uint64]string{}
	for i := 0; i < others; i++ {
		if signalID, ok := at[i]; ok {
			recs = append(recs, metricRecord(signalID, int64(i)+1))
			want[uint64(len(recs))] = signalID
		}
		recs = append(recs, metricRecord("busy", int64(i)+1))
	}
	if _, _, err := s.Append("metrics", recs); err != nil {
		t.Fatal(err)
	}
	return want
}

// countIndexEntries counts the signal index entries of one stream.
func countIndexEntries(t *testing.T, s *Store, stream string) map[string]int {
	t.Helper()
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte("si\x00" + stream + "\x00"),
		UpperBound: []byte("si\x00" + stream + "\x01"),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer iter.Close()
	counts := map[string]int{}
	prefix := len("si\x00" + stream + "\x00")
	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		counts[string(key[prefix:len(key)-9])]++
	}
	return counts
}

func TestReadSignalsReturnsOnlyTheWantedSignalsAndItsBudgetCountsOnlyThem(t *testing.T) {
	s := mustOpen(t)
	want := appendSparse(t, s, 3000, map[int]string{10: "a", 1500: "b", 2999: "a"})
	head := s.NextOffset("metrics")

	// A budget of 3 index entries reaches all three, 3000 busy records apart.
	// The same budget on a scan would not leave the first busy records.
	got, next, err := s.ReadSignals(context.Background(), "metrics", 1, 10, 3, []string{"a", "b", "a"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(want) {
		t.Fatalf("got %d records, want %d: %+v", len(got), len(want), got)
	}
	var last uint64
	for _, r := range got {
		if want[r.Offset] != r.SignalID || r.Offset <= last {
			t.Fatalf("record %d carries %q (want %q), or is out of order after %d", r.Offset, r.SignalID, want[r.Offset], last)
		}
		last = r.Offset
	}
	if next != head {
		t.Fatalf("next = %d, want the head %d: no wanted record remains", next, head)
	}

	// A budget smaller than the matches pages through them without skipping one.
	page, next, err := s.ReadSignals(context.Background(), "metrics", 1, 10, 2, []string{"a", "b"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(page) != 2 || next != page[1].Offset+1 {
		t.Fatalf("first page of two: %d records next=%d", len(page), next)
	}
	rest, next, err := s.ReadSignals(context.Background(), "metrics", next, 10, 2, []string{"a", "b"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(rest) != 1 || rest[0].Offset != last || next != head {
		t.Fatalf("second page: %+v next=%d, want the record at %d and the head %d", rest, next, last, head)
	}

	// The limit stops the page at the last record it returns.
	one, next, err := s.ReadSignals(context.Background(), "metrics", 1, 1, 0, []string{"a", "b"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(one) != 1 || next != one[0].Offset+1 {
		t.Fatalf("limit 1: %+v next=%d", one, next)
	}

	// The filter still decides on what the index yields.
	none, next, err := s.ReadSignals(context.Background(), "metrics", 1, 10, 0, []string{"a", "b"}, func(r StoredRecord) bool { return r.SignalID == "b" })
	if err != nil {
		t.Fatal(err)
	}
	if len(none) != 1 || none[0].SignalID != "b" || next != head {
		t.Fatalf("filtered to b: %+v next=%d", none, next)
	}
}

func TestPruneCompactionAndEvictionTakeTheirIndexEntriesWithThem(t *testing.T) {
	s := mustOpen(t)
	appendSparse(t, s, 10, map[int]string{2: "a", 8: "b"})
	if before := countIndexEntries(t, s, "metrics"); before["busy"] != 10 || before["a"] != 1 || before["b"] != 1 {
		t.Fatalf("index before = %v, want 10 busy, 1 a, 1 b", before)
	}

	// Offsets 1..6 are busy, busy, a, busy, busy, busy.
	if _, err := s.Prune("metrics", 7, nil, nil); err != nil {
		t.Fatal(err)
	}
	after := countIndexEntries(t, s, "metrics")
	if after["busy"] != 5 || after["a"] != 0 || after["b"] != 1 {
		t.Fatalf("index after prune = %v, want 5 busy, no a, 1 b", after)
	}

	if _, err := s.EvictRecords("metrics", 1, 100, func(topic string) bool { return topic == "colca/v1/_Metric/n/b" }); err != nil {
		t.Fatal(err)
	}
	if got := countIndexEntries(t, s, "metrics"); got["b"] != 0 || got["busy"] != 5 {
		t.Fatalf("index after evicting b = %v, want 5 busy and no b", got)
	}

	// Every busy record but the last is superseded by the one after it.
	if _, err := s.Compact("metrics"); err != nil {
		t.Fatal(err)
	}
	if got := countIndexEntries(t, s, "metrics"); got["busy"] != 1 {
		t.Fatalf("index after compaction = %v, want the one live busy record", got)
	}
	got, _, err := s.ReadSignals(context.Background(), "metrics", 1, 10, 0, []string{"busy"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("busy after compaction = %+v, want one record", got)
	}
}

func TestTheIndexIsTrustedAfterACleanCloseAndRestartsAfterAWriteWithoutIt(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	appendSparse(t, s, 2000, map[int]string{1999: "a"})
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopened after a clean close, the index still covers the whole stream: a
	// budget of one entry finds the record behind 2000 busy ones.
	if s, err = Open(dir); err != nil {
		t.Fatal(err)
	}
	got, _, err := s.ReadSignals(context.Background(), "metrics", 1, 10, 1, []string{"a"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("after a clean close: %d records, want the indexed one", len(got))
	}
	head := s.NextOffset("metrics")
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// A version without the index appends a record of a, and does not index it.
	db, err := pebble.Open(dir, &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	val, err := json.Marshal(recEnc{Topic: "colca/v1/_Metric/n/a", Payload: []byte(`{"signal_id":"a","value":1}`), TS: 1, SignalID: "a"})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Set(streamKey("metrics", head), val, pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if err := db.Set(metaKey("metrics"), be64(head+1), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// The clean mark no longer matches, so the coverage restarts at the head and
	// a read from the start scans: it finds both records, the unindexed one too.
	if s, err = Open(dir); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	got, next, err := s.ReadSignals(context.Background(), "metrics", 1, 10, 0, []string{"a"}, func(r StoredRecord) bool { return r.SignalID == "a" })
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[1].Offset != head || next != head+1 {
		t.Fatalf("after an unindexed write: %+v next=%d, want two records ending at %d", got, next, head)
	}
	// From the new coverage on, appends are indexed again.
	if _, _, err := s.Append("metrics", []Record{metricRecord("a", 5)}); err != nil {
		t.Fatal(err)
	}
	got, _, err = s.ReadSignals(context.Background(), "metrics", head+1, 10, 1, []string{"a"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Offset != head+1 {
		t.Fatalf("after the restart: %+v, want the record appended at %d", got, head+1)
	}
}

// mergedRead is what ReadSignals has to return, computed the plain way: every
// index entry of the wanted signals from `from` on, visited in offset order
// until the limit or the scan budget ends the page.
func mergedRead(t *testing.T, s *Store, from uint64, limit, maxScan int, signals []string, filter func(StoredRecord) bool) (offsets []uint64, next uint64) {
	t.Helper()
	wanted := map[string]bool{}
	for _, signalID := range signals {
		wanted[signalID] = true
	}
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte("si\x00metrics\x00"),
		UpperBound: []byte("si\x00metrics\x01"),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer iter.Close()
	var entries []uint64
	prefix := len("si\x00metrics\x00")
	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		if off, _ := offsetOf(key); wanted[string(key[prefix:len(key)-9])] && off >= from {
			entries = append(entries, off)
		}
	}
	slices.Sort(entries)
	next = from
	scanned := 0
	for len(entries) > 0 && len(offsets) < limit {
		if maxScan > 0 && scanned >= maxScan {
			return offsets, next
		}
		scanned++
		off := entries[0]
		entries = entries[1:]
		next = off + 1
		recs, _, err := s.ReadRecordsBounded(context.Background(), "metrics", off, 1, 1, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(recs) == 0 || recs[0].Offset != off || (filter != nil && !filter(recs[0])) {
			continue
		}
		offsets = append(offsets, off)
	}
	if len(entries) == 0 {
		next = s.NextOffset("metrics")
	}
	return offsets, next
}

func TestReadSignalsVisitsTheSameEntriesAsAMergeOfEverySignal(t *testing.T) {
	s := mustOpen(t)
	// 40 signals of very different rates, interleaved: s00 has a record in every
	// round, s39 in every 40th; "silent" has none at all.
	var recs []Record
	var ids []string
	for i := range 40 {
		ids = append(ids, fmt.Sprintf("s%02d", i))
	}
	for round := range 120 {
		for i, id := range ids {
			if round%(i+1) == 0 {
				recs = append(recs, metricRecord(id, int64(round)))
			}
		}
		recs = append(recs, metricRecord("other", int64(round)))
	}
	if _, _, err := s.Append("metrics", recs); err != nil {
		t.Fatal(err)
	}
	head := s.NextOffset("metrics")
	// The index entry of a record that is gone is stepped over, and still counts
	// as visited.
	if err := s.db.Delete(streamKey("metrics", 7), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Prune("metrics", 4, nil, nil); err != nil {
		t.Fatal(err)
	}

	total := len(recs) - 120
	odd := func(r StoredRecord) bool { return r.Offset%2 == 1 }
	rare := func(r StoredRecord) bool { return r.Offset%97 == 0 }
	none := func(StoredRecord) bool { return false }
	all := append([]string{"silent", "s05"}, ids...) // s05 twice, in no order
	for _, tc := range []struct {
		name           string
		from           uint64
		limit, maxScan int
		signals        []string
		filter         func(StoredRecord) bool
	}{
		{"everything in one page", 1, total + 10, 0, all, nil},
		{"the limit is exactly what is left", 4, total - 3, 0, all, nil},
		{"one less than what is left", 4, total - 4, 0, all, nil},
		{"small pages", 1, 7, 0, all, nil},
		{"the budget ends the page", 1, 100, 13, all, nil},
		{"the budget is exactly what is left", 4, 1000, total - 3, all, nil},
		{"limit and budget end together", 1, 9, 9, all, nil},
		{"the filter rejects half", 1, 50, 0, all, odd},
		{"the filter rejects nearly all", 1, 3, 0, all, rare},
		{"the filter rejects nearly all, on a budget", 1, 3, 150, all, rare},
		{"the filter rejects all", 1, 10, 0, all, none},
		{"the filter rejects all, on a budget", 1, 10, 200, all, none},
		{"rare signals only", 1, 10, 0, []string{"s39", "s37", "silent"}, nil},
		{"signals without records", 1, 10, 5, []string{"silent", "absent"}, nil},
		{"no signals", 1, 10, 0, nil, nil},
		{"from the middle", head / 2, 25, 0, all, nil},
		{"the last record", head - 1, 10, 0, all, nil},
		{"past the last wanted record", head - 1, 10, 0, []string{"s39"}, nil},
		{"at the head", head, 10, 0, all, nil},
		{"below the low-water mark", 2, 5, 0, all, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Page through to the head: every page has to match, and so the pages
			// together neither skip nor repeat a record.
			from := tc.from
			for page := 0; ; page++ {
				want, wantNext := mergedRead(t, s, from, tc.limit, tc.maxScan, tc.signals, tc.filter)
				got, next, err := s.ReadSignals(context.Background(), "metrics", from, tc.limit, tc.maxScan, tc.signals, tc.filter)
				if err != nil {
					t.Fatal(err)
				}
				var offsets []uint64
				for _, r := range got {
					offsets = append(offsets, r.Offset)
				}
				if !slices.Equal(offsets, want) || next != wantNext {
					t.Fatalf("page %d from %d: offsets %v next=%d, want %v next=%d", page, from, offsets, next, want, wantNext)
				}
				if next >= head {
					return
				}
				if next <= from {
					t.Fatalf("page %d from %d made no progress", page, from)
				}
				from = next
			}
		})
	}
}

func TestReadSignalsStopsWhenItsContextEnds(t *testing.T) {
	s := mustOpen(t)
	appendSparse(t, s, 10, map[int]string{2: "a"})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if got, _, err := s.ReadSignals(ctx, "metrics", 1, 10, 0, []string{"a", "busy"}, nil); !errors.Is(err, context.Canceled) || got != nil {
		t.Fatalf("a cancelled read returned %d records and %v", len(got), err)
	}
}

func TestPruneLeavesOneRangeTombstonePerPruneHoweverManySignals(t *testing.T) {
	s := mustOpen(t)
	const signals, prunes, perPrune = 50, 20, 2
	ids := prunedSignalStore(t, s, signals, prunes, perPrune, false)
	// One over the stream's records per prune; a range per signal would be 1000
	// more. Compaction may split a tombstone at a table boundary.
	if n := rangeTombstones(t, s); n == 0 || n > 2*prunes {
		t.Fatalf("%d range tombstones after %d prunes of %d signals, want about one per prune", n, prunes, signals)
	}

	// The index holds exactly the entries of the records that are left.
	lwm, head := s.LWM("metrics"), s.NextOffset("metrics")
	index := countIndexEntries(t, s, "metrics")
	if len(index) != signals {
		t.Fatalf("the index holds %d signals, want %d", len(index), signals)
	}
	for _, id := range ids {
		if index[id] != perPrune {
			t.Fatalf("the index holds %d entries of %s, want the %d of the last batch", index[id], id, perPrune)
		}
	}
	got, next, err := s.ReadSignals(context.Background(), "metrics", 1, 1000, 0, ids, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != signals*perPrune || got[0].Offset != lwm || next != head {
		t.Fatalf("read %d records from %d, next=%d; want the %d from the low-water mark %d and the head %d", len(got), got[0].Offset, next, signals*perPrune, lwm, head)
	}
}

func TestPruneScannedReadsThePrefixAgainOnlyWhenItChangedUnderTheScan(t *testing.T) {
	policyScan := func(t *testing.T, s *Store, upTo uint64) *PruneScan {
		t.Helper()
		// Every record is older than the policy allows; the clamp ends the scan.
		scan, _, _, err := s.PolicyPruneScan("metrics", s.LWM("metrics"), s.NextOffset("metrics"), time.UnixMilli(1<<40), time.Millisecond, 0, 0, upTo, 0)
		if err != nil {
			t.Fatal(err)
		}
		if scan.UpTo() != upTo {
			t.Fatalf("the policy scan ends at %d, want %d", scan.UpTo(), upTo)
		}
		return scan
	}
	// liveBytes is what the records still in the stream cost.
	liveBytes := func(t *testing.T, s *Store) (n uint64) {
		t.Helper()
		if err := s.ScanRecords("metrics", 1, s.NextOffset("metrics"), func(_ uint64, _ int64, size uint64) bool {
			n += size
			return true
		}); err != nil {
			t.Fatal(err)
		}
		return n
	}

	t.Run("untouched", func(t *testing.T) {
		s := mustOpen(t)
		appendSparse(t, s, 10, map[int]string{2: "a", 8: "b"})
		scan := policyScan(t, s, 7)
		var span PruneSpan
		removed, err := s.PruneScanned(scan, nil, func(sp PruneSpan) PruneOutcome { span = sp; return PruneOutcome{} })
		if err != nil {
			t.Fatal(err)
		}
		// Offsets 1..6 are busy, busy, a, busy, busy, busy, with ts 1,2,3,3,4,5.
		if removed != 6 || span.From != 1 || span.To != 6 || span.FirstTS != 1 || span.LastTS != 5 {
			t.Fatalf("removed %d, span %+v", removed, span)
		}
		if index := countIndexEntries(t, s, "metrics"); index["busy"] != 5 || index["a"] != 0 || index["b"] != 1 {
			t.Fatalf("index after the prune = %v, want 5 busy, no a, 1 b", index)
		}
		if got, want := s.StreamBytes("metrics"), liveBytes(t, s); got != want {
			t.Fatalf("stream bytes = %d, want the %d of the records left", got, want)
		}
	})

	t.Run("records evicted after the scan", func(t *testing.T) {
		s := mustOpen(t)
		appendSparse(t, s, 10, map[int]string{2: "a", 8: "b"})
		scan := policyScan(t, s, 7)
		if _, err := s.EvictRecords("metrics", 1, 100, func(topic string) bool { return topic == "colca/v1/_Metric/n/a" }); err != nil {
			t.Fatal(err)
		}
		var span PruneSpan
		removed, err := s.PruneScanned(scan, nil, func(sp PruneSpan) PruneOutcome { span = sp; return PruneOutcome{} })
		if err != nil {
			t.Fatal(err)
		}
		// The record of a is gone already: it is neither counted nor shed twice.
		if removed != 5 || span.To != 6 {
			t.Fatalf("removed %d, span %+v; want the 5 records the eviction left", removed, span)
		}
		if got, want := s.StreamBytes("metrics"), liveBytes(t, s); got != want {
			t.Fatalf("stream bytes = %d, want the %d of the records left", got, want)
		}
	})

	t.Run("a cursor moved into the prefix", func(t *testing.T) {
		s := mustOpen(t)
		appendSparse(t, s, 10, map[int]string{2: "a", 8: "b"})
		scan := policyScan(t, s, 7)
		if !s.CursorAck("reader", "metrics", 4) {
			t.Fatal("the cursor did not move")
		}
		removed, err := s.PruneScanned(scan, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		if removed != 3 || s.LWM("metrics") != 4 {
			t.Fatalf("removed %d up to %d, want the 3 records below the cursor at 4", removed, s.LWM("metrics"))
		}
		// a, at offset 3, went; the busy records from 4 on and b stay indexed.
		if index := countIndexEntries(t, s, "metrics"); index["busy"] != 8 || index["a"] != 0 || index["b"] != 1 {
			t.Fatalf("index after the prune = %v, want 8 busy, no a, 1 b", index)
		}
		if got, want := s.StreamBytes("metrics"), liveBytes(t, s); got != want {
			t.Fatalf("stream bytes = %d, want the %d of the records left", got, want)
		}
	})

	t.Run("another prune committed first", func(t *testing.T) {
		s := mustOpen(t)
		appendSparse(t, s, 10, map[int]string{2: "a", 8: "b"})
		scan := policyScan(t, s, 7)
		if _, err := s.Prune("metrics", 3, nil, nil); err != nil {
			t.Fatal(err)
		}
		if removed, err := s.PruneScanned(scan, nil, nil); err == nil || removed != 0 {
			t.Fatalf("a scan from a low-water mark that moved pruned %d records, err=%v", removed, err)
		}
	})
}
