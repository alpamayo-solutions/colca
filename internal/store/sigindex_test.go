package store

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

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
			recs = append(recs, metricRecord(signalID, int64(i)))
			want[uint64(len(recs))] = signalID
		}
		recs = append(recs, metricRecord("busy", int64(i)))
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
