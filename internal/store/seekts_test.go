package store

import (
	"fmt"
	"testing"
)

// appendLogs appends one _Log record per timestamp to the logs stream.
func appendLogs(t *testing.T, s *Store, ts ...int64) {
	t.Helper()
	var recs []Record
	for i, at := range ts {
		recs = append(recs, Record{Topic: fmt.Sprintf("colca/v1/_Log/n/svc%d/INFO", i), Payload: []byte(`{}`), TS: at})
	}
	if _, _, err := s.Append("logs", recs); err != nil {
		t.Fatal(err)
	}
}

func mustSeek(t *testing.T, s *Store, ts int64) uint64 {
	t.Helper()
	off, err := s.SeekTS("logs", ts)
	if err != nil {
		t.Fatal(err)
	}
	return off
}

func TestSeekTSEmptyStream(t *testing.T) {
	s := mustOpen(t)
	if got, want := mustSeek(t, s, 0), s.NextOffset("logs"); got != want {
		t.Fatalf("SeekTS on an empty stream = %d, want NextOffset %d", got, want)
	}
	if got := mustSeek(t, mustOpen(t), 5); got != 1 {
		t.Fatalf("SeekTS on an empty stream = %d, want 1", got)
	}
}

func TestSeekTSFindsWindowStart(t *testing.T) {
	s := mustOpen(t)
	appendLogs(t, s, 10, 20, 20, 30, 40, 50) // offsets 1..6
	for _, tc := range []struct {
		ts   int64
		want uint64
	}{
		{0, 1},   // before everything
		{10, 1},  // exactly the first
		{11, 2},  // between
		{20, 2},  // the first of two equal timestamps
		{21, 4},  // after the equal run
		{50, 6},  // exactly the last
		{51, 7},  // past the head: NextOffset
		{999, 7}, // far past the head
	} {
		if got := mustSeek(t, s, tc.ts); got != tc.want {
			t.Errorf("SeekTS(%d) = %d, want %d", tc.ts, got, tc.want)
		}
	}
}

// A window that starts before the low-water mark starts at the first retained
// record: the pruned prefix is gone, not searched.
func TestSeekTSBeforeLWM(t *testing.T) {
	s := mustOpen(t)
	appendLogs(t, s, 10, 20, 30, 40, 50)
	if _, err := s.Prune("logs", 3, nil, nil); err != nil {
		t.Fatal(err)
	}
	if got := s.LWM("logs"); got != 3 {
		t.Fatalf("LWM = %d, want 3", got)
	}
	if got := mustSeek(t, s, 0); got != 3 {
		t.Fatalf("SeekTS before LWM = %d, want 3", got)
	}
	if got := mustSeek(t, s, 35); got != 4 {
		t.Fatalf("SeekTS(35) = %d, want 4", got)
	}
}

func TestEachRecordStopsAndBounds(t *testing.T) {
	s := mustOpen(t)
	appendLogs(t, s, 10, 20, 30, 40)
	var offs []uint64
	if err := s.EachRecord("logs", 2, 4, func(r StoredRecord) bool {
		offs = append(offs, r.Offset)
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(offs) != "[2 3]" {
		t.Fatalf("EachRecord [2,4) = %v, want [2 3]", offs)
	}
	offs = nil
	if err := s.EachRecord("logs", 1, 99, func(r StoredRecord) bool {
		offs = append(offs, r.Offset)
		return r.TS < 20
	}); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(offs) != "[1 2]" {
		t.Fatalf("EachRecord with early stop = %v, want [1 2]", offs)
	}
}
