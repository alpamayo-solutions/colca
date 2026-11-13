package store

import "testing"

// A set-aside copy is written with the cursor move, never moves a cursor back,
// and outlives the stream's own retention.
func TestASetAsideRecordMovesTheCursorAndOutlivesRetention(t *testing.T) {
	s := mustOpen(t)
	if _, _, err := s.Append("metrics", []Record{
		{Topic: "colca/v1/_Metric/n/a", Payload: []byte(`{"v":1}`), TS: 1},
		{Topic: "colca/v1/_Metric/n/a", Payload: []byte(`{"v":2}`), TS: 2},
	}); err != nil {
		t.Fatal(err)
	}
	recs, _, err := s.ReadRecords("metrics", 1, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	r := SetAsideRecord{Parent: "p", Stream: "metrics", Offset: 1, Record: recs[0], Status: 413, Reason: "too large"}
	if moved, err := s.SetAsideUplink("up", r); err != nil || !moved {
		t.Fatalf("SetAsideUplink = %v, %v", moved, err)
	}
	if pos := s.CursorGet("up", "metrics"); pos != 2 {
		t.Fatalf("cursor = %d, want 2: past the set-aside record", pos)
	}
	// A cursor already past the record is not moved back, and nothing is copied.
	if moved, err := s.SetAsideUplink("up", SetAsideRecord{Parent: "p", Stream: "metrics", Offset: 0}); err != nil || moved {
		t.Fatalf("a set-aside behind the cursor = %v, %v; want nothing written", moved, err)
	}
	if _, err := s.Prune("metrics", 3, []string{"up"}, nil); err != nil {
		t.Fatal(err)
	}
	got, err := s.SetAsideRecords("p")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Offset != 1 || string(got[0].Record.Payload) != `{"v":1}` {
		t.Fatalf("after pruning the stream: %+v, want the copy", got)
	}
	if other, _ := s.SetAsideRecords("q"); len(other) != 0 {
		t.Fatalf("another parent's list = %+v", other)
	}
	if found, err := s.ClearSetAside("p", "metrics", 1); err != nil || !found {
		t.Fatalf("ClearSetAside = %v, %v", found, err)
	}
	if found, _ := s.ClearSetAside("p", "metrics", 1); found {
		t.Fatal("cleared twice")
	}
}
