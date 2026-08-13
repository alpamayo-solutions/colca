package store

import (
	"fmt"
	"testing"
)

func mustOpen(t *testing.T) *Store {
	t.Helper()
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestAppendReadOffsets(t *testing.T) {
	s := mustOpen(t)
	var recs []Record
	for i := 0; i < 5; i++ {
		recs = append(recs, Record{Topic: fmt.Sprintf("colca/v1/_Metric/m1/t%d", i), Payload: []byte(`{"v":1}`), TS: int64(1000 + i)})
	}
	first, last, err := s.Append("metrics", recs)
	if err != nil {
		t.Fatal(err)
	}
	if first != 1 || last != 5 {
		t.Fatalf("want 1..5 got %d..%d", first, last)
	}

	got, next, err := s.Read("metrics", 1, 100, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 5 || next != 6 {
		t.Fatalf("read %d next %d", len(got), next)
	}
	if got[2].Offset != 3 || got[2].Topic != "colca/v1/_Metric/m1/t2" {
		t.Fatalf("rec: %+v", got[2])
	}

	// filter
	got, _, _ = s.Read("metrics", 1, 100, func(topic string) bool { return topic == "colca/v1/_Metric/m1/t4" })
	if len(got) != 1 || got[0].Offset != 5 {
		t.Fatalf("filtered: %+v", got)
	}
}

func TestDurabilityAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	s, _ := Open(dir)
	s.Append("metrics", []Record{{Topic: "a", Payload: []byte("1"), TS: 1}})
	s.Close()
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	first, last, _ := s2.Append("metrics", []Record{{Topic: "b", Payload: []byte("2"), TS: 2}})
	if first != 2 || last != 2 {
		t.Fatalf("offset continuity broken: %d..%d", first, last)
	}
}
