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

func TestCursorMonotonicAck(t *testing.T) {
	s := mustOpen(t)
	if got := s.CursorGet("hub", "metrics"); got != 1 {
		t.Fatalf("initial cursor: %d", got)
	}
	if !s.CursorAck("hub", "metrics", 10) {
		t.Fatal("ack 10 should move")
	}
	if s.CursorAck("hub", "metrics", 5) {
		t.Fatal("ack 5 must be no-op (monotonic)")
	}
	if got := s.CursorGet("hub", "metrics"); got != 10 {
		t.Fatalf("cursor: %d", got)
	}
}

func TestApplyReplicatedDedupe(t *testing.T) {
	s := mustOpen(t)
	batch := []ReplRecord{
		{ChildOffset: 1, Topic: "colca/v1/_Metric/m1/edge1/m1/a", Payload: []byte("1"), TS: 1, KVPath: "edge1/m1/a", KVNode: "m1"},
		{ChildOffset: 2, Topic: "colca/v1/_Metric/m1/edge1/m1/b", Payload: []byte("2"), TS: 2, KVPath: "edge1/m1/b", KVNode: "m1"},
	}
	applied, hwm, err := s.ApplyReplicated("n-edge1", "metrics", batch)
	if err != nil {
		t.Fatal(err)
	}
	if len(applied) != 2 || hwm != 2 {
		t.Fatalf("applied %d hwm %d", len(applied), hwm)
	}
	// the returned slice IS the set of records written, in write order — that is
	// what the engine mirrors onto the local MQTT bus.
	if applied[0].ChildOffset != 1 || applied[0].Topic != batch[0].Topic ||
		applied[1].ChildOffset != 2 || applied[1].Topic != batch[1].Topic {
		t.Fatalf("returned records are not the applied ones, in order: %+v", applied)
	}
	// exact same batch again → full dedupe
	applied, hwm, _ = s.ApplyReplicated("n-edge1", "metrics", batch)
	if len(applied) != 0 || hwm != 2 {
		t.Fatalf("dedupe failed: applied %d hwm %d", len(applied), hwm)
	}
	// overlapping batch → partial
	batch = append(batch, ReplRecord{ChildOffset: 3, Topic: "colca/v1/_Metric/m1/edge1/m1/c", Payload: []byte("3"), TS: 3, KVPath: "edge1/m1/c", KVNode: "m1"})
	applied, hwm, _ = s.ApplyReplicated("n-edge1", "metrics", batch)
	if len(applied) != 1 || hwm != 3 {
		t.Fatalf("partial dedupe: applied %d hwm %d", len(applied), hwm)
	}
	// exactly the new record, never one of the two already-applied ones
	if applied[0].ChildOffset != 3 || applied[0].Topic != "colca/v1/_Metric/m1/edge1/m1/c" {
		t.Fatalf("partial dedupe returned the wrong record: %+v", applied)
	}
	if s.NextOffset("metrics") != 4 {
		t.Fatalf("local offsets: %d", s.NextOffset("metrics"))
	}
}

func TestKVScan(t *testing.T) {
	s := mustOpen(t)
	s.Append("metrics", []Record{
		{Topic: "colca/v1/_Metric/m1/m1/temp", Payload: []byte(`{"v":1}`), TS: 1, KVPath: "m1/temp", KVNode: "m1"},
		{Topic: "colca/v1/_Metric/m1/m1/temp", Payload: []byte(`{"v":2}`), TS: 2, KVPath: "m1/temp", KVNode: "m1"}, // overwrites
		{Topic: "colca/v1/_Metric/m2/m2/temp", Payload: []byte(`{"v":9}`), TS: 3, KVPath: "m2/temp", KVNode: "m2"},
	})
	entries := s.KVScan("m1/")
	if len(entries) != 1 {
		t.Fatalf("want 1 entry, got %d", len(entries))
	}
	if string(entries[0].Payload) != `{"v":2}` {
		t.Fatalf("last value wrong: %s", entries[0].Payload)
	}
	if len(s.KVScan("")) != 2 {
		t.Fatal("full scan should see 2 keys")
	}
}

func TestDiskMetricsGrowWithWrites(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	before := s.DiskMetrics()
	recs := make([]Record, 100)
	for i := range recs {
		recs[i] = Record{Topic: "colca/v1/_Metric/m1/m1/temp", Payload: []byte(`{"v":1.5}`), TS: int64(i)}
	}
	if _, _, err := s.Append("metrics", recs); err != nil {
		t.Fatal(err)
	}
	after := s.DiskMetrics()
	if after.WALBytesWritten <= before.WALBytesWritten {
		t.Fatalf("WAL bytes did not grow: before=%d after=%d", before.WALBytesWritten, after.WALBytesWritten)
	}
	if after.DiskUsageBytes == 0 {
		t.Fatal("disk usage reported as 0 after writes")
	}
}
