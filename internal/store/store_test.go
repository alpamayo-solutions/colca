package store

import (
	"errors"
	"fmt"
	"testing"

	"github.com/cockroachdb/pebble/v2"
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
	got, _, err = s.Read("metrics", 1, 100, func(topic string) bool { return topic == "colca/v1/_Metric/m1/t4" })
	if err != nil {
		t.Fatalf("filtered read: %v", err)
	}
	if len(got) != 1 || got[0].Offset != 5 {
		t.Fatalf("filtered: %+v", got)
	}
}

func TestAtomicBatchAppendsConsecutiveOffsetsAndProjectsEveryKVRow(t *testing.T) {
	s := mustOpen(t)
	originalApply := s.appendApply
	applyCalls := 0
	s.appendApply = func(batch *pebble.Batch, opts *pebble.WriteOptions) error {
		applyCalls++
		if opts != pebble.Sync {
			t.Fatalf("append used write options %p, want pebble.Sync %p", opts, pebble.Sync)
		}
		return originalApply(batch, opts)
	}

	first, last, err := s.Append("entities", []Record{
		{
			Topic: "colca/v1/_Signal/n-edge1/line1/temp", Payload: []byte(`{"id":"sig-temp","name":"Temperature"}`),
			TS: 1, KVPath: "line1/temp", KVNode: "n-edge1",
		},
		{
			Topic: "colca/v1/_Signal/n-edge1/line1/speed", Payload: []byte(`{"id":"sig-speed","name":"Speed"}`),
			TS: 1, KVPath: "line1/speed", KVNode: "n-edge1",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if first != 1 || last != 2 {
		t.Fatalf("atomic batch offsets = %d..%d, want 1..2", first, last)
	}
	if applyCalls != 1 {
		t.Fatalf("atomic batch applied %d Pebble batches, want exactly 1", applyCalls)
	}
	if got := s.NextOffset("entities"); got != 3 {
		t.Fatalf("next entities offset = %d, want 3", got)
	}
	if got := s.KVScan("line1/"); len(got) != 2 {
		t.Fatalf("projected KV rows = %+v, want both batch records", got)
	}
}

func TestAtomicBatchStorageFailureLeavesNoStreamOrKVState(t *testing.T) {
	s := mustOpen(t)
	s.appendApply = func(*pebble.Batch, *pebble.WriteOptions) error {
		return errors.New("injected apply failure")
	}

	_, _, err := s.Append("entities", []Record{
		{
			Topic: "colca/v1/_Signal/n-edge1/line1/temp", Payload: []byte(`{"id":"sig-temp","name":"Temperature"}`),
			TS: 1, KVPath: "line1/temp", KVNode: "n-edge1",
		},
		{
			Topic: "colca/v1/_Signal/n-edge1/line1/speed", Payload: []byte(`{"id":"sig-speed","name":"Speed"}`),
			TS: 1, KVPath: "line1/speed", KVNode: "n-edge1",
		},
	})
	if err == nil || err.Error() != "injected apply failure" {
		t.Fatalf("Append error = %v, want injected apply failure", err)
	}
	if got := s.NextOffset("entities"); got != 1 {
		t.Fatalf("failed batch advanced next offset to %d, want 1", got)
	}
	records, next, readErr := s.Read("entities", 1, 10, nil)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if len(records) != 0 || next != 1 {
		t.Fatalf("failed batch left stream state: records=%+v next=%d", records, next)
	}
	if got := s.KVScan("line1/"); len(got) != 0 {
		t.Fatalf("failed batch left KV state: %+v", got)
	}
}

func TestDurabilityAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatalf("first open: %v", err)
	}
	if _, _, err := s.Append("metrics", []Record{{
		Topic: "a", Payload: []byte("1"), TS: 1,
		WrittenBy: "svc-connector", ActorID: "user-anna",
		ActorLabel: "anna@example.com", ActorKind: "human",
	}}); err != nil {
		t.Fatalf("append before reopen: %v", err)
	}
	s.Close()
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	first, last, err := s2.Append("metrics", []Record{{Topic: "b", Payload: []byte("2"), TS: 2}})
	if err != nil {
		t.Fatalf("append after reopen: %v", err)
	}
	if first != 2 || last != 2 {
		t.Fatalf("offset continuity broken: %d..%d", first, last)
	}
	got, _, err := s2.Read("metrics", 1, 1, nil)
	if err != nil || len(got) != 1 || got[0].WrittenBy != "svc-connector" ||
		got[0].ActorID != "user-anna" || got[0].ActorLabel != "anna@example.com" || got[0].ActorKind != "human" {
		t.Fatalf("attribution not durable across reopen: %+v, %v", got, err)
	}
}

// A hierarchy position can carry several kinds of current state. The
// contract-bearing topic is therefore part of KV identity: a _Signal and its
// latest _Metric at the same node/path must coexist, and an update or tombstone
// for one must not affect the other.
func TestKVProjectionSeparatesContractsAtTheSameNodeAndPath(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	const (
		path        = "line1/press/temp"
		nodeID      = "n-edge1"
		signalTopic = "colca/v1/_Signal/n-edge1/line1/press/temp"
		metricTopic = "colca/v1/_Metric/n-edge1/line1/press/temp"
	)
	if _, _, err := s.Append("entities", []Record{{
		Topic: signalTopic, Payload: []byte(`{"id":"01HSIG","name":"temp"}`),
		TS: 1, KVPath: path, KVNode: nodeID,
	}}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Append("metrics", []Record{{
		Topic: metricTopic, Payload: []byte(`{"v":7}`),
		TS: 2, KVPath: path, KVNode: nodeID,
	}}); err != nil {
		t.Fatal(err)
	}

	entries := s.KVScan(path)
	if len(entries) != 2 {
		t.Fatalf("same-path KV entries = %d, want 2 contracts: %+v", len(entries), entries)
	}
	byTopic := map[string]KVEntry{}
	for _, entry := range entries {
		byTopic[entry.Topic] = entry
	}
	if string(byTopic[signalTopic].Payload) != `{"id":"01HSIG","name":"temp"}` ||
		string(byTopic[metricTopic].Payload) != `{"v":7}` {
		t.Fatalf("same-path contracts did not coexist independently: %+v", entries)
	}

	if _, _, err := s.Append("metrics", []Record{{
		Topic: metricTopic, Payload: []byte(`{"v":8}`),
		TS: 3, KVPath: path, KVNode: nodeID,
	}}); err != nil {
		t.Fatal(err)
	}
	entries = s.KVScan(path)
	if len(entries) != 2 {
		t.Fatalf("metric update replaced the signal: %+v", entries)
	}
	byTopic = map[string]KVEntry{}
	for _, entry := range entries {
		byTopic[entry.Topic] = entry
	}
	if string(byTopic[signalTopic].Payload) != `{"id":"01HSIG","name":"temp"}` ||
		string(byTopic[metricTopic].Payload) != `{"v":8}` {
		t.Fatalf("metric update changed the wrong contract: %+v", entries)
	}

	// The retention refresh compare-and-swap is contract-specific too. The
	// metric now has a different current offset at this same path; it must not
	// make a refresh of the still-current signal look stale.
	if _, applied, err := s.AppendIfKVUnchanged("entities", Record{
		Topic: signalTopic, Payload: []byte(`{"id":"01HSIG","name":"temperature"}`),
		TS: 4, KVPath: path, KVNode: nodeID,
	}, byTopic[signalTopic].Offset); err != nil || !applied {
		t.Fatalf("same-path metric interfered with signal CAS: applied=%v err=%v", applied, err)
	}

	if _, _, err := s.Append("metrics", []Record{{
		Topic: metricTopic, TS: 5, KVPath: path, KVNode: nodeID, Delete: true,
	}}); err != nil {
		t.Fatal(err)
	}
	entries = s.KVScan(path)
	if len(entries) != 1 || entries[0].Topic != signalTopic {
		t.Fatalf("metric tombstone removed another contract: %+v", entries)
	}
	if string(entries[0].Payload) != `{"id":"01HSIG","name":"temperature"}` {
		t.Fatalf("signal refresh did not survive metric tombstone: %+v", entries)
	}

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	entries = s.KVScan(path)
	if len(entries) != 1 || entries[0].Topic != signalTopic {
		t.Fatalf("contract-aware KV state changed across reopen: %+v", entries)
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

func TestOriginOffsetSurvivesMultipleReplicationHops(t *testing.T) {
	topic := "colca/v1/_SystemElement/n-edge1/line1"
	child := mustOpen(t)
	if _, _, err := child.Append("entities", []Record{{
		Topic: topic, Payload: []byte(`{"id":"line1"}`), TS: 1,
		KVPath: "line1", KVNode: "n-edge1",
	}}); err != nil {
		t.Fatal(err)
	}
	childRecords, _, err := child.Read("entities", 1, 10, nil)
	if err != nil || len(childRecords) != 1 {
		t.Fatalf("child read = (%+v, %v)", childRecords, err)
	}

	parent := mustOpen(t)
	if _, _, err := parent.Append("entities", []Record{{
		Topic: "colca/v1/_SystemElement/n-parent/local", Payload: []byte(`{"id":"local"}`), TS: 1,
	}}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := parent.ApplyReplicated("n-edge1", "entities", []ReplRecord{{
		ChildOffset: childRecords[0].Offset, OriginOffset: childRecords[0].OriginOffset,
		Topic: topic, Payload: childRecords[0].Payload, TS: childRecords[0].TS,
		KVPath: "edge1/line1", KVNode: "n-edge1",
	}}); err != nil {
		t.Fatal(err)
	}
	parentRecords, _, err := parent.Read("entities", 2, 10, nil)
	if err != nil || len(parentRecords) != 1 {
		t.Fatalf("parent read = (%+v, %v)", parentRecords, err)
	}
	if parentRecords[0].Offset != 2 || parentRecords[0].OriginOffset != 1 {
		t.Fatalf("parent coordinates = local %d origin %d, want 2/1", parentRecords[0].Offset, parentRecords[0].OriginOffset)
	}
	entries := parent.KVScan("edge1/line1")
	if len(entries) != 1 || entries[0].Offset != 2 || entries[0].OriginOffset != 1 {
		t.Fatalf("parent KV did not preserve owner coordinate: %+v", entries)
	}

	grandparent := mustOpen(t)
	if _, _, err := grandparent.ApplyReplicated("n-parent", "entities", []ReplRecord{{
		ChildOffset: parentRecords[0].Offset, OriginOffset: parentRecords[0].OriginOffset,
		Topic: topic, Payload: parentRecords[0].Payload, TS: parentRecords[0].TS,
		KVPath: "site1/edge1/line1", KVNode: "n-edge1",
	}}); err != nil {
		t.Fatal(err)
	}
	grandparentRecords, _, err := grandparent.Read("entities", 1, 10, nil)
	if err != nil || len(grandparentRecords) != 1 || grandparentRecords[0].OriginOffset != 1 {
		t.Fatalf("grandparent lost owner coordinate: records=%+v err=%v", grandparentRecords, err)
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

// Cursors()/HWMs() report exactly the persisted read-only state the metrics
// collector derives gauges from, and tolerate malformed keys/values the same
// way KVScan does (skip, never fail).
func TestCursorsAndHWMsScan(t *testing.T) {
	s := mustOpen(t)
	if got := s.Cursors(); len(got) != 0 {
		t.Fatalf("fresh store must have no cursors, got %v", got)
	}
	if got := s.HWMs(); len(got) != 0 {
		t.Fatalf("fresh store must have no HWMs, got %v", got)
	}

	// Seed through the public APIs only.
	if !s.CursorAck("hub", "metrics", 7) {
		t.Fatal("CursorAck(hub, metrics, 7) should move")
	}
	if !s.CursorAck("archiver", "entities", 3) {
		t.Fatal("CursorAck(archiver, entities, 3) should move")
	}
	if _, _, err := s.ApplyReplicated("n-child", "metrics", []ReplRecord{
		{ChildOffset: 1, Topic: "colca/v1/_Metric/m1/a", Payload: []byte("1"), TS: 1},
		{ChildOffset: 5, Topic: "colca/v1/_Metric/m1/b", Payload: []byte("2"), TS: 2},
	}); err != nil {
		t.Fatalf("ApplyReplicated: %v", err)
	}

	cursors := map[string]CursorInfo{}
	for _, c := range s.Cursors() {
		cursors[c.Name+"/"+c.Stream] = c
	}
	if len(cursors) != 2 {
		t.Fatalf("want 2 cursors, got %v", cursors)
	}
	if c := cursors["hub/metrics"]; c.Position != 7 {
		t.Fatalf("hub/metrics position = %d, want 7 (%+v)", c.Position, c)
	}
	if c := cursors["archiver/entities"]; c.Position != 3 {
		t.Fatalf("archiver/entities position = %d, want 3 (%+v)", c.Position, c)
	}

	hwms := s.HWMs()
	if len(hwms) != 1 {
		t.Fatalf("want 1 HWM, got %v", hwms)
	}
	if h := hwms[0]; h.Child != "n-child" || h.Stream != "metrics" || h.HWM != 5 {
		t.Fatalf("HWM = %+v, want {n-child metrics 5}", h)
	}

	// Malformed entries must be skipped, not returned and not fatal:
	// a cursor key without the name/stream separator, and a value that is
	// not an 8-byte counter.
	if err := s.db.Set([]byte("c\x00no-separator"), be64(9), nil); err != nil {
		t.Fatal(err)
	}
	if err := s.db.Set([]byte("h\x00bad\x00metrics"), []byte("short"), nil); err != nil {
		t.Fatal(err)
	}
	if got := s.Cursors(); len(got) != 2 {
		t.Fatalf("malformed cursor key must be skipped, got %v", got)
	}
	if got := s.HWMs(); len(got) != 1 {
		t.Fatalf("malformed HWM value must be skipped, got %v", got)
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

// sumRecordBytes recomputes a stream's live logical bytes from the records
// themselves (ScanRecords reports the exact per-record cost the b/ accounting
// uses), so a drifted counter cannot hide behind its own bookkeeping.
func sumRecordBytes(t *testing.T, s *Store, stream string) uint64 {
	t.Helper()
	var sum uint64
	if err := s.ScanRecords(stream, 1, s.NextOffset(stream), func(_ uint64, _ int64, size uint64) bool {
		sum += size
		return true
	}); err != nil {
		t.Fatalf("ScanRecords(%s): %v", stream, err)
	}
	return sum
}

// Retention design §7.1: a Record with the Delete flag appends the tombstone to
// the stream as history AND deletes the KV key — one atomic batch, byte
// accounting intact, and the deletion durable across a reopen (which is what
// makes the reseed correct with zero reseed changes).
func TestAppendTombstoneDeletesKVInBatch(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Append("metrics", []Record{
		{Topic: "colca/v1/_Metric/m1/m1/temp", Payload: []byte(`{"v":7}`), TS: 1, KVPath: "m1/temp", KVNode: "m1"},
		{Topic: "colca/v1/_Metric/m1/m1/keep", Payload: []byte(`{"v":1}`), TS: 2, KVPath: "m1/keep", KVNode: "m1"},
	}); err != nil {
		t.Fatal(err)
	}
	if got := len(s.KVScan("")); got != 2 {
		t.Fatalf("pre-tombstone KV entries = %d, want 2", got)
	}

	if _, _, err := s.Append("metrics", []Record{
		{Topic: "colca/v1/_Metric/m1/m1/temp", TS: 3, KVPath: "m1/temp", KVNode: "m1", Delete: true},
	}); err != nil {
		t.Fatal(err)
	}

	if got := s.KVScan("m1/temp"); len(got) != 0 {
		t.Fatalf("tombstoned KV key survived the batch: %+v", got)
	}
	if got := s.KVScan("m1/keep"); len(got) != 1 {
		t.Fatalf("untouched sibling key must survive, got %d entries", len(got))
	}
	// The tombstone IS history: the stream keeps all three records.
	recs, _, err := s.Read("metrics", 1, 10, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 3 {
		t.Fatalf("stream records = %d, want 3 (tombstone appended as history)", len(recs))
	}
	if recs[2].Offset != 3 || len(recs[2].Payload) != 0 || recs[2].Topic != "colca/v1/_Metric/m1/m1/temp" {
		t.Fatalf("tombstone record = %+v, want offset 3, empty payload, original topic", recs[2])
	}
	// Byte accounting counts the tombstone record like any other.
	if got, want := s.StreamBytes("metrics"), sumRecordBytes(t, s, "metrics"); got != want {
		t.Fatalf("StreamBytes = %d, want %d (sum of per-record costs incl. the tombstone)", got, want)
	}

	// Durability: the deletion is part of the synced batch, so a reopen shows
	// the same picture — the KV key stays gone, nothing to reseed from.
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if got := s2.KVScan("m1/temp"); len(got) != 0 {
		t.Fatalf("tombstoned KV key resurrected across reopen: %+v", got)
	}
	if got := len(s2.KVScan("")); got != 1 {
		t.Fatalf("KV entries after reopen = %d, want 1 (only m1/keep)", got)
	}
	if s2.NextOffset("metrics") != 4 {
		t.Fatalf("next offset after reopen = %d, want 4", s2.NextOffset("metrics"))
	}
}

// Retention design §7.1: ApplyReplicated applies a replicated tombstone
// identically — KV key deleted in the same batch as the appended record and the
// HWM advance. A REPLAYED tombstone is dropped by the HWM dedupe, and a
// tombstone for an already-absent key applies cleanly (batch delete is
// idempotent), so replication can never wedge on a delete.
func TestApplyReplicatedTombstone(t *testing.T) {
	s := mustOpen(t)
	set := ReplRecord{ChildOffset: 1, Topic: "colca/v1/_Metric/m1/edge1/m1/a", Payload: []byte(`{"v":1}`), TS: 1, KVPath: "edge1/m1/a", KVNode: "m1"}
	if _, _, err := s.ApplyReplicated("n-edge1", "metrics", []ReplRecord{set}); err != nil {
		t.Fatal(err)
	}
	if len(s.KVScan("edge1/m1/a")) != 1 {
		t.Fatal("setup: replicated KV entry missing")
	}

	tomb := ReplRecord{ChildOffset: 2, Topic: "colca/v1/_Metric/m1/edge1/m1/a", TS: 2, KVPath: "edge1/m1/a", KVNode: "m1", Delete: true}
	applied, hwm, err := s.ApplyReplicated("n-edge1", "metrics", []ReplRecord{tomb})
	if err != nil {
		t.Fatal(err)
	}
	if len(applied) != 1 || hwm != 2 {
		t.Fatalf("tombstone apply: applied %d hwm %d, want 1/2", len(applied), hwm)
	}
	if got := s.KVScan("edge1/m1/a"); len(got) != 0 {
		t.Fatalf("replicated tombstone did not retire the KV key: %+v", got)
	}

	// Replay of the same tombstone: HWM dedupe drops it, no error, no state change.
	applied, hwm, err = s.ApplyReplicated("n-edge1", "metrics", []ReplRecord{set, tomb})
	if err != nil {
		t.Fatalf("replayed tombstone must not error: %v", err)
	}
	if len(applied) != 0 || hwm != 2 {
		t.Fatalf("replayed tombstone: applied %d hwm %d, want 0/2", len(applied), hwm)
	}
	if got := s.KVScan("edge1/m1/a"); len(got) != 0 {
		t.Fatalf("replay resurrected the KV key: %+v", got)
	}

	// A tombstone for a path this node never had: delete of an absent key is a
	// no-op inside the batch — the record still lands, the HWM still advances.
	ghost := ReplRecord{ChildOffset: 3, Topic: "colca/v1/_Metric/m1/edge1/m1/never", TS: 3, KVPath: "edge1/m1/never", KVNode: "m1", Delete: true}
	applied, hwm, err = s.ApplyReplicated("n-edge1", "metrics", []ReplRecord{ghost})
	if err != nil {
		t.Fatalf("tombstone for an absent KV key must apply cleanly: %v", err)
	}
	if len(applied) != 1 || hwm != 3 {
		t.Fatalf("ghost tombstone: applied %d hwm %d, want 1/3", len(applied), hwm)
	}
	if s.NextOffset("metrics") != 4 {
		t.Fatalf("next offset = %d, want 4", s.NextOffset("metrics"))
	}
	if got, want := s.StreamBytes("metrics"), sumRecordBytes(t, s, "metrics"); got != want {
		t.Fatalf("StreamBytes = %d, want %d after tombstone applies", got, want)
	}
}
