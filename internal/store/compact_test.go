package store

import "testing"

func defStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// write appends one definition record and returns its offset.
func write(t *testing.T, s *Store, topic string, payload string) uint64 {
	t.Helper()
	rec := Record{Topic: topic, TS: 1, KVPath: topic, KVNode: "n"}
	if payload != "" {
		rec.Payload = []byte(payload)
	} else {
		rec.Delete = true // the tombstone: KV key removed in the same batch
	}
	off, _, err := s.Append("definitions", []Record{rec})
	if err != nil {
		t.Fatal(err)
	}
	return off
}

func offsets(t *testing.T, s *Store) []uint64 {
	t.Helper()
	recs, _, err := s.Read("definitions", 1, 1000, nil)
	if err != nil {
		t.Fatal(err)
	}
	var out []uint64
	for _, r := range recs {
		out = append(out, r.Offset)
	}
	return out
}

// A superseded record goes immediately, whatever any cursor is doing: the later
// record carries the truth and sits further along the stream, so a consumer
// behind the dropped one still ends up with the current value.
func TestCompactionDropsSupersededRecordsRegardlessOfCursors(t *testing.T) {
	s := defStore(t)
	first := write(t, s, "01HGRP-OPS", `{"id":"01HGRP-OPS","v":1}`)
	second := write(t, s, "01HGRP-OPS", `{"id":"01HGRP-OPS","v":2}`)
	other := write(t, s, "01HGRP-VIEW", `{"id":"01HGRP-VIEW"}`)
	// A cursor parked at the very beginning — behind everything.
	s.CursorAck("downlink-def:n-child", "definitions", 1)

	st, err := s.Compact("definitions")
	if err != nil {
		t.Fatal(err)
	}
	if st.Superseded != 1 {
		t.Fatalf("superseded = %d, want 1", st.Superseded)
	}
	got := offsets(t, s)
	if len(got) != 2 || got[0] != second || got[1] != other {
		t.Fatalf("surviving offsets = %v, want the latest of each topic (%d, %d)", got, second, other)
	}
	_ = first
	if st.Bytes == 0 {
		t.Fatal("compaction reclaimed no bytes")
	}
}

// A topic nobody superseded is untouched, however old it is: compaction is
// per-topic, not per-age.
func TestCompactionLeavesTheOnlyRecordOfATopicAlone(t *testing.T) {
	s := defStore(t)
	only := write(t, s, "01HGRP-OPS", `{"id":"01HGRP-OPS"}`)
	for i := 0; i < 5; i++ {
		write(t, s, "01HGRP-NOISE", `{"id":"01HGRP-NOISE"}`)
	}

	if _, err := s.Compact("definitions"); err != nil {
		t.Fatal(err)
	}
	got := offsets(t, s)
	if len(got) != 2 || got[0] != only {
		t.Fatalf("surviving offsets = %v, want the untouched %d plus one noise record", got, only)
	}
}

// The rule that is a floor and not an optimisation: a consumer behind a dropped
// tombstone would never learn the definition was retracted, and it already holds
// the value being retracted — so it would keep a withdrawn group forever.
func TestCompactionKeepsATombstoneUntilEveryCursorHasPassedIt(t *testing.T) {
	s := defStore(t)
	write(t, s, "01HGRP-OPS", `{"id":"01HGRP-OPS"}`)
	tomb := write(t, s, "01HGRP-OPS", "") // retracted

	// A child that has not read the retraction yet.
	s.CursorAck("downlink-def:n-child", "definitions", tomb)

	st, err := s.Compact("definitions")
	if err != nil {
		t.Fatal(err)
	}
	if st.Tombstones != 0 {
		t.Fatalf("tombstones dropped = %d — a child has not read the retraction yet", st.Tombstones)
	}
	if got := offsets(t, s); len(got) != 1 || got[0] != tomb {
		t.Fatalf("surviving offsets = %v, want the retraction %d to remain", got, tomb)
	}

	// The child reads past it: now the retraction has done its job everywhere.
	s.CursorAck("downlink-def:n-child", "definitions", tomb+1)
	st, err = s.Compact("definitions")
	if err != nil {
		t.Fatal(err)
	}
	if st.Tombstones != 1 {
		t.Fatalf("tombstones dropped = %d, want 1 once every cursor is past it", st.Tombstones)
	}
	if got := offsets(t, s); len(got) != 0 {
		t.Fatalf("surviving offsets = %v, want the topic gone entirely", got)
	}
}

// A stream with no cursors at all has no consumer to protect, so a retraction
// that has done its local job may go.
func TestCompactionDropsATombstoneWhenAStreamHasNoCursors(t *testing.T) {
	s := defStore(t)
	write(t, s, "01HGRP-OPS", `{"id":"01HGRP-OPS"}`)
	write(t, s, "01HGRP-OPS", "")

	st, err := s.Compact("definitions")
	if err != nil {
		t.Fatal(err)
	}
	if st.Tombstones != 1 || st.Superseded != 1 {
		t.Fatalf("stats = %+v, want the value and its retraction both gone", st)
	}
}

// Compaction reports nothing to the gap contract. A pruned metric is
// information nobody will ever see again; a compacted definition still exists,
// in the record that superseded it.
func TestCompactionOpensNoGap(t *testing.T) {
	s := defStore(t)
	write(t, s, "01HGRP-OPS", `{"id":"01HGRP-OPS","v":1}`)
	write(t, s, "01HGRP-OPS", `{"id":"01HGRP-OPS","v":2}`)

	if _, err := s.Compact("definitions"); err != nil {
		t.Fatal(err)
	}
	if _, hasGap := s.Gap("definitions", 1); hasGap {
		t.Fatal("compaction must not report a gap — nothing was lost, only superseded")
	}
	if lwm := s.LWM("definitions"); lwm != 1 {
		t.Fatalf("LWM = %d, want 1 — compaction leaves the low-water mark alone", lwm)
	}
}

// Idempotent: a second pass over an already-compacted stream changes nothing.
func TestCompactionIsIdempotent(t *testing.T) {
	s := defStore(t)
	write(t, s, "01HGRP-OPS", `{"id":"01HGRP-OPS","v":1}`)
	write(t, s, "01HGRP-OPS", `{"id":"01HGRP-OPS","v":2}`)
	if _, err := s.Compact("definitions"); err != nil {
		t.Fatal(err)
	}
	before := offsets(t, s)

	st, err := s.Compact("definitions")
	if err != nil {
		t.Fatal(err)
	}
	if st.Superseded != 0 || st.Tombstones != 0 || st.Bytes != 0 {
		t.Fatalf("second pass = %+v, want a no-op", st)
	}
	if got := offsets(t, s); len(got) != len(before) {
		t.Fatalf("second pass changed the stream: %v → %v", before, got)
	}
}

func TestCompactionRejectsAnUnknownStream(t *testing.T) {
	s := defStore(t)
	if _, err := s.Compact("nosuchstream"); err == nil {
		t.Fatal("an unknown stream must be an error, not a silent no-op")
	}
}
