package store

import (
	"testing"
)

// Registry family tests (auth design §2.2): one atomic batch writes the
// r/{ulid} entry together with its _EdgeNode entity record. Atomicity is by
// construction — RegistryPut/RegistryDelete build ONE pebble batch applied
// with Sync. Mutation check (documented, not automated): splitting either
// method into two Apply calls makes the reopen assertions below racy against
// a crash between them; the singular batch is asserted structurally by these
// tests plus review of the implementation.

func testEntryJSON(ulid string) []byte {
	return []byte(`{"ulid":"` + ulid + `","pubkey":"abab","kind":"machine","mount":"z/` + ulid + `"}`)
}

func entityRec(ulid string) Record {
	return Record{
		Topic:   "colca/v1/_EdgeNode/" + ulid + "/z/" + ulid,
		Payload: testEntryJSON(ulid),
		TS:      42,
		KVPath:  "z/" + ulid,
		KVNode:  ulid,
	}
}

func TestRegistryPutScanAndReopen(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	off, err := s.RegistryPut("01M1", testEntryJSON("01M1"), "entities", entityRec("01M1"))
	if err != nil {
		t.Fatalf("RegistryPut: %v", err)
	}
	if off != 1 {
		t.Fatalf("first entity offset = %d, want 1", off)
	}
	if _, err := s.RegistryPut("01M2", testEntryJSON("01M2"), "entities", entityRec("01M2")); err != nil {
		t.Fatal(err)
	}

	scan := s.RegistryScan()
	if len(scan) != 2 || string(scan["01M1"]) != string(testEntryJSON("01M1")) {
		t.Fatalf("RegistryScan = %v entries, want 2 with intact JSON", len(scan))
	}

	// The entity record itself landed in the stream with its KV projection.
	recs, _, err := s.Read("entities", off, 1, nil)
	if err != nil || len(recs) != 1 || recs[0].Topic != entityRec("01M1").Topic {
		t.Fatalf("entity record not in stream: recs=%v err=%v", recs, err)
	}
	kv := s.KVScan("z/01M1")
	if len(kv) != 1 || kv[0].NodeID != "01M1" {
		t.Fatalf("KV projection missing: %v", kv)
	}

	// Everything survives reopen: r/ entries, offsets, byte accounting.
	bytesBefore := s.StreamBytes("entities")
	nextBefore := s.NextOffset("entities")
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if got := s2.RegistryScan(); len(got) != 2 {
		t.Fatalf("after reopen RegistryScan = %d entries, want 2", len(got))
	}
	if s2.NextOffset("entities") != nextBefore || s2.StreamBytes("entities") != bytesBefore {
		t.Fatalf("counters drifted across reopen: next %d→%d bytes %d→%d",
			nextBefore, s2.NextOffset("entities"), bytesBefore, s2.StreamBytes("entities"))
	}
	if bytesBefore == 0 {
		t.Fatal("byte accounting did not track RegistryPut appends")
	}
}

func TestRegistryPutUnknownStream(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := s.RegistryPut("01M1", testEntryJSON("01M1"), "nosuch", entityRec("01M1")); err == nil {
		t.Fatal("RegistryPut on unknown stream must fail")
	}
}

func TestRegistryDelete(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.RegistryPut("01M1", testEntryJSON("01M1"), "entities", entityRec("01M1")); err != nil {
		t.Fatal(err)
	}
	tomb := Record{Topic: entityRec("01M1").Topic, Payload: nil, TS: 43, KVPath: "z/01M1", KVNode: "01M1"}
	off, err := s.RegistryDelete("01M1", "entities", tomb)
	if err != nil {
		t.Fatalf("RegistryDelete: %v", err)
	}
	if off != 2 {
		t.Fatalf("tombstone offset = %d, want 2", off)
	}
	if got := s.RegistryScan(); len(got) != 0 {
		t.Fatalf("registry entry survived delete: %v", got)
	}
	// The identity's KV projection is retired in the same batch — a restart's
	// retained-set reseed must not resurrect a revoked identity.
	if kv := s.KVScan("z/01M1"); len(kv) != 0 {
		t.Fatalf("KV entry survived revocation: %v", kv)
	}
	// The tombstone record is in the stream (history keeps the retirement).
	recs, _, err := s.Read("entities", off, 1, nil)
	if err != nil || len(recs) != 1 || len(recs[0].Payload) != 0 {
		t.Fatalf("tombstone record missing or has payload: %v err=%v", recs, err)
	}
	// Deletion state survives reopen.
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if got := s2.RegistryScan(); len(got) != 0 {
		t.Fatalf("registry entry resurrected after reopen: %v", got)
	}
}
