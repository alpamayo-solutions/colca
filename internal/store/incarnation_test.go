package store

import "testing"

// A store's incarnation survives restarts and is new for a store built from
// nothing; that is the whole distinction a parent needs.
func TestStoreIDIsStableAcrossReopenAndNewForANewStore(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	first := s.StoreID()
	if len(first) != 32 {
		t.Fatalf("store id %q, want 32 hex chars", first)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got := s.StoreID(); got != first {
		t.Fatalf("store id after reopen = %q, want %q", got, first)
	}
	other, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if other.StoreID() == first {
		t.Fatal("a store built from nothing reused another store's id")
	}
}

// A child whose store was rebuilt restarts its offsets at 1. Against the marks
// kept for its old store every record was dropped as a duplicate and the reply
// told the child it had landed: its whole plant model never reached the parent.
func TestARebuiltChildStoreStartsItsReplicationClean(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if reset, err := s.AdoptChildStore("01CHILD", "old-store"); err != nil || reset {
		t.Fatalf("first incarnation: reset=%v err=%v, want no reset", reset, err)
	}
	for _, stream := range []string{"entities", "metrics"} {
		if _, _, err := s.ApplyReplicated("01CHILD", stream, []ReplRecord{{ChildOffset: 900, Topic: "prekit/v1/_Signal/01CHILD/old"}}); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, err := s.ApplyReplicated("01OTHER", "entities", []ReplRecord{{ChildOffset: 7, Topic: "prekit/v1/_Signal/01OTHER/x"}}); err != nil {
		t.Fatal(err)
	}

	reset, err := s.AdoptChildStore("01CHILD", "new-store")
	if err != nil || !reset {
		t.Fatalf("rebuilt store: reset=%v err=%v, want reset", reset, err)
	}
	for _, stream := range []string{"entities", "metrics"} {
		if got := s.HWMGet("01CHILD", stream); got != 0 {
			t.Fatalf("HWM(%s) after rebuild = %d, want 0", stream, got)
		}
	}
	if got := s.HWMGet("01OTHER", "entities"); got != 7 {
		t.Fatalf("sibling HWM = %d, want 7 untouched", got)
	}
	applied, hwm, err := s.ApplyReplicated("01CHILD", "entities", []ReplRecord{{ChildOffset: 1, Topic: "prekit/v1/_Signal/01CHILD/new"}})
	if err != nil || len(applied) != 1 || hwm != 1 {
		t.Fatalf("rebuilt child's first record: applied=%d hwm=%d err=%v, want 1/1", len(applied), hwm, err)
	}
}

// The same store coming back (a restart, a revoke and enroll) keeps its marks:
// they are what stop a resumed child's re-offered records from applying twice.
func TestTheSameChildStoreKeepsItsMarks(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.AdoptChildStore("01CHILD", "store-a"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.ApplyReplicated("01CHILD", "entities", []ReplRecord{{ChildOffset: 5, Topic: "prekit/v1/_Signal/01CHILD/a"}}); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"store-a", ""} {
		if reset, err := s.AdoptChildStore("01CHILD", id); err != nil || reset {
			t.Fatalf("AdoptChildStore(%q): reset=%v err=%v, want no reset", id, reset, err)
		}
	}
	applied, _, err := s.ApplyReplicated("01CHILD", "entities", []ReplRecord{{ChildOffset: 5, Topic: "prekit/v1/_Signal/01CHILD/a"}})
	if err != nil || len(applied) != 0 {
		t.Fatalf("re-offered record applied=%d err=%v, want deduped", len(applied), err)
	}
}
