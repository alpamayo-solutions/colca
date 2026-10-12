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

// A store whose history may have regressed takes a new incarnation: the id
// changes for good, the reason is kept, and whoever waits for the change wakes.
func TestReincarnateGivesTheStoreANewIDThatSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	first := s.StoreID()
	if _, ok := s.LastIncarnationChange(); ok {
		t.Fatal("a store that never changed incarnation reports a change")
	}
	changed := s.IncarnationChanges()
	select {
	case <-changed:
		t.Fatal("the change channel is closed before any change")
	default:
	}

	second, err := s.Reincarnate("restored from a backup")
	if err != nil {
		t.Fatal(err)
	}
	if second == first || len(second) != 32 || s.StoreID() != second {
		t.Fatalf("after Reincarnate: id %q, StoreID %q, was %q — want a new 32-char id in force", second, s.StoreID(), first)
	}
	select {
	case <-changed:
	default:
		t.Fatal("the change channel was not closed by the change")
	}
	change, ok := s.LastIncarnationChange()
	if !ok || change.Previous != first || change.Reason != "restored from a backup" || change.AtMS == 0 {
		t.Fatalf("recorded change = %+v ok=%v, want previous %q and the reason", change, ok, first)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got := s.StoreID(); got != second {
		t.Fatalf("store id after reopen = %q, want the new incarnation %q", got, second)
	}
	if change, ok := s.LastIncarnationChange(); !ok || change.Previous != first {
		t.Fatalf("recorded change after reopen = %+v ok=%v", change, ok)
	}
}

// Several children prove the same restore at once. Each names the incarnation
// it holds the proof against; only the first changes it.
func TestReincarnateFromChangesTheIDOnlyOnce(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	first := s.StoreID()
	second, changed, err := s.ReincarnateFrom(first, "child a")
	if err != nil || !changed || second == first {
		t.Fatalf("first proof: id=%q changed=%v err=%v, want a new id", second, changed, err)
	}
	again, changed, err := s.ReincarnateFrom(first, "child b")
	if err != nil || changed || again != second {
		t.Fatalf("second proof against the old id: id=%q changed=%v err=%v, want %q unchanged", again, changed, err, second)
	}
	if change, _ := s.LastIncarnationChange(); change.Reason != "child a" {
		t.Fatalf("recorded reason = %q, want the first proof's", change.Reason)
	}
}

// What a parent confirmed names offsets of the incarnation it confirmed them
// for. A store that takes a new one must not hold them against that parent.
func TestReincarnateForgetsWhatParentsConfirmed(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if moved, err := s.CursorAckMarked("up:p", "metrics", 8, "p", 7); err != nil || !moved {
		t.Fatalf("CursorAckMarked: moved=%v err=%v", moved, err)
	}
	if got := s.AckedMark("p", "metrics"); got != 7 {
		t.Fatalf("mark = %d, want 7", got)
	}
	if _, err := s.Reincarnate("restored"); err != nil {
		t.Fatal(err)
	}
	if got := s.AckedMark("p", "metrics"); got != 0 {
		t.Fatalf("mark after Reincarnate = %d, want 0", got)
	}
	if got := s.CursorGet("up:p", "metrics"); got != 8 {
		t.Fatalf("uplink cursor after Reincarnate = %d, want 8 untouched", got)
	}
}

// The mark follows what the parent said, in the same write as the cursor, and
// the cursor still only moves forward.
func TestCursorAckMarkedMovesTheCursorForwardAndSetsTheMark(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if moved, err := s.CursorAckMarked("up:p", "metrics", 10, "p", 9); err != nil || !moved {
		t.Fatalf("first: moved=%v err=%v", moved, err)
	}
	// A page of records that are not pushed moved the cursor on; the parent
	// confirmed nothing new.
	s.CursorAck("up:p", "metrics", 14)
	if moved, err := s.CursorAckMarked("up:p", "metrics", 12, "p", 11); err != nil || moved {
		t.Fatalf("behind the cursor: moved=%v err=%v, want the cursor left alone", moved, err)
	}
	if cur, mark := s.CursorGet("up:p", "metrics"), s.AckedMark("p", "metrics"); cur != 14 || mark != 11 {
		t.Fatalf("cursor=%d mark=%d, want 14 and 11", cur, mark)
	}
	if mark := s.AckedMark("other", "metrics"); mark != 0 {
		t.Fatalf("another parent's mark = %d, want 0", mark)
	}
}

// A re-sync places cursors where the parent's store stands, which is behind
// where they stood. CursorAck cannot do that; ResyncParent does, with the marks
// and the record of the re-sync in the same batch.
func TestResyncParentPlacesCursorsBackwards(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	s.CursorAck("up:p", "metrics", 40)
	s.CursorAck("down:p", "commands-parent", 90)
	s.CursorAck("up:p", "entities", 12)
	if s.CursorAck("up:p", "metrics", 20) {
		t.Fatal("CursorAck moved a cursor backwards — the precondition of this test is that it cannot")
	}
	err = s.ResyncParent("p", []byte(`{"store":"new"}`), []CursorPlacement{
		{Name: "up:p", Stream: "metrics", To: 20},
		{Name: "down:p", Stream: "commands-parent", To: 3},
	}, map[string]uint64{"metrics": 19, "entities": 11})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []struct {
		name, stream string
		pos          uint64
	}{{"up:p", "metrics", 20}, {"down:p", "commands-parent", 3}, {"up:p", "entities", 12}} {
		if got := s.CursorGet(want.name, want.stream); got != want.pos {
			t.Fatalf("cursor %s/%s = %d, want %d", want.name, want.stream, got, want.pos)
		}
	}
	if m, e := s.AckedMark("p", "metrics"), s.AckedMark("p", "entities"); m != 19 || e != 11 {
		t.Fatalf("marks = %d, %d, want 19 and 11", m, e)
	}
	state, err := s.ParentState("p")
	if err != nil || string(state) != `{"store":"new"}` {
		t.Fatalf("parent state = %q err=%v", state, err)
	}
	if state, err := s.ParentState("unknown"); err != nil || state != nil {
		t.Fatalf("state of an unknown parent = %q err=%v, want none", state, err)
	}
}

// Only a store opened with an id it already had can be a restored copy. An id
// minted in this process, with a new store or by Reincarnate, names a history
// that only grew since.
func TestOnlyAStoreOpenedWithItsIDMayHaveRegressed(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if s.MayHaveRegressed() {
		t.Fatal("a store created in this process may have regressed")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if !s.MayHaveRegressed() {
		t.Fatal("a store opened with the id it had cannot have regressed — a restored copy looks exactly like it")
	}
	if _, err := s.Reincarnate("restored"); err != nil {
		t.Fatal(err)
	}
	if s.MayHaveRegressed() {
		t.Fatal("a store may have regressed under an incarnation it just took")
	}
}
