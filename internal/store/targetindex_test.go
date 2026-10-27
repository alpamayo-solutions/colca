package store

import (
	"context"
	"fmt"
	"testing"
)

func cmdRecord(target string, i int) Record {
	return Record{
		Topic:   "colca/v1/_CmdParam/" + target + "/m/go",
		Payload: []byte(fmt.Sprintf(`{"correlation_id":"c%d","expires_at":99999999999}`, i)),
		TS:      int64(i) + 1,
	}
}

// A machine's commands are read through the target index: the records of other
// machines are never visited (here they would not even decode), and next jumps
// over them to the end of the range.
func TestReadTargetVisitsOnlyTheTargetsCommands(t *testing.T) {
	s := mustOpen(t)
	if _, _, err := s.Append("commands", []Record{cmdRecord("m1", 0)}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 50; i++ {
		putHeadOnly(t, s, "commands", "colca/v1/_CmdParam/m2/m/go", int64(i))
	}
	if _, _, err := s.Append("commands", []Record{cmdRecord("m1", 1), {Topic: "colca/v1/_Ack/m1/m/go", Payload: []byte(`{}`), TS: 1}}); err != nil {
		t.Fatal(err)
	}
	head := s.NextOffset("commands")
	got, next, err := s.ReadTarget(context.Background(), "commands", "m1", 1, head, 10, 2, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Offset != 1 || got[1].Offset != 52 || next != head {
		t.Fatalf("got %+v next=%d, want m1's commands 1 and 52 within a budget of 2 and next at the head %d", got, next, head)
	}
	// A limit of one stops after the first, and next resumes behind it.
	got, next, err = s.ReadTarget(context.Background(), "commands", "m1", 1, head, 1, 0, nil)
	if err != nil || len(got) != 1 || next != 2 {
		t.Fatalf("limit 1: %+v next=%d err=%v, want record 1 and next 2", got, next, err)
	}
	// upTo ends the read: nothing of m1 in [2, 52).
	got, next, err = s.ReadTarget(context.Background(), "commands", "m1", 2, 52, 10, 0, nil)
	if err != nil || len(got) != 0 || next != 52 {
		t.Fatalf("[2,52): %+v next=%d err=%v, want nothing and next 52", got, next, err)
	}
}

// Below the index's coverage the read scans, for the target only, and then
// switches to the index; a prune takes the index entries with its records.
func TestReadTargetBelowTheCoverageAndAfterAPrune(t *testing.T) {
	s := mustOpen(t)
	if _, _, err := s.Append("commands", []Record{cmdRecord("m1", 0), cmdRecord("m2", 1), cmdRecord("m1", 2)}); err != nil {
		t.Fatal(err)
	}
	s.state.Lock()
	s.targetFrom["commands"] = 3 // records 1 and 2 as a version without the index wrote them
	s.state.Unlock()
	if _, _, err := s.Append("commands", []Record{cmdRecord("m2", 3), cmdRecord("m1", 4)}); err != nil {
		t.Fatal(err)
	}
	got, next, err := s.ReadTarget(context.Background(), "commands", "m1", 1, 6, 10, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	var offs []uint64
	for _, r := range got {
		offs = append(offs, r.Offset)
	}
	if fmt.Sprint(offs) != "[1 3 5]" || next != 6 {
		t.Fatalf("offsets %v next=%d, want [1 3 5] and 6", offs, next)
	}
	if _, err := s.Prune("commands", 4, nil, nil); err != nil {
		t.Fatal(err)
	}
	if n := countKeys(t, s, "ti\x00commands\x00"); n != 2 {
		t.Fatalf("%d target index entries after the prune, want 2 (records 4 and 5)", n)
	}
	got, _, err = s.ReadTarget(context.Background(), "commands", "m1", 1, 6, 10, 0, nil)
	if err != nil || len(got) != 1 || got[0].Offset != 5 {
		t.Fatalf("after the prune: %+v err=%v, want record 5", got, err)
	}
}

func countKeys(t *testing.T, s *Store, prefix string) int {
	t.Helper()
	iter, err := s.db.NewIter(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer iter.Close()
	n := 0
	for valid := iter.SeekGE([]byte(prefix)); valid && len(iter.Key()) >= len(prefix) && string(iter.Key()[:len(prefix)]) == prefix; valid = iter.Next() {
		n++
	}
	return n
}

// A store a version without the target index wrote holds commands without
// entries: the coverage starts at the head the first time this version opens
// it, and a read from below finds the old commands by scanning.
func TestTheTargetIndexCoverageStartsWhereThisVersionFirstOpened(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Append("commands", []Record{cmdRecord("m1", 0), cmdRecord("m2", 1)}); err != nil {
		t.Fatal(err)
	}
	// What a version without the index leaves: no entries, no tf/.
	b := s.db.NewBatch()
	for _, key := range [][]byte{targetKey("commands", "m1", 1), targetKey("commands", "m2", 2), targetFromKey("commands")} {
		if err := b.Delete(key, nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.Commit(nil); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if s, err = Open(dir); err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if covered := s.targetFrom["commands"]; covered != 3 {
		t.Fatalf("target coverage starts at %d, want the head 3", covered)
	}
	if covered := s.sigFrom["commands"]; covered != 1 {
		t.Fatalf("signal coverage starts at %d, want 1: it is not the target index's", covered)
	}
	if _, _, err := s.Append("commands", []Record{cmdRecord("m1", 2)}); err != nil {
		t.Fatal(err)
	}
	got, _, err := s.ReadTarget(context.Background(), "commands", "m1", 1, 10, 10, 0, nil)
	if err != nil || len(got) != 2 || got[0].Offset != 1 || got[1].Offset != 3 {
		t.Fatalf("got %+v err=%v, want the old command 1 and the new 3", got, err)
	}
}
