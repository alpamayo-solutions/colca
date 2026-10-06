package store

import (
	"sync"
	"testing"
)

// The marks a parent answers from memory are the durable ones: a reopened store
// reads the same values from disk, and a reader racing the commits never sees a
// mark go backwards (which would make the parent apply a batch twice).
func TestHWMsServedFromMemoryMatchTheDurableMarks(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	const pushes = 200
	var wg sync.WaitGroup
	stop := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		var last uint64
		for {
			select {
			case <-stop:
				return
			default:
			}
			got := s.HWMGet("n-a", "metrics")
			if got < last {
				t.Errorf("HWM went back from %d to %d", last, got)
				return
			}
			last = got
		}
	}()
	for off := uint64(1); off <= pushes; off++ {
		if _, hwm, err := s.ApplyReplicated("n-a", "metrics", []ReplRecord{replRec("n-a", off)}); err != nil || hwm != off {
			t.Fatalf("push %d: hwm %d err %v", off, hwm, err)
		}
		// A redelivered batch applies nothing, also when answered from memory.
		if applied, _, err := s.ApplyReplicated("n-a", "metrics", []ReplRecord{replRec("n-a", off)}); err != nil || len(applied) != 0 {
			t.Fatalf("redelivered push %d applied %d err %v", off, len(applied), err)
		}
	}
	close(stop)
	wg.Wait()
	if got := s.NextOffset("metrics"); got != pushes+1 {
		t.Fatalf("metrics next = %d, want %d", got, pushes+1)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if got := s.HWMGet("n-a", "metrics"); got != pushes {
		t.Fatalf("HWM after reopen = %d, want %d", got, pushes)
	}
	if applied, _, err := s.ApplyReplicated("n-a", "metrics", []ReplRecord{replRec("n-a", pushes)}); err != nil || len(applied) != 0 {
		t.Fatalf("a batch redelivered after a restart applied %d err %v", len(applied), err)
	}
}
