package historian

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/alpamayo-solutions/colca/door"
)

// streamDoor serves a metrics stream of n records from a cursor, and reads
// ahead of it when asked (door.Client's FetchWithOptions with From).
type streamDoor struct {
	mu      sync.Mutex
	n       int64
	cursor  int64 // next offset the cursor reads
	fetches []string
	acked   []int64
	// writing is set while the store writes a page; a fetch that arrives then
	// is a read-ahead.
	writing   bool
	overlaps  int
	fetchHook chan struct{}
}

func (d *streamDoor) serve(from int64, limit int) door.Page {
	var recs []door.Record
	for off := from; off <= d.n && len(recs) < limit; off++ {
		recs = append(recs, record(off, fmt.Sprintf(`{"signal_id":"s1","value":%d,"timestamp":%d}`, off, off)))
	}
	return door.Page{Records: recs, Next: from + int64(len(recs))}
}

func (d *streamDoor) Fetch(_ context.Context, _, _ string, limit int) (door.Page, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.fetches = append(d.fetches, fmt.Sprintf("cursor@%d", d.cursor))
	if d.fetchHook != nil {
		d.fetchHook <- struct{}{}
	}
	return d.serve(d.cursor, limit), nil
}

func (d *streamDoor) FetchWithOptions(_ context.Context, o door.FetchOptions) (door.Page, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.fetches = append(d.fetches, fmt.Sprintf("from@%d", o.From))
	if d.writing {
		d.overlaps++
	}
	if d.fetchHook != nil {
		d.fetchHook <- struct{}{}
	}
	return d.serve(int64(o.From), o.Max), nil //nolint:gosec // test offsets
}

func (d *streamDoor) Ack(_ context.Context, _, _ string, offset int64) (bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.acked = append(d.acked, offset)
	d.cursor = offset + 1
	return true, nil
}

// orderedStore records the offsets it wrote and how often the marker was read.
type orderedStore struct {
	door    *streamDoor
	mu      sync.Mutex
	applied int64
	reads   int
	values  []int64
	failAt  int // the failAt-th Apply fails once (1-based), 0 never
	applies int
}

func (s *orderedStore) Applied(context.Context, string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reads++
	return s.applied, nil
}

func (s *orderedStore) Apply(_ context.Context, rows []Row, _ string, offset int64) ([]Rejection, error) {
	s.door.mu.Lock()
	s.door.writing = true
	s.door.mu.Unlock()
	time.Sleep(5 * time.Millisecond) // a write long enough for the read-ahead to arrive in it
	s.door.mu.Lock()
	s.door.writing = false
	s.door.mu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.applies++
	if s.applies == s.failAt {
		return nil, errors.New("timescale went away")
	}
	for _, r := range rows {
		s.values = append(s.values, int64(*r.Number))
	}
	s.applied = offset
	return nil, nil
}

func runUntilDrained(t *testing.T, b *Bridge, d *streamDoor, n int64) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- b.Run(ctx) }()
	deadline := time.Now().Add(5 * time.Second)
	for {
		d.mu.Lock()
		acked := len(d.acked) > 0 && d.acked[len(d.acked)-1] == n
		d.mu.Unlock()
		if acked {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("never acked %d: fetches %v acked %v", n, d.fetches, d.acked)
		}
		time.Sleep(time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond) // anything the bridge still does once drained
	cancel()
	<-done
}

// A backlog is written in order, every record once, with the next page read
// while the current one is written, and the marker read from the database
// once, not per page.
func TestABacklogIsReadAheadWhileEachPageIsWritten(t *testing.T) {
	d := &streamDoor{n: 10, cursor: 1}
	store := &orderedStore{door: d}
	var signal door.Signal
	b := &Bridge{Door: d, Store: store, Max: 3, Changes: signal.Changes}
	runUntilDrained(t, b, d, 10)

	if fmt.Sprint(store.values) != "[1 2 3 4 5 6 7 8 9 10]" {
		t.Fatalf("written %v, want 1..10 once each, in order", store.values)
	}
	if d.overlaps == 0 {
		t.Fatalf("no page was read while another was written: fetches %v", d.fetches)
	}
	if store.reads != 1 {
		t.Fatalf("the marker was read %d times, want once", store.reads)
	}
	if fmt.Sprint(d.acked) != "[3 6 9 10]" {
		t.Fatalf("acked %v, want each page after its write", d.acked)
	}
}

// A failed write drops the page read ahead of it: the failed page comes back
// from the cursor, nothing is skipped, and the marker is read again.
func TestAFailedWriteDropsTheReadAheadAndRereadsFromTheCursor(t *testing.T) {
	d := &streamDoor{n: 9, cursor: 1}
	store := &orderedStore{door: d, failAt: 2}
	var signal door.Signal
	b := &Bridge{Door: d, Store: store, Max: 3, Changes: signal.Changes}
	runUntilDrained(t, b, d, 9)

	if fmt.Sprint(store.values) != "[1 2 3 4 5 6 7 8 9]" {
		t.Fatalf("written %v, want 1..9 once each, in order", store.values)
	}
	if store.reads != 2 {
		t.Fatalf("the marker was read %d times, want twice (start, after the failure)", store.reads)
	}
	sawRetry := false
	for _, f := range d.fetches {
		if f == "cursor@4" {
			sawRetry = true
		}
	}
	if !sawRetry {
		t.Fatalf("the failed page was not fetched again from the cursor: %v", d.fetches)
	}
}

// With the head the node announced, a drain that reached it waits for the next
// hint instead of fetching an empty page to find out.
func TestADrainThatReachedTheAnnouncedHeadNeedsNoEmptyFetch(t *testing.T) {
	d := &streamDoor{n: 2, cursor: 1, fetchHook: make(chan struct{}, 16)}
	store := &orderedStore{door: d}
	var signal door.Signal
	b := &Bridge{Door: d, Store: store, Max: 3, Changes: signal.Changes, Head: func() int64 { return 3 }}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- b.Run(ctx) }()
	select {
	case <-d.fetchHook:
	case <-time.After(2 * time.Second):
		t.Fatal("no first fetch")
	}
	select {
	case <-d.fetchHook:
		t.Fatalf("fetched again after reaching the announced head: %v", d.fetches)
	case <-time.After(150 * time.Millisecond):
	}
	// A new record and its hint wake the next drain.
	d.mu.Lock()
	d.n = 3
	d.mu.Unlock()
	b.Head = func() int64 { return 4 }
	signal.Notify()
	select {
	case <-d.fetchHook:
	case <-time.After(2 * time.Second):
		t.Fatal("a hint did not wake the next drain")
	}
	cancel()
	<-done
	if fmt.Sprint(store.values) != "[1 2 3]" {
		t.Fatalf("written %v", store.values)
	}
}
