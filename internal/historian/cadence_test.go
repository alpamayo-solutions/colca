package historian

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/alpamayo-solutions/colca/door"
)

// countingDoor serves pages in order and signals every fetch.
type countingDoor struct {
	mu      sync.Mutex
	pages   []door.Page
	fetches chan struct{}
}

func (d *countingDoor) Fetch(context.Context, string, string, int) (door.Page, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.fetches <- struct{}{}
	if len(d.pages) == 0 {
		return door.Page{}, nil
	}
	p := d.pages[0]
	d.pages = d.pages[1:]
	return p, nil
}

func (d *countingDoor) Ack(context.Context, string, string, int64) (bool, error) { return true, nil }

func runCadence(t *testing.T, pages []door.Page, wantImmediate int) {
	t.Helper()
	d := &countingDoor{pages: pages, fetches: make(chan struct{}, 16)}
	var signal door.Signal
	bridge := &Bridge{Door: d, Store: &fakeStore{}, Max: 2, Changes: signal.Changes}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- bridge.Run(ctx) }()

	for i := 0; i < wantImmediate; i++ {
		select {
		case <-d.fetches:
		case <-time.After(2 * time.Second):
			t.Fatalf("fetch %d did not follow at once", i+1)
		}
	}
	select {
	case <-d.fetches:
		t.Fatalf("fetched again after a page that was not full")
	case <-time.After(100 * time.Millisecond):
	}
	cancel()
	<-done
}

func TestRunDrainsFullAndPartialPagesWithoutWaiting(t *testing.T) {
	runCadence(t, []door.Page{
		page(3, record(1, `{"signal_id":"s1","value":1}`), record(2, `{"signal_id":"s1","value":2}`)),
		page(4, record(3, `{"signal_id":"s1","value":3}`)),
		page(5, record(4, `{"signal_id":"s1","value":4}`)),
	}, 4)
}

func TestRunDrainsPartialPagesBeforeWaiting(t *testing.T) {
	runCadence(t, []door.Page{
		page(2, record(1, `{"signal_id":"s1","value":1}`)),
		page(3, record(2, `{"signal_id":"s1","value":2}`)),
	}, 3)
}

func TestRunFollowsAFullPageOfSkippedRecordsAtOnce(t *testing.T) {
	// Tombstones write nothing, but a full page of them still means more waits.
	runCadence(t, []door.Page{
		page(3, record(1, `null`), record(2, `null`)),
		page(4, record(3, `{"signal_id":"s1","value":3}`)),
	}, 3)
}

func TestCoordinatedRunWaitsForWakeWithoutEmptyFetches(t *testing.T) {
	d := &countingDoor{fetches: make(chan struct{}, 16)}
	checks := make(chan struct{}, 16)
	wake := make(chan struct{}, 1)
	b := &Bridge{Door: d, Store: &fakeStore{}, Coordinate: func(context.Context) (bool, error) { checks <- struct{}{}; return false, nil },
		WaitCoordinate: func(ctx context.Context) {
			select {
			case <-ctx.Done():
			case <-wake:
			}
		}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- b.Run(ctx) }()
	<-checks
	select {
	case <-checks:
		t.Fatal("polled coordination without a wakeup")
	case <-time.After(40 * time.Millisecond):
	}
	wake <- struct{}{}
	select {
	case <-checks:
	case <-time.After(time.Second):
		t.Fatal("missed commit wakeup")
	}
	cancel()
	<-done
	if len(d.fetches) != 0 {
		t.Fatal("fetched ahead of upstream completion")
	}
}
