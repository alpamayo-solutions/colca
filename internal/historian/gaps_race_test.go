package historian

import (
	"context"
	"encoding/json"
	"sync"
	"testing"

	"github.com/alpamayo-solutions/colca/door"
)

// The follow loop writes the gap counter while /metrics reads it, so it must be
// atomic. With a plain int64 this fails under -race, which is how the suite
// runs.
func TestTheGapCounterIsSafeToScrapeWhileTheBridgeRuns(t *testing.T) {
	const pages = 200
	d := &fakeDoor{}
	for i := 0; i < pages; i++ {
		off := int64(i + 1)
		d.pages = append(d.pages, page(off+1, door.Record{
			Offset: off, Topic: "colca/v1/_StreamGap/n1/metrics",
			Payload: json.RawMessage(`{}`), TS: 1,
		}))
	}
	bridge := &Bridge{Door: d, Store: &fakeStore{}}

	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { // the scrape
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			default:
				_ = bridge.Gaps()
			}
		}
	}()

	for i := 0; i < pages; i++ { // the follow loop
		if _, err := bridge.Once(context.Background()); err != nil {
			close(done)
			wg.Wait()
			t.Fatalf("Once: %v", err)
		}
	}
	close(done)
	wg.Wait()

	// Denominator: without this the reader could be racing a counter that
	// never moves, and a broken fixture would look like a clean run.
	if got := bridge.Gaps(); got != pages {
		t.Fatalf("Gaps = %d, want %d — the writer never ran, so nothing was raced", got, pages)
	}
}
