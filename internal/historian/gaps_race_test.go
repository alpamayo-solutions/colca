package historian

import (
	"context"
	"encoding/json"
	"sync"
	"testing"

	"github.com/alpamayo-solutions/colca/door"
)

// The gap counter is written by the follow loop and read by the /metrics
// handler (cmd/colca-historian serves the scrape from its own goroutine while
// the bridge runs). Those are two goroutines on one counter, so it has to be
// atomic — a plain int64 there is a data race whatever number the scrape
// happens to print.
//
// This is the exact production shape: one writer calling Once, one reader
// scraping, no synchronisation between them. It only ever fails under -race,
// which is how the suite runs (`make test`) — mutation-checked by putting the field back to a plain int64, which
// makes it fail there.
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
