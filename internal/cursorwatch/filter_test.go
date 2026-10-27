package cursorwatch

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/alpamayo-solutions/colca/internal/store"
)

// A consumer fetches after every page with the same filter: that does not
// restart the watchdog's scan of its cursor. A changed filter does.
func TestTheFilterGenerationMovesOnlyWhenTheFilterChanges(t *testing.T) {
	f := NewFilters()
	changed := f.changed.Changes()
	f.Remember("c", "metrics", Filter{Key: "a"})
	_, first := f.get("c", "metrics")
	select {
	case <-changed:
	default:
		t.Fatal("the first fetch did not wake the watchdog")
	}
	changed = f.changed.Changes()
	calls := 0
	f.Remember("c", "metrics", Filter{Key: "a", Keep: func(store.StoredRecord) bool { calls++; return true }})
	filter, again := f.get("c", "metrics")
	if again != first {
		t.Fatalf("generation %d -> %d on a fetch with the same filter", first, again)
	}
	select {
	case <-changed:
		t.Fatal("a fetch with the same filter woke the watchdog")
	default:
	}
	if filter.Keep(store.StoredRecord{}); calls != 1 {
		t.Fatal("the latest filter functions were not kept")
	}
	f.Remember("c", "metrics", Filter{Key: "b"})
	if _, moved := f.get("c", "metrics"); moved == first {
		t.Fatal("a changed filter kept its generation")
	}
}

// countingGauges counts the checks: every check sets each cursor's gauge.
type countingGauges struct{ n atomic.Int64 }

func (g *countingGauges) CursorUnreadAge(string, string, float64) { g.n.Add(1) }
func (g *countingGauges) ForgetCursorUnreadAge(string, string)    {}

// A cursor far behind is scanned a slice per check, and the next slice comes
// at most a second after the last check started: the watchdog never spins
// through a backlog.
func TestABoundedScanThatHasMoreWaitsTheOneSecondFloor(t *testing.T) {
	f := newFixture(t)
	f.read(t, "c/dataops-line/metrics", "metrics")
	defer func(was uint64) { scanCap = was }(scanCap)
	scanCap = 10
	recs := make([]store.Record, 100)
	for i := range recs {
		recs[i] = store.Record{Topic: "colca/v1/_Metric/NODE/x", Payload: []byte(`{}`), TS: 1}
	}
	if _, _, err := f.st.Append("metrics", recs); err != nil {
		t.Fatal(err)
	}
	// The consumer reads nothing of them, so every check finds no match and
	// the scan has more to go until it reached the head.
	f.w.Filters.Remember("c/dataops-line/metrics", "metrics", Filter{Key: "none", Head: func(string, string) bool { return false }})
	g := &countingGauges{}
	f.w.Gauges = g
	f.w.After, f.w.StaleAfter = 0, 0
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() { defer close(done); f.w.Run(stop) }()
	time.Sleep(1500 * time.Millisecond)
	close(stop)
	<-done
	if n := g.n.Load(); n > 2 {
		t.Fatalf("%d checks in 1.5 s, want at most 2: one a second", n)
	}
}
