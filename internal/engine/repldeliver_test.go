package engine

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/store"
)

type recordingBus struct {
	mu     sync.Mutex
	topics []string
	gate   chan struct{} // nil: deliver at once; else each delivery waits for a receive
}

func (b *recordingBus) deliver(topic string, _ []byte, _ bool) {
	if b.gate != nil {
		<-b.gate
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.topics = append(b.topics, topic)
}

func (b *recordingBus) seen() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]string(nil), b.topics...)
}

func busEngine(t *testing.T, bus *recordingBus) *Engine {
	t.Helper()
	s, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	e := New(s, &config.Config{ULID: "n-hub"}, testIDs(), bus.deliver, nil, nil)
	placeTestElements(t, e)
	bus.mu.Lock()
	bus.topics = nil // the fixture's own elements
	bus.mu.Unlock()
	return e
}

func metricPush(child string, from, n int) []store.ReplRecord {
	recs := make([]store.ReplRecord, n)
	for i := range recs {
		off := from + i
		recs[i] = store.ReplRecord{ChildOffset: uint64(off), Topic: fmt.Sprintf("colca/v1/_Metric/%s/m/s%d", child, off),
			Payload: []byte(`{"value":1,"signal_id":"s"}`), TS: int64(off)}
	}
	return recs
}

// With the deliverer running, replicated records reach the bus from it, in the
// order their pushes committed; without it, from the push itself.
func TestReplicatedRecordsReachTheBusInCommitOrder(t *testing.T) {
	bus := &recordingBus{}
	e := busEngine(t, bus)
	if _, _, err := e.IngestReplicated("n-c1", "metrics", metricPush("n-c1", 1, 2)); err != nil {
		t.Fatal(err)
	}
	if got := len(bus.seen()); got != 2 {
		t.Fatalf("without a deliverer the push published %d records itself, want 2", got)
	}

	stop := make(chan struct{})
	done := make(chan struct{})
	go func() { defer close(done); e.RunReplicatedDelivery(stop) }()
	waitUntil(t, func() bool { return e.replBus.stop.Load() != nil })
	for push := 0; push < 10; push++ {
		if _, _, err := e.IngestReplicated("n-c1", "metrics", metricPush("n-c1", 3+2*push, 2)); err != nil {
			t.Fatal(err)
		}
	}
	waitUntil(t, func() bool { return len(bus.seen()) == 22 })
	waitUntil(t, func() bool { return e.QueuedBytes() == 0 })
	for i, topic := range bus.seen() {
		if want := fmt.Sprintf("colca/v1/_Metric/n-c1/m/s%d", i+1); topic != want {
			t.Fatalf("record %d on the bus is %s, want %s", i, topic, want)
		}
	}
	close(stop)
	<-done
}

// A push whose records cannot be queued because the bus is behind waits; when
// the node stops meanwhile, it publishes them itself instead of hanging.
func TestAPushWaitingForAFullBusQueueIsReleasedByStop(t *testing.T) {
	bus := &recordingBus{}
	e := busEngine(t, bus)
	bus.gate = make(chan struct{})
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() { defer close(done); e.RunReplicatedDelivery(stop) }()
	waitUntil(t, func() bool { return e.replBus.stop.Load() != nil })
	// The deliverer blocks on the first record; then the queue fills.
	off := 1
	for i := 0; i < replBusQueue+1; i++ {
		if _, _, err := e.IngestReplicated("n-c1", "metrics", metricPush("n-c1", off, 1)); err != nil {
			t.Fatal(err)
		}
		off++
	}
	pushed := make(chan struct{})
	go func() {
		defer close(pushed)
		_, _, _ = e.IngestReplicated("n-c1", "metrics", metricPush("n-c1", off, 1))
	}()
	select {
	case <-pushed:
		t.Fatal("a push went past a full bus queue")
	case <-time.After(100 * time.Millisecond):
	}
	if e.QueuedBytes() <= 0 {
		t.Fatal("records waiting for the bus are not counted in QueuedBytes")
	}
	close(stop)
	close(bus.gate) // every delivery may proceed now
	select {
	case <-pushed:
	case <-time.After(5 * time.Second):
		t.Fatal("a push waiting for the bus hung after stop")
	}
	<-done
	if got := e.QueuedBytes(); got < 0 {
		t.Fatalf("QueuedBytes %d after the queue was abandoned", got)
	}
}

func waitUntil(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition never held")
		}
		time.Sleep(time.Millisecond)
	}
}
