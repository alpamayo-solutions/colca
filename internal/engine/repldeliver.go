package engine

import "sync/atomic"

// Replicated records reach the local bus through one goroutine.
//
// A parent mirrors every replicated record onto its MQTT bus after the record
// is durable. The broker publishes under one lock for its retained index, so
// a thousand children's push handlers each publishing their own records
// queued on that lock: under load nearly all of the parent's lock wait was
// there, and its ingest stopped near 13,000 records a second while the CPU
// was half idle (fleet scale benchmark, 2026-10). One goroutine publishes the
// pushes in the order they committed; a push hands its records over and is
// answered. The bus still never shows a record that is not durable, and a
// child's records keep their order: its next push commits after this one was
// handed over. When the queue is full a push waits for room, so the bus
// cannot fall arbitrarily far behind the store.

// busMsg is one record for the local bus.
type busMsg struct {
	topic   string
	payload []byte
	retain  bool
}

// replBusQueue is how many pushes may wait for the bus.
const replBusQueue = 256

type replBus struct {
	queue chan []busMsg
	// stop is the running deliverer's stop channel, nil while none runs.
	stop atomic.Pointer[<-chan struct{}]
}

// RunReplicatedDelivery publishes replicated records onto the local bus until
// stop closes. Without it running, IngestReplicated publishes them itself.
func (e *Engine) RunReplicatedDelivery(stop <-chan struct{}) {
	if e.deliver == nil {
		return
	}
	e.replBus.stop.Store(&stop)
	defer e.replBus.stop.Store(nil)
	for {
		select {
		case <-stop:
			return
		case msgs := <-e.replBus.queue:
			for _, m := range msgs {
				e.deliver(m.topic, m.payload, m.retain)
			}
		}
	}
}

// deliverReplicated hands msgs to the deliverer, or publishes them here when
// none runs (a node shutting down, or an engine in a test).
func (e *Engine) deliverReplicated(msgs []busMsg) {
	if len(msgs) == 0 || e.deliver == nil {
		return
	}
	if stop := e.replBus.stop.Load(); stop != nil {
		select {
		case e.replBus.queue <- msgs:
			return
		case <-*stop:
		}
	}
	for _, m := range msgs {
		e.deliver(m.topic, m.payload, m.retain)
	}
}
