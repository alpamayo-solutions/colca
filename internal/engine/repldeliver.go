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
// pushes in the order they were handed over; a push hands its records over
// and is answered.
//
// What holds: the bus never shows a record that is not durable, and one
// child's records, so every topic's, reach it in their order, because a
// child's next push commits only after this one was handed over. Pushes of
// different children committed in one group may reach the bus in another
// order than the store has them; no consumer may rely on cross-topic order
// on the bus (the store's offsets give it).
//
// The queued records count as memory: QueuedBytes is part of the replication
// door's push budget, and when the queue is full a push waits for room, so
// the bus cannot fall arbitrarily far behind the store. Records still queued
// when the node stops are not published; the broker stops with the node, and
// subscribers get the current values on reconnect from the retained set the
// node reseeds from KV at startup, or from /kv.

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
	// bytes is the topic and payload bytes queued, see QueuedBytes.
	bytes atomic.Int64
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
			e.replBus.bytes.Add(-msgBytes(msgs))
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
		n := msgBytes(msgs)
		e.replBus.bytes.Add(n)
		select {
		case e.replBus.queue <- msgs:
			return
		case <-*stop:
			e.replBus.bytes.Add(-n)
		}
	}
	for _, m := range msgs {
		e.deliver(m.topic, m.payload, m.retain)
	}
}

// QueuedBytes is the topic and payload bytes of replicated records waiting for
// the bus. The replication door counts them in its push budget.
func (e *Engine) QueuedBytes() int64 { return e.replBus.bytes.Load() }

func msgBytes(msgs []busMsg) int64 {
	var n int64
	for _, m := range msgs {
		n += int64(len(m.topic) + len(m.payload))
	}
	return n
}
