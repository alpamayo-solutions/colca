package mqttsrv

import (
	"sync"
	"sync/atomic"
	"time"

	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"
)

// unackedStallAfter is how long a connected client may hold more than the
// per-client bounds without acknowledging anything before the broker stops
// queueing for it. A new subscriber acknowledging a retained replay of any
// size is never stalled: it acknowledges all the time.
const unackedStallAfter = 30 * time.Second

// watchedClient is a connected client that holds more unacknowledged messages
// than an offline session may keep.
type watchedClient struct {
	client *mqtt.Client
	// since is when it was first seen over the bounds, or last acknowledged.
	since time.Time
	// acked is set by an acknowledgement and read by the next sweep.
	acked bool
	// stalled: nothing more is queued for it until it acknowledges.
	stalled bool
	dropped uint64
}

// unackedClients bounds what the broker holds for a connected client that
// reads but does not acknowledge. mochi keeps every QoS 1 or 2 message until
// its acknowledgement, up to 65,535 per client; only an offline session was
// bounded. A client over the per-client bounds of an offline session that
// acknowledged nothing for unackedStallAfter is cut back to those bounds,
// newest first, and gets nothing more queued until it acknowledges again: the
// same policy, the drops counted the same way.
type unackedClients struct {
	maxMessages int
	maxBytes    int64
	stallAfter  time.Duration

	// n is len(m), read on the publish and acknowledge paths without the lock.
	n  atomic.Int64
	mu sync.Mutex
	m  map[string]*watchedClient
}

func newUnackedClients(o *offlineSessions) *unackedClients {
	return &unackedClients{maxMessages: o.maxMessages, maxBytes: o.maxBytes,
		stallAfter: unackedStallAfter, m: map[string]*watchedClient{}}
}

// OnQosComplete runs when a client acknowledged a message: a watched client
// that acknowledges is not stalled.
func (h *colcaHook) OnQosComplete(cl *mqtt.Client, _ packets.Packet) {
	u := h.unacked
	if u.n.Load() == 0 {
		return
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	w, ok := u.m[cl.ID]
	if !ok || w.client != cl {
		return
	}
	w.acked = true
	if w.stalled {
		w.stalled = false
		h.metrics.SetMQTTStalledClients(u.stalledCount())
		h.log.Info("mqtt client acknowledges again: messages are queued for it again",
			"client", cl.ID, "identity", string(cl.Properties.Username), "listener", cl.Net.Listener,
			"dropped", w.dropped)
		w.dropped = 0
	}
}

// stalledCount counts the stalled clients. The caller holds the lock.
func (u *unackedClients) stalledCount() int {
	n := 0
	for _, w := range u.m {
		if w.stalled {
			n++
		}
	}
	return n
}

// forgetUnacked ends the watch of a client whose connection ended. Its session,
// if it has one, is bounded as an offline session from here on.
func (h *colcaHook) forgetUnacked(cl *mqtt.Client) {
	u := h.unacked
	if u.n.Load() == 0 {
		return
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	if w, ok := u.m[cl.ID]; ok && w.client == cl {
		delete(u.m, cl.ID)
		u.n.Store(int64(len(u.m)))
		h.metrics.SetMQTTStalledClients(u.stalledCount())
	}
}

// boundUnacked takes the stalled clients out of a publish's subscribers. Only
// a QoS 1 or 2 delivery is held until it is acknowledged.
func (h *colcaHook) boundUnacked(subs *mqtt.Subscribers, pk packets.Packet) {
	u := h.unacked
	if u.n.Load() == 0 || pk.FixedHeader.Qos == 0 {
		return
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	for id, w := range u.m {
		if !w.stalled {
			continue
		}
		if sub, subscribed := subs.Subscriptions[id]; !subscribed || sub.Qos == 0 {
			continue
		}
		delete(subs.Subscriptions, id)
		w.dropped++
		h.metrics.MQTTUnackedDropped()
	}
}

// sweepUnackedClients finds the connected clients over the bounds, and cuts
// back those that acknowledged nothing for stallAfter.
func (s *Server) sweepUnackedClients(now time.Time) {
	h := s.hook
	u := h.unacked
	over := map[string]*mqtt.Client{}
	for _, cl := range s.S.Clients.GetAll() {
		if cl.Net.Inline || cl.Closed() || cl.State.Inflight.Len() == 0 {
			continue
		}
		if cl.State.Inflight.Len() > u.maxMessages || unackedBytes(cl) > u.maxBytes {
			over[cl.ID] = cl
		}
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	for id, w := range u.m {
		if over[id] == w.client {
			continue
		}
		// Back under the bounds, gone, or replaced by another connection.
		delete(u.m, id)
		if w.dropped > 0 {
			h.log.Info("mqtt client is back under its unacknowledged bounds",
				"client", id, "identity", string(w.client.Properties.Username), "dropped", w.dropped)
		}
	}
	for id, cl := range over {
		w, ok := u.m[id]
		if !ok {
			u.m[id] = &watchedClient{client: cl, since: now}
			continue
		}
		if w.acked {
			w.acked, w.since = false, now
			continue
		}
		if now.Sub(w.since) < u.stallAfter {
			continue
		}
		kept, bytes, dropped := u.cutBack(cl)
		for range dropped {
			h.metrics.MQTTUnackedDropped()
		}
		w.dropped += uint64(dropped) //nolint:gosec // a count
		if !w.stalled {
			w.stalled = true
			h.log.Warn("mqtt client does not acknowledge: what it holds is cut back and further messages for it are dropped until it acknowledges",
				"client", id, "identity", string(cl.Properties.Username), "listener", cl.Net.Listener,
				"unacknowledged_for", now.Sub(w.since).Round(time.Second),
				"kept_messages", kept, "kept_bytes", bytes, "dropped", dropped,
				"max_messages", u.maxMessages, "max_bytes", u.maxBytes)
		}
	}
	u.n.Store(int64(len(u.m)))
	h.metrics.SetMQTTStalledClients(u.stalledCount())
}

func unackedBytes(cl *mqtt.Client) int64 {
	var total int64
	for _, pk := range cl.State.Inflight.GetAll(false) {
		total += offlineCost(pk)
	}
	return total
}

// cutBack drops what a client holds over the bounds, newest first, and returns
// what it keeps and how many messages were dropped. A message that was sent
// took one of the client's receive slots; dropping it gives the slot back, as
// mochi does when it drops a publish itself.
func (u *unackedClients) cutBack(cl *mqtt.Client) (kept int, bytes int64, dropped int) {
	for _, pk := range cl.State.Inflight.GetAll(false) {
		cost := offlineCost(pk)
		if kept < u.maxMessages && bytes+cost <= u.maxBytes {
			kept++
			bytes += cost
			continue
		}
		if cl.State.Inflight.Delete(pk.PacketID) {
			if pk.Expiry >= 0 { // sent; a negative expiry marks one still waiting for a slot
				cl.State.Inflight.IncreaseSendQuota()
			}
			dropped++
		}
	}
	return kept, bytes, dropped
}
