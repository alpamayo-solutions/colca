package mqttsrv

import (
	"sync"
	"sync/atomic"
	"time"

	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"

	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/engine"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// offlineMessageOverhead is what one message queued for an offline session
// costs on the heap beside its payload and topic: mochi keeps a whole packet
// per message. Measured at 0.95 KiB with 200-byte payloads (65,535 queued
// messages held 75 MiB), rounded up.
const offlineMessageOverhead = 1024

// Reasons an offline session ended without its client coming back.
const (
	offlineEndedExpired         = "expired"
	offlineEndedIdentityRemoved = "identity_removed"
	offlineEndedRetired         = "retired"
)

// offlineSession is the queue of one session whose client is away. mochi keeps
// every QoS 1 or 2 message such a session's subscriptions match in memory until
// the client returns or the session expires; this is the account of it.
type offlineSession struct {
	client   *mqtt.Client
	messages int
	bytes    int64
	dropped  uint64
	full     bool // the transition was logged
}

// offlineSessions bounds what the broker queues for clients that are away. A
// session keeps at most maxMessages messages and maxBytes bytes, and all offline
// sessions together at most maxTotalBytes. A message over a bound is not queued
// for that session: it keeps what was queued first, in order, and loses the
// rest until it reconnects. Without the bound one abandoned session subscribed
// to everything held 65,535 messages (mochi's inflight ceiling), and each
// further one as much again.
type offlineSessions struct {
	maxMessages   int
	maxBytes      int64
	maxTotalBytes int64

	// n is len(m), read on the publish path without the lock: a broker with no
	// offline session pays one atomic load per publish.
	n     atomic.Int64
	mu    sync.Mutex
	m     map[string]*offlineSession
	total int64
	queue int
}

func newOfflineSessions(l config.MQTTLimits) *offlineSessions {
	return &offlineSessions{
		maxMessages:   l.EffectiveMaxOfflineMessagesPerClient(),
		maxBytes:      l.EffectiveMaxOfflineBytesPerClient(),
		maxTotalBytes: l.EffectiveMaxOfflineBytes(),
		m:             map[string]*offlineSession{},
	}
}

func offlineCost(pk packets.Packet) int64 {
	return int64(len(pk.Payload)) + int64(len(pk.TopicName)) + offlineMessageOverhead
}

// wentOffline starts the account of a session that outlives its connection. What
// the session already holds unacknowledged counts; whatever of it is over the
// bounds is dropped at once, newest first.
func (h *colcaHook) wentOffline(cl *mqtt.Client) {
	o := h.offline
	o.mu.Lock()
	defer o.mu.Unlock()
	if old, ok := o.m[cl.ID]; ok {
		o.forget(cl.ID, old)
	}
	s := &offlineSession{client: cl}
	for _, pk := range cl.State.Inflight.GetAll(false) {
		cost := offlineCost(pk)
		if o.fits(s, cost) {
			s.messages++
			s.bytes += cost
			o.total += cost
			o.queue++
			continue
		}
		if cl.State.Inflight.Delete(pk.PacketID) {
			h.dropped(s, cl)
		}
	}
	o.m[cl.ID] = s
	o.n.Store(int64(len(o.m)))
	h.reportOffline()
}

func (o *offlineSessions) fits(s *offlineSession, cost int64) bool {
	return s.messages < o.maxMessages && s.bytes+cost <= o.maxBytes && o.total+cost <= o.maxTotalBytes
}

// forget removes a session's account. The caller holds the lock.
func (o *offlineSessions) forget(id string, s *offlineSession) {
	delete(o.m, id)
	o.total -= s.bytes
	o.queue -= s.messages
	o.n.Store(int64(len(o.m)))
}

// dropped counts one message not kept for an offline session and logs the first
// of them: one line per session and absence, not one per message.
func (h *colcaHook) dropped(s *offlineSession, cl *mqtt.Client) {
	s.dropped++
	h.metrics.MQTTOfflineDropped()
	if s.full {
		return
	}
	s.full = true
	h.log.Warn("offline mqtt session's queue is full: further messages for it are dropped until it reconnects",
		"client", cl.ID, "identity", string(cl.Properties.Username), "listener", cl.Net.Listener,
		"queued_messages", s.messages, "queued_bytes", s.bytes,
		"max_messages", h.offline.maxMessages, "max_bytes", h.offline.maxBytes,
		"all_sessions_bytes", h.offline.total, "max_all_sessions_bytes", h.offline.maxTotalBytes)
}

func (h *colcaHook) reportOffline() {
	h.metrics.SetMQTTOffline(len(h.offline.m), h.offline.queue, h.offline.total)
}

// cameBack ends the account when the client id connects again, and says once
// what the session lost while it was away.
func (h *colcaHook) cameBack(cl *mqtt.Client) {
	o := h.offline
	if o.n.Load() == 0 {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	s, ok := o.m[cl.ID]
	if !ok {
		return
	}
	o.forget(cl.ID, s)
	h.reportOffline()
	if s.dropped > 0 {
		h.log.Info("mqtt session reconnected: messages were dropped while it was offline",
			"client", cl.ID, "identity", string(cl.Properties.Username), "dropped", s.dropped, "queued_messages", s.messages)
	}
}

// boundOfflineQueues takes the offline sessions whose queue is full out of a
// publish's subscribers, and accounts the message to the others. Only a QoS 1
// or 2 delivery is queued for a client that is away.
func (h *colcaHook) boundOfflineQueues(subs *mqtt.Subscribers, pk packets.Packet) {
	o := h.offline
	if o.n.Load() == 0 || pk.FixedHeader.Qos == 0 {
		return
	}
	cost := offlineCost(pk)
	o.mu.Lock()
	defer o.mu.Unlock()
	changed := false
	for id, s := range o.m {
		sub, subscribed := subs.Subscriptions[id]
		if !subscribed || sub.Qos == 0 {
			continue
		}
		if current, ok := h.broker.Clients.Get(id); !ok || current != s.client {
			// The id reconnected or the session is gone, and its end was not seen
			// here yet: nothing is queued for this account any more.
			o.forget(id, s)
			changed = true
			continue
		}
		if !o.fits(s, cost) {
			delete(subs.Subscriptions, id)
			h.dropped(s, s.client)
			continue
		}
		s.messages++
		s.bytes += cost
		o.total += cost
		o.queue++
		changed = true
	}
	if changed {
		h.reportOffline()
	}
}

// OnSessionEstablished runs when a client id has connected and its session is
// set up, taken over or new.
func (h *colcaHook) OnSessionEstablished(cl *mqtt.Client, _ packets.Packet) {
	h.cameBack(cl)
}

// OnClientExpired runs when mochi drops a session that stayed away longer than
// its expiry. mochi deletes the client and leaves its subscriptions in the
// topic index, where every later publish would still match them, so they are
// removed here together with the queue.
func (h *colcaHook) OnClientExpired(cl *mqtt.Client) {
	h.endOffline(cl, offlineEndedExpired)
}

// endOffline removes an offline session's queue, subscriptions and account.
func (h *colcaHook) endOffline(cl *mqtt.Client, reason string) {
	o := h.offline
	o.mu.Lock()
	s, tracked := o.m[cl.ID]
	if tracked && s.client == cl {
		o.forget(cl.ID, s)
		h.reportOffline()
	} else {
		s = &offlineSession{}
	}
	o.mu.Unlock()
	cl.ClearInflights()
	h.broker.UnsubscribeClient(cl)
	h.metrics.MQTTOfflineSessionEnded(reason)
	h.log.Info("offline mqtt session removed", "client", cl.ID, "identity", string(cl.Properties.Username),
		"reason", reason, "discarded_messages", s.messages, "dropped_before", s.dropped,
		"offline_since", time.Unix(cl.StopTime(), 0).UTC().Format(time.RFC3339))
}

// sweepOfflineSessions removes the offline sessions of identities the registry
// no longer holds: a retired service or a revoked machine cannot reconnect, so
// nothing would ever read what its session queued. Sessions of the token doors
// are left to their expiry; a person's identity is not in the registry.
func (s *Server) sweepOfflineSessions() {
	o := s.hook.offline
	if o.n.Load() == 0 {
		return
	}
	o.mu.Lock()
	var gone []*mqtt.Client
	for id, session := range o.m {
		current, ok := s.S.Clients.Get(id)
		if !ok || current != session.client {
			o.forget(id, session)
			continue
		}
		if isHumanListener(session.client) {
			continue
		}
		if _, enrolled := s.hook.reg.Get(string(session.client.Properties.Username)); !enrolled {
			gone = append(gone, session.client)
		}
	}
	s.hook.reportOffline()
	o.mu.Unlock()
	for _, cl := range gone {
		s.hook.endOffline(cl, offlineEndedIdentityRemoved)
		s.S.Clients.Delete(cl.ID)
	}
}

// retiresOwnRecord reports whether pk is a service retiring its own
// _ServiceDetails: an empty payload at a record this identity authored. It is
// asked before the publish is stored, while the record still says who wrote it.
func (h *colcaHook) retiresOwnRecord(eng *engine.Engine, ident string, pk packets.Packet) bool {
	if len(pk.Payload) != 0 {
		return false
	}
	p, err := uns.Parse(pk.TopicName)
	if err != nil || p.Contract != "_ServiceDetails" {
		return false
	}
	entry, ok := h.reg.Get(ident)
	if !ok {
		return false
	}
	held, ok := eng.EntityStore().KVGet(pk.TopicName)
	return ok && uns.ServiceRecordAuthor(p.Contract, held) == entry.ULID
}

// noteServiceRecord remembers that a client retired its own _ServiceDetails,
// and forgets it when the client announces itself again on the same connection.
func (h *colcaHook) noteServiceRecord(cl *mqtt.Client, pk packets.Packet, retires bool) {
	if retires {
		h.retiring.Store(cl, struct{}{})
		return
	}
	if len(pk.Payload) == 0 {
		return
	}
	if _, marked := h.retiring.Load(cl); !marked {
		return
	}
	if p, err := uns.Parse(pk.TopicName); err == nil && p.Contract == "_ServiceDetails" {
		h.retiring.Delete(cl)
	}
}

// endRetired ends the session of a service that retired itself, when its
// connection ends. A retired service does not come back for what its session
// would queue; before, the session of a service inside the deployment stayed
// until its expiry, because nothing removes a local service's identity.
// expire says mochi removes the session itself right after the hook.
func (h *colcaHook) endRetired(cl *mqtt.Client, expire bool) {
	queued := cl.State.Inflight.Len()
	if !expire {
		cl.ClearInflights()
		h.broker.UnsubscribeClient(cl)
		h.broker.Clients.Delete(cl.ID)
	}
	h.metrics.MQTTOfflineSessionEnded(offlineEndedRetired)
	h.log.Info("mqtt session of a retired service removed", "client", cl.ID,
		"identity", string(cl.Properties.Username), "listener", cl.Net.Listener, "discarded_messages", queued)
}
