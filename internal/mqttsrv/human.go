// Human MQTT doors (human-authz design §5.1): two optional TLS listeners —
// raw MQTT ("human-tcp") and MQTT over WebSocket ("human-ws") — with NO
// client-certificate requirement. The CONNECT password carries a JWT, the
// username must equal the token's sub, and the session lives exactly as long
// as the token: per-delivery ACL checks evaluate grants AND exp, and a 10s
// sweeper kicks expired sessions with the same mechanism revocation uses.
package mqttsrv

import (
	"sync"
	"time"

	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"

	"github.com/alpamayo-solutions/colca/internal/metrics"
	"github.com/alpamayo-solutions/colca/internal/tokenauth"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

const (
	listenerHumanTCP = "human-tcp"
	listenerHumanWS  = "human-ws"
	sweepInterval    = 10 * time.Second
)

// humanSession is one live token-authenticated session.
type humanSession struct {
	entry *uns.Entry
	sub   string
	exp   time.Time
}

// humanSessions is the session table keyed by MQTT client id. Client ids are
// broker-unique across listeners, and sessions are only ever created on the
// human listeners.
type humanSessions struct {
	mu sync.RWMutex
	m  map[string]humanSession
}

func newHumanSessions() *humanSessions { return &humanSessions{m: map[string]humanSession{}} }

func (h *humanSessions) put(clientID string, s humanSession) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.m[clientID] = s
	return len(h.m)
}

func (h *humanSessions) get(clientID string) (humanSession, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	s, ok := h.m[clientID]
	return s, ok
}

func (h *humanSessions) drop(clientID string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.m, clientID)
	return len(h.m)
}

// expired returns the client ids of sessions past exp at now.
func (h *humanSessions) expired(now time.Time) []string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	var out []string
	for id, s := range h.m {
		if now.After(s.exp) {
			out = append(out, id)
		}
	}
	return out
}

// isHumanListener reports whether the client connected through a human door.
func isHumanListener(cl *mqtt.Client) bool {
	return cl.Net.Listener == listenerHumanTCP || cl.Net.Listener == listenerHumanWS
}

// authenticateHuman is the human branch of OnConnectAuthenticate: verify the
// JWT in the CONNECT password, require username == sub, store the session.
func (h *colcaHook) authenticateHuman(cl *mqtt.Client, pk packets.Packet) bool {
	if h.ver == nil {
		// Config validation forbids human listeners without an auth block, so
		// this is a programming error — but a door must fail closed, not open.
		h.log.Error("human door with no verifier — rejecting", "listener", cl.Net.Listener)
		h.metrics.AuthReject(metrics.DoorMQTT, tokenauth.ReasonBadToken)
		return false
	}
	user := string(pk.Connect.Username)
	v, reason, err := h.ver.Verify(string(pk.Connect.Password))
	if err != nil {
		h.log.Warn("human auth rejected", "user", user, "reason", reason, "err", err)
		h.metrics.AuthReject(metrics.DoorMQTT, reason)
		return false
	}
	if user != v.Sub {
		h.log.Warn("human auth rejected: username != sub", "user", user, "sub", v.Sub)
		h.metrics.AuthReject(metrics.DoorMQTT, metrics.AuthUsernameMismatch)
		return false
	}
	n := h.humans.put(cl.ID, humanSession{entry: v.Entry, sub: v.Sub, exp: v.Exp})
	h.metrics.SetHumanSessions(n)
	h.log.Debug("human authenticated", "sub", v.Sub, "username", v.Username,
		"exp", v.Exp, "listener", cl.Net.Listener)
	return true
}

// humanACL is the human branch of OnACLCheck's read path: the session must
// exist, must not be expired (per-delivery enforcement — an expired session
// stops receiving before the sweeper kicks it), and the filter/topic must be
// covered by the session's read grants.
func (h *colcaHook) humanACL(cl *mqtt.Client, topic string) bool {
	s, ok := h.humans.get(cl.ID)
	if !ok || time.Now().After(s.exp) {
		return false
	}
	if !uns.Authorize(s.entry, uns.ActSub, topic) {
		h.log.Warn("human subscribe/read denied", "sub", s.sub, "filter", topic)
		h.metrics.ACLDeny(metrics.ACLSub)
		return false
	}
	return true
}

// OnDisconnect drops the session table entry (nil-op for machine clients).
func (h *colcaHook) OnDisconnect(cl *mqtt.Client, _ error, _ bool) {
	if isHumanListener(cl) {
		h.metrics.SetHumanSessions(h.humans.drop(cl.ID))
	}
}

// sweepExpired kicks every session past exp — the §5.1 sweeper body, exposed
// on Server for the ticker (and tests).
func (s *Server) sweepExpired(now time.Time) {
	for _, id := range s.hook.humans.expired(now) {
		if cl, ok := s.S.Clients.Get(id); ok {
			_ = s.S.DisconnectClient(cl, packets.ErrNotAuthorized)
			s.metrics.SessionKick()
			s.hook.log.Info("human session expired — kicked", "client", id)
		}
		// The OnDisconnect hook removes the table entry; drop defensively in
		// case the client vanished without a disconnect event.
		s.hook.metrics.SetHumanSessions(s.hook.humans.drop(id))
	}
}

// runSweeper ticks until stop closes.
func (s *Server) runSweeper(stop <-chan struct{}) {
	t := time.NewTicker(sweepInterval)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
			s.sweepExpired(time.Now())
		}
	}
}
