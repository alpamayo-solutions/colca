// Human MQTT doors: optional TLS listeners for raw MQTT (human-tcp) and MQTT over
// WebSocket (human-ws), with no client certificate. The CONNECT password carries a
// JWT or PAT and the username must equal its sub. A session lasts as long as the
// token: every delivery checks grants and expiry, and a sweeper kicks expired
// sessions every 10 seconds. A client that connected with the authentication
// method AuthMethodToken renews its token on the open connection (reauth.go), and
// a back-channel logout from the identity provider ends the sessions it names.

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
	entry            *uns.Entry
	sub              string
	username         string
	exp              time.Time
	credential       string
	credentialID     string
	credentialDigest string
	sid              string    // the identity provider's session; empty for PATs
	issuedAt         time.Time // of the token in force
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

// snapshot returns a stable copy for the sweeper. It must not hold the table
// lock while the broker disconnect path calls OnDisconnect and drops entries.
func (h *humanSessions) snapshot() map[string]humanSession {
	h.mu.RLock()
	defer h.mu.RUnlock()
	out := make(map[string]humanSession, len(h.m))
	for id, session := range h.m {
		out[id] = session
	}
	return out
}

// isHumanListener reports whether the client connected through a human door.
func isHumanListener(cl *mqtt.Client) bool {
	return cl.Net.Listener == listenerHumanTCP || cl.Net.Listener == listenerHumanWS
}

// authenticateHuman is the human branch of OnConnectAuthenticate: verify the
// credential in the CONNECT password, require username == sub, store the session.
func (h *colcaHook) authenticateHuman(cl *mqtt.Client, pk packets.Packet) bool {
	if h.ver == nil {
		// Config validation forbids human listeners without an auth block, so this is a
		// bug, but the door must still fail closed.
		h.log.Error("human door with no verifier — rejecting", "listener", cl.Net.Listener)
		h.metrics.AuthReject(metrics.DoorMQTT, tokenauth.ReasonBadToken)
		h.auditDenied("authenticate", tokenauth.ReasonBadToken, metrics.DoorMQTT, nil, nil)
		return false
	}
	user := string(pk.Connect.Username)
	v, reason, err := h.ver.VerifyForScope(string(pk.Connect.Password), uns.ScopeBrokerMQTT)
	if err != nil {
		h.log.Warn("human auth rejected", "user", user, "reason", reason, "err", err)
		h.metrics.AuthReject(metrics.DoorMQTT, reason)
		h.auditDenied("authenticate", reason, metrics.DoorMQTT, nil, nil)
		return false
	}
	if user != v.Sub {
		h.log.Warn("human auth rejected: username != sub", "user", user, "sub", v.Sub)
		h.metrics.AuthReject(metrics.DoorMQTT, metrics.AuthUsernameMismatch)
		h.auditDenied("authenticate", metrics.AuthUsernameMismatch, metrics.DoorMQTT, v.Entry, nil)
		return false
	}
	if method := cl.Properties.Props.AuthenticationMethod; method != "" && method != AuthMethodToken {
		// MQTT 5 requires refusing an authentication method the server does not know.
		h.log.Warn("human auth rejected: unknown authentication method", "user", user, "method", method)
		h.metrics.AuthReject(metrics.DoorMQTT, metrics.AuthMethod)
		h.auditDenied("authenticate", metrics.AuthMethod, metrics.DoorMQTT, v.Entry, nil)
		return false
	}
	n := h.humans.put(cl.ID, sessionFor(v))
	h.metrics.SetHumanSessions(n)
	h.log.Debug("human authenticated", "sub", v.Sub, "username", v.Username,
		"exp", v.Exp, "listener", cl.Net.Listener)
	return true
}

// sessionFor is the session a verified token opens or renews.
func sessionFor(v *tokenauth.Verified) humanSession {
	return humanSession{
		entry: v.Entry, sub: v.Sub, username: v.Username, exp: v.Exp,
		credential: v.Credential, credentialID: v.CredentialID,
		credentialDigest: v.CredentialDigest,
		sid:              v.SessionID, issuedAt: v.IssuedAt,
	}
}

// humanACL is the human branch of OnACLCheck: the session must exist and not be
// expired, so an expired session stops receiving before the sweeper kicks it, and
// the topic must be covered by its read grants.
func (h *colcaHook) humanACL(cl *mqtt.Client, topic string) bool {
	s, ok := h.humans.get(cl.ID)
	if !ok || time.Now().After(s.exp) {
		return false
	}
	if !uns.Authorize(h.scope(), s.entry, uns.ActSub, topic) {
		h.log.Warn("human subscribe/read denied", "sub", s.sub, "filter", topic)
		h.metrics.ACLDeny(metrics.ACLSub)
		path, _ := uns.SubscriptionPath(topic)
		h.auditDeniedAt("read", "subscribe_denied", metrics.DoorMQTT, s.entry,
			map[string]any{"filter": topic}, "mqtt-subscription", path)
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

// sweepInvalidHumanSessions kicks sessions past expiry and PAT sessions whose
// local replicated credential was tombstoned, became ambiguous/corrupt, or no
// longer grants broker-mqtt. Exposed on Server for the ticker and tests.
func (s *Server) sweepInvalidHumanSessions(now time.Time) {
	for id, session := range s.hook.humans.snapshot() {
		reason := ""
		switch {
		case !now.Before(session.exp):
			reason = tokenauth.ReasonExpired
		case session.credential == "pat":
			var err error
			reason, err = s.hook.ver.VerifyPersonalAccessTokenSession(
				session.credentialID, session.credentialDigest, uns.ScopeBrokerMQTT, now,
			)
			if err == nil {
				continue
			}
		default:
			continue
		}
		if cl, ok := s.S.Clients.Get(id); ok {
			_ = s.S.DisconnectClient(cl, packets.ErrNotAuthorized)
			s.metrics.SessionKick()
			s.hook.log.Info("human session invalid — kicked", "client", id, "reason", reason)
		}
		// The OnDisconnect hook removes the table entry; drop defensively in
		// case the client vanished without a disconnect event.
		s.hook.metrics.SetHumanSessions(s.hook.humans.drop(id))
	}
}

// endLoggedOutSessions disconnects every token session a back-channel logout
// covers. PAT sessions are not a login at the identity provider and stay.
func (s *Server) endLoggedOutSessions(logout tokenauth.Logout) {
	for id, session := range s.hook.humans.snapshot() {
		if session.credential != "oidc" || !logout.Ends(session.sid, session.sub, session.issuedAt) {
			continue
		}
		if cl, ok := s.S.Clients.Get(id); ok {
			_ = s.S.DisconnectClient(cl, packets.ErrAdministrativeAction)
			s.metrics.SessionKick()
			s.hook.log.Info("human session logged out — kicked", "client", id, "sub", session.sub)
		}
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
			s.sweepInvalidHumanSessions(time.Now())
		}
	}
}
