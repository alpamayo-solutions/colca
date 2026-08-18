// Package mqttsrv embeds a mochi-mqtt broker and wires it to the engine. The
// listener is TLS-only (auth design §6.1): a machine's key is its TLS client
// key, pinned against the local registry — no CA, no tokens, no plaintext.
// Every client PUBLISH is funnelled through engine.IngestClient, so the
// broker can never persist anything the engine would not accept, and every
// SUBSCRIBE filter is checked against the client's read grants.
package mqttsrv

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"log/slog"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/listeners"
	"github.com/mochi-mqtt/server/v2/packets"

	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/engine"
	"github.com/alpamayo-solutions/colca/internal/identity"
	"github.com/alpamayo-solutions/colca/internal/metrics"
	"github.com/alpamayo-solutions/colca/internal/registry"
	"github.com/alpamayo-solutions/colca/internal/tokenauth"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// Server is the embedded broker plus its Colca hook.
type Server struct {
	S        *mqtt.Server
	tcp      *listeners.TCP
	humanTCP *listeners.TCP
	humanWS  *listeners.Websocket
	hook     *colcaHook
	cfg      *config.Config
	metrics  *metrics.Metrics

	sweepStop chan struct{}
	sweepOnce sync.Once

	closeOnce sync.Once
	closeErr  error
}

// colcaHook authenticates clients against the registry (TLS peer key →
// entry) and enforces the engine's rules on publish and the read grants on
// subscribe.
//
// The engine is late-bound: node assembly needs the broker's DeliverLocal to
// build the engine, and the hook needs that same engine — so New may be called
// with a nil engine and SetEngine swaps it in afterwards. mu guards that swap
// against the broker's connection goroutines.
type colcaHook struct {
	mqtt.HookBase
	mu      sync.RWMutex
	eng     *engine.Engine
	reg     *registry.Manager
	ver     *tokenauth.Verifier // nil when the node has no auth: block
	humans  *humanSessions
	cfg     *config.Config
	log     *slog.Logger
	metrics *metrics.Metrics // nil-safe: every Metrics method is a no-op on nil
	// broker is the SAME *mqtt.Server New() builds around this hook — set at
	// construction (New creates the server before the hook), never nil in
	// practice. It exists so the hook can publish the time-sync beacon
	// (design §2.2) directly, the same way Server.DeliverLocal does, without
	// a back-reference to *Server (which does not exist yet when the hook is
	// built).
	broker *mqtt.Server
	// closing is raised by Server.Close before it disconnects anyone. While it
	// is set the door refuses every new connection, so the listener shutdown
	// that follows is not chasing arrivals it can never outrun. See
	// Server.Close for the deadlock this is half of the guard against.
	closing atomic.Bool
}

func (h *colcaHook) engine() *engine.Engine {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.eng
}

// scope is what the ACL checks resolve grants through. Nil before the engine is
// late-bound, which denies every scoped grant — a broker not yet wired to its
// node knows neither where things sit nor where it sits itself.
func (h *colcaHook) scope() uns.Scope {
	if eng := h.engine(); eng != nil {
		return eng.Scope()
	}
	return nil
}

func (h *colcaHook) setEngine(e *engine.Engine) {
	h.mu.Lock()
	h.eng = e
	h.mu.Unlock()
}

func (h *colcaHook) ID() string { return "colca" }

func (h *colcaHook) Provides(b byte) bool {
	return bytes.Contains([]byte{
		mqtt.OnConnectAuthenticate,
		mqtt.OnACLCheck,
		mqtt.OnPublish,
		mqtt.OnDisconnect,
		mqtt.OnSubscribed,
	}, []byte{b})
}

// OnConnectAuthenticate resolves the TLS peer key against the local registry
// (auth §6.1): the entry must exist, be kind machine, and the CONNECT
// username must equal the enrolled ULID — the username is how mochi carries
// the identity to OnPublish/OnACLCheck, so the equality check pins it to the
// key.
func (h *colcaHook) OnConnectAuthenticate(cl *mqtt.Client, pk packets.Packet) bool {
	// Refused before anything else once the node is shutting down: a rejected
	// connection returns from attachClient before Clients.Add, so it can never
	// become the queued writer Server.Close must avoid.
	if h.closing.Load() {
		return false
	}
	if isHumanListener(cl) {
		return h.authenticateHuman(cl, pk)
	}
	user := string(pk.Connect.Username)
	tc, ok := cl.Net.Conn.(*tls.Conn)
	if !ok || len(tc.ConnectionState().PeerCertificates) == 0 {
		h.log.Warn("mqtt auth rejected: no client certificate", "user", user)
		h.metrics.AuthReject(metrics.DoorMQTT, metrics.AuthUnknownKey)
		return false
	}
	pub, err := identity.PeerPubHex(tc.ConnectionState().PeerCertificates[0].Raw)
	if err != nil {
		h.log.Warn("mqtt auth rejected: unusable client certificate", "user", user, "err", err)
		h.metrics.AuthReject(metrics.DoorMQTT, metrics.AuthUnknownKey)
		return false
	}
	entry, ok := h.reg.ByPubkey(pub)
	if !ok {
		h.log.Warn("mqtt auth rejected: key not enrolled", "user", user)
		h.metrics.AuthReject(metrics.DoorMQTT, metrics.AuthUnknownKey)
		return false
	}
	if !entry.MayUseDoor(uns.DoorMQTT) {
		h.log.Warn("mqtt auth rejected: kind may not use this door", "user", user, "kind", entry.Kind)
		h.metrics.AuthReject(metrics.DoorMQTT, metrics.AuthKind)
		return false
	}
	if user != entry.ULID {
		h.log.Warn("mqtt auth rejected: username != enrolled ulid", "user", user, "ulid", entry.ULID)
		h.metrics.AuthReject(metrics.DoorMQTT, metrics.AuthUsernameMismatch)
		return false
	}
	h.log.Debug("mqtt client authenticated", "ulid", entry.ULID)
	return true
}

// OnACLCheck: subscribe filters are checked against the client's read grants
// (auth §5.3 ActSub); the entry is re-fetched per check so a revoked or
// re-enrolled client is judged by current state. write=true always passes —
// the write door is OnPublish → engine, which rejects with reasons instead
// of silently dropping.
func (h *colcaHook) OnACLCheck(cl *mqtt.Client, topic string, write bool) bool {
	if write {
		return true
	}
	if isHumanListener(cl) {
		return h.humanACL(cl, topic)
	}
	entry, ok := h.reg.Get(string(cl.Properties.Username))
	if !ok || !uns.Authorize(h.scope(), entry, uns.ActSub, topic) {
		h.log.Warn("mqtt subscribe denied", "ulid", string(cl.Properties.Username), "filter", topic)
		h.metrics.ACLDeny(metrics.ACLSub)
		return false
	}
	return true
}

// OnSubscribed fires after mochi has registered a client's subscription(s)
// (auth §6: every filter already passed the read-grant ACL check by this
// point — see OnACLCheck below). Time-sync design §2.2 (as amended,
// [delta]): beacon on subscribe to colca/v1/_TimeSync/+ (or the exact
// per-node topic), not on bare session establishment — see
// matchesOwnBeacon's doc comment for why session-establishment was
// deterministically racy and had to be replaced, not merely supplemented.
// Subscribing to _TimeSync is the moment delivery to THIS client becomes
// possible at all, so triggering here is unconditionally correct: no
// earlier point could have worked, and no later point is needed. A
// reconnecting machine (cmd/colca-machine's onConnect: subscribe cmd, then
// subscribe beacon) gets its beacon within one broker round trip of its own
// SUBSCRIBE packet — milliseconds, deterministically, not racing anything.
//
// A client's SUBSCRIBE packet may carry several filters; this only needs to
// fire once even if more than one happens to match (never possible in
// practice today — colca-machine always subscribes _TimeSync alone — but
// correct regardless).
func (h *colcaHook) OnSubscribed(cl *mqtt.Client, pk packets.Packet, reasonCodes []byte) {
	for _, sub := range pk.Filters {
		if h.matchesOwnBeacon(sub.Filter) {
			h.publishTimeSync()
			return
		}
	}
}

// matchesOwnBeacon reports whether filter would let its subscriber receive
// THIS node's _TimeSync beacon (colca/v1/_TimeSync/{this node's ulid} —
// uns.TimeSyncTopic). _TimeSync topics are always exactly 4 segments
// (plugins/uns.Parse's dedicated special case), so a matching filter is
// either the exact topic or ends in a single-level wildcard at that
// position — cmd/colca-machine always subscribes "colca/v1/_TimeSync/+"
// (design §2.2), matched here alongside the exact form for any other
// well-behaved subscriber (e.g. the observer identity).
func (h *colcaHook) matchesOwnBeacon(filter string) bool {
	seg := strings.Split(filter, "/")
	return len(seg) == 4 && seg[0] == "colca" && seg[1] == "v1" && seg[2] == "_TimeSync" &&
		(seg[3] == "+" || seg[3] == h.cfg.ULID)
}

// publishTimeSync publishes one _TimeSync beacon on this node's local bus
// (design §2.2): topic colca/v1/_TimeSync/{node-ulid}, payload {"now_ms":
// <int64>}, never retained — a retained time message is by definition stale.
// now_ms is the engine's own authoritative-now estimate (AuthoritativeNow),
// matching the convention repl/server.go already uses for /downlink and
// /replicate responses. A no-op before the engine is bound (startup race
// with a fast-connecting client) or if marshaling somehow fails (can't
// happen for this fixed shape — defensive only).
//
// Published at QoS 0, deliberately: time is
// inherently ephemeral — only the latest sample is ever meaningful — and
// mochi only ever queues a message into a client's persistent-session
// Inflight map for QoS>0 (mqtt/server/v2@v2.7.9 server.go's
// publishToClient: the Inflight.Set/resend path is entirely inside
// `if out.FixedHeader.Qos > 0`). At QoS 0 a beacon published while a machine
// is offline is simply dropped for that machine, never queued and replayed
// on reconnect. That removes the whole class of stale-beacon redelivery: a
// machine that was offline for ≥1 beacon_interval no longer receives a
// backdated now_ms racing ahead of the fresh, connect-triggered one — it
// only ever sees beacons published while it is actually connected.
func (h *colcaHook) publishTimeSync() {
	eng := h.engine()
	if eng == nil {
		return
	}
	payload, err := json.Marshal(struct {
		NowMS int64 `json:"now_ms"`
	}{eng.AuthoritativeNow().UnixMilli()})
	if err != nil {
		h.log.Warn("time-sync beacon not published: encode failed", "err", err)
		return
	}
	topic := uns.TimeSyncTopic(h.cfg.ULID)
	if err := h.broker.Publish(topic, payload, false, 0); err != nil {
		h.log.Warn("time-sync beacon publish failed", "topic", topic, "err", err)
	}
}

// OnPublish routes every authenticated client publish through the engine. A
// rejected packet is dropped by mochi (no PUBACK under MQTT 3.1.1) and nothing
// is persisted. Publishes from the inline client (empty identity, i.e.
// DeliverLocal) bypass ingest and pass through untouched.
//
// A persisted packet is answered with packets.CodeSuccessIgnore: mochi sets
// pk.Ignore, which makes publishToSubscribers and the retain handling return
// early while the client still gets its PUBACK. The client's RAW topic is
// therefore never distributed — the engine publishes the CANONICAL,
// mount-rewritten form instead, so a subscriber on colca/# sees each record once.
// A non-UNS topic is not persisted and must be distributed normally: outside
// colca/# Colca is just a broker.
// rejectCode maps an engine rejection to the MQTT-5 PUBACK reason code
// (schema-bundle design §8.1). MQTT 3.1.1 clients and QoS 0 publishes have no
// protocol channel for a negative ack — mochi only writes these codes into a
// QoS>=1 PUBACK for MQTT 5 sessions; everywhere else the packet is simply
// dropped, exactly as before, and the metrics reason stays the observable.
func rejectCode(cl *mqtt.Client, err error) error {
	if cl.Properties.ProtocolVersion < 5 {
		// MQTT 3.1.1 has no PUBACK reason field — an acked packet reads as
		// SUCCESS there, so the pre-bundle silent drop is the only honest
		// answer for v3 sessions.
		return packets.ErrRejectPacket
	}
	switch engine.ReasonOf(err) {
	case metrics.ReasonGrammar:
		// The topic itself is not a valid uns coordinate — includes
		// "unknown contract", which lives in the topic.
		return packets.ErrTopicNameInvalid
	case metrics.ReasonValidation:
		return packets.ErrPayloadFormatInvalid
	case metrics.ReasonIdentity, metrics.ReasonCmdDenied, metrics.ReasonRegistryContract,
		metrics.ReasonNoMount, metrics.ReasonHumanWrite, metrics.ReasonTimeSync:
		// All authorization facts: who may write what where. no_mount is an
		// authorization fact too (§8.1 [delta]) — a read-only observer has
		// no write standing.
		return packets.ErrNotAuthorized
	case metrics.ReasonDraining:
		// Temporarily refused: the destination is being decommissioned —
		// wait or retarget; not an authorization verdict.
		return packets.ErrServerBusy
	default:
		return packets.ErrRejectPacket // untyped: keep the silent-drop behavior
	}
}

func (h *colcaHook) OnPublish(cl *mqtt.Client, pk packets.Packet) (packets.Packet, error) {
	ident := string(cl.Properties.Username)
	if ident == "" {
		return pk, nil
	}
	eng := h.engine()
	if eng == nil {
		h.log.Warn("publish rejected: no engine bound", "identity", ident, "topic", pk.TopicName)
		return pk, packets.ErrRejectPacket
	}
	if isHumanListener(cl) {
		// Humans: uns topics go through IngestHuman (commands only, §5.2);
		// non-UNS stays plain broker traffic exactly like machines.
		if !uns.IsUns(pk.TopicName) {
			return pk, nil
		}
		s, ok := h.humans.get(cl.ID)
		if !ok || time.Now().After(s.exp) {
			return pk, packets.ErrRejectPacket
		}
		res, err := eng.IngestHuman(s.entry, pk.TopicName, pk.Payload)
		if err != nil {
			h.log.Warn("human publish rejected", "sub", s.sub, "topic", pk.TopicName, "err", err)
			return pk, rejectCode(cl, err)
		}
		h.log.Debug("human ingest", "sub", s.sub, "topic", res.Topic, "stream", res.Stream, "offset", res.Offset)
		return pk, packets.CodeSuccessIgnore
	}
	res, err := eng.IngestClient(ident, pk.TopicName, pk.Payload)
	if err != nil {
		h.log.Warn("publish rejected", "identity", ident, "topic", pk.TopicName, "err", err)
		return pk, rejectCode(cl, err)
	}
	if res.Persisted {
		h.log.Debug("mqtt ingest", "identity", ident, "topic_in", pk.TopicName,
			"topic_stored", res.Topic, "stream", res.Stream, "offset", res.Offset)
		return pk, packets.CodeSuccessIgnore
	}
	return pk, nil
}

// New builds the broker with a TLS listener bound immediately (AddListener
// calls Init → net.Listen), so Addr reports the resolved address before Serve
// runs. The server cert wraps the node's own key (cert = key container, trust
// = pinning — repl's exact posture). eng may be nil and be supplied later via
// SetEngine. m may be nil (every Metrics method is nil-safe).
func New(cfg *config.Config, id *identity.Identity, reg *registry.Manager, ver *tokenauth.Verifier, eng *engine.Engine, m *metrics.Metrics) (*Server, error) {
	cert, err := id.SelfSignedCert(cfg.ULID)
	if err != nil {
		return nil, err
	}
	tlsCfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tls.RequireAnyClientCert, // pinning happens in OnConnectAuthenticate
		MinVersion:   tls.VersionTLS13,
	}
	// Human doors carry no client certs — the credential is the JWT (§5.1).
	humanTLS := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS13,
	}

	// Every listener gets its own *tls.Config instance. crypto/tls mutates a
	// Config it is handed (e.g. net/http's ServeTLS calling
	// http2ConfigureServer, which appends to NextProtos via
	// onceSetNextProtoDefaults) concurrently with crypto/tls itself reading
	// that same Config during another listener's handshake — sharing one
	// pointer across listeners is a data race even when the listeners never
	// touch each other's connections. humanTLS was previously handed to both
	// human-tcp and human-ws as the same instance; tlsCfg is only handed to
	// one listener today but is cloned too so "one Config per listener"
	// stays an invariant, not something that happens to hold.
	s := mqtt.New(&mqtt.Options{InlineClient: true})
	// A fresh subscriber replaying the retained set (the bus's "current state
	// on connect" contract) can burst thousands of QoS-1 messages to one
	// client. mochi's default MaximumInflight (8192) silently drops anything
	// beyond that with no retry — proven by the cardinality benchmark scenario
	// at its default 10000 paths (colca/bench/cardinality.go). Raise it to the
	// protocol maximum so replay at realistic path cardinalities can't be
	// silently truncated. This moves the truncation cliff, it does not remove
	// it: silent drops now start above 65,535 retained paths in a single
	// namespace. If that cardinality becomes realistic, the real fix is
	// chunked/paginated retained replay, not a further bump of this field.
	s.Options.Capabilities.MaximumInflight = 65535
	hook := &colcaHook{eng: eng, reg: reg, ver: ver, humans: newHumanSessions(),
		cfg: cfg, log: slog.Default().With("node", cfg.ULID, "comp", "mqtt"), metrics: m, broker: s}
	if err := s.AddHook(hook, nil); err != nil {
		return nil, err
	}
	srv := &Server{S: s, hook: hook, cfg: cfg, metrics: m, sweepStop: make(chan struct{})}
	if cfg.MQTT.Addr != "" {
		tcp := listeners.NewTCP(listeners.Config{ID: "tls", Address: cfg.MQTT.Addr, TLSConfig: tlsCfg.Clone()})
		if err := s.AddListener(tcp); err != nil {
			return nil, err
		}
		srv.tcp = tcp
	}
	if cfg.MQTTHuman.TCPAddr != "" {
		h := listeners.NewTCP(listeners.Config{ID: listenerHumanTCP, Address: cfg.MQTTHuman.TCPAddr, TLSConfig: humanTLS.Clone()})
		if err := s.AddListener(h); err != nil {
			return nil, err
		}
		srv.humanTCP = h
	}
	if cfg.MQTTHuman.WSAddr != "" {
		// mochi's Websocket listener binds inside Serve and its Address()
		// echoes the CONFIG string, so a ":0" port would be unreportable.
		// Pre-resolve it: bind, read the kernel-assigned port, release, and
		// hand the concrete address to the listener. The reuse window is
		// microseconds and only ":0" configs (tests) take this path.
		wsAddr, err := resolveAddr(cfg.MQTTHuman.WSAddr)
		if err != nil {
			return nil, err
		}
		w := listeners.NewWebsocket(listeners.Config{ID: listenerHumanWS, Address: wsAddr, TLSConfig: humanTLS.Clone()})
		if err := s.AddListener(w); err != nil {
			return nil, err
		}
		srv.humanWS = w
	}
	if srv.humanTCP != nil || srv.humanWS != nil {
		go srv.runSweeper(srv.sweepStop)
	}
	return srv, nil
}

// resolveAddr turns a ":0" listen address into a concrete one by briefly
// binding it. Addresses with fixed ports pass through untouched.
func resolveAddr(addr string) (string, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil || port != "0" {
		return addr, err
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return "", err
	}
	resolved := ln.Addr().String()
	_ = ln.Close()
	if host == "" {
		return resolved, nil
	}
	_, p, err := net.SplitHostPort(resolved)
	if err != nil {
		return "", err
	}
	return net.JoinHostPort(host, p), nil
}

// SetEngine late-binds the engine the publish hook ingests into.
func (s *Server) SetEngine(e *engine.Engine) { s.hook.setEngine(e) }

// PublishTimeSync publishes one _TimeSync beacon (design §2.2). Exported so
// node.Start's periodic loop (RunBeacon) and any direct caller (tests) can
// trigger a beacon without going through a real MQTT subscribe event.
func (s *Server) PublishTimeSync() { s.hook.publishTimeSync() }

// RunBeacon publishes a _TimeSync beacon every interval until stop is closed
// (design §2.2's periodic cadence — the per-subscribe publish is the
// separate OnSubscribed trigger above; together they give a reconnecting
// machine time within milliseconds of its own subscribe and every attached
// machine a periodic refresh). interval <= 0 disables the periodic beacon
// entirely — config.TimeSync.EffectiveBeaconInterval never itself returns
// <= 0, so this guard is for direct callers/tests only.
func (s *Server) RunBeacon(interval time.Duration, stop <-chan struct{}) {
	if interval <= 0 {
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			s.PublishTimeSync()
		}
	}
}

// Kick disconnects every live session of the given identity (auth §7) — the
// registry manager calls this on revoke and re-enroll. The next CONNECT is
// judged against the updated registry.
func (s *Server) Kick(ulid string) {
	for _, cl := range s.S.Clients.GetAll() {
		if string(cl.Properties.Username) == ulid {
			_ = s.S.DisconnectClient(cl, packets.ErrNotAuthorized)
			s.metrics.SessionKick()
		}
	}
}

// Addr returns the resolved machine-listener address ("" when the node has
// no machine door — a human-only broker is legal, design §4).
func (s *Server) Addr() string {
	if s.tcp == nil {
		return ""
	}
	return s.tcp.Address()
}

// HumanTCPAddr / HumanWSAddr report the resolved human-door addresses ("" when
// not configured).
func (s *Server) HumanTCPAddr() string {
	if s.humanTCP == nil {
		return ""
	}
	return s.humanTCP.Address()
}
func (s *Server) HumanWSAddr() string {
	if s.humanWS == nil {
		return ""
	}
	return s.humanWS.Address()
}

func (s *Server) Serve() error { return s.S.Serve() }

// Close shuts the broker down without ever letting mochi walk the client map
// while a client is unwinding.
//
// The hazard is upstream. mochi's Clients.GetByListener (clients.go:95) takes a
// read lock and then calls Clients.Len, which takes the same read lock a second
// time. Go blocks a recursive read lock as soon as a writer is queued, and the
// writer is Clients.Delete — exactly what a client runs as it disconnects
// (server.go, end of attachClient). Server.Close reaches GetByListener through
// closeListenerClients, so closing while any client unwinds can deadlock the two
// against each other permanently. Seen as a 10-minute timeout in
// TestTimeSyncBeaconPeriodicCadence.
//
// So this never calls closeListenerClients with clients in flight. It refuses
// new connections, disconnects the clients it has itself — iterating a COPY from
// GetAll, holding no lock, which is the part mochi gets wrong — and then closes
// the listeners with a closer that does nothing. That drains every attachClient
// goroutine (CloseAll waits on ClientsWg, listeners.go:134) with no walk of the
// map. mochi's own Close then finds the TCP listeners already ended, so their
// closer is skipped entirely; the websocket listener calls it unconditionally,
// but by then nothing can be queued behind it.
//
// Waiting on ClientsWg directly instead was the first attempt and is wrong: a
// connection accepted before the listener closes calls ClientsWg.Add inside
// attachClient BEFORE any hook can refuse it, so the wait races the add and the
// race detector fails the build. Letting mochi do its own waiting, after its own
// listeners are shut, has no such window.
//
// Close is idempotent: mochi's Server.Close closes an unbuffered done channel
// and panics on a second call, so a node that shuts down twice — a deferred
// close plus an explicit one — would crash on the way out.
func (s *Server) Close() error {
	s.closeOnce.Do(func() {
		s.sweepOnce.Do(func() { close(s.sweepStop) })

		s.hook.closing.Store(true)
		for _, cl := range s.S.Clients.GetAll() {
			_ = s.S.DisconnectClient(cl, packets.ErrServerShuttingDown)
		}
		s.S.Listeners.CloseAll(func(string) {})
		s.closeErr = s.S.Close()
	})
	return s.closeErr
}

// DeliverLocal publishes into the local broker. It matches engine.LocalDeliver
// and is how every record the engine appends reaches this node's MQTT bus,
// under the stored (canonical) topic. retain is passed through: state contracts
// are retained so a fresh subscriber gets the current value on connect, events
// are not.
func (s *Server) DeliverLocal(topic string, payload []byte, retain bool) {
	if err := s.S.Publish(topic, payload, retain, 1); err != nil {
		slog.Default().Warn("local delivery failed", "topic", topic, "retain", retain, "err", err)
	}
}
