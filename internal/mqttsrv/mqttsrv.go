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
	"sync"
	"time"

	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/listeners"
	"github.com/mochi-mqtt/server/v2/packets"

	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/engine"
	"github.com/alpamayo-solutions/colca/internal/identity"
	"github.com/alpamayo-solutions/colca/internal/metrics"
	"github.com/alpamayo-solutions/colca/internal/registry"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// Server is the embedded broker plus its Colca hook.
type Server struct {
	S       *mqtt.Server
	tcp     *listeners.TCP
	hook    *colcaHook
	cfg     *config.Config
	metrics *metrics.Metrics
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
}

func (h *colcaHook) engine() *engine.Engine {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.eng
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
		mqtt.OnSessionEstablished,
	}, []byte{b})
}

// OnConnectAuthenticate resolves the TLS peer key against the local registry
// (auth §6.1): the entry must exist, be kind machine, and the CONNECT
// username must equal the enrolled ULID — the username is how mochi carries
// the identity to OnPublish/OnACLCheck, so the equality check pins it to the
// key.
func (h *colcaHook) OnConnectAuthenticate(cl *mqtt.Client, pk packets.Packet) bool {
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
	if entry.Kind != uns.KindMachine {
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
	entry, ok := h.reg.Get(string(cl.Properties.Username))
	if !ok || !uns.Authorize(entry, uns.ActSub, topic) {
		h.log.Warn("mqtt subscribe denied", "ulid", string(cl.Properties.Username), "filter", topic)
		h.metrics.ACLDeny(metrics.ACLSub)
		return false
	}
	return true
}

// OnSessionEstablished fires once per (re)connect, after OnConnectAuthenticate
// has already run and the CONNACK has been sent (mochi server.go
// attachClient) — i.e. every call here is an authenticated machine session
// (the only kind this door accepts). Time-sync design §2.2: beacon on every
// machine session establishment, so a reconnecting machine gets time within
// milliseconds, before or with its redelivered command backlog. The periodic
// beacon_interval cadence is a separate trigger (Server.RunBeacon), not this
// hook.
func (h *colcaHook) OnSessionEstablished(cl *mqtt.Client, pk packets.Packet) {
	h.publishTimeSync()
}

// publishTimeSync publishes one _TimeSync beacon on this node's local bus
// (design §2.2): topic colca/v1/_TimeSync/{node-ulid}, payload {"now_ms":
// <int64>}, never retained — a retained time message is by definition stale.
// now_ms is the engine's own authoritative-now estimate (AuthoritativeNow),
// matching the convention repl/server.go already uses for /downlink and
// /replicate responses. A no-op before the engine is bound (startup race
// with a fast-connecting client) or if marshaling somehow fails (can't
// happen for this fixed shape — defensive only).
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
	if err := h.broker.Publish(topic, payload, false, 1); err != nil {
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
	res, err := eng.IngestClient(ident, pk.TopicName, pk.Payload)
	if err != nil {
		h.log.Warn("publish rejected", "identity", ident, "topic", pk.TopicName, "err", err)
		return pk, packets.ErrRejectPacket
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
func New(cfg *config.Config, id *identity.Identity, reg *registry.Manager, eng *engine.Engine, m *metrics.Metrics) (*Server, error) {
	cert, err := id.SelfSignedCert(cfg.ULID)
	if err != nil {
		return nil, err
	}
	tlsCfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tls.RequireAnyClientCert, // pinning happens in OnConnectAuthenticate
		MinVersion:   tls.VersionTLS13,
	}

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
	hook := &colcaHook{eng: eng, reg: reg, cfg: cfg, log: slog.Default().With("node", cfg.ULID, "comp", "mqtt"), metrics: m, broker: s}
	if err := s.AddHook(hook, nil); err != nil {
		return nil, err
	}
	tcp := listeners.NewTCP(listeners.Config{ID: "tls", Address: cfg.MQTT.Addr, TLSConfig: tlsCfg})
	if err := s.AddListener(tcp); err != nil {
		return nil, err
	}
	return &Server{S: s, tcp: tcp, hook: hook, cfg: cfg, metrics: m}, nil
}

// SetEngine late-binds the engine the publish hook ingests into.
func (s *Server) SetEngine(e *engine.Engine) { s.hook.setEngine(e) }

// PublishTimeSync publishes one _TimeSync beacon (design §2.2). Exported so
// node.Start's periodic loop (RunBeacon) and any direct caller (tests) can
// trigger a beacon without going through a real MQTT session-establish event.
func (s *Server) PublishTimeSync() { s.hook.publishTimeSync() }

// RunBeacon publishes a _TimeSync beacon every interval until stop is closed
// (design §2.2's periodic cadence — the per-session publish on establishment
// is the separate OnSessionEstablished trigger above; together they give a
// reconnecting machine time within milliseconds and every attached machine a
// periodic refresh). interval <= 0 disables the periodic beacon entirely —
// config.TimeSync.EffectiveBeaconInterval never itself returns <= 0, so this
// guard is for direct callers/tests only.
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

// Addr returns the resolved listener address (meaningful for ":0" configs).
func (s *Server) Addr() string { return s.tcp.Address() }

func (s *Server) Serve() error { return s.S.Serve() }
func (s *Server) Close() error { return s.S.Close() }

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
