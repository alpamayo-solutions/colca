// Package mqttsrv embeds a mochi-mqtt broker and wires it to the engine: every
// client authenticates against config.Clients and every client PUBLISH is
// funnelled through engine.IngestClient, so the broker can never persist
// anything the engine would not accept.
package mqttsrv

import (
	"bytes"
	"crypto/subtle"
	"log/slog"
	"sync"

	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/listeners"
	"github.com/mochi-mqtt/server/v2/packets"

	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/engine"
	"github.com/alpamayo-solutions/colca/internal/metrics"
)

// Server is the embedded broker plus its Colca hook.
type Server struct {
	S    *mqtt.Server
	tcp  *listeners.TCP
	hook *colcaHook
	cfg  *config.Config
}

// colcaHook authenticates clients against config.Clients (ulid+token) and
// enforces the engine's rules on publish.
//
// The engine is late-bound: node assembly needs the broker's DeliverLocal to
// build the engine, and the hook needs that same engine — so New may be called
// with a nil engine and SetEngine swaps it in afterwards. mu guards that swap
// against the broker's connection goroutines.
type colcaHook struct {
	mqtt.HookBase
	mu      sync.RWMutex
	eng     *engine.Engine
	cfg     *config.Config
	log     *slog.Logger
	metrics *metrics.Metrics // nil-safe: every Metrics method is a no-op on nil
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
	}, []byte{b})
}

// OnConnectAuthenticate accepts a client only if its username matches a
// configured client ULID and its password matches that client's token. With no
// configured clients nobody can connect.
func (h *colcaHook) OnConnectAuthenticate(cl *mqtt.Client, pk packets.Packet) bool {
	user := string(pk.Connect.Username)
	pass := pk.Connect.Password
	for _, c := range h.cfg.Clients {
		if c.ULID == user && subtle.ConstantTimeCompare([]byte(c.Token), pass) == 1 {
			h.log.Debug("mqtt client authenticated", "ulid", user)
			return true
		}
	}
	h.log.Warn("mqtt auth rejected", "user", user)
	h.metrics.RejectPublish(metrics.ReasonAuth)
	return false
}

// OnACLCheck is intentionally permissive: subscribe-side ACLs are out of MVP
// scope. Write-side rules (grammar, identity, mount, validation) are enforced
// in OnPublish via the engine, which is the only path that can persist.
func (h *colcaHook) OnACLCheck(cl *mqtt.Client, topic string, write bool) bool {
	return true
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
	identity := string(cl.Properties.Username)
	if identity == "" {
		return pk, nil
	}
	eng := h.engine()
	if eng == nil {
		h.log.Warn("publish rejected: no engine bound", "identity", identity, "topic", pk.TopicName)
		return pk, packets.ErrRejectPacket
	}
	res, err := eng.IngestClient(identity, pk.TopicName, pk.Payload)
	if err != nil {
		h.log.Warn("publish rejected", "identity", identity, "topic", pk.TopicName, "err", err)
		return pk, packets.ErrRejectPacket
	}
	if res.Persisted {
		h.log.Debug("mqtt ingest", "identity", identity, "topic_in", pk.TopicName,
			"topic_stored", res.Topic, "stream", res.Stream, "offset", res.Offset)
		return pk, packets.CodeSuccessIgnore
	}
	return pk, nil
}

// New builds the broker and binds its TCP listener immediately (AddListener
// calls Init → net.Listen), so Addr reports the resolved address before Serve
// runs. eng may be nil and be supplied later via SetEngine. m may be nil
// (every Metrics method is nil-safe).
func New(cfg *config.Config, eng *engine.Engine, m *metrics.Metrics) (*Server, error) {
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
	hook := &colcaHook{eng: eng, cfg: cfg, log: slog.Default().With("node", cfg.ULID, "comp", "mqtt"), metrics: m}
	if err := s.AddHook(hook, nil); err != nil {
		return nil, err
	}
	tcp := listeners.NewTCP(listeners.Config{ID: "tcp", Address: cfg.MQTT.Addr})
	if err := s.AddListener(tcp); err != nil {
		return nil, err
	}
	return &Server{S: s, tcp: tcp, hook: hook, cfg: cfg}, nil
}

// SetEngine late-binds the engine the publish hook ingests into.
func (s *Server) SetEngine(e *engine.Engine) { s.hook.setEngine(e) }

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
