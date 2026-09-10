// Package mqttsrv embeds a mochi-mqtt broker and wires it to the engine. The
// machine listener is TLS-only: a machine's TLS client key is pinned against the
// registry, with no CA and no tokens. Every client publish goes through
// engine.IngestClient, and every subscribe filter is checked against the client's
// read grants.
package mqttsrv

import (
	"crypto/tls"
	"encoding/json"
	"log/slog"
	"slices"
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
	local    *listeners.TCP
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

// colcaHook authenticates clients against the registry and enforces the engine's
// rules on publish and read grants on subscribe.
//
// The engine is bound late: node assembly needs the broker to build the engine, so
// New accepts a nil engine and SetEngine fills it in. mu guards that swap.
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
	// broker is the server New builds around this hook, never nil in practice. The
	// hook uses it to publish the time-sync beacon.
	broker *mqtt.Server
	// closing is set by Server.Close before it disconnects anyone. While set, the door
	// refuses new connections, so shutdown is not chasing arrivals. See Server.Close.
	closing atomic.Bool
}

func (h *colcaHook) engine() *engine.Engine {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.eng
}

// scope is what the ACL checks resolve grants through. Nil before the engine is
// bound, which denies every scoped grant.
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

func (h *colcaHook) auditDenied(operation, reason, door string, entry *uns.Entry, metadata map[string]any) {
	h.auditDeniedAt(operation, reason, door, entry, metadata, "", "")
}

func (h *colcaHook) auditDeniedAt(operation, reason, door string, entry *uns.Entry,
	metadata map[string]any, entityType, entityID string) {
	eng := h.engine()
	if eng == nil {
		return
	}
	if metadata == nil {
		metadata = map[string]any{}
	}
	metadata["door"] = door
	d := engine.AuditDenial{
		Operation: operation, ReasonCode: reason, Metadata: metadata,
		EntityType: entityType, EntityID: entityID,
	}
	if entry != nil {
		d.ActorID, d.ActorLabel, d.ActorKind = entry.ULID, entry.Name, entry.ActorKind()
	}
	_ = eng.RecordDenial(d)
}

func (h *colcaHook) ID() string { return "colca" }

func (h *colcaHook) Provides(b byte) bool {
	return slices.Contains([]byte{
		mqtt.OnConnectAuthenticate,
		mqtt.OnACLCheck,
		mqtt.OnPublish,
		mqtt.OnWillSent,
		mqtt.OnDisconnect,
		mqtt.OnSubscribe,
		mqtt.OnSubscribed,
		mqtt.OnPublishDropped,
	}, b)
}

// OnWillSent runs after mochi broadcast a client's last will to live subscribers
// and its retained cache. That path skips OnPublish, so without this the will
// never reaches the engine and a crashed service's _ServiceDetails would not
// change in KV. The will goes through the same ingest as OnPublish; mochi has
// already delivered it, so there is nothing to reject.
func (h *colcaHook) OnWillSent(cl *mqtt.Client, pk packets.Packet) {
	ident := string(cl.Properties.Username)
	if ident == "" || !uns.IsUns(pk.TopicName) {
		return
	}
	eng := h.engine()
	if eng == nil {
		return
	}
	if _, err := eng.IngestClient(ident, pk.TopicName, pk.Payload); err != nil {
		h.log.Warn("will publish rejected", "identity", ident, "topic", pk.TopicName, "err", err)
	}
}

// OnPublishDropped runs when mochi discards a publish because the client's
// outbound queue is full. mochi only logs that at Debug, so count it here and name
// the client, or a truncated retained replay leaves no trace.
func (h *colcaHook) OnPublishDropped(cl *mqtt.Client, pk packets.Packet) {
	h.metrics.PublishDropped()
	h.log.Warn("publish dropped: client outbound queue full", "client", cl.ID, "topic", pk.TopicName)
}

const quotaDeniedSubscription = "$COLCA/quota-exceeded"

// OnSubscribe keeps one authenticated client from growing the broker's topic
// index without bound. Mochi's hook API cannot return a per-filter quota code,
// so excess filters are replaced with a reserved filter that OnACLCheck denies;
// MQTT 5 receives Not Authorized and MQTT 3 receives the standard failed-SUBACK
// value. The audit event retains the real filter and the precise quota reason.
func (h *colcaHook) OnSubscribe(cl *mqtt.Client, pk packets.Packet) packets.Packet {
	limit := h.cfg.MQTTLimits.EffectiveMaxSubscriptionsPerClient()
	known := cl.State.Subscriptions.GetAll()
	count := len(known)
	for i := range pk.Filters {
		filter := pk.Filters[i].Filter
		if _, exists := known[filter]; exists {
			continue
		}
		if count < limit {
			known[filter] = pk.Filters[i]
			count++
			continue
		}

		h.log.Warn("mqtt subscription rejected: client quota reached",
			"identity", string(cl.Properties.Username), "filter", filter, "limit", limit)
		h.auditDenied("read", "subscription_quota", metrics.DoorMQTT, h.entryForClient(cl),
			map[string]any{"filter": filter, "limit": limit})
		pk.Filters[i].Filter = quotaDeniedSubscription
	}
	return pk
}

// OnConnectAuthenticate resolves the TLS peer key against the registry: the entry
// must exist and be external, and the username must equal its ULID. mochi carries
// the username to OnPublish and OnACLCheck, so the check ties it to the key.
func (h *colcaHook) OnConnectAuthenticate(cl *mqtt.Client, pk packets.Packet) bool {
	// Refuse everything once shutdown began: a refused connection never joins the
	// client map Close drains.
	if h.closing.Load() {
		return false
	}
	if isLocalListener(cl) {
		return h.authenticateLocal(cl, pk)
	}
	if isHumanListener(cl) {
		return h.authenticateHuman(cl, pk)
	}
	user := string(pk.Connect.Username)
	tc, ok := cl.Net.Conn.(*tls.Conn)
	if !ok || len(tc.ConnectionState().PeerCertificates) == 0 {
		h.log.Warn("mqtt auth rejected: no client certificate", "user", user)
		h.metrics.AuthReject(metrics.DoorMQTT, metrics.AuthUnknownKey)
		h.auditDenied("authenticate", metrics.AuthUnknownKey, metrics.DoorMQTT, nil, nil)
		return false
	}
	pub, err := identity.PeerPubHex(tc.ConnectionState().PeerCertificates[0].Raw)
	if err != nil {
		h.log.Warn("mqtt auth rejected: unusable client certificate", "user", user, "err", err)
		h.metrics.AuthReject(metrics.DoorMQTT, metrics.AuthUnknownKey)
		h.auditDenied("authenticate", metrics.AuthUnknownKey, metrics.DoorMQTT, nil, nil)
		return false
	}
	entry, ok := h.reg.ByPubkey(pub)
	if !ok {
		h.log.Warn("mqtt auth rejected: key not enrolled", "user", user)
		h.metrics.AuthReject(metrics.DoorMQTT, metrics.AuthUnknownKey)
		h.auditDenied("authenticate", metrics.AuthUnknownKey, metrics.DoorMQTT, nil, nil)
		return false
	}
	if !entry.MayUseDoor(uns.DoorMQTT) {
		h.log.Warn("mqtt auth rejected: kind may not use this door", "user", user, "kind", entry.Kind)
		h.metrics.AuthReject(metrics.DoorMQTT, metrics.AuthKind)
		h.auditDenied("authenticate", metrics.AuthKind, metrics.DoorMQTT, entry, nil)
		return false
	}
	if user != entry.ULID {
		h.log.Warn("mqtt auth rejected: username != enrolled ulid", "user", user, "ulid", entry.ULID)
		h.metrics.AuthReject(metrics.DoorMQTT, metrics.AuthUsernameMismatch)
		h.auditDenied("authenticate", metrics.AuthUsernameMismatch, metrics.DoorMQTT, entry, nil)
		return false
	}
	h.log.Debug("mqtt client authenticated", "ulid", entry.ULID)
	return true
}

// OnACLCheck checks subscribe filters against the client's read grants, fetching
// the entry each time so revocation takes effect. Writes always pass here;
// OnPublish sends them through the engine, which rejects with reasons.
func (h *colcaHook) OnACLCheck(cl *mqtt.Client, topic string, write bool) bool {
	if write {
		return true
	}
	if topic == quotaDeniedSubscription {
		return false
	}
	if isHumanListener(cl) {
		return h.humanACL(cl, topic)
	}
	entry, ok := h.reg.Get(string(cl.Properties.Username))
	if !ok || !uns.Authorize(h.scope(), entry, uns.ActSub, topic) {
		h.log.Warn("mqtt subscribe denied", "ulid", string(cl.Properties.Username), "filter", topic)
		h.metrics.ACLDeny(metrics.ACLSub)
		h.auditDenied("read", "subscribe_denied", metrics.DoorMQTT, entry,
			map[string]any{"filter": topic})
		return false
	}
	return true
}

func (h *colcaHook) entryForClient(cl *mqtt.Client) *uns.Entry {
	if isHumanListener(cl) {
		if session, ok := h.humans.get(cl.ID); ok {
			return session.entry
		}
		return nil
	}
	entry, _ := h.reg.Get(string(cl.Properties.Username))
	return entry
}

// OnSubscribed runs after mochi registered a client's subscriptions, which already
// passed the ACL.
//
// It sends a time-sync beacon when a filter matches this node's beacon topic:
// subscribing is the first moment the client can receive it, so a reconnecting
// machine gets one within a round trip. It also tries to replay commands owed to a
// machine identity. The replay decides per record whether the machine is
// listening, so running it on every subscribe costs nothing when nothing is owed.
func (h *colcaHook) OnSubscribed(cl *mqtt.Client, pk packets.Packet, reasonCodes []byte) {
	for _, sub := range pk.Filters {
		if h.matchesOwnBeacon(sub.Filter) {
			h.publishTimeSync()
			break
		}
	}
	h.replayOwedCommands(cl)
}

// replayOwedCommands hands the subscribing identity to the engine's replay. Local
// and human clients are skipped: neither can be the target of a command at level
// 4. Which records are owed is decided by uns.OwedCommand.
func (h *colcaHook) replayOwedCommands(cl *mqtt.Client) {
	if isLocalListener(cl) || isHumanListener(cl) {
		return
	}
	eng := h.engine()
	if eng == nil {
		return
	}
	entry, ok := h.reg.Get(string(cl.Properties.Username))
	if !ok {
		return
	}
	if n := eng.ReplayOwedCommands(entry); n > 0 {
		h.log.Info("replayed commands owed to a reconnecting machine",
			"ulid", entry.ULID, "count", n)
	}
}

// matchesOwnBeacon reports whether filter delivers this node's beacon topic
// (uns.TimeSyncTopic). _TimeSync topics always have four segments, so a match is
// the exact topic or a single-level wildcard in the last position.
func (h *colcaHook) matchesOwnBeacon(filter string) bool {
	seg := strings.Split(filter, "/")
	return len(seg) == 4 && seg[0] == uns.Root() && seg[1] == uns.Version && seg[2] == "_TimeSync" &&
		(seg[3] == "+" || seg[3] == h.cfg.ULID)
}

// publishTimeSync publishes one beacon on the local bus: topic
// colca/v1/_TimeSync/{node ulid}, payload {"now_ms": ...} from the engine's
// AuthoritativeNow, never retained. A no-op before the engine is bound.
//
// It publishes at QoS 0 on purpose: mochi queues only QoS>0 messages for offline
// persistent sessions, so a machine never receives stale beacons on reconnect.
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

// rejectCode maps an engine rejection to an MQTT 5 PUBACK reason code. MQTT 3.1.1
// clients and QoS 0 publishes have no negative ack; for them the packet is dropped
// and the metric is the only trace.
func rejectCode(cl *mqtt.Client, err error) error {
	if cl.Properties.ProtocolVersion < 5 {
		// MQTT 3.1.1 has no PUBACK reason code, so dropping the packet is the only honest
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
	case metrics.ReasonNodeID, metrics.ReasonCmdDenied, metrics.ReasonRegistryContract,
		metrics.ReasonWriteDenied, metrics.ReasonHumanWrite, metrics.ReasonTimeSync:
		// Authorization verdicts, write_denied included: the identity has no write
		// standing at that topic.
		return packets.ErrNotAuthorized
	case metrics.ReasonDraining:
		// Temporarily refused: the destination is being decommissioned. Wait or retarget;
		// this is not an authorization verdict.
		return packets.ErrServerBusy
	default:
		return packets.ErrRejectPacket // untyped: keep the silent-drop behavior
	}
}

// OnPublish sends every authenticated client publish through the engine; a
// rejected packet is dropped and nothing is persisted. Publishes from the inline
// client (DeliverLocal) pass through.
//
// A persisted packet is answered with CodeSuccessIgnore, which skips mochi's own
// fanout while the client still gets its PUBACK. Subscribers receive the engine's
// mirror of the stored record, so each record arrives once. Non-UNS topics are
// not persisted and are delivered normally.
func (h *colcaHook) OnPublish(cl *mqtt.Client, pk packets.Packet) (packets.Packet, error) {
	ident := string(cl.Properties.Username)
	if ident == "" {
		return pk, nil
	}
	// Colca's retained set is the engine-owned UNS KV projection. Allowing an
	// external client to retain arbitrary non-UNS topics would create a second,
	// unauthoritative in-memory store with no cardinality bound. Transient
	// non-UNS broker traffic remains available.
	if pk.FixedHeader.Retain && !uns.IsUns(pk.TopicName) {
		h.log.Warn("mqtt retained publish rejected outside UNS",
			"identity", ident, "topic", pk.TopicName)
		h.auditDenied("publish", "retained_non_uns_denied", metrics.DoorMQTT,
			h.entryForClient(cl), map[string]any{"topic": pk.TopicName})
		return pk, packets.ErrRetainNotSupported
	}
	eng := h.engine()
	if eng == nil {
		h.log.Warn("publish rejected: no engine bound", "identity", ident, "topic", pk.TopicName)
		return pk, packets.ErrRejectPacket
	}
	if isHumanListener(cl) {
		// Humans: UNS topics go through IngestHuman (commands only); other topics are
		// plain broker traffic, as for machines.
		if !uns.IsUns(pk.TopicName) {
			return pk, nil
		}
		s, ok := h.humans.get(cl.ID)
		if !ok || time.Now().After(s.exp) {
			return pk, packets.ErrRejectPacket
		}
		actor := s.username
		if actor == "" {
			actor = s.sub
		}
		res, err := eng.IngestHumanAttributed(s.entry, actor, pk.TopicName, pk.Payload)
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

// New builds the broker with the TLS listener bound immediately, so Addr works
// before Serve. The server certificate wraps the node's own key; trust is pinning.
// eng may be nil and set later with SetEngine; m may be nil. maxRecordBytes is the
// record cap, from which New derives the packet-size limit.
func New(cfg *config.Config, id *identity.Identity, reg *registry.Manager, ver *tokenauth.Verifier, eng *engine.Engine, m *metrics.Metrics, maxRecordBytes uint64) (*Server, error) {
	// The machine door always uses the key container: the node pins the client's key,
	// colca-machine never verifies the server, and replication depends on the same
	// symmetry.
	cert, err := id.SelfSignedCert(cfg.ULID)
	if err != nil {
		return nil, err
	}
	tlsCfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tls.RequireAnyClientCert, // pinning happens in OnConnectAuthenticate
		MinVersion:   tls.VersionTLS13,
	}
	// Human doors take no client certificate, the token is the credential, so a
	// browser-trusted certificate from the tls: block is useful here.
	humanCert, err := identity.ServerCert(id, cfg.ULID, cfg.TLS.CertFile, cfg.TLS.KeyFile)
	if err != nil {
		return nil, err
	}
	humanTLS := &tls.Config{
		Certificates: []tls.Certificate{humanCert},
		MinVersion:   tls.VersionTLS13,
	}

	// Every listener gets its own *tls.Config: crypto/tls and net/http mutate a Config
	// while other handshakes read it, so sharing one across listeners is a data race.
	s := mqtt.New(&mqtt.Options{InlineClient: true})
	// The library's own logging, bounded and named — see mochiLogHandler.
	s.Log = slog.New(newMochiLogHandler(slog.Default().Handler()))
	mqttLimits := cfg.MQTTLimits
	s.Options.Capabilities.MaximumClients = mqttLimits.EffectiveMaxClients()
	s.Options.Capabilities.ReceiveMaximum = mqttLimits.EffectiveReceiveMaximum()
	// The per-client outbound queue. When it is full mochi drops the publish (see
	// OnPublishDropped), so like MaximumInflight it must fit a new subscriber's
	// retained replay burst.
	s.Options.Capabilities.MaximumClientWritesPending = mqttLimits.EffectiveMaxPendingWritesPerClient()
	s.Options.Capabilities.MaximumSessionExpiryInterval = uint32(mqttLimits.EffectiveMaxSessionExpiry() / time.Second) //nolint:gosec // validated against the MQTT maximum
	s.Options.Capabilities.TopicAliasMaximum = mqttLimits.EffectiveMaxTopicAliasesPerClient()
	// A new subscriber's retained replay can burst thousands of QoS 1 messages, and
	// mochi drops anything beyond MaximumInflight without retry, so raise it to the
	// protocol maximum. A replay above 65,535 retained paths would still be truncated;
	// the fix then is a paginated replay.
	s.Options.Capabilities.MaximumInflight = mqttLimits.EffectiveMaximumInflight()
	// The packet limit sits above the record cap to leave room for topic and headers;
	// Store.Append still enforces the cap. mochi's default of 0 means unlimited.
	s.Options.Capabilities.MaximumPacketSize = uint32(maxRecordBytes + 64*1024) //nolint:gosec // config caps max_record_bytes at 1 GiB
	// Shared subscriptions are refused at the ACL door, so CONNACK says they are
	// unavailable. mochi never reads this field; the door enforces it.
	s.Options.Capabilities.SharedSubAvailable = 0
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
	if cfg.MQTTLocal.Addr != "" {
		// No TLS: this listener never leaves the deployment network, and a certificate
		// would be one more thing every local service must obtain.
		l := listeners.NewTCP(listeners.Config{ID: listenerLocal, Address: cfg.MQTTLocal.Addr})
		if err := s.AddListener(l); err != nil {
			return nil, err
		}
		srv.local = l
	}
	if cfg.MQTTHuman.TCPAddr != "" {
		h := listeners.NewTCP(listeners.Config{ID: listenerHumanTCP, Address: cfg.MQTTHuman.TCPAddr, TLSConfig: humanTLS.Clone()})
		if err := s.AddListener(h); err != nil {
			return nil, err
		}
		srv.humanTCP = h
	}
	if cfg.MQTTHuman.WSAddr != "" {
		// Our mochi fork binds at Init, so a ":0" door holds its port from AddListener on
		// and reports it correctly.
		w := listeners.NewWebsocket(listeners.Config{ID: listenerHumanWS, Address: cfg.MQTTHuman.WSAddr, TLSConfig: humanTLS.Clone()})
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

// SetEngine late-binds the engine the publish hook ingests into.
func (s *Server) SetEngine(e *engine.Engine) { s.hook.setEngine(e) }

// PublishTimeSync publishes one _TimeSync beacon, for RunBeacon and tests.
func (s *Server) PublishTimeSync() { s.hook.publishTimeSync() }

// RunBeacon publishes a beacon every interval until stop closes; with the beacon
// on subscribe, attached machines also get periodic refreshes. interval <= 0
// disables it.
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

// Kick disconnects every live session of the identity. The registry calls it on
// revoke and re-enroll; the next CONNECT is judged against the updated registry.
func (s *Server) Kick(ulid string) {
	for _, cl := range s.S.Clients.GetAll() {
		if string(cl.Properties.Username) == ulid {
			_ = s.S.DisconnectClient(cl, packets.ErrNotAuthorized)
			s.metrics.SessionKick()
		}
	}
}

// Addr returns the machine listener's address, or "" when the node has no machine
// door.
func (s *Server) Addr() string {
	if s.tcp == nil {
		return ""
	}
	return s.tcp.Address()
}

// LocalAddr returns the local door's address, or "" without one. It is never
// published; it exists for local configuration and tests.
func (s *Server) LocalAddr() string {
	if s.local == nil {
		return ""
	}
	return s.local.Address()
}

// Registry exposes the identity registry this broker authenticates against.
func (s *Server) Registry() *registry.Manager { return s.hook.reg }

// Elements exposes the node's element index, so a caller can resolve a bound
// identity's placement (e.g. a self-registered local service's declared
// mount) without a second reference to the engine.
func (s *Server) Elements() *uns.ElementIndex {
	if eng := s.hook.engine(); eng != nil {
		return eng.Elements()
	}
	return nil
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

// Close shuts the broker down without letting mochi walk its client map while a
// client is disconnecting.
//
// mochi's Clients.GetByListener takes a read lock and then calls Len, which takes
// it again; a queued writer blocks the second acquisition, and the writer is
// Clients.Delete, run by every disconnecting client. Close reaches GetByListener
// through closeListenerClients, so it could deadlock.
//
// So Close refuses new connections, disconnects a copy of the client list without
// holding the lock, and closes the listeners with a no-op closer, which waits for
// every attachClient without walking the map. It is idempotent, because mochi's
// Close panics when called twice.
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

// DeliverLocal publishes into the local broker (engine.LocalDeliver). Every
// appended record reaches the bus this way under its stored topic; state is
// retained so new subscribers get the current value.
func (s *Server) DeliverLocal(topic string, payload []byte, retain bool) {
	if err := s.S.Publish(topic, payload, retain, 1); err != nil {
		slog.Default().Warn("local delivery failed", "topic", topic, "retain", retain, "err", err)
	}
}

// HasSubscriberFor reports whether identity ulid has a live subscription matching
// topic on this bus. It repeats the lookup mochi does before fan-out and keeps only
// that identity's subscriptions; the engine calls it right after DeliverLocal.
//
// It must answer per identity: true advances the machine's delivery cursor, so an
// observer with read:# subscribed to command topics would otherwise mark an
// offline machine's commands delivered. Shared subscriptions are ignored for the
// same reason.
//
// It reports that a subscription existed, not that bytes arrived. mochi swallows
// per-client write failures, and a client can connect or drop between the publish
// and this call. colca_command_undelivered_total therefore undercounts real
// failures and never overcounts them.
func (s *Server) HasSubscriberFor(topic, ulid string) bool {
	if ulid == "" {
		// Fail closed, the same rule uns.OwedCommand and Ancestry.Covers
		// apply: "no identity" must never read as "any identity will do".
		return false
	}
	for clientID := range s.S.Topics.Subscribers(topic).Subscriptions {
		cl, ok := s.S.Clients.Get(clientID)
		if ok && !cl.Closed() && string(cl.Properties.Username) == ulid {
			return true
		}
	}
	return false
}
