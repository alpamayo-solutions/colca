package mqttsrv

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	pahov5 "github.com/eclipse/paho.golang/paho"
	paho "github.com/eclipse/paho.mqtt.golang"

	"github.com/alpamayo-solutions/colca/internal/authtest"
	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/engine"
	"github.com/alpamayo-solutions/colca/internal/identity"
	"github.com/alpamayo-solutions/colca/internal/metrics"
	"github.com/alpamayo-solutions/colca/internal/metrics/metricstest"
	"github.com/alpamayo-solutions/colca/internal/registry"
	"github.com/alpamayo-solutions/colca/internal/store"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// scrapeMetric reads back one metric value through the shared test helper
// (metricstest.Value) — see that package's doc comment for why this goes
// through Handler() rather than a Collector/Gatherer accessor.
var scrapeMetric = metricstest.Value

// mustKVScan is KVScan with the error handled the only way a test fixture
// can: fail loud (resources design §8).
func mustKVScan(t *testing.T, st *store.Store, prefix string) []store.KVEntry {
	t.Helper()
	entries, err := st.KVScan(prefix)
	if err != nil {
		t.Fatal(err)
	}
	return entries
}

// world is the broker fixture: TLS listener, registry with two enrolled
// machines (m1 mounted at "m1" with an explicit write:el-m1/# grant — a
// machine gets no implicit write, auth §5; obs mounted at "obs" with read:#
// — reads everything through the grant, not through its own placement),
// engine late-bound like node assembly does, kick wired.
type world struct {
	srv     *Server
	st      *store.Store
	reg     *registry.Manager
	m       *metrics.Metrics
	m1, obs *authtest.Machine
	eng     *engine.Engine
}

func newWorld(t *testing.T) *world {
	return newWorldWithConfig(t, nil)
}

func newWorldWithConfig(t *testing.T, configure func(*config.Config)) *world {
	t.Helper()
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	nodeID, err := identity.Generate(filepath.Join(t.TempDir(), "n1.key"))
	if err != nil {
		t.Fatal(err)
	}
	reg, err := registry.New(st, "n1")
	if err != nil {
		t.Fatal(err)
	}
	w := &world{
		st:  st,
		reg: reg,
		m1:  authtest.NewMachine(t, "m1"),
		obs: authtest.NewMachine(t, "observer"),
	}
	cfg := &config.Config{ULID: "n1", DataDir: t.TempDir(), KeyFile: "unused",
		MQTT: config.Endpoint{Addr: "127.0.0.1:0"}}
	if configure != nil {
		configure(cfg)
	}
	m := metrics.New(st, config.Retention{}, nil)
	w.m = m
	s, err := New(cfg, nodeID, reg, nil, nil, m, config.Limits{}.EffectiveMaxRecordBytes())
	if err != nil {
		st.Close()
		t.Fatalf("New: %v", err)
	}
	eng := engine.New(st, cfg, reg, s.DeliverLocal, m, nil)
	s.SetEngine(eng)
	w.eng = eng
	// Placement resolves through the engine's element index, so the element m1
	// binds to is authored before it enrolls (id-grants design §4).
	reg.SetNamespace(eng.Elements())
	authtest.EnrollAt(t, reg, eng, w.m1, "m1", "write:"+authtest.ElementID("m1")+"/#")
	authtest.EnrollAt(t, reg, eng, w.obs, "obs", "read:#")
	reg.SetKick(s.Kick)
	go func() { _ = s.Serve() }()
	t.Cleanup(func() {
		s.Close()
		st.Close()
	})
	w.srv = s
	return w
}

// TestBrokerCapsPacketSize pins that New derives mochi's packet-size ceiling
// from the configured record cap instead of leaving it at mochi's default of
// 0 (unlimited) — the gap that let an oversize publish reach Pebble at all.
func TestBrokerCapsPacketSize(t *testing.T) {
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	nodeID, err := identity.Generate(filepath.Join(t.TempDir(), "n1.key"))
	if err != nil {
		t.Fatal(err)
	}
	reg, err := registry.New(st, "n1")
	if err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{ULID: "n1", DataDir: t.TempDir(), KeyFile: "unused",
		MQTT: config.Endpoint{Addr: "127.0.0.1:0"}}
	s, err := New(cfg, nodeID, reg, nil, nil, nil, 1024)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { s.Close() })

	if got := s.S.Options.Capabilities.MaximumPacketSize; got == 0 {
		t.Fatal("MaximumPacketSize is 0 (unlimited) — the cap was not applied")
	}
	if got := s.S.Options.Capabilities.MaximumPacketSize; got < 1024 {
		t.Fatalf("MaximumPacketSize = %d, must not be below the record cap", got)
	}
}

func TestBrokerAppliesTheConfiguredResourceLimits(t *testing.T) {
	w := newWorldWithConfig(t, func(cfg *config.Config) {
		cfg.MQTTLimits = config.MQTTLimits{
			MaxClients:                12,
			MaxSubscriptionsPerClient: 34,
			ReceiveMaximum:            56,
			MaximumInflight:           78,
			MaxPendingWritesPerClient: 90,
			MaxTopicAliasesPerClient:  123,
			MaxSessionExpiry:          config.Duration(48 * time.Hour),
		}
	})
	caps := w.srv.S.Options.Capabilities
	if caps.MaximumClients != 12 || caps.ReceiveMaximum != 56 || caps.MaximumInflight != 78 ||
		caps.MaximumClientWritesPending != 90 || caps.TopicAliasMaximum != 123 ||
		caps.MaximumSessionExpiryInterval != uint32((48*time.Hour)/time.Second) {
		t.Fatalf("broker capabilities = %+v", caps)
	}
}

func TestBrokerRefusesConnectionsAboveTheConfiguredLimit(t *testing.T) {
	w := newWorldWithConfig(t, func(cfg *config.Config) {
		cfg.MQTTLimits.MaxClients = 1
	})
	first := connect(t, w.srv.Addr(), "limit-first", w.m1)
	if !first.IsConnected() {
		t.Fatal("first client did not connect")
	}
	second, err := tryConnect(w.srv.Addr(), "limit-second", w.obs, w.obs.ULID)
	if err == nil {
		second.Disconnect(100)
		t.Fatal("second client connected above max_clients=1")
	}
}

// connect dials the TLS listener with the machine's client cert.
func connect(t *testing.T, addr, clientID string, m *authtest.Machine) paho.Client {
	t.Helper()
	c, err := tryConnect(addr, clientID, m, m.ULID)
	if err != nil {
		t.Fatalf("connect %s: %v", clientID, err)
	}
	t.Cleanup(func() { c.Disconnect(100) })
	return c
}

// tryConnect is connect without the test-failure: for assertions on REJECTED
// connects. username lets a test present a mismatched name on purpose.
func tryConnect(addr, clientID string, m *authtest.Machine, username string) (paho.Client, error) {
	opts := paho.NewClientOptions().
		AddBroker("ssl://" + addr).
		SetTLSConfig(m.TLSConfig()).
		SetClientID(clientID).
		SetUsername(username).
		SetProtocolVersion(4). // one physical connect per attempt (no 3.1 downgrade retry)
		// paho reconnects on its own by default, which puts the client
		// population outside the test's control: every test that drops a
		// connection (revocation kicks, shutdown) gets a reconnect it never
		// asked for. In the shutdown tests that reconnect lands DURING
		// Listeners.CloseAll — an attachClient Add(1) concurrent with the
		// Wait() that ends CloseAll, which is mochi's documented WaitGroup
		// misuse (server.go:407-408 vs listeners.go:134) and fails -race.
		// Tests here reconnect by calling tryConnect again, never implicitly.
		SetAutoReconnect(false).
		SetConnectRetry(false).
		SetConnectTimeout(5 * time.Second)
	c := paho.NewClient(opts)
	tok := c.Connect()
	if !tok.WaitTimeout(5 * time.Second) {
		return c, fmt.Errorf("timed out")
	}
	return c, tok.Error()
}

// waitRecords polls a stream until it holds at least want records.
func waitRecords(t *testing.T, st *store.Store, stream string, want int, d time.Duration) []store.StoredRecord {
	t.Helper()
	deadline := time.Now().Add(d)
	for {
		recs, _, err := st.Read(stream, 1, 100, nil)
		if err != nil {
			t.Fatalf("read %s: %v", stream, err)
		}
		if len(recs) >= want {
			return recs
		}
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for %d record(s) in %s, have %d: %+v", want, stream, len(recs), recs)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// startServerWithLocalDoor builds a world with the local MQTT door enabled and
// self-registration's mount-authoring wired EXACTLY as node.New wires it
// (node.go, right after reg.SetNamespace): domain.Execute("_CmdConfigure",
// "element/upsert", ...) is the one authoring path in this system. A declared
// mount must go through it here too — a test that wired a shortcut instead
// could pass while node.go's own wiring stayed missing, which is precisely the
// gap found earlier (nothing called Manager.SetAuthoring anywhere).
func startServerWithLocalDoor(t *testing.T) *world {
	t.Helper()
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	nodeID, err := identity.Generate(filepath.Join(t.TempDir(), "n1.key"))
	if err != nil {
		t.Fatal(err)
	}
	reg, err := registry.New(st, "n1")
	if err != nil {
		t.Fatal(err)
	}
	w := &world{st: st, reg: reg}
	cfg := &config.Config{ULID: "n1", DataDir: t.TempDir(), KeyFile: "unused",
		MQTT:      config.Endpoint{Addr: "127.0.0.1:0"},
		MQTTLocal: config.Endpoint{Addr: "127.0.0.1:0"},
	}
	m := metrics.New(st, config.Retention{}, nil)
	w.m = m
	s, err := New(cfg, nodeID, reg, nil, nil, m, config.Limits{}.EffectiveMaxRecordBytes())
	if err != nil {
		st.Close()
		t.Fatalf("New: %v", err)
	}
	eng := engine.New(st, cfg, reg, s.DeliverLocal, m, nil)
	s.SetEngine(eng)
	w.eng = eng
	domain := uns.NewConfigExec(eng.EntityStore(), reg, eng.Elements(), nil, registry.NewULID, cfg.Plugin)
	eng.SetExecutor(engine.Executors(engine.NewAdminExecutor(reg), domain))
	eng.SetObserver(domain)
	reg.SetNamespace(eng.Elements())
	reg.SetAuthoring(eng.Elements(), func(path, elementID string) error {
		name := path
		if i := strings.LastIndexByte(path, '/'); i >= 0 {
			name = path[i+1:]
		}
		payload, err := json.Marshal(map[string]any{
			"elements": []map[string]any{
				{"path": path, "element": map[string]any{"id": elementID, "name": name}},
			},
		})
		if err != nil {
			return err
		}
		code, msg, _ := domain.Execute("_CmdConfigure", "element/upsert", payload)
		if code != 200 {
			return fmt.Errorf("author element at %s: %s", path, msg)
		}
		return nil
	})
	reg.SetKick(s.Kick)
	go func() { _ = s.Serve() }()
	t.Cleanup(func() {
		s.Close()
		st.Close()
	})
	w.srv = s
	return w
}

func (w *world) LocalAddr() string           { return w.srv.LocalAddr() }
func (w *world) Registry() *registry.Manager { return w.srv.Registry() }
func (w *world) Elements() *uns.ElementIndex { return w.srv.Elements() }

// enrollMachine enrolls a fresh machine identity at ulid, placed at path, the
// ordinary way (auth §6.1) — for tests asserting the local door refuses to
// hand out a keyed identity by name.
func enrollMachine(t *testing.T, w *world, ulid, path string) *authtest.Machine {
	t.Helper()
	m := authtest.NewMachine(t, ulid)
	authtest.EnrollAt(t, w.reg, w.eng, m, path)
	return m
}

// localClient is a not-yet-connected local-door dial. Connect() performs the
// actual net.Dial + MQTT v5 CONNECT and returns its error instead of failing
// the test, mirroring tryConnect's split from connect — a test asserting a
// REJECTED connect needs the error, not a t.Fatal baked into the dial itself.
//
// This uses paho.golang (MQTT v5), not the usual test client (tryConnect,
// paho.mqtt.golang pinned via SetProtocolVersion(4)): the `mount` declaration
// rides a CONNECT user property, which MQTT 3.1.1 has no room for. The dial is
// plain net.Dial, never tls.Dial — the local door carries no TLSConfig at all
// (design §4).
type localClient struct {
	t        *testing.T
	addr     string
	username string
	mount    string
}

func dialPlainMQTT(t *testing.T, addr, username string, opts ...func(*localClient)) *localClient {
	t.Helper()
	c := &localClient{t: t, addr: addr, username: username}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

func withMountProperty(mount string) func(*localClient) {
	return func(c *localClient) { c.mount = mount }
}

func (c *localClient) Connect() error {
	c.t.Helper()
	conn, err := net.Dial("tcp", c.addr)
	if err != nil {
		return err
	}
	props := &pahov5.ConnectProperties{}
	if c.mount != "" {
		props.User.Add("mount", c.mount)
	}
	pc := pahov5.NewClient(pahov5.ClientConfig{Conn: conn})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ca, err := pc.Connect(ctx, &pahov5.Connect{
		ClientID:     "local-" + c.username,
		Username:     c.username,
		UsernameFlag: true,
		KeepAlive:    30,
		CleanStart:   true,
		Properties:   props,
	})
	if err != nil {
		return err
	}
	if ca.ReasonCode != 0 {
		return fmt.Errorf("local CONNACK refused: reason %d", ca.ReasonCode)
	}
	c.t.Cleanup(func() { _ = pc.Disconnect(&pahov5.Disconnect{ReasonCode: 0}) })
	return nil
}

// The local door is the door with no key to present at all (local-service-
// trust design §4): a plain TCP dial, no TLS, admitted on a name alone. The
// `mount` CONNECT user property is read only at the moment the entry is
// created (§3.2), and Register's own auto-authoring must resolve it — which
// exercises the SetAuthoring wiring end to end, not merely Register in
// isolation (see startServerWithLocalDoor's doc comment for why that
// distinction matters here).
func TestTheLocalDoorAdmitsAClientWithNoCertificate(t *testing.T) {
	s := startServerWithLocalDoor(t)
	c := dialPlainMQTT(t, s.LocalAddr(), "connector-opcua", withMountProperty("line1/press3"))

	if err := c.Connect(); err != nil {
		t.Fatalf("the local door refused a certless client: %v", err)
	}
	e, ok := s.Registry().ByName("connector-opcua")
	if !ok {
		t.Fatal("connecting did not register the service")
	}
	if e.Kind != uns.KindLocal {
		t.Fatalf("registered kind = %q, want %q", e.Kind, uns.KindLocal)
	}
	path, ok := s.Elements().PathOf(e.Element)
	if !ok {
		t.Fatalf("entry's element %q does not resolve to any path", e.Element)
	}
	if path != "line1/press3" {
		t.Fatalf("registered at %q; want the declared mount line1/press3", path)
	}
}

func TestTheLocalDoorRequiresAName(t *testing.T) {
	s := startServerWithLocalDoor(t)
	const line = `colca_auth_rejections_total{door="local",reason="no_name"}`
	if v := scrapeMetric(t, s.m, line); v != 0 {
		t.Fatalf("%s = %v before any connect, want 0", line, v)
	}
	if err := dialPlainMQTT(t, s.LocalAddr(), "").Connect(); err == nil {
		t.Fatal("a nameless client was admitted; the name is how its scope is found")
	}
	if v := scrapeMetric(t, s.m, line); v != 1 {
		t.Fatalf("%s = %v after the nameless CONNECT, want exactly 1 — the rejection must fire for the reason this test names", line, v)
	}
}

func TestAMachineKeyIsNotAcceptedOnTheLocalDoor(t *testing.T) {
	s := startServerWithLocalDoor(t)
	// A machine ULID enrolled on 8883 must not be assumable by name here.
	enrollMachine(t, s, "01JMACHINE", "el-press3")
	const line = `colca_auth_rejections_total{door="local",reason="kind"}`
	if v := scrapeMetric(t, s.m, line); v != 0 {
		t.Fatalf("%s = %v before any connect, want 0", line, v)
	}

	c := dialPlainMQTT(t, s.LocalAddr(), "01JMACHINE")
	if err := c.Connect(); err == nil {
		t.Fatal("a machine's identity was claimed by name on the local door")
	}
	if v := scrapeMetric(t, s.m, line); v != 1 {
		t.Fatalf("%s = %v after the collision CONNECT, want exactly 1 — the rejection must fire for the reason this test names, not merely fail some other way", line, v)
	}
	// And the local door must not have quietly minted an unrelated kind=local
	// entry under this name instead of refusing outright.
	if _, ok := s.Registry().ByName("01JMACHINE"); ok {
		t.Fatal("the collision silently registered a new local entry instead of being refused")
	}
}

// A machine (or child node) may be given a friendly `name` — uns.Entry.Validate
// permits it on any kind, and Manager.Enroll indexes any non-empty Name into
// byName regardless of kind (registry.go). That makes it resolvable through
// ByName, not merely the ULID collision TestAMachineKeyIsNotAcceptedOnTheLocalDoor
// covers, and Register's own idempotent-reconnect branch ("entry exists? return
// it") does no kind check — so without the post-Register MayUseDoor(DoorLocal)
// check, a certless local session could present that name and be handed the
// machine's own ULID: its topic identity, its grants, its mount. Nothing in the
// tree sets Name on a machine or node today, so this was latent rather than
// live — one operator action away.
func TestALocalNameThatResolvesToAKeyedIdentityByNameIsRefused(t *testing.T) {
	s := startServerWithLocalDoor(t)
	element := authtest.Place(t, s.eng, "press3")
	m := authtest.NewMachine(t, "01JNAMEDMACHINE")
	entry := uns.Entry{ULID: m.ULID, Pubkey: m.Pubkey, Kind: uns.KindExternal, Name: "friendly-name", Element: element}
	raw, err := json.Marshal(&entry)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.reg.Enroll(raw); err != nil {
		t.Fatalf("enroll a named machine: %v", err)
	}
	// Get(name) must NOT be the thing catching this: "friendly-name" is not
	// anyone's ULID, so that check passes clean through, and only the
	// post-Register MayUseDoor check can still refuse it.
	if _, ok := s.reg.Get("friendly-name"); ok {
		t.Fatal("precondition broken: \"friendly-name\" must not itself be a ulid")
	}

	const line = `colca_auth_rejections_total{door="local",reason="kind"}`
	if v := scrapeMetric(t, s.m, line); v != 0 {
		t.Fatalf("%s = %v before any connect, want 0", line, v)
	}

	c := dialPlainMQTT(t, s.LocalAddr(), "friendly-name")
	if err := c.Connect(); err == nil {
		t.Fatal("a machine's friendly name was claimed by a certless client on the local door")
	}
	if v := scrapeMetric(t, s.m, line); v != 1 {
		t.Fatalf("%s = %v after the named-machine CONNECT, want exactly 1", line, v)
	}
}

// TestRetainedReplayDeliversAllMessages is a behavioral regression test for
// the data-loss bug the cardinality benchmark scenario found
// (colca/bench/cardinality.go): a fresh subscriber replaying the retained set
// can burst more QoS-1 messages than mochi's inflight window, and anything
// past the window used to be silently dropped with no retry. It seeds
// retainedCount retained messages — just past mochi's 8192 default
// MaximumInflight — then connects ONE fresh subscriber and asserts it
// receives every single one. Seeding goes straight to the store (one batched
// Append) and DeliverLocal to stay fsync-cheap; see git history for the full
// rationale.
func TestRetainedReplayDeliversAllMessages(t *testing.T) {
	// Just past mochi's 8192 default for BOTH per-client ceilings — MaximumInflight
	// and MaximumClientWritesPending. Either one, set below the burst, drops
	// part of the replay silently; #370 proved that for the second.
	const retainedCount = 9000
	w := newWorld(t)

	recs := make([]store.Record, retainedCount)
	for i := range recs {
		recs[i] = store.Record{
			Topic:   fmt.Sprintf("colca/v1/_Metric/n1/m1/sig%d", i),
			Payload: []byte(fmt.Sprintf(`{"v":%d}`, i)),
			TS:      int64(i),
		}
	}
	if _, _, err := w.st.Append("metrics", recs); err != nil {
		t.Fatalf("seed store append: %v", err)
	}
	for _, r := range recs {
		w.srv.DeliverLocal(r.Topic, r.Payload, true) // retain=true, same as a real data/entity ingest
	}

	obs := connect(t, w.srv.Addr(), "obs", w.obs)

	var got atomic.Int64
	tok := obs.Subscribe("colca/#", 1, func(_ paho.Client, m paho.Message) {
		if m.Retained() {
			got.Add(1)
		}
	})
	if !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("subscribe: %v", tok.Error())
	}

	// The full core suite runs with the race detector. On a contended CI
	// runner its instrumentation can make 9,000 sequential QoS-1 callbacks
	// take longer than the broker replay itself, so allow enough time to
	// distinguish slow callback processing from an actually truncated replay.
	deadline := time.Now().Add(60 * time.Second)
	for got.Load() < int64(retainedCount) {
		if time.Now().After(deadline) {
			t.Fatalf("retained replay delivered %d of %d (a per-client ceiling — MaximumInflight or "+
				"MaximumClientWritesPending — dropped the rest; see colca_mqtt_publish_dropped_total)",
				got.Load(), retainedCount)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestBrokerAuthIngestAndDeliverLocal(t *testing.T) {
	w := newWorld(t)
	addr := w.srv.Addr()
	if addr == "" {
		t.Fatal("Addr() is empty after AddListener")
	}

	t.Run("unknown key is rejected", func(t *testing.T) {
		stranger := authtest.NewMachine(t, "stranger")
		c, err := tryConnect(addr, "stranger", stranger, "stranger")
		defer c.Disconnect(100)
		if err == nil {
			t.Fatal("connect with an un-enrolled key succeeded, want rejection")
		}
	})

	t.Run("username mismatch is rejected", func(t *testing.T) {
		c, err := tryConnect(addr, "m1-as-other", w.m1, "not-m1")
		defer c.Disconnect(100)
		if err == nil {
			t.Fatal("connect with mismatched username succeeded, want rejection")
		}
	})

	// OnPublish trusts that no session which reached it can carry an empty
	// identity (mqttsrv.go: `if ident == ""` lets the packet through
	// UNVALIDATED, straight to mochi's own fanout). That trust rests entirely
	// on OnConnectAuthenticate refusing every path that could produce one — on
	// the machine door, "" can never equal an enrolled entry's (non-empty)
	// ULID, so this must fail exactly like any other username mismatch.
	t.Run("empty username is rejected", func(t *testing.T) {
		c, err := tryConnect(addr, "m1-empty-user", w.m1, "")
		defer c.Disconnect(100)
		if err == nil {
			t.Fatal("connect with an empty username succeeded, want rejection — this is the identity OnPublish's ident==\"\" branch would trust")
		}
	})

	t.Run("client publish is ingested with no rewrite", func(t *testing.T) {
		c := connect(t, addr, "m1-pub", w.m1)

		tok := c.Publish("colca/v1/_Metric/n1/m1/temp", 1, false, []byte(`{"v":1}`))
		if !tok.WaitTimeout(5 * time.Second) {
			t.Fatal("publish: timed out waiting for PUBACK")
		}
		if err := tok.Error(); err != nil {
			t.Fatalf("publish: %v", err)
		}

		recs := waitRecords(t, w.st, "metrics", 1, 5*time.Second)
		if len(recs) != 1 {
			t.Fatalf("metrics records = %d, want 1: %+v", len(recs), recs)
		}
		if want := "colca/v1/_Metric/n1/m1/temp"; recs[0].Topic != want {
			t.Errorf("topic = %q, want %q", recs[0].Topic, want)
		}
	})

	t.Run("wrong level-4 is rejected and never persisted", func(t *testing.T) {
		c := connect(t, addr, "m1-spoof", w.m1)

		// A rejected packet gets no PUBACK under MQTT 3.1.1, so the token never
		// completes — the assertion is on the store, not on the token.
		tok := c.Publish("colca/v1/_Metric/OTHER/temp", 1, false, []byte(`{"v":2}`))
		tok.WaitTimeout(500 * time.Millisecond)

		time.Sleep(300 * time.Millisecond)
		recs, _, err := w.st.Read("metrics", 1, 100, nil)
		if err != nil {
			t.Fatalf("read metrics: %v", err)
		}
		if len(recs) != 1 {
			t.Fatalf("metrics records = %d, want 1 (wrong-level-4 publish must not persist): %+v", len(recs), recs)
		}
	})

	t.Run("DeliverLocal reaches subscribers and bypasses ingest", func(t *testing.T) {
		c := connect(t, addr, "m1-sub", w.m1)

		msgs := make(chan paho.Message, 1)
		// Path-anchored filter: the node-id level is a wildcard for readers,
		// the PATH must sit inside the granted zone (a `#` straight after the
		// node-id level would be path-open and need read:#).
		tok := c.Subscribe("colca/v1/_CmdParam/+/m1/#", 1, func(_ paho.Client, m paho.Message) {
			msgs <- m
		})
		if !tok.WaitTimeout(5 * time.Second) {
			t.Fatal("subscribe: timed out")
		}
		if err := tok.Error(); err != nil {
			t.Fatalf("subscribe: %v", err)
		}

		topic := "colca/v1/_CmdParam/m1/m1/go"
		payload := []byte(`{"correlation_id":"c1","expires_at":1}`)
		w.srv.DeliverLocal(topic, payload, false)

		select {
		case m := <-msgs:
			if m.Topic() != topic {
				t.Errorf("delivered topic = %q, want %q", m.Topic(), topic)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("DeliverLocal message never arrived")
		}

		// The inline delivery must not run through the engine.
		if got := w.st.NextOffset("commands"); got != 1 {
			t.Errorf("commands next offset = %d, want 1 (inline publish must bypass ingest)", got)
		}
	})
}

// Subscribe-side ACLs (auth §6.1): a machine's default read scope is its own
// zone; filters outside it are denied at SUBSCRIBE time and counted.
func TestSubscribeACLScopes(t *testing.T) {
	w := newWorld(t)
	c := connect(t, w.srv.Addr(), "m1-acl", w.m1)

	const denyLine = `colca_acl_denials_total{action="sub"}`
	if v := scrapeMetric(t, w.m, denyLine); v != 0 {
		t.Fatalf("%s = %v before any subscribe, want 0", denyLine, v)
	}

	subErr := func(filter string) error {
		tok := c.Subscribe(filter, 1, func(paho.Client, paho.Message) {})
		if !tok.WaitTimeout(5 * time.Second) {
			return fmt.Errorf("timeout")
		}
		if err := tok.Error(); err != nil {
			return err
		}
		// paho surfaces a rejected subscription as granted QoS 0x80 in the
		// SUBACK; token.Error is nil on v3.1.1, so inspect the result map.
		if st, ok := tok.(*paho.SubscribeToken); ok {
			if qos, found := st.Result()[filter]; found && qos == 0x80 {
				return fmt.Errorf("subscription rejected (0x80)")
			}
		}
		return nil
	}

	if err := subErr("colca/v1/+/+/m1/#"); err != nil {
		t.Fatalf("own-zone subscribe denied: %v", err)
	}
	if err := subErr("colca/v1/_Metric/+/m1/temp"); err != nil {
		t.Fatalf("own-zone concrete subscribe denied: %v", err)
	}
	if err := subErr("other/raw/#"); err != nil {
		t.Fatalf("non-UNS subscribe denied: %v", err)
	}
	if err := subErr("colca/v1/+/+/sibling/#"); err == nil {
		t.Fatal("sibling-zone subscribe allowed, want denial")
	}
	if err := subErr("colca/#"); err == nil {
		t.Fatal("colca/# subscribe allowed for a zone-scoped machine, want denial")
	}
	if v := scrapeMetric(t, w.m, denyLine); v != 2 {
		t.Fatalf("%s = %v after two denials, want 2", denyLine, v)
	}

	// The observer's read:# grant covers everything.
	obs := connect(t, w.srv.Addr(), "obs-acl", w.obs)
	tok := obs.Subscribe("colca/#", 1, func(paho.Client, paho.Message) {})
	if !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("observer colca/# subscribe: %v", tok.Error())
	}
}

// A shared subscription is an ALIAS the broker resolves after the ACL hook
// has already judged the raw filter, so "$share/<group>/colca/#" used to be
// read as non-UNS traffic, granted unconditionally, and then registered as
// "colca/#" — every record on the node, delivered to a machine scoped to its
// own zone. This pins the whole path through the real broker: the SUBACK
// refuses it, and nothing published outside the machine's zone arrives.
//
// The unaliased subscription is the denominator: the same client, the same
// broker, a filter it IS allowed, receiving a record. Without it "nothing
// arrived" would also pass with delivery broken entirely.
func TestSharedSubscriptionCannotSmuggleAFilterPastTheACL(t *testing.T) {
	w := newWorld(t)
	// Two clients for one machine: the smuggler's callbacks must not also see
	// the traffic the legitimate subscription earns, or "nothing smuggled"
	// would be indistinguishable from paho fanning one delivery out to every
	// matching local route.
	smuggler := connect(t, w.srv.Addr(), "m1-share", w.m1)
	zoned := connect(t, w.srv.Addr(), "m1-zone", w.m1)

	// paho rewrites a "$share/<group>/" filter to the aliased one before it
	// records the SUBACK result, so the result map is keyed by whatever it
	// ended up asking for. Each Subscribe here carries exactly one filter, so
	// read the single entry rather than guessing the key.
	suback := func(c paho.Client, filter string, sink func(paho.Client, paho.Message)) byte {
		t.Helper()
		tok := c.Subscribe(filter, 1, sink)
		if !tok.WaitTimeout(5 * time.Second) {
			t.Fatalf("subscribe %q timed out", filter)
		}
		if err := tok.Error(); err != nil {
			t.Fatalf("subscribe %q: %v", filter, err)
		}
		st, ok := tok.(*paho.SubscribeToken)
		if !ok {
			t.Fatalf("subscribe %q returned %T, want *paho.SubscribeToken", filter, tok)
		}
		result := st.Result()
		if len(result) != 1 {
			t.Fatalf("subscribe %q returned %d results, want 1: %v", filter, len(result), result)
		}
		for _, qos := range result {
			return qos
		}
		return 0
	}

	var smuggled, own atomic.Int64
	countSmuggled := func(_ paho.Client, m paho.Message) {
		t.Errorf("a refused subscription delivered %q", m.Topic())
		smuggled.Add(1)
	}
	if got := suback(smuggler, "$share/g/colca/#", countSmuggled); got != 0x80 {
		t.Fatalf("$share/g/colca/# granted QoS 0x%x, want 0x80 — the share alias reached the topic index", got)
	}
	if got := suback(smuggler, "$SYS/#", countSmuggled); got != 0x80 {
		t.Fatalf("$SYS/# granted QoS 0x%x, want 0x80", got)
	}
	if got := suback(zoned, "colca/v1/+/+/m1/#", func(paho.Client, paho.Message) { own.Add(1) }); got == 0x80 {
		t.Fatal("the machine's own zone was refused — the denominator is broken, not the share rule")
	}

	w.srv.DeliverLocal("colca/v1/_Metric/n1/sibling/temp", []byte(`{"v":1}`), true)
	w.srv.DeliverLocal("colca/v1/_Metric/n1/m1/temp", []byte(`{"v":2}`), true)

	deadline := time.Now().Add(10 * time.Second)
	for own.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the in-zone record never arrived; nothing about the share rule is proven")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := smuggled.Load(); got != 0 {
		t.Fatalf("a refused subscription delivered %d messages", got)
	}
}

func TestSubscriptionQuotaAllowsReplacementButRejectsGrowth(t *testing.T) {
	w := newWorldWithConfig(t, func(cfg *config.Config) {
		cfg.MQTTLimits.MaxSubscriptionsPerClient = 2
	})
	const clientID = "obs-subscription-quota"
	c := connect(t, w.srv.Addr(), clientID, w.obs)

	subscribe := func(filter string) byte {
		t.Helper()
		tok := c.Subscribe(filter, 1, func(paho.Client, paho.Message) {})
		if !tok.WaitTimeout(5 * time.Second) {
			t.Fatalf("subscribe %s timed out", filter)
		}
		if err := tok.Error(); err != nil {
			t.Fatalf("subscribe %s: %v", filter, err)
		}
		st, ok := tok.(*paho.SubscribeToken)
		if !ok {
			t.Fatalf("subscribe %s returned %T, want *paho.SubscribeToken", filter, tok)
		}
		return st.Result()[filter]
	}

	if got := subscribe("other/one"); got == 0x80 {
		t.Fatal("first subscription was rejected")
	}
	if got := subscribe("other/two"); got == 0x80 {
		t.Fatal("second subscription was rejected")
	}
	if got := subscribe("other/one"); got == 0x80 {
		t.Fatal("replacing an existing subscription consumed another quota slot")
	}
	if got := subscribe("other/three"); got != 0x80 {
		t.Fatalf("third distinct subscription result = 0x%02x, want failed SUBACK 0x80", got)
	}

	client, ok := w.srv.S.Clients.Get(clientID)
	if !ok {
		t.Fatal("connected client is absent from broker state")
	}
	if got := client.State.Subscriptions.Len(); got != 2 {
		t.Fatalf("stored subscriptions = %d, want 2", got)
	}
}

// A denied subscription must not leak the retained set (the replay happens at
// SUBSCRIBE time, so the ACL denial suppresses it).
func TestDeniedSubscribeLeaksNoRetained(t *testing.T) {
	w := newWorld(t)
	// Seed one retained record OUTSIDE m1's zone.
	w.srv.DeliverLocal("colca/v1/_Metric/x/sibling/temp", []byte(`{"v":9}`), true)

	c := connect(t, w.srv.Addr(), "m1-leak", w.m1)
	got := make(chan paho.Message, 8)
	tok := c.Subscribe("colca/v1/+/+/sibling/#", 1, func(_ paho.Client, m paho.Message) { got <- m })
	tok.WaitTimeout(2 * time.Second)

	select {
	case m := <-got:
		t.Fatalf("retained message leaked through a denied subscription: %s", m.Topic())
	case <-time.After(700 * time.Millisecond):
	}
}

// Time-sync design §2.2: the node beacons on every machine session
// establishment — a client already subscribed to the beacon filter sees a
// fresh, unretained {"now_ms": ...} message the instant ANOTHER client
// connects.
// Time-sync design §2.2 (as amended, [delta]): the beacon fires on
// SUBSCRIBE to colca/v1/_TimeSync/+ (OnSubscribed, mqttsrv.go), not on bare
// session establishment — see the erratum above §2.3's normative rule for
// why the original session-establishment trigger was deterministically
// racy (mochi fires OnSessionEstablished right after CONNACK, structurally
// before the client can have completed its own SUBSCRIBE, so at QoS 0 that
// first publish was reliably lost to the very client it targeted) and had
// to be REPLACED, not merely supplemented.
//
// This is the direct regression test for that defect: the SUBSCRIBING
// client itself — not just an already-subscribed bystander — must receive
// its own beacon, deterministically, within one broker round trip of its
// own SUBSCRIBE packet. It also re-proves the broadcast property the old
// test covered (a bystander subscribed beforehand still sees a beacon
// triggered by someone else's subscribe), so both properties stay pinned
// in one place instead of silently regressing if only the bystander path
// were re-tested.
func TestTimeSyncBeaconOnSubscribe(t *testing.T) {
	w := newWorld(t)

	bystander := connect(t, w.srv.Addr(), "obs-timesync", w.obs)
	byMsgs := make(chan paho.Message, 8)
	btok := bystander.Subscribe("colca/v1/_TimeSync/+", 1, func(_ paho.Client, m paho.Message) { byMsgs <- m })
	if !btok.WaitTimeout(5*time.Second) || btok.Error() != nil {
		t.Fatalf("bystander subscribe: %v", btok.Error())
	}
	// The bystander's OWN subscribe already triggered (and this test does
	// not care about) its own baseline beacon; drain it so the assertion
	// below is unambiguously about the client-under-test's subscribe.
	select {
	case <-byMsgs:
	case <-time.After(5 * time.Second):
		t.Fatal("no beacon for the bystander's own subscribe (baseline)")
	}

	c := connect(t, w.srv.Addr(), "m1-subscribe-timesync", w.m1)
	ownMsgs := make(chan paho.Message, 8)
	tok := c.Subscribe("colca/v1/_TimeSync/+", 1, func(_ paho.Client, m paho.Message) { ownMsgs <- m })
	if !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("subscribe: %v", tok.Error())
	}

	checkBeacon := func(t *testing.T, msgs chan paho.Message, who string) {
		t.Helper()
		select {
		case m := <-msgs:
			if want := "colca/v1/_TimeSync/n1"; m.Topic() != want {
				t.Fatalf("%s: beacon topic = %q, want %q", who, m.Topic(), want)
			}
			if m.Retained() {
				t.Fatalf("%s: beacon must never be retained (design §2.2: a retained time message is by definition stale)", who)
			}
			var p struct {
				NowMS int64 `json:"now_ms"`
			}
			if err := json.Unmarshal(m.Payload(), &p); err != nil {
				t.Fatalf("%s: beacon payload not JSON: %v (%s)", who, err, m.Payload())
			}
			if p.NowMS <= 0 {
				t.Fatalf("%s: beacon now_ms = %d, want a positive unix-ms timestamp", who, p.NowMS)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("%s: no beacon received", who)
		}
	}

	// The defect this hook fixes: the SUBSCRIBING client's own subscribe
	// must deterministically produce a beacon for itself.
	checkBeacon(t, ownMsgs, "subscribing client")
	// The broadcast still reaches an unrelated bystander too.
	checkBeacon(t, byMsgs, "bystander")
}

// Time-sync design §2.2: the periodic beacon_interval trigger is independent
// of connection activity — RunBeacon keeps publishing on a live subscriber
// with no further connects.
func TestTimeSyncBeaconPeriodicCadence(t *testing.T) {
	w := newWorld(t)
	sub := connect(t, w.srv.Addr(), "obs-cadence", w.obs)
	msgs := make(chan paho.Message, 8)
	tok := sub.Subscribe("colca/v1/_TimeSync/+", 1, func(_ paho.Client, m paho.Message) { msgs <- m })
	if !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("subscribe: %v", tok.Error())
	}
	// obs's own subscribe above already triggered (and lands in msgs as)
	// one baseline beacon (design §2.2 as amended: beacon-on-subscribe) —
	// not drained separately, just folded into the "at least 3" tally below
	// alongside the periodic ones; this test's actual claim (RunBeacon
	// keeps publishing independent of further connection activity) does
	// not depend on distinguishing which beacon came from which trigger.

	stop := make(chan struct{})
	defer close(stop)
	go w.srv.RunBeacon(30*time.Millisecond, stop)

	got := 0
	deadline := time.After(3 * time.Second)
	for got < 3 {
		select {
		case <-msgs:
			got++
		case <-deadline:
			t.Fatalf("only received %d periodic beacon(s) in 3s at a 30ms interval, want at least 3", got)
		}
	}
}

// Time-sync design §2.2/§4: _TimeSync is node-local-publish-only — a client
// attempting to publish it is rejected and counted with the dedicated reason.
func TestTimeSyncPublishRejectedFromClient(t *testing.T) {
	w := newWorld(t)
	c := connect(t, w.srv.Addr(), "m1-pub-timesync", w.m1)

	const rejectedLine = `colca_rejected_publishes_total{reason="time_sync"}`
	before := scrapeMetric(t, w.m, rejectedLine)

	// A rejected packet gets no PUBACK under MQTT 3.1.1, so the token never
	// completes — the assertion is on the metric, not on the token.
	tok := c.Publish("colca/v1/_TimeSync/n1", 1, false, []byte(`{"now_ms":1}`))
	tok.WaitTimeout(500 * time.Millisecond)

	deadline := time.Now().Add(3 * time.Second)
	for scrapeMetric(t, w.m, rejectedLine) == before {
		if time.Now().After(deadline) {
			t.Fatalf("%s never incremented after a client _TimeSync publish attempt", rejectedLine)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// The beacon publishes at QoS 0 specifically so it is
// NEVER queued for an offline subscriber. Confirmed against the vendored
// mochi-mqtt/server/v2 source: publishToClient only ever touches
// cl.State.Inflight (the map attachClient's ResendInflightMessages replays
// on reconnect) inside `if out.FixedHeader.Qos > 0` — at QoS 0 a publish to
// an offline client is simply dropped, never queued. A persistent-session
// (CleanSession=false) machine that misses beacons while disconnected must
// NOT receive them backdated on reconnect — only fresh beacons published
// while it is actually connected.
func TestTimeSyncBeaconAtQoS0NeverQueuedForOfflineSubscriber(t *testing.T) {
	w := newWorld(t)

	dial := func(clientID string) paho.Client {
		t.Helper()
		opts := paho.NewClientOptions().
			AddBroker("ssl://" + w.srv.Addr()).
			SetTLSConfig(w.m1.TLSConfig()).
			SetClientID(clientID).
			SetUsername(w.m1.ULID).
			SetProtocolVersion(4).
			SetCleanSession(false). // persistent session — the property the bug depends on
			SetConnectTimeout(5 * time.Second)
		c := paho.NewClient(opts)
		tok := c.Connect()
		if !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
			t.Fatalf("connect: %v", tok.Error())
		}
		return c
	}

	// Subscribe at QoS 1 deliberately, even though the real colca-machine
	// subscribes at QoS 0 post-fix: mochi's effective delivered QoS is
	// min(publish_qos, subscribe_qos), so subscribing at 0 here would clamp
	// delivery to 0 regardless of what the node publishes at, and this test
	// would no longer isolate — and could no longer catch a regression of —
	// the PUBLISH-side QoS this fix is actually about.
	msgs := make(chan paho.Message, 8)
	c1 := dial("m1-persistent")
	tok := c1.Subscribe("colca/v1/_TimeSync/+", 1, func(_ paho.Client, m paho.Message) { msgs <- m })
	if !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("subscribe: %v", tok.Error())
	}
	// c1's own SUBSCRIBE deterministically triggers its baseline beacon
	// (design §2.2 as amended, [delta]: beacon-on-subscribe, not on bare
	// session establishment) — no manual trigger-and-drain needed to
	// establish a known-good baseline before the "outage" below, unlike
	// the old connect-triggered design this replaced.
	select {
	case <-msgs:
	case <-time.After(5 * time.Second):
		t.Fatal("no beacon received for c1's own subscribe")
	}

	// "Outage": disconnect WITHOUT unsubscribing — the persistent session
	// survives server-side (mochi's expire guard on Clean=false).
	c1.Disconnect(100)
	deadline := time.Now().Add(5 * time.Second)
	for c1.IsConnectionOpen() {
		if time.Now().After(deadline) {
			t.Fatal("c1 never registered as disconnected")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Beacons published while offline — the stale ones a QoS-1 beacon would
	// have queued and redelivered on reconnect.
	for i := 0; i < 3; i++ {
		w.srv.PublishTimeSync()
	}

	// Reconnect with the SAME persistent session (same client ID) and
	// re-subscribe. Only ONE beacon may arrive: this reconnect's own
	// subscribe-triggered beacon (design §2.2 as amended).
	c2 := dial("m1-persistent")
	defer c2.Disconnect(100)
	tok = c2.Subscribe("colca/v1/_TimeSync/+", 1, func(_ paho.Client, m paho.Message) { msgs <- m })
	if !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("re-subscribe on reconnect: %v", tok.Error())
	}

	select {
	case <-msgs:
	case <-time.After(5 * time.Second):
		t.Fatal("no beacon on reconnect")
	}
	select {
	case m := <-msgs:
		t.Fatalf("a beacon arrived that must have been dropped for the offline subscriber (QoS 0 must never queue): topic=%s", m.Topic())
	case <-time.After(700 * time.Millisecond):
	}
}

// Time-sync design §2.2/§4: every machine session may subscribe the beacon
// filter regardless of its zone grants — m1 has no read:# grant and its own
// zone is "m1", nothing to do with _TimeSync, yet the subscribe must still
// succeed.
func TestTimeSyncSubscribeAllowedRegardlessOfZone(t *testing.T) {
	w := newWorld(t)
	c := connect(t, w.srv.Addr(), "m1-timesync-acl", w.m1)

	const filter = "colca/v1/_TimeSync/+"
	tok := c.Subscribe(filter, 1, func(paho.Client, paho.Message) {})
	if !tok.WaitTimeout(5 * time.Second) {
		t.Fatal("subscribe: timed out")
	}
	if err := tok.Error(); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if st, ok := tok.(*paho.SubscribeToken); ok {
		if qos, found := st.Result()[filter]; found && qos == 0x80 {
			t.Fatal("_TimeSync subscribe rejected (0x80) despite m1 having no read:# grant")
		}
	}
	const denyLine = `colca_acl_denials_total{action="sub"}`
	if v := scrapeMetric(t, w.m, denyLine); v != 0 {
		t.Fatalf("%s = %v, want 0 (the _TimeSync subscribe must never be denied)", denyLine, v)
	}
}

// Revocation kicks the live session and the key cannot reconnect (auth §7).
func TestRevocationKicksAndBlocksReconnect(t *testing.T) {
	w := newWorld(t)
	c := connect(t, w.srv.Addr(), "m1-kick", w.m1)
	if !c.IsConnected() {
		t.Fatal("precondition: not connected")
	}

	const kickLine = `colca_session_kicks_total`
	if _, _, err := w.reg.Revoke("m1"); err != nil {
		t.Fatalf("revoke: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for c.IsConnectionOpen() {
		if time.Now().After(deadline) {
			t.Fatal("revoked client still connected after 5s")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if v := scrapeMetric(t, w.m, kickLine); v < 1 {
		t.Fatalf("%s = %v after a revoke of a live session, want >= 1", kickLine, v)
	}

	// Reconnect with the revoked key must fail.
	c2, err := tryConnect(w.srv.Addr(), "m1-again", w.m1, "m1")
	defer c2.Disconnect(100)
	if err == nil {
		t.Fatal("revoked key reconnected, want rejection")
	}
}

// A failed CONNECT counts against colca_auth_rejections_total{door="mqtt"} —
// the door's own family, distinct from every engine publish-reject path.
func TestBrokerAuthFailureIncrementsDoorMetric(t *testing.T) {
	w := newWorld(t)

	const line = `colca_auth_rejections_total{door="mqtt",reason="unknown_key"}`
	if v := scrapeMetric(t, w.m, line); v != 0 {
		t.Fatalf("%s = %v before any failed connect, want 0", line, v)
	}

	stranger := authtest.NewMachine(t, "stranger")
	c, err := tryConnect(w.srv.Addr(), "stranger", stranger, "stranger")
	defer c.Disconnect(100)
	if err == nil {
		t.Fatal("connect with un-enrolled key succeeded, want rejection")
	}

	if v := scrapeMetric(t, w.m, line); v != 1 {
		t.Fatalf("%s = %v after one failed CONNECT, want exactly 1", line, v)
	}

	// A successful connect right after must not move the counter.
	good := connect(t, w.srv.Addr(), "m1-ok", w.m1)
	_ = good
	if v := scrapeMetric(t, w.m, line); v != 1 {
		t.Fatalf("%s = %v after a successful connect, want unchanged 1", line, v)
	}
}

// The no-double-delivery guarantee: mochi's own fanout of a client's raw
// publish is suppressed (CodeSuccessIgnore) and only the engine's LocalDeliver
// mirror of the STORED record is distributed. A subscriber on colca/# must see
// each record exactly once.
func TestClientPublishDistributedOnlyAsCanonicalTopic(t *testing.T) {
	w := newWorld(t)

	sub := connect(t, w.srv.Addr(), "observer-sub", w.obs)
	msgs := make(chan paho.Message, 32)
	tok := sub.Subscribe("colca/#", 1, func(_ paho.Client, m paho.Message) { msgs <- m })
	if !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("subscribe: %v", tok.Error())
	}

	pub := connect(t, w.srv.Addr(), "m1-pub", w.m1)
	ptok := pub.Publish("colca/v1/_Metric/n1/m1/temp", 1, false, []byte(`{"v":42}`))
	if !ptok.WaitTimeout(5 * time.Second) {
		t.Fatal("publish: timed out waiting for PUBACK (CodeSuccessIgnore must still ack)")
	}

	var got []paho.Message
	deadline := time.After(1500 * time.Millisecond)
drain:
	for {
		select {
		case m := <-msgs:
			// The time-sync beacon (design §2.2) fires on every session
			// establishment, including both connects above — it is an
			// unrelated feature to the no-double-delivery invariant this
			// test pins, so it is filtered out here rather than asserted on.
			if strings.HasPrefix(m.Topic(), "colca/v1/_TimeSync/") {
				continue
			}
			// Likewise the fixture's own element record: it is retained state
			// authored during setup, replayed to any new colca/# subscriber, and
			// says nothing about how THIS publish was delivered.
			if strings.HasPrefix(m.Topic(), "colca/v1/_SystemElement/") {
				continue
			}
			got = append(got, m)
		case <-deadline:
			break drain
		}
	}
	if len(got) != 1 {
		var topics []string
		for _, m := range got {
			topics = append(topics, m.Topic())
		}
		t.Fatalf("want exactly 1 message on colca/#, got %d: %v", len(got), topics)
	}
	if want := "colca/v1/_Metric/n1/m1/temp"; got[0].Topic() != want {
		t.Fatalf("topic = %q, want the stored topic unchanged %q", got[0].Topic(), want)
	}
	if w.st.NextOffset("metrics") != 2 {
		t.Fatalf("metrics next offset = %d, want 2", w.st.NextOffset("metrics"))
	}
}

// Outside colca/# Colca is just a broker: a non-UNS publish is not persisted and
// must be distributed unchanged — and non-UNS subscriptions need no grant.
func TestNonUnsTopicStillDistributed(t *testing.T) {
	w := newWorld(t)
	// Baseline: enrollments already appended _EnrolledIdentity entities.
	base := map[string]uint64{}
	for _, stream := range []string{"metrics", "entities", "commands", "definitions", "audit"} {
		base[stream] = w.st.NextOffset(stream)
	}

	sub := connect(t, w.srv.Addr(), "m1-other-sub", w.m1)
	msgs := make(chan paho.Message, 8)
	tok := sub.Subscribe("other/#", 1, func(_ paho.Client, m paho.Message) { msgs <- m })
	if !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("subscribe other/#: %v", tok.Error())
	}

	pub := connect(t, w.srv.Addr(), "m1-other-pub", w.m1)
	ptok := pub.Publish("other/thing", 1, false, []byte("hello"))
	if !ptok.WaitTimeout(5 * time.Second) {
		t.Fatal("publish: timed out")
	}

	select {
	case m := <-msgs:
		if m.Topic() != "other/thing" || string(m.Payload()) != "hello" {
			t.Fatalf("non-UNS message altered: topic=%q payload=%q", m.Topic(), m.Payload())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("non-UNS message never arrived")
	}
	for _, stream := range []string{"metrics", "entities", "commands", "definitions", "audit"} {
		if off := w.st.NextOffset(stream); off != base[stream] {
			t.Fatalf("%s next offset moved %d → %d (non-UNS must not persist)", stream, base[stream], off)
		}
	}
}

// Retention design §7.1 step 3, against the real broker: the engine's
// empty-payload delivery with retain=true makes mochi clear the retained
// message per the MQTT spec. Asserted from both sides of the contract — a
// subscriber online at tombstone time receives the empty-payload clear, and a
// FRESH subscriber connecting afterwards gets no retained message for the
// retired path while an untouched sibling path still replays.
func TestTombstoneClearsRetainedOnBroker(t *testing.T) {
	w := newWorld(t)
	s, st := w.srv, w.st
	const (
		tombTopic = "colca/v1/_Metric/n1/m1/temp" // level 4 is the node; no rewrite
		keepTopic = "colca/v1/_Metric/n1/m1/keep"
	)

	m1 := connect(t, s.Addr(), "m1-tomb", w.m1)
	defer m1.Disconnect(100)
	pub := func(topic, payload string) {
		t.Helper()
		tok := m1.Publish(topic, 1, false, []byte(payload))
		if !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
			t.Fatalf("publish %s: %v", topic, tok.Error())
		}
	}
	pub("colca/v1/_Metric/n1/m1/temp", `{"v":7}`)
	pub("colca/v1/_Metric/n1/m1/keep", `{"v":1}`)

	// A subscriber online BEFORE the tombstone: it must see the live clear.
	type msg struct {
		topic, payload string
		retained       bool
	}
	liveMsgs := make(chan msg, 16)
	live := connect(t, s.Addr(), "obs-live", w.obs)
	defer live.Disconnect(100)
	tok := live.Subscribe("colca/#", 1, func(_ paho.Client, m paho.Message) {
		liveMsgs <- msg{m.Topic(), string(m.Payload()), m.Retained()}
	})
	if !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("subscribe: %v", tok.Error())
	}
	// Drain the retained replay of both set values first.
	for seen := 0; seen < 2; {
		select {
		case m := <-liveMsgs:
			if m.topic == tombTopic || m.topic == keepTopic {
				seen++
			}
		case <-time.After(10 * time.Second):
			t.Fatal("retained replay of the two set values never arrived")
		}
	}

	// The tombstone: empty payload through the normal client publish path.
	pub("colca/v1/_Metric/n1/m1/temp", "")

	// PUBACK means the engine persisted it — the KV key must be gone.
	if got := mustKVScan(t, st, "m1/temp"); len(got) != 0 {
		t.Fatalf("KV key survived the tombstone: %+v", got)
	}
	if got := mustKVScan(t, st, "m1/keep"); len(got) != 1 {
		t.Fatalf("sibling KV key must survive, got %d", len(got))
	}

	// The online subscriber receives the empty-payload clear on the canonical topic.
	for {
		select {
		case m := <-liveMsgs:
			if m.topic == tombTopic && m.payload == "" {
				goto cleared
			}
		case <-time.After(10 * time.Second):
			t.Fatal("online subscriber never received the empty-payload clear")
		}
	}
cleared:

	// A FRESH subscriber gets the sibling's retained value but nothing —
	// retained or otherwise — for the retired path.
	freshMsgs := make(chan msg, 16)
	fresh := connect(t, s.Addr(), "obs-fresh", w.obs)
	defer fresh.Disconnect(100)
	tok = fresh.Subscribe("colca/#", 1, func(_ paho.Client, m paho.Message) {
		freshMsgs <- msg{m.Topic(), string(m.Payload()), m.Retained()}
	})
	if !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("fresh subscribe: %v", tok.Error())
	}
	keepSeen := false
	deadline := time.After(10 * time.Second)
	grace := time.NewTimer(0)
	<-grace.C
	for {
		select {
		case m := <-freshMsgs:
			if m.topic == tombTopic {
				t.Fatalf("fresh subscriber received the tombstoned path: %+v — mochi did not clear the retained message", m)
			}
			if m.topic == keepTopic && m.retained {
				keepSeen = true
				// The sibling arrived: the retained replay is demonstrably
				// working, so give the retired path a short grace window to
				// prove its absence, then finish.
				grace.Reset(500 * time.Millisecond)
			}
		case <-grace.C:
			if keepSeen {
				return
			}
		case <-deadline:
			t.Fatal("fresh subscriber never received the sibling's retained value")
		}
	}
}

// --- a supplied certificate reaches the human doors, and only those ---------

// suppliedPair writes a throwaway certificate whose CommonName is "supplied",
// so a test can tell it apart from the node's own key container.
func suppliedPair(t *testing.T) (certFile, keyFile string) {
	t.Helper()
	dir := t.TempDir()
	certFile, keyFile = filepath.Join(dir, "s.crt"), filepath.Join(dir, "s.key")

	other, err := identity.Generate(filepath.Join(dir, "other.key"))
	if err != nil {
		t.Fatal(err)
	}
	cert, err := other.SelfSignedCert("supplied")
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(other.Priv)
	if err != nil {
		t.Fatal(err)
	}
	for path, block := range map[string]*pem.Block{
		certFile: {Type: "CERTIFICATE", Bytes: cert.Certificate[0]},
		keyFile:  {Type: "PRIVATE KEY", Bytes: keyDER},
	} {
		if err := os.WriteFile(path, pem.EncodeToMemory(block), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return certFile, keyFile
}

// dialCommonName opens a TLS connection (presenting a client cert, which the
// machine door requires) and reports the CommonName the server presented.
func dialCommonName(t *testing.T, addr string, client *tls.Config) string {
	t.Helper()
	cfg := client.Clone()
	cfg.InsecureSkipVerify = true // #nosec G402 -- reading the cert IS the test
	conn, err := tls.Dial("tcp", addr, cfg)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	defer conn.Close()
	return conn.ConnectionState().PeerCertificates[0].Subject.CommonName
}

func TestASuppliedCertificateServesTheHumanDoorAndNotTheMachineDoor(t *testing.T) {
	// The machine door must keep the key container: trust on the pinned doors is
	// "the certificate carries the peer's ed25519 key", and replication's child
	// pins its parent exactly that way. A CA-issued certificate there would
	// break every uplink beneath the node.
	certFile, keyFile := suppliedPair(t)

	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	nodeID, err := identity.Generate(filepath.Join(t.TempDir(), "n1.key"))
	if err != nil {
		t.Fatal(err)
	}
	reg, err := registry.New(st, "n1")
	if err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{
		ULID: "n1", DataDir: t.TempDir(), KeyFile: "unused",
		MQTT:      config.Endpoint{Addr: "127.0.0.1:0"},
		MQTTHuman: config.MQTTHuman{TCPAddr: "127.0.0.1:0"},
		Auth:      &config.Auth{Issuer: "http://issuer.test", Audience: "colca", JWKSURL: "http://issuer.test/jwks"},
		TLS:       config.TLS{CertFile: certFile, KeyFile: keyFile},
	}
	s, err := New(cfg, nodeID, reg, nil, nil, nil, config.Limits{}.EffectiveMaxRecordBytes())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer s.Close()
	go func() { _ = s.Serve() }()

	client := authtest.NewMachine(t, "probe").TLSConfig()
	if got := dialCommonName(t, s.HumanTCPAddr(), client); got != "supplied" {
		t.Errorf("human door served %q, want the supplied certificate", got)
	}
	if got := dialCommonName(t, s.Addr(), client); got != "n1" {
		t.Errorf("machine door served %q, want the node's key container — a child "+
			"pinning this node can no longer verify it", got)
	}
}
