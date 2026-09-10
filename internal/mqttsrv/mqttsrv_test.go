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

// scrapeMetric reads one metric value through metricstest.Value.
var scrapeMetric = metricstest.Value

// mustKVScan is KVScan that fails the test on error.
func mustKVScan(t *testing.T, st *store.Store, prefix string) []store.KVEntry {
	t.Helper()
	entries, err := st.KVScan(prefix)
	if err != nil {
		t.Fatal(err)
	}
	return entries
}

// world is the broker fixture: TLS listener, registry with m1 at mount m1
// (explicit write grant) and obs at obs (read:#), engine bound late as in node
// assembly, kick wired.
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
	// The element m1 binds to is authored before it enrolls.
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

// New derives mochi's packet-size limit from the record cap instead of leaving it
// unlimited.
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

// tryConnect is connect without failing the test, for rejected connects. username
// lets a test present a mismatched name.
func tryConnect(addr, clientID string, m *authtest.Machine, username string) (paho.Client, error) {
	opts := paho.NewClientOptions().
		AddBroker("ssl://" + addr).
		SetTLSConfig(m.TLSConfig()).
		SetClientID(clientID).
		SetUsername(username).
		SetProtocolVersion(4). // one physical connect per attempt (no 3.1 downgrade retry)
		// No automatic reconnect: paho would reconnect after kicks and shutdowns, and in
		// the shutdown tests that races mochi's WaitGroup and fails -race. Tests reconnect
		// explicitly.
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

// startServerWithLocalDoor builds a world with the local door and mount authoring
// wired as node startup wires it, so the test cannot pass while the real wiring is
// missing.
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
	reg.SetAuthoring(func(path string) (string, error) {
		payload, err := json.Marshal(map[string]string{"path": path})
		if err != nil {
			return "", err
		}
		code, msg, _ := domain.Execute(uns.CommandContext{}, "_CmdConfigure", "element/author", payload)
		if code != 200 {
			return "", fmt.Errorf("author element at %s: %s", path, msg)
		}
		return msg, nil
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

// enrollMachine enrolls a machine at ulid, placed at path, for the tests that check
// the local door refuses keyed identities.
func enrollMachine(t *testing.T, w *world, ulid, path string) *authtest.Machine {
	t.Helper()
	m := authtest.NewMachine(t, ulid)
	authtest.EnrollAt(t, w.reg, w.eng, m, path)
	return m
}

// localClient is an unconnected local-door dial; Connect returns the error instead
// of failing the test. It uses the MQTT 5 client, because the mount declaration is
// a CONNECT user property, and a plain TCP dial, because the local door has no
// TLS.
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

// The local door admits a plain TCP client on a name alone. The mount property is
// read when the entry is created and authored through Register, which exercises the
// SetAuthoring wiring.
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

// A machine given a friendly name must not be handed out by name at the local door;
// the MayUseDoor check after Register refuses it.
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
	// friendly-name is nobody's ULID, so only the MayUseDoor check can refuse it.
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

// A new subscriber receives the whole retained set, just past mochi's default
// inflight window of 8192. Seeding goes straight to the store and DeliverLocal to
// stay fast.
func TestRetainedReplayDeliversAllMessages(t *testing.T) {
	// Just past mochi's default of 8192 for both MaximumInflight and
	// MaximumClientWritesPending; either one set below the burst drops messages.
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

	// OnPublish lets an empty identity through unvalidated, so OnConnectAuthenticate
	// must refuse it; an empty username can never equal an enrolled ULID.
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
		// The node-id level is a wildcard for readers; the path must lie inside the granted
		// zone. A # right after the node id would need read:#.
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

// A machine reads its own zone by default; other filters are denied at SUBSCRIBE
// and counted.
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

// A shared subscription is resolved after the ACL judged the raw filter, so
// $share/<group>/colca/# must be refused rather than read as non-UNS traffic. The
// allowed subscription is the denominator.
func TestSharedSubscriptionCannotSmuggleAFilterPastTheACL(t *testing.T) {
	w := newWorld(t)
	// Two clients for one machine, so the smuggler cannot see deliveries the legitimate
	// subscription earns.
	smuggler := connect(t, w.srv.Addr(), "m1-share", w.m1)
	zoned := connect(t, w.srv.Addr(), "m1-zone", w.m1)

	// paho rewrites a $share filter before recording the SUBACK, so read the single
	// entry instead of guessing the key.
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

// A denied subscription does not leak the retained set.
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

// The beacon fires on SUBSCRIBE to the beacon topic: the subscribing client gets
// its own beacon within a round trip, and a bystander subscribed earlier sees it
// too. A beacon on connect arrived before the client could subscribe and was lost
// at QoS 0.
func TestTimeSyncBeaconOnSubscribe(t *testing.T) {
	w := newWorld(t)

	bystander := connect(t, w.srv.Addr(), "obs-timesync", w.obs)
	byMsgs := make(chan paho.Message, 8)
	btok := bystander.Subscribe("colca/v1/_TimeSync/+", 1, func(_ paho.Client, m paho.Message) { byMsgs <- m })
	if !btok.WaitTimeout(5*time.Second) || btok.Error() != nil {
		t.Fatalf("bystander subscribe: %v", btok.Error())
	}
	// The bystander's own subscribe produced a beacon of its own; drain it so the
	// assertion below is about the client under test.
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
				t.Fatalf("%s: beacon must never be retained: a retained time message is stale by definition", who)
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

	// The subscribing client's own subscribe produces its beacon.
	checkBeacon(t, ownMsgs, "subscribing client")
	// The broadcast still reaches an unrelated bystander too.
	checkBeacon(t, byMsgs, "bystander")
}

// RunBeacon keeps publishing to a live subscriber without further connects.
func TestTimeSyncBeaconPeriodicCadence(t *testing.T) {
	w := newWorld(t)
	sub := connect(t, w.srv.Addr(), "obs-cadence", w.obs)
	msgs := make(chan paho.Message, 8)
	tok := sub.Subscribe("colca/v1/_TimeSync/+", 1, func(_ paho.Client, m paho.Message) { msgs <- m })
	if !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("subscribe: %v", tok.Error())
	}
	// obs's own subscribe already produced one beacon; it counts toward the three
	// below, which does not change what this test shows.

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

// A client publishing _TimeSync is rejected and counted with the time_sync reason.
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

// The beacon is QoS 0 so it is never queued for an offline subscriber: mochi only
// queues QoS>0 messages. A persistent-session machine must not get stale beacons on
// reconnect.
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
			SetCleanSession(false). // persistent session
			SetConnectTimeout(5 * time.Second)
		c := paho.NewClient(opts)
		tok := c.Connect()
		if !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
			t.Fatalf("connect: %v", tok.Error())
		}
		return c
	}

	// Subscribe at QoS 1: the delivered QoS is the lower of publish and subscribe, so a
	// QoS 0 subscription would hide the publish QoS this test is about.
	msgs := make(chan paho.Message, 8)
	c1 := dial("m1-persistent")
	tok := c1.Subscribe("colca/v1/_TimeSync/+", 1, func(_ paho.Client, m paho.Message) { msgs <- m })
	if !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("subscribe: %v", tok.Error())
	}
	// c1's own subscribe produces its baseline beacon.
	select {
	case <-msgs:
	case <-time.After(5 * time.Second):
		t.Fatal("no beacon received for c1's own subscribe")
	}

	// Disconnect without unsubscribing; the persistent session survives on the server.
	c1.Disconnect(100)
	deadline := time.Now().Add(5 * time.Second)
	for c1.IsConnectionOpen() {
		if time.Now().After(deadline) {
			t.Fatal("c1 never registered as disconnected")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Beacons published while offline, which a QoS 1 beacon would queue.
	for i := 0; i < 3; i++ {
		w.srv.PublishTimeSync()
	}

	// Reconnect with the same session and resubscribe. Only this subscribe's own
	// beacon may arrive.
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

// Any machine may subscribe the beacon topic regardless of its grants; m1 has no
// read:#.
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

// Revocation kicks the live session and the key cannot reconnect.
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

// A failed CONNECT counts against colca_auth_rejections_total{door="mqtt"},
// separate from engine rejections.
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

// mochi's fanout of the raw publish is suppressed and only the engine's mirror of
// the stored record goes out, so a subscriber on colca/# sees each record once.
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
			// Beacons are unrelated to this test and filtered out.
			if strings.HasPrefix(m.Topic(), "colca/v1/_TimeSync/") {
				continue
			}
			// So is the fixture's own retained element record.
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

// mochi broadcasts a last will without calling OnPublish, so OnWillSent must feed
// it to the engine; otherwise a crashed service's _ServiceDetails never reaches
// /kv.
func TestWillDeliveryReachesTheEngineTooNotJustLiveSubscribers(t *testing.T) {
	w := newWorld(t)

	conn, err := tls.Dial("tcp", w.srv.Addr(), w.m1.TLSConfig())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	pc := pahov5.NewClient(pahov5.ClientConfig{Conn: conn})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	willTopic := "colca/v1/_Metric/n1/m1/will-test"
	willPayload := []byte(`{"v":42}`)
	ca, err := pc.Connect(ctx, &pahov5.Connect{
		ClientID:     "m1-will",
		Username:     w.m1.ULID,
		UsernameFlag: true,
		KeepAlive:    30,
		CleanStart:   true,
		WillMessage: &pahov5.WillMessage{
			Topic:   willTopic,
			Payload: willPayload,
			QoS:     1,
			Retain:  true,
		},
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	if ca.ReasonCode != 0 {
		t.Fatalf("CONNACK refused: reason %d", ca.ReasonCode)
	}

	// Drop the connection without DISCONNECT, the case a last will is for.
	if err := conn.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	recs := waitRecords(t, w.st, "metrics", 1, 5*time.Second)
	if len(recs) != 1 {
		t.Fatalf("metrics stream has %d record(s), want 1 — the will never reached the engine", len(recs))
	}
	if recs[0].Topic != willTopic {
		t.Fatalf("stored topic = %q, want %q", recs[0].Topic, willTopic)
	}
}

// Outside colca/# Colca is just a broker: non-UNS publishes are delivered unchanged
// and not persisted, and non-UNS subscriptions need no grant.
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

// A tombstone clears the retained message on the real broker: an online subscriber
// receives the empty clear, and a new subscriber gets nothing for the retired path
// while a sibling still replays.
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

	// A subscriber online before the tombstone sees the live clear.
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

	// A new subscriber gets the sibling's retained value and nothing for the retired
	// path.
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
				// The sibling arrived, so replay works; give the retired path a short window to
				// show up, then finish.
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
	// The machine door keeps the key container: replication pins its parent by the key
	// in the certificate, and a CA-issued one would break every uplink below.
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

// A retained DeliverLocal racing a wildcard subscribe's retained scan. Upstream
// mochi reads retainPath without the lock (mochi-mqtt/server#200); we pin a fork
// that locks it, and this test fails under -race if the pin is dropped.
func TestRetainedDeliveryRacingAWildcardSubscribeDoesNotRace(t *testing.T) {
	w := newWorld(t)
	obs := connect(t, w.srv.Addr(), "obs", w.obs)

	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			topic := fmt.Sprintf("colca/v1/_Metric/n1/m1/race%d", i%32)
			w.srv.DeliverLocal(topic, []byte(`{"v":1}`), true)
			w.srv.DeliverLocal(topic, nil, true) // tombstone: the "" write path
		}
	}()
	for i := 0; i < 200; i++ {
		tok := obs.Subscribe("colca/v1/_Metric/n1/#", 1, func(paho.Client, paho.Message) {})
		if !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
			t.Fatalf("subscribe %d: %v", i, tok.Error())
		}
		if tok := obs.Unsubscribe("colca/v1/_Metric/n1/#"); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
			t.Fatalf("unsubscribe %d: %v", i, tok.Error())
		}
	}
	close(stop)
	<-done
}
