package mqttsrv

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	paho5 "github.com/eclipse/paho.golang/paho"
	paho "github.com/eclipse/paho.mqtt.golang"
	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"

	"github.com/alpamayo-solutions/colca/internal/authtest"
	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/engine"
	"github.com/alpamayo-solutions/colca/internal/identity"
	"github.com/alpamayo-solutions/colca/internal/store"
)

// holdPuback holds one PUBACK between the store and the write, at packet encoding.
// It does not depend on an inbound QoS 1 publish entering mochi's inflight store.
// It lets go once Close has begun and the client is gone, or after 200 ms.
type holdPuback struct {
	mqtt.HookBase
	clientID string
	closing  func() bool
	armed    atomic.Bool
	held     chan struct{}
}

func (h *holdPuback) ID() string           { return "hold-puback" }
func (h *holdPuback) Provides(b byte) bool { return b == mqtt.OnPacketEncode }

func (h *holdPuback) OnPacketEncode(cl *mqtt.Client, pk packets.Packet) packets.Packet {
	if cl.ID != h.clientID || pk.FixedHeader.Type != packets.Puback || !h.armed.CompareAndSwap(true, false) {
		return pk
	}
	close(h.held)
	for deadline := time.Now().Add(5 * time.Second); !h.closing() && time.Now().Before(deadline); {
		time.Sleep(time.Millisecond)
	}
	for deadline := time.Now().Add(200 * time.Millisecond); !cl.Closed() && time.Now().Before(deadline); {
		time.Sleep(time.Millisecond)
	}
	return pk
}

// connectResuming connects with a kept session, so paho resends a publish that got
// no PUBACK when it connects again, as a machine does after a node restart. Like
// tryConnect, it never reconnects on its own.
func connectResuming(t *testing.T, addr, clientID string, m *authtest.Machine) paho.Client {
	t.Helper()
	c := paho.NewClient(paho.NewClientOptions().
		AddBroker("ssl://" + addr).
		SetTLSConfig(m.TLSConfig()).
		SetClientID(clientID).
		SetUsername(m.ULID).
		SetProtocolVersion(4).
		SetCleanSession(false).
		SetAutoReconnect(false).
		SetConnectRetry(false).
		SetConnectTimeout(5 * time.Second))
	mustConnect(t, c)
	t.Cleanup(func() { c.Disconnect(100) })
	return c
}

// mustConnect connects c, retrying while paho is still tearing down a lost connection.
func mustConnect(t *testing.T, c paho.Client) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		tok := c.Connect()
		if tok.WaitTimeout(5*time.Second) && tok.Error() == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("connect: %v", tok.Error())
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func publishAcked(t *testing.T, c paho.Client, topic, payload string) {
	t.Helper()
	if tok := c.Publish(topic, 1, false, []byte(payload)); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("publish %s %s: %v", topic, payload, tok.Error())
	}
}

// storedPayloads counts the records on topic in stream, by payload.
func storedPayloads(t *testing.T, st *store.Store, stream, topic string) map[string]int {
	t.Helper()
	recs, _, err := st.Read(stream, 1, 1000, nil)
	if err != nil {
		t.Fatalf("read %s: %v", stream, err)
	}
	got := map[string]int{}
	for _, r := range recs {
		if r.Topic == topic {
			got[string(r.Payload)]++
		}
	}
	return got
}

// restartOn replaces the closed broker with a new one on the same store and address,
// as restarting a node on its data directory does.
func restartOn(t *testing.T, w *world, addr string) {
	t.Helper()
	nodeID, err := identity.Generate(filepath.Join(t.TempDir(), "n1.key"))
	if err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{ULID: "n1", DataDir: t.TempDir(), KeyFile: "unused",
		MQTT: config.Endpoint{Addr: addr}}
	s, err := New(cfg, nodeID, w.reg, nil, nil, w.m, config.Limits{}.EffectiveMaxRecordBytes())
	if err != nil {
		t.Fatalf("New on %s: %v", addr, err)
	}
	eng := engine.New(w.st, cfg, w.reg, s.DeliverLocal, w.m, nil)
	s.SetEngine(eng)
	w.reg.SetNamespace(eng.Elements())
	w.reg.SetKick(s.Kick)
	go func() { _ = s.Serve() }()
	t.Cleanup(func() { s.Close() })
	w.srv, w.eng = s, eng
}

// A stop that lands after a publish was stored but before its PUBACK went out must
// wait for the PUBACK. Otherwise the client resends the publish after the restart
// and it is stored twice.
func TestAStopBetweenStoreAndPubackStoresThePublishOnce(t *testing.T) {
	w := newWorld(t)
	addr := w.srv.Addr()
	hold := &holdPuback{clientID: "m1-drain", closing: w.srv.hook.closing.Load, held: make(chan struct{})}
	if err := w.srv.S.AddHook(hold, nil); err != nil {
		t.Fatal(err)
	}
	c := connectResuming(t, addr, "m1-drain", w.m1)
	const topic = "colca/v1/_Metric/n1/m1/temp"
	publishAcked(t, c, topic, `{"v": 1}`)

	hold.armed.Store(true)
	tok := c.Publish(topic, 1, false, []byte(`{"v": 2}`))
	select {
	case <-hold.held:
	case <-time.After(5 * time.Second):
		t.Fatal("the publish never reached its PUBACK")
	}
	start := time.Now()
	if err := w.srv.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if took := time.Since(start); took >= publishDrainTimeout {
		t.Errorf("Close took %v, the whole drain timeout, instead of returning once the PUBACK was written", took)
	}
	if !tok.WaitTimeout(time.Second) || tok.Error() != nil {
		t.Errorf("the publish stored during shutdown got no PUBACK before the disconnect (err %v)", tok.Error())
	}

	restartOn(t, w, addr)
	mustConnect(t, c)
	publishAcked(t, c, topic, `{"v": 3}`)
	got := storedPayloads(t, w.st, "metrics", topic)
	if len(got) != 3 || got[`{"v": 1}`] != 1 || got[`{"v": 2}`] != 1 || got[`{"v": 3}`] != 1 {
		t.Fatalf("stored after the restart: %v, want each publish once", got)
	}
}

// Once Close has begun, no publish is answered, stored or delivered, at any version
// or QoS. A client that keeps the publish unacked resends it after the restart, and
// it is stored once.
func TestAPublishArrivingWhileClosingIsStoredOnceAfterTheRestart(t *testing.T) {
	w := newWorld(t)
	addr := w.srv.Addr()

	var mu sync.Mutex
	seen := map[string]int{}
	obs := connect(t, addr, "obs-closing", w.obs)
	if tok := obs.Subscribe("#", 1, func(_ paho.Client, m paho.Message) {
		mu.Lock()
		seen[m.Topic()]++
		mu.Unlock()
	}); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("subscribe: %v", tok.Error())
	}
	c5 := connect5(t, addr, w.m1)
	c3 := connect(t, addr, "m1-closing", w.m1)
	resuming := connectResuming(t, addr, "m1-resuming", w.m1)

	w.srv.hook.closing.Store(true)

	const unanswered = 300 * time.Millisecond
	const topic = "colca/v1/_Metric/n1/m1/temp"
	if resuming.Publish(topic, 1, false, []byte(`{"v": 7}`)).WaitTimeout(unanswered) {
		t.Error("MQTT 3 QoS 1 publish was answered while closing")
	}
	refused := []string{topic}
	for qos := range byte(3) {
		raw3 := fmt.Sprintf("factory/raw/closing-v3-qos%d", qos)
		raw5 := fmt.Sprintf("factory/raw/closing-v5-qos%d", qos)
		refused = append(refused, raw3, raw5)
		for _, to := range []string{topic, raw3} {
			if answered := c3.Publish(to, qos, false, []byte(`{"v": 1}`)).WaitTimeout(unanswered); answered && qos > 0 {
				t.Errorf("MQTT 3 QoS %d publish on %s was answered while closing", qos, to)
			}
		}
		for _, to := range []string{topic, raw5} {
			ctx, cancel := context.WithTimeout(context.Background(), unanswered)
			_, err := c5.Publish(ctx, &paho5.Publish{Topic: to, QoS: qos, Payload: []byte(`{"v": 1}`)})
			cancel()
			if err == nil && qos > 0 {
				t.Errorf("MQTT 5 QoS %d publish on %s was answered while closing", qos, to)
			}
		}
	}

	// A publish that got through would have reached the observer by now.
	time.Sleep(200 * time.Millisecond)
	mu.Lock()
	for _, to := range refused {
		if seen[to] > 0 {
			t.Errorf("a publish on %s reached a subscriber while closing", to)
		}
	}
	mu.Unlock()
	if got := storedPayloads(t, w.st, "metrics", topic); len(got) > 0 {
		t.Fatalf("stored while closing: %v", got)
	}

	if err := w.srv.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	restartOn(t, w, addr)
	mustConnect(t, resuming)
	publishAcked(t, resuming, topic, `{"v": 8}`)
	got := storedPayloads(t, w.st, "metrics", topic)
	if len(got) != 2 || got[`{"v": 7}`] != 1 || got[`{"v": 8}`] != 1 {
		t.Fatalf("stored after the restart: %v, want the resent publish and the next one, once each", got)
	}
}

// A person's publish is refused the same way once Close has begun.
func TestAHumanPublishIsNotAnsweredOrStoredWhileClosing(t *testing.T) {
	w := newHumanWorld(t)
	tok := w.iss.Mint("hmi-user", []string{"cmd:" + authtest.ElementID("m1") + "/#:param", "read:" + authtest.ElementID("m1") + "/#"}, time.Now().Add(5*time.Minute))
	c, err := humanConnect(t, w.srv.HumanTCPAddr(), "ssl", "hmi-user", tok)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Disconnect(100)

	w.srv.hook.closing.Store(true)
	before := w.st.NextOffset("commands")
	payload := `{"correlation_id":"h-closing","expires_at":99999999999999}`
	if c.Publish("colca/v1/_CmdParam/m1/m1/set-speed", 1, false, payload).WaitTimeout(300 * time.Millisecond) {
		t.Error("a granted human command was answered while closing")
	}
	if got := w.st.NextOffset("commands"); got != before {
		t.Fatalf("human command stored while closing: next offset %d -> %d", before, got)
	}
}
