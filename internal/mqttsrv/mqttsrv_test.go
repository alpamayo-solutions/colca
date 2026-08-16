package mqttsrv

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"

	"github.com/alpamayo-solutions/colca/internal/authtest"
	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/engine"
	"github.com/alpamayo-solutions/colca/internal/identity"
	"github.com/alpamayo-solutions/colca/internal/metrics"
	"github.com/alpamayo-solutions/colca/internal/metrics/metricstest"
	"github.com/alpamayo-solutions/colca/internal/registry"
	"github.com/alpamayo-solutions/colca/internal/store"
)

// scrapeMetric reads back one metric value through the shared test helper
// (metricstest.Value) — see that package's doc comment for why this goes
// through Handler() rather than a Collector/Gatherer accessor.
var scrapeMetric = metricstest.Value

// world is the broker fixture: TLS listener, registry with two enrolled
// machines (m1 mounted at "m1"; observer mountless with read:#), engine
// late-bound like node assembly does, kick wired.
type world struct {
	srv     *Server
	st      *store.Store
	reg     *registry.Manager
	m       *metrics.Metrics
	m1, obs *authtest.Machine
}

func newWorld(t *testing.T) *world {
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
	authtest.Enroll(t, reg, w.m1, "m1")
	authtest.Enroll(t, reg, w.obs, "", "read:#")

	cfg := &config.Config{ULID: "n1", DataDir: t.TempDir(), KeyFile: "unused",
		MQTT: config.Endpoint{Addr: "127.0.0.1:0"}}
	m := metrics.New(st, config.Retention{}, nil)
	w.m = m
	s, err := New(cfg, nodeID, reg, nil, m)
	if err != nil {
		st.Close()
		t.Fatalf("New: %v", err)
	}
	s.SetEngine(engine.New(st, cfg, reg, s.DeliverLocal, m, nil))
	reg.SetKick(s.Kick)
	go func() { _ = s.Serve() }()
	t.Cleanup(func() {
		s.Close()
		st.Close()
	})
	w.srv = s
	return w
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
	const retainedCount = 9000 // just past mochi's 8192 default MaximumInflight
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

	deadline := time.Now().Add(15 * time.Second)
	for got.Load() < int64(retainedCount) {
		if time.Now().After(deadline) {
			t.Fatalf("retained replay delivered %d of %d (mochi's MaximumInflight cap dropped the rest)", got.Load(), retainedCount)
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

	t.Run("client publish is ingested with mount rewrite", func(t *testing.T) {
		c := connect(t, addr, "m1-pub", w.m1)

		tok := c.Publish("colca/v1/_Metric/m1/temp", 1, false, []byte(`{"v":1}`))
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
		if want := "colca/v1/_Metric/m1/m1/temp"; recs[0].Topic != want {
			t.Errorf("topic = %q, want %q", recs[0].Topic, want)
		}
	})

	t.Run("spoofed identity is rejected and never persisted", func(t *testing.T) {
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
			t.Fatalf("metrics records = %d, want 1 (spoofed publish must not persist): %+v", len(recs), recs)
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
func TestTimeSyncBeaconOnSessionEstablish(t *testing.T) {
	w := newWorld(t)
	sub := connect(t, w.srv.Addr(), "obs-timesync", w.obs)
	msgs := make(chan paho.Message, 8)
	tok := sub.Subscribe("colca/v1/_TimeSync/+", 1, func(_ paho.Client, m paho.Message) { msgs <- m })
	if !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("subscribe: %v", tok.Error())
	}

	// obs's own connect already fired one beacon before this subscribe even
	// existed (session establishment happens server-side before the client
	// can issue SUBSCRIBE) — this connect is the one under test.
	connect(t, w.srv.Addr(), "m1-timesync", w.m1)

	select {
	case m := <-msgs:
		if want := "colca/v1/_TimeSync/n1"; m.Topic() != want {
			t.Fatalf("beacon topic = %q, want %q", m.Topic(), want)
		}
		if m.Retained() {
			t.Fatal("beacon must never be retained (design §2.2: a retained time message is by definition stale)")
		}
		var p struct {
			NowMS int64 `json:"now_ms"`
		}
		if err := json.Unmarshal(m.Payload(), &p); err != nil {
			t.Fatalf("beacon payload not JSON: %v (%s)", err, m.Payload())
		}
		if p.NowMS <= 0 {
			t.Fatalf("beacon now_ms = %d, want a positive unix-ms timestamp", p.NowMS)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no beacon received for m1's session establishment")
	}
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
	// obs's own connect-triggered beacon (session establishment happens
	// server-side before the client can issue SUBSCRIBE) is already gone by
	// the time the subscribe above lands — nothing to drain. This test pins
	// the PERIODIC trigger only.

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
	if _, err := w.reg.Revoke("m1"); err != nil {
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

// The no-double-delivery guarantee: a client's raw publish is suppressed
// (CodeSuccessIgnore) and only the engine's canonical, mount-rewritten form is
// distributed. A subscriber on colca/# must see each record exactly once.
func TestClientPublishDistributedOnlyAsCanonicalTopic(t *testing.T) {
	w := newWorld(t)

	sub := connect(t, w.srv.Addr(), "observer-sub", w.obs)
	msgs := make(chan paho.Message, 32)
	tok := sub.Subscribe("colca/#", 1, func(_ paho.Client, m paho.Message) { msgs <- m })
	if !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("subscribe: %v", tok.Error())
	}

	pub := connect(t, w.srv.Addr(), "m1-pub", w.m1)
	ptok := pub.Publish("colca/v1/_Metric/m1/temp", 1, false, []byte(`{"v":42}`))
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
	if want := "colca/v1/_Metric/m1/m1/temp"; got[0].Topic() != want {
		t.Fatalf("topic = %q, want the canonical %q", got[0].Topic(), want)
	}
	if w.st.NextOffset("metrics") != 2 {
		t.Fatalf("metrics next offset = %d, want 2", w.st.NextOffset("metrics"))
	}
}

// Outside colca/# Colca is just a broker: a non-UNS publish is not persisted and
// must be distributed unchanged — and non-UNS subscriptions need no grant.
func TestNonUnsTopicStillDistributed(t *testing.T) {
	w := newWorld(t)
	// Baseline: enrollments already appended _EdgeNode entities.
	base := map[string]uint64{}
	for _, stream := range []string{"metrics", "entities", "commands"} {
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
	for _, stream := range []string{"metrics", "entities", "commands"} {
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
		tombTopic = "colca/v1/_Metric/m1/m1/temp" // canonical (post-mount) form
		keepTopic = "colca/v1/_Metric/m1/m1/keep"
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
	pub("colca/v1/_Metric/m1/temp", `{"v":7}`)
	pub("colca/v1/_Metric/m1/keep", `{"v":1}`)

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
	pub("colca/v1/_Metric/m1/temp", "")

	// PUBACK means the engine persisted it — the KV key must be gone.
	if got := st.KVScan("m1/temp"); len(got) != 0 {
		t.Fatalf("KV key survived the tombstone: %+v", got)
	}
	if got := st.KVScan("m1/keep"); len(got) != 1 {
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
