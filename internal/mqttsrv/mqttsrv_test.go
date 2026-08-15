package mqttsrv

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"

	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/engine"
	"github.com/alpamayo-solutions/colca/internal/metrics"
	"github.com/alpamayo-solutions/colca/internal/metrics/metricstest"
	"github.com/alpamayo-solutions/colca/internal/store"
)

// scrapeMetric reads back one metric value through the shared test helper
// (metricstest.Value) — see that package's doc comment for why this goes
// through Handler() rather than a Collector/Gatherer accessor.
var scrapeMetric = metricstest.Value

// newBroker starts a broker on a random loopback port with one configured
// client (m1/m1-secret mounted at "m1") and a real store. The engine is bound
// after construction, exercising the late-binding path node assembly uses.
func newBroker(t *testing.T) (*Server, *store.Store) {
	t.Helper()
	s, st, _ := newBrokerWithMetrics(t)
	return s, st
}

// newBrokerWithMetrics is newBroker plus the *metrics.Metrics wired into both
// the broker (for auth-reject counting) and the engine (for the same registry
// a real node would share).
func newBrokerWithMetrics(t *testing.T) (*Server, *store.Store, *metrics.Metrics) {
	t.Helper()
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	cfg := &config.Config{
		ULID:    "n1",
		DataDir: t.TempDir(),
		KeyFile: "unused.pem",
		MQTT:    config.Endpoint{Addr: "127.0.0.1:0"},
		Clients: []config.Client{
			{ULID: "m1", Token: "m1-secret", Mount: "m1"},
			// mount-less → read-only observer: connects and subscribes, never publishes
			{ULID: "observer", Token: "observer-secret"},
		},
	}
	m := metrics.New(st)
	s, err := New(cfg, nil, m)
	if err != nil {
		st.Close()
		t.Fatalf("New: %v", err)
	}
	s.SetEngine(engine.New(st, cfg, s.DeliverLocal, m))
	go func() { _ = s.Serve() }()
	t.Cleanup(func() {
		s.Close()
		st.Close()
	})
	return s, st, m
}

func connect(t *testing.T, addr, clientID, user, pass string) paho.Client {
	t.Helper()
	opts := paho.NewClientOptions().
		AddBroker("tcp://" + addr).
		SetClientID(clientID).
		SetUsername(user).
		SetPassword(pass).
		SetConnectTimeout(5 * time.Second)
	c := paho.NewClient(opts)
	tok := c.Connect()
	if !tok.WaitTimeout(5 * time.Second) {
		t.Fatalf("connect %s: timed out", clientID)
	}
	if err := tok.Error(); err != nil {
		t.Fatalf("connect %s: %v", clientID, err)
	}
	return c
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
// past the window used to be silently dropped with no retry
// ("client store quota reached" / packets.ErrQuotaExceeded in
// publishToClient). It seeds retainedCount retained messages — just past
// mochi's 8192 default MaximumInflight — then connects ONE fresh subscriber
// and asserts it receives every single one.
//
// Seeding goes straight to the store (one batched Append) and then straight
// to the broker's inline publish (Server.DeliverLocal, the same call the
// engine makes after every real ingest) instead of through engine.IngestAdmin
// or awaited MQTT publishes: each of those does one fsync per record, and at
// the store's single-record fsync rate (~230 rec/s, see
// BenchmarkAppendBatch/size=1 in internal/store) seeding 9000 records that
// way would take minutes. Bypassing the fsync-bound path keeps this test
// under a second to seed; the mechanism under test — mochi's per-subscriber
// inflight cap at SUBSCRIBE-time retained replay — is unaffected by how the
// retained set was populated.
//
// Reverting the MaximumInflight fix in New (or setting it back to mochi's
// 8192 default) makes this test fail with "retained replay delivered 8192 of
// 9000" — verified manually; see task-11-report.md for the revert/RED and
// fixed/GREEN command output.
func TestRetainedReplayDeliversAllMessages(t *testing.T) {
	const retainedCount = 9000 // just past mochi's 8192 default MaximumInflight

	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	defer st.Close()

	cfg := &config.Config{
		ULID:    "n1",
		DataDir: t.TempDir(),
		KeyFile: "unused.pem",
		MQTT:    config.Endpoint{Addr: "127.0.0.1:0"},
		Clients: []config.Client{
			// mount-less → read-only observer: connects and subscribes, never publishes
			{ULID: "observer", Token: "observer-secret"},
		},
	}
	s, err := New(cfg, nil, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	go func() { _ = s.Serve() }()
	t.Cleanup(func() { s.Close() })

	recs := make([]store.Record, retainedCount)
	for i := range recs {
		recs[i] = store.Record{
			Topic:   fmt.Sprintf("colca/v1/_Metric/n1/m1/sig%d", i),
			Payload: []byte(fmt.Sprintf(`{"v":%d}`, i)),
			TS:      int64(i),
		}
	}
	if _, _, err := st.Append("metrics", recs); err != nil {
		t.Fatalf("seed store append: %v", err)
	}
	for _, r := range recs {
		s.DeliverLocal(r.Topic, r.Payload, true) // retain=true, same as a real data/entity ingest
	}

	obs := connect(t, s.Addr(), "obs", "observer", "observer-secret")
	defer obs.Disconnect(100)

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
	s, st := newBroker(t)
	addr := s.Addr()
	if addr == "" {
		t.Fatal("Addr() is empty after AddListener")
	}

	t.Run("wrong password is rejected", func(t *testing.T) {
		opts := paho.NewClientOptions().
			AddBroker("tcp://" + addr).
			SetClientID("bad-pass").
			SetUsername("m1").
			SetPassword("wrong").
			SetConnectTimeout(5 * time.Second)
		c := paho.NewClient(opts)
		defer c.Disconnect(100)
		tok := c.Connect()
		if !tok.WaitTimeout(5 * time.Second) {
			t.Fatal("connect with wrong password: timed out instead of being refused")
		}
		if tok.Error() == nil {
			t.Fatal("connect with wrong password succeeded, want error")
		}
	})

	t.Run("client publish is ingested with mount rewrite", func(t *testing.T) {
		c := connect(t, addr, "m1-pub", "m1", "m1-secret")
		defer c.Disconnect(100)

		tok := c.Publish("colca/v1/_Metric/m1/temp", 1, false, []byte(`{"v":1}`))
		if !tok.WaitTimeout(5 * time.Second) {
			t.Fatal("publish: timed out waiting for PUBACK")
		}
		if err := tok.Error(); err != nil {
			t.Fatalf("publish: %v", err)
		}

		recs := waitRecords(t, st, "metrics", 1, 5*time.Second)
		if len(recs) != 1 {
			t.Fatalf("metrics records = %d, want 1: %+v", len(recs), recs)
		}
		if recs[0].Offset != 1 {
			t.Errorf("offset = %d, want 1", recs[0].Offset)
		}
		if want := "colca/v1/_Metric/m1/m1/temp"; recs[0].Topic != want {
			t.Errorf("topic = %q, want %q", recs[0].Topic, want)
		}
		if string(recs[0].Payload) != `{"v":1}` {
			t.Errorf("payload = %q, want %q", recs[0].Payload, `{"v":1}`)
		}
	})

	t.Run("spoofed identity is rejected and never persisted", func(t *testing.T) {
		c := connect(t, addr, "m1-spoof", "m1", "m1-secret")
		defer c.Disconnect(100)

		// A rejected packet gets no PUBACK under MQTT 3.1.1, so the token never
		// completes — the assertion is on the store, not on the token.
		tok := c.Publish("colca/v1/_Metric/OTHER/temp", 1, false, []byte(`{"v":2}`))
		tok.WaitTimeout(500 * time.Millisecond)

		time.Sleep(300 * time.Millisecond)
		recs, _, err := st.Read("metrics", 1, 100, nil)
		if err != nil {
			t.Fatalf("read metrics: %v", err)
		}
		if len(recs) != 1 {
			t.Fatalf("metrics records = %d, want 1 (spoofed publish must not persist): %+v", len(recs), recs)
		}
	})

	t.Run("DeliverLocal reaches subscribers and bypasses ingest", func(t *testing.T) {
		c := connect(t, addr, "m1-sub", "m1", "m1-secret")
		defer c.Disconnect(100)

		msgs := make(chan paho.Message, 1)
		tok := c.Subscribe("colca/v1/_CmdParam/m1/#", 1, func(_ paho.Client, m paho.Message) {
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
		s.DeliverLocal(topic, payload, false)

		select {
		case m := <-msgs:
			if m.Topic() != topic {
				t.Errorf("delivered topic = %q, want %q", m.Topic(), topic)
			}
			if string(m.Payload()) != string(payload) {
				t.Errorf("delivered payload = %q, want %q", m.Payload(), payload)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("DeliverLocal message never arrived")
		}

		// The inline delivery must not run through the engine.
		if got := st.NextOffset("commands"); got != 1 {
			t.Errorf("commands next offset = %d, want 1 (inline publish must bypass ingest)", got)
		}
		if got := st.NextOffset("metrics"); got != 2 {
			t.Errorf("metrics next offset = %d, want 2", got)
		}
		if got := st.NextOffset("entities"); got != 1 {
			t.Errorf("entities next offset = %d, want 1", got)
		}
	})
}

// collect subscribes to filter and returns a function that drains everything
// that arrived so far. Sub-second settling is deliberate: the assertions below
// are about what must NOT arrive, so the test has to give it time to arrive.
func collect(t *testing.T, c paho.Client, filter string) func(settle time.Duration) []paho.Message {
	t.Helper()
	msgs := make(chan paho.Message, 32)
	tok := c.Subscribe(filter, 1, func(_ paho.Client, m paho.Message) { msgs <- m })
	if !tok.WaitTimeout(5 * time.Second) {
		t.Fatalf("subscribe %s: timed out", filter)
	}
	if err := tok.Error(); err != nil {
		t.Fatalf("subscribe %s: %v", filter, err)
	}
	return func(settle time.Duration) []paho.Message {
		deadline := time.After(settle)
		var out []paho.Message
		for {
			select {
			case m := <-msgs:
				out = append(out, m)
			case <-deadline:
				return out
			}
		}
	}
}

// A failed CONNECT (unknown user or wrong token) must count against
// colca_rejected_publishes_total{reason="auth"} — the broker's own reject
// path, distinct from every engine reject path.
func TestBrokerAuthFailureIncrementsRejectedReasonAuth(t *testing.T) {
	s, _, m := newBrokerWithMetrics(t)
	addr := s.Addr()

	const line = `colca_rejected_publishes_total{reason="auth"}`
	if v := scrapeMetric(t, m, line); v != 0 {
		t.Fatalf("%s = %v before any failed connect, want 0", line, v)
	}

	opts := paho.NewClientOptions().
		AddBroker("tcp://" + addr).
		SetClientID("bad-pass").
		SetUsername("m1").
		SetPassword("wrong").
		// Pin the protocol version so paho makes exactly one physical connection.
		// With protocolVersionExplicit left false, paho's attemptConnection retries
		// a rejected CONNACK by opening a SECOND TCP connection and reconnecting
		// with MQTT 3.1 (goto CONN in the client internals) — that second
		// connection is a second, genuine CONNECT the broker's hook correctly
		// rejects again, not a double-count bug in the hook. Mochi itself only
		// ever invokes OnConnectAuthenticate once per TCP connection.
		SetProtocolVersion(4).
		SetConnectTimeout(5 * time.Second)
	c := paho.NewClient(opts)
	defer c.Disconnect(100)
	tok := c.Connect()
	if !tok.WaitTimeout(5 * time.Second) {
		t.Fatal("connect with wrong password: timed out instead of being refused")
	}
	if tok.Error() == nil {
		t.Fatal("connect with wrong password succeeded, want error")
	}

	if v := scrapeMetric(t, m, line); v != 1 {
		t.Fatalf("%s = %v after one failed CONNECT, want exactly 1", line, v)
	}

	// A successful connect right after must not move the auth counter.
	good := connect(t, addr, "m1-ok", "m1", "m1-secret")
	defer good.Disconnect(100)
	if v := scrapeMetric(t, m, line); v != 1 {
		t.Fatalf("%s = %v after a successful connect, want unchanged 1", line, v)
	}
}

// The no-double-delivery guarantee: a client's raw publish is suppressed
// (CodeSuccessIgnore) and only the engine's canonical, mount-rewritten form is
// distributed. A subscriber on colca/# must see each record exactly once.
func TestClientPublishDistributedOnlyAsCanonicalTopic(t *testing.T) {
	s, st := newBroker(t)

	sub := connect(t, s.Addr(), "observer-sub", "observer", "observer-secret")
	defer sub.Disconnect(100)
	drain := collect(t, sub, "colca/#")

	pub := connect(t, s.Addr(), "m1-pub", "m1", "m1-secret")
	defer pub.Disconnect(100)
	tok := pub.Publish("colca/v1/_Metric/m1/temp", 1, false, []byte(`{"v":42}`))
	if !tok.WaitTimeout(5 * time.Second) {
		t.Fatal("publish: timed out waiting for PUBACK (CodeSuccessIgnore must still ack)")
	}
	if err := tok.Error(); err != nil {
		t.Fatalf("publish: %v", err)
	}

	got := drain(1500 * time.Millisecond)
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
	if string(got[0].Payload()) != `{"v":42}` {
		t.Fatalf("payload = %q", got[0].Payload())
	}
	for _, m := range got {
		if m.Topic() == "colca/v1/_Metric/m1/temp" {
			t.Fatal("the raw client topic must never be distributed")
		}
	}
	if st.NextOffset("metrics") != 2 {
		t.Fatalf("metrics next offset = %d, want 2", st.NextOffset("metrics"))
	}
}

// Outside colca/# Colca is just a broker: a non-UNS publish is not persisted and
// must be distributed unchanged.
func TestNonUnsTopicStillDistributed(t *testing.T) {
	s, st := newBroker(t)

	sub := connect(t, s.Addr(), "observer-other", "observer", "observer-secret")
	defer sub.Disconnect(100)
	drain := collect(t, sub, "other/#")

	pub := connect(t, s.Addr(), "m1-other", "m1", "m1-secret")
	defer pub.Disconnect(100)
	tok := pub.Publish("other/thing", 1, false, []byte("hello"))
	if !tok.WaitTimeout(5 * time.Second) {
		t.Fatal("publish: timed out")
	}

	got := drain(1500 * time.Millisecond)
	if len(got) != 1 {
		t.Fatalf("want exactly 1 message on other/#, got %d", len(got))
	}
	if got[0].Topic() != "other/thing" || string(got[0].Payload()) != "hello" {
		t.Fatalf("non-UNS message altered: topic=%q payload=%q", got[0].Topic(), got[0].Payload())
	}
	for _, stream := range []string{"metrics", "entities", "commands"} {
		if off := st.NextOffset(stream); off != 1 {
			t.Fatalf("%s next offset = %d, want 1 (non-UNS must not persist)", stream, off)
		}
	}
}
