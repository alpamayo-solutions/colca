package mqttsrv

import (
	"testing"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"

	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/engine"
	"github.com/alpamayo-solutions/colca/internal/store"
)

// newBroker starts a broker on a random loopback port with one configured
// client (m1/m1-secret mounted at "m1") and a real store. The engine is bound
// after construction, exercising the late-binding path node assembly uses.
func newBroker(t *testing.T) (*Server, *store.Store) {
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
		Clients: []config.Client{{ULID: "m1", Token: "m1-secret", Mount: "m1"}},
	}
	s, err := New(cfg, nil)
	if err != nil {
		st.Close()
		t.Fatalf("New: %v", err)
	}
	s.SetEngine(engine.New(st, cfg, s.DeliverLocal))
	go func() { _ = s.Serve() }()
	t.Cleanup(func() {
		s.Close()
		st.Close()
	})
	return s, st
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
		s.DeliverLocal(topic, payload)

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
