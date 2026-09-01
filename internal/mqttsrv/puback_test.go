package mqttsrv

import (
	"context"
	"crypto/tls"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/packets"
	"github.com/eclipse/paho.golang/paho"

	"github.com/alpamayo-solutions/colca/internal/authtest"
)

// mqtt5Conn dials the machine door and hands back a connection whose packet
// writes are SERIALISED.
//
// paho.golang writes a control packet as several Write calls (fixed header,
// then each buffer of the body) and only holds a lock around them when the
// connection implements sync.Locker — `ClientConfig.Conn`'s own doc says so:
// "BEWARE that most wrapped net.Conn implementations like tls.Conn are not
// thread safe for writing." A *tls.Conn is not, and its pinger writes the
// first PINGREQ from a separate goroutine the instant the client connects
// (paho/pinger.go: `time.NewTimer(0) // Immediately send first pingreq`).
//
// Handing the raw tls.Conn over therefore let that PINGREQ land BETWEEN a
// PUBLISH's fixed header and its body. The broker framed `33 1e` (PUBLISH,
// remaining 30) and then read `c0 00 …` as the body, so the topic length came
// out as 0xc000 and it answered "malformed packet: topic", dropped the
// connection, and sent no PUBACK — the test then waited out its whole context
// and failed with a deadline. Rare when idle (the ping goroutine usually
// finishes first) and reproducible under load: 3 of 12 concurrent -race
// batches before this wrapper, 0 of 14 after.
func mqtt5Conn(t *testing.T, addr string, m *authtest.Machine) net.Conn {
	t.Helper()
	conn, err := tls.Dial("tcp", addr, m.TLSConfig())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	return packets.NewThreadSafeConn(conn)
}

// connect5 dials the machine door as an MQTT 5 client (paho.golang) — the
// PUBACK reason-code surface only exists for MQTT 5 sessions at QoS >= 1
// (schema-bundle design §8.1).
func connect5(t *testing.T, addr string, m *authtest.Machine) *paho.Client {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c := paho.NewClient(paho.ClientConfig{Conn: mqtt5Conn(t, addr, m)})
	ca, err := c.Connect(ctx, &paho.Connect{
		ClientID:     m.ULID + "-mqtt5",
		Username:     m.ULID,
		UsernameFlag: true,
		KeepAlive:    30,
		CleanStart:   true,
		Properties:   &paho.ConnectProperties{},
	})
	if err != nil {
		t.Fatalf("mqtt5 connect: %v", err)
	}
	if ca.ReasonCode != 0 {
		t.Fatalf("mqtt5 CONNACK refused: %d", ca.ReasonCode)
	}
	t.Cleanup(func() { _ = c.Disconnect(&paho.Disconnect{ReasonCode: 0}) })
	return c
}

func publish5(t *testing.T, c *paho.Client, topic string, payload []byte) byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := c.Publish(ctx, &paho.Publish{Topic: topic, QoS: 1, Payload: payload})
	if err != nil && resp == nil {
		t.Fatalf("publish %s: %v", topic, err)
	}
	return resp.ReasonCode
}

func publishRetained5(t *testing.T, c *paho.Client, topic string, payload []byte) byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := c.Publish(ctx, &paho.Publish{Topic: topic, QoS: 1, Retain: true, Payload: payload})
	if err != nil && resp == nil {
		t.Fatalf("retained publish %s: %v", topic, err)
	}
	return resp.ReasonCode
}

// The §8.1 table on the wire: an MQTT 5 client at QoS 1 sees the engine's
// verdicts as PUBACK reason codes instead of silence.
func TestPubackReasonCodesMQTT5(t *testing.T) {
	w := newWorld(t)
	c := connect5(t, w.srv.Addr(), w.m1)

	cases := []struct {
		name    string
		topic   string
		payload string
		want    byte
	}{
		{"valid publish", "colca/v1/_Metric/n1/m1/temp", `{"v": 1}`, 0x00},
		{"schema invalid", "colca/v1/_Metric/n1/m1/temp", `{"nope": 1}`, 0x99},
		{"wrong level-4", "colca/v1/_Metric/m2/temp", `{"v": 1}`, 0x87},
		{"unknown contract", "colca/v1/_Bogus/m1/x", `{}`, 0x90},
		{"cmd without grant", "colca/v1/_CmdParam/m1/m1/go", `{"correlation_id":"c","expires_at":9000000000000}`, 0x87},
		{"registry contract", "colca/v1/_EnrolledIdentity/m1/_colca/identities/u", `{"ulid":"u"}`, 0x87},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := publish5(t, c, tc.topic, []byte(tc.payload))
			if got != tc.want {
				t.Fatalf("PUBACK for %s = 0x%02x, want 0x%02x", tc.topic, got, tc.want)
			}
		})
	}
	// Non-uns topics pass through as plain broker traffic (0x00 or 0x10
	// no-matching-subscribers — both success-class).
	if got := publish5(t, c, "factory/raw/x", []byte("y")); got != 0x00 && got != 0x10 {
		t.Fatalf("non-UNS publish must succeed, got 0x%02x", got)
	}
}

// The predicate paho actually branches on, pinned so the wrapper cannot be
// dropped again: packets.ControlPacket.WriteTo serialises a multi-Write packet
// if and only if the writer is a sync.Locker, and every MQTT 5 test here
// shares one connection with a pinger goroutine that writes on its own. Assert
// it the same way the library asks the question — a check on the type name, or
// on the wrapper being called, would go green against a wrapper that had
// stopped satisfying it.
//
// It is deliberately not a repetition test. The corruption is a genuine race:
// hammering it would prove the wrapper works only on the runs where the race
// happened to be lost, which is the flaky shape this replaces.
func TestTheMQTT5ClientConnectionSerialisesPacketWrites(t *testing.T) {
	w := newWorld(t)
	conn := mqtt5Conn(t, w.srv.Addr(), w.m1)
	t.Cleanup(func() { _ = conn.Close() })

	if _, ok := conn.(sync.Locker); !ok {
		t.Fatalf("the MQTT 5 test connection (%T) is not a sync.Locker, so paho writes a packet's "+
			"header and body without a lock — its pinger's immediate PINGREQ then lands inside a "+
			"PUBLISH and the broker rightly refuses a malformed packet, with no PUBACK to answer", conn)
	}
}

func TestExternalNonUnsRetainedMessagesAreRefused(t *testing.T) {
	w := newWorld(t)
	c := connect5(t, w.srv.Addr(), w.m1)
	before := w.srv.S.Info.Retained

	if got := publishRetained5(t, c, "factory/raw/retained", []byte("value")); got != 0x9A {
		t.Fatalf("retained non-UNS PUBACK = 0x%02x, want retain-not-supported 0x9A", got)
	}
	if got := w.srv.S.Info.Retained; got != before {
		t.Fatalf("retained set grew from %d to %d after rejected publish", before, got)
	}
}
