package mqttsrv

import (
	"context"
	"crypto/tls"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"

	"github.com/alpamayo-solutions/colca/internal/authtest"
)

// connect5 dials the machine door as an MQTT 5 client (paho.golang) — the
// PUBACK reason-code surface only exists for MQTT 5 sessions at QoS >= 1
// (schema-bundle design §8.1).
func connect5(t *testing.T, addr string, m *authtest.Machine) *paho.Client {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := tls.Dial("tcp", addr, m.TLSConfig())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	c := paho.NewClient(paho.ClientConfig{Conn: conn})
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
