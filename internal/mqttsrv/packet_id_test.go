package mqttsrv

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"
)

// A client that subscribes while the broker still has its own QoS 1 deliveries in
// flight to it may pick a packet id the broker is using for one of them. Packet ids
// are scoped per direction (MQTT v3.1.1 section 2.3.1), so the SUBSCRIBE and the
// UNSUBSCRIBE must still take effect. Upstream mochi refused both with "packet
// identifier in use"; we pin a fork that does not, and this test fails if the pin
// is dropped before upstream has the fix.
func TestASubscribeReusingAnInflightDeliveryIDStillSubscribes(t *testing.T) {
	const retained = 20
	w := newWorld(t)
	for i := range retained {
		w.srv.DeliverLocal(fmt.Sprintf("colca/v1/_Metric/n1/m1/sig%d", i), []byte(`{"v":1}`), true)
	}

	// Deliveries are left unacknowledged, so the broker keeps ids 1..retained in
	// flight while the client subscribes again.
	c := paho.NewClient(paho.NewClientOptions().
		AddBroker("ssl://" + w.srv.Addr()).
		SetTLSConfig(w.obs.TLSConfig()).
		SetClientID("obs-pktid").
		SetUsername(w.obs.ULID).
		SetProtocolVersion(4).
		SetAutoReconnect(false).
		SetConnectRetry(false).
		SetConnectTimeout(5 * time.Second).
		SetAutoAckDisabled(true))
	mustConnect(t, c)
	t.Cleanup(func() { c.Disconnect(100) })

	var burst atomic.Int64
	if tok := c.Subscribe("colca/#", 1, func(paho.Client, paho.Message) { burst.Add(1) }); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("subscribe colca/#: %v", tok.Error())
	}
	for deadline := time.Now().Add(5 * time.Second); burst.Load() < retained; {
		if time.Now().After(deadline) {
			t.Fatalf("got %d of %d retained deliveries", burst.Load(), retained)
		}
		time.Sleep(5 * time.Millisecond)
	}

	// paho numbers its packets 1, 2, 3, ..., so these reuse ids the broker holds.
	const cmd = "factory/cmd/m1"
	var got atomic.Int64
	tok := c.Subscribe(cmd, 1, func(_ paho.Client, m paho.Message) { got.Add(1); m.Ack() })
	if !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("subscribe %s: %v", cmd, tok.Error())
	}
	if code := tok.(*paho.SubscribeToken).Result()[cmd]; code != 1 {
		t.Fatalf("SUBACK for %s = %#x, want QoS 1 granted", cmd, code)
	}
	w.srv.DeliverLocal(cmd, []byte("on"), false)
	for deadline := time.Now().Add(5 * time.Second); got.Load() == 0; {
		if time.Now().After(deadline) {
			t.Fatal("the subscription was acknowledged but no message arrived on it")
		}
		time.Sleep(5 * time.Millisecond)
	}

	// MQTT 3.1.1 UNSUBACK carries no reason code; a refused UNSUBSCRIBE shows as a
	// message that still arrives.
	if tok := c.Unsubscribe(cmd); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("unsubscribe %s: %v", cmd, tok.Error())
	}
	w.srv.DeliverLocal(cmd, []byte("off"), false)
	time.Sleep(200 * time.Millisecond)
	if n := got.Load(); n != 1 {
		t.Fatalf("got %d messages on %s, want 1: the UNSUBSCRIBE was refused", n, cmd)
	}
}
