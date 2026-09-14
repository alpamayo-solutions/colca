package mqttsrv

import (
	"sync/atomic"
	"testing"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"
	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"

	"github.com/alpamayo-solutions/colca/internal/authtest"
)

// holdDeliveries parks the client's write loop on its first delivery once armed, so
// the deliveries behind it stay queued and mochi only buffers a PUBACK written
// meanwhile. It lets go once the client is closed.
type holdDeliveries struct {
	mqtt.HookBase
	clientID string
	armed    atomic.Bool
	held     chan struct{}
}

func (h *holdDeliveries) ID() string           { return "hold-deliveries" }
func (h *holdDeliveries) Provides(b byte) bool { return b == mqtt.OnPacketEncode }

func (h *holdDeliveries) OnPacketEncode(cl *mqtt.Client, pk packets.Packet) packets.Packet {
	if cl.ID != h.clientID || pk.FixedHeader.Type != packets.Publish || !h.armed.CompareAndSwap(true, false) {
		return pk
	}
	close(h.held)
	for deadline := time.Now().Add(10 * time.Second); !cl.Closed() && time.Now().Before(deadline); {
		time.Sleep(time.Millisecond)
	}
	return pk
}

// A client that keeps receiving deliveries has one queued whenever its PUBACK is
// written, and mochi then only buffers the PUBACK. Shutdown must still deliver it, or
// the client resends the publish after the restart and it is stored twice.
func TestAPubackBufferedBehindQueuedDeliveriesReachesTheClientBeforeShutdown(t *testing.T) {
	w := newWorld(t)
	addr := w.srv.Addr()
	m3 := authtest.NewMachine(t, "m3")
	authtest.EnrollAt(t, w.reg, w.eng, m3, "m3", "write:"+authtest.ElementID("m3")+"/#", "read:#")
	hold := &holdDeliveries{clientID: "m3-busy", held: make(chan struct{})}
	if err := w.srv.S.AddHook(hold, nil); err != nil {
		t.Fatal(err)
	}
	c := connectResuming(t, addr, "m3-busy", m3)
	const feed = "factory/raw/feed"
	if tok := c.Subscribe(feed, 0, func(paho.Client, paho.Message) {}); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("subscribe: %v", tok.Error())
	}

	hold.armed.Store(true)
	for range 3 {
		if err := w.srv.S.Publish(feed, []byte("x"), false, 0); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case <-hold.held:
	case <-time.After(5 * time.Second):
		t.Fatal("no delivery reached the client's write loop")
	}

	const topic = "colca/v1/_Metric/n1/m3/temp"
	tok := c.Publish(topic, 1, false, []byte(`{"v": 1}`))
	waitRecords(t, w.st, "metrics", 1, 5*time.Second)
	if tok.WaitTimeout(300 * time.Millisecond) {
		t.Fatal("the PUBACK was written at once, so no delivery was queued ahead of it")
	}

	if err := w.srv.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !tok.WaitTimeout(time.Second) || tok.Error() != nil {
		t.Errorf("the buffered PUBACK never reached the client (err %v)", tok.Error())
	}

	restartOn(t, w, addr)
	mustConnect(t, c)
	publishAcked(t, c, topic, `{"v": 2}`)
	got := storedPayloads(t, w.st, "metrics", topic)
	if len(got) != 2 || got[`{"v": 1}`] != 1 || got[`{"v": 2}`] != 1 {
		t.Fatalf("stored after the restart: %v, want each publish once", got)
	}
}
