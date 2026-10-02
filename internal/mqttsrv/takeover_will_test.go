package mqttsrv

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	pahov5 "github.com/eclipse/paho.golang/paho"
	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"
)

// holdFirstPublish holds the first PUBLISH read from a connection until release
// closes, the way a write stuck until its deadline holds a connection's goroutine
// in the middle of a packet. done closes when that connection has disconnected.
// The hooks run on each connection's own goroutine, so mu guards held.
type holdFirstPublish struct {
	mqtt.HookBase
	mu      sync.Mutex
	held    *mqtt.Client
	holding chan struct{}
	release chan struct{}
	done    chan struct{}
}

func (h *holdFirstPublish) ID() string { return "hold-first-publish" }

func (h *holdFirstPublish) Provides(b byte) bool {
	return b == mqtt.OnPacketRead || b == mqtt.OnDisconnect
}

func (h *holdFirstPublish) OnPacketRead(cl *mqtt.Client, pk packets.Packet) (packets.Packet, error) {
	if pk.FixedHeader.Type != packets.Publish {
		return pk, nil
	}
	h.mu.Lock()
	first := h.held == nil
	if first {
		h.held = cl
	}
	h.mu.Unlock()
	if first {
		close(h.holding)
		<-h.release
	}
	return pk, nil
}

func (h *holdFirstPublish) OnDisconnect(cl *mqtt.Client, _ error, _ bool) {
	h.mu.Lock()
	held := h.held
	h.mu.Unlock()
	if cl == held {
		close(h.done)
	}
}

// serviceRecord is the _ServiceDetails payload a local service named bqc announces.
func serviceRecord(t *testing.T, ulid string, active bool) []byte {
	t.Helper()
	b, err := json.Marshal(map[string]any{
		"id": ulid, "name": "bqc", "display_name": "BQC", "description": "", "service_type": "app",
		"colca_node_id": "n1", "hierarchy": []string{"bqc"}, "is_active": active,
		"metadata": map[string]any{}, "architecture_metadata": map[string]any{}, "health_metrics": []any{},
	})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func dialAnnouncer(t *testing.T, addr string, will []byte) *pahov5.Client {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	c := pahov5.NewClient(pahov5.ClientConfig{Conn: conn})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cp := &pahov5.Connect{ClientID: "bqc", Username: "bqc", UsernameFlag: true, KeepAlive: 30, CleanStart: true}
	if will != nil {
		cp.WillMessage = &pahov5.WillMessage{Topic: announceTopic, Payload: will, QoS: 1, Retain: true}
	}
	ack, err := c.Connect(ctx, cp)
	if err != nil || ack.ReasonCode != 0 {
		t.Fatalf("connect: %v, %v", ack, err)
	}
	return c
}

const announceTopic = "colca/v1/_ServiceDetails/n1/bqc/_service"

func announce(c *pahov5.Client, record []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	res, err := c.Publish(ctx, &pahov5.Publish{Topic: announceTopic, QoS: 1, Retain: true, Payload: record})
	if err == nil && res != nil && res.ReasonCode != 0 {
		return fmt.Errorf("PUBACK reason %d: %s", res.ReasonCode, res.Properties.ReasonString)
	}
	return err
}

// A service reconnects under the same client id while its old connection is stuck
// in the middle of a publish. The old connection's will must not land after the
// new connection's announce, or the service reads as inactive while connected.
func TestATakenOverConnectionsWillDoesNotOverwriteTheNewAnnounce(t *testing.T) {
	w := startServerWithLocalDoor(t)
	hold := &holdFirstPublish{holding: make(chan struct{}), release: make(chan struct{}), done: make(chan struct{})}
	if err := w.srv.S.AddHook(hold, nil); err != nil {
		t.Fatal(err)
	}

	var releaseOnce sync.Once
	releaseOld := func() { releaseOnce.Do(func() { close(hold.release) }) }
	t.Cleanup(releaseOld)

	// The local door registers bqc on its first CONNECT; its ULID is in the record.
	probe := dialAnnouncer(t, w.LocalAddr(), nil)
	_ = probe.Disconnect(&pahov5.Disconnect{})
	entry, ok := w.Registry().ByName("bqc")
	if !ok {
		t.Fatal("bqc was not registered")
	}
	active, inactive := serviceRecord(t, entry.ULID, true), serviceRecord(t, entry.ULID, false)

	old := dialAnnouncer(t, w.LocalAddr(), inactive)
	go func() { _ = announce(old, active) }()
	<-hold.holding

	current := dialAnnouncer(t, w.LocalAddr(), inactive)
	t.Cleanup(func() { _ = current.Disconnect(&pahov5.Disconnect{}) })
	if err := announce(current, active); err != nil {
		t.Fatalf("announce on the new connection: %v", err)
	}

	releaseOld()
	select {
	case <-hold.done:
	case <-time.After(5 * time.Second):
		t.Fatal("the replaced connection never finished disconnecting")
	}

	var stored []byte
	for _, e := range mustKVScan(t, w.st, "") {
		if e.Topic == announceTopic {
			stored = e.Payload
		}
	}
	var details struct {
		IsActive *bool `json:"is_active"`
	}
	if err := json.Unmarshal(stored, &details); err != nil || details.IsActive == nil || !*details.IsActive {
		t.Fatalf("stored record = %s, want is_active true", stored)
	}
	retained := w.srv.S.Topics.Messages(announceTopic)
	if len(retained) != 1 || string(retained[0].Payload) == string(inactive) {
		t.Fatalf("broker retains %d message(s) for the record, the last will among them", len(retained))
	}
}
