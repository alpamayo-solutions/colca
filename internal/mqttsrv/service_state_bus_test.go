package mqttsrv

import (
	"context"
	"net"
	"testing"
	"time"

	pahov5 "github.com/eclipse/paho.golang/paho"
)

// A service's private state is never on the bus: a local service that reads
// the whole node and subscribes to everything receives the record delivered
// after it, never the state, retained or live.
func TestServiceStateIsNeverOnTheBus(t *testing.T) {
	s := startServerWithLocalDoor(t)
	state := "colca/v1/_ServiceState/n1/notifications/evaluation/01ALARM"
	s.srv.DeliverLocal(state, []byte(`{"version":1}`), true)

	conn, err := net.Dial("tcp", s.LocalAddr())
	if err != nil {
		t.Fatal(err)
	}
	messages := make(chan string, 4)
	client := pahov5.NewClient(pahov5.ClientConfig{Conn: conn,
		OnPublishReceived: []func(pahov5.PublishReceived) (bool, error){func(p pahov5.PublishReceived) (bool, error) {
			messages <- p.Packet.Topic
			return true, nil
		}},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ack, err := client.Connect(ctx, &pahov5.Connect{ClientID: "observer", Username: "observer",
		UsernameFlag: true, CleanStart: true, KeepAlive: 30})
	if err != nil || ack.ReasonCode != 0 {
		t.Fatalf("connect: %v, %v", ack, err)
	}
	t.Cleanup(func() { _ = client.Disconnect(&pahov5.Disconnect{}) })
	suback, err := client.Subscribe(ctx, &pahov5.Subscribe{Subscriptions: []pahov5.SubscribeOptions{{Topic: "colca/#", QoS: 1}}})
	if err != nil || len(suback.Reasons) != 1 || suback.Reasons[0] > 2 {
		t.Fatalf("subscribe: %v, %v", suback, err)
	}
	// The filter that names the contract is refused outright.
	named, err := client.Subscribe(ctx, &pahov5.Subscribe{Subscriptions: []pahov5.SubscribeOptions{{Topic: "colca/v1/_ServiceState/#", QoS: 1}}})
	if named == nil || len(named.Reasons) != 1 || named.Reasons[0] <= 2 {
		t.Fatalf("subscribe to _ServiceState: %v, %v, want refused", named, err)
	}
	s.srv.DeliverLocal(state, []byte(`{"version":2}`), true)
	marker := "colca/v1/_ServiceDetails/n1/marker/_service"
	s.srv.DeliverLocal(marker, []byte(`{"id":"x"}`), true)
	for {
		select {
		case got := <-messages:
			if got == state {
				t.Fatal("service state reached a bus subscriber")
			}
			if got == marker {
				return
			}
		case <-ctx.Done():
			t.Fatal("the marker never arrived")
		}
	}
}
