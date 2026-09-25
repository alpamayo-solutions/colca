package mqttsrv

import (
	"context"
	"net"
	"testing"
	"time"

	pahov5 "github.com/eclipse/paho.golang/paho"
)

func TestMountedLocalConnectorReceivesSharedClockRecords(t *testing.T) {
	s := startServerWithLocalDoor(t)
	conn, err := net.Dial("tcp", s.LocalAddr())
	if err != nil {
		t.Fatal(err)
	}
	messages := make(chan string, 3)
	client := pahov5.NewClient(pahov5.ClientConfig{Conn: conn,
		OnPublishReceived: []func(pahov5.PublishReceived) (bool, error){func(p pahov5.PublishReceived) (bool, error) {
			messages <- p.Packet.Topic
			return true, nil
		}},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	props := &pahov5.ConnectProperties{}
	props.User.Add("mount", "line1/press3")
	ack, err := client.Connect(ctx, &pahov5.Connect{ClientID: "clock-connector", Username: "clock-connector",
		UsernameFlag: true, CleanStart: true, KeepAlive: 30, Properties: props})
	if err != nil || ack.ReasonCode != 0 {
		t.Fatalf("connect: %v, %v", ack, err)
	}
	t.Cleanup(func() { _ = client.Disconnect(&pahov5.Disconnect{}) })
	for _, contract := range []string{"_ClockDefinition", "_ClockProgress", "_ServiceDetails"} {
		topic := "colca/v1/" + contract + "/n1/shared-clock/worker"
		s.srv.DeliverLocal(topic, []byte(`{"fixture":true}`), true)
		suback, err := client.Subscribe(ctx, &pahov5.Subscribe{Subscriptions: []pahov5.SubscribeOptions{{Topic: "colca/v1/" + contract + "/n1/#", QoS: 1}}})
		if err != nil || len(suback.Reasons) != 1 || suback.Reasons[0] > 2 {
			t.Fatalf("subscribe %s: %v, %v", contract, suback, err)
		}
		select {
		case got := <-messages:
			if got != topic {
				t.Fatalf("received %q, want %q", got, topic)
			}
		case <-ctx.Done():
			t.Fatalf("mounted connector did not receive retained %s", contract)
		}
	}
}
