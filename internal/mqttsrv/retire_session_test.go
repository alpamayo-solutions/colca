package mqttsrv

import (
	"context"
	"net"
	"testing"
	"time"

	pahov5 "github.com/eclipse/paho.golang/paho"
)

// dialService connects the local service bqc the way a service inside the
// deployment does: a session that outlives the connection.
func dialService(t *testing.T, addr string) *pahov5.Client {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	c := pahov5.NewClient(pahov5.ClientConfig{Conn: conn})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	forever := uint32(0xFFFFFFFF)
	ack, err := c.Connect(ctx, &pahov5.Connect{ClientID: "bqc", Username: "bqc", UsernameFlag: true, KeepAlive: 30,
		Properties: &pahov5.ConnectProperties{SessionExpiryInterval: &forever}})
	if err != nil || ack.ReasonCode != 0 {
		t.Fatalf("connect: %v, %v", ack, err)
	}
	return c
}

func subscribeService(t *testing.T, c *pahov5.Client, filter string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ack, err := c.Subscribe(ctx, &pahov5.Subscribe{Subscriptions: []pahov5.SubscribeOptions{{Topic: filter, QoS: 1}}})
	if err != nil || len(ack.Reasons) != 1 || ack.Reasons[0] > 2 {
		t.Fatalf("subscribe %s: %v, %v", filter, ack, err)
	}
}

const (
	retiredFilter    = "colca/v1/_Metric/n1/#"
	retiredEndedLine = `colca_mqtt_offline_sessions_ended_total{reason="retired"}`
)

func sessionSubscribed(w *world, clientID string) bool {
	_, ok := w.srv.S.Topics.Subscribers(offlineTopic).Subscriptions[clientID]
	return ok
}

// A service inside a deployment that retires itself publishes an empty
// _ServiceDetails and disconnects. Nothing removes a local service's identity,
// so its session used to stay, subscribed and queueing, until its expiry: 35
// minutes in the run that found it. It ends with the connection.
func TestTheSessionOfAServiceThatRetiredItselfEndsWithItsConnection(t *testing.T) {
	w := startServerWithLocalDoor(t)
	probe := dialService(t, w.LocalAddr())
	_ = probe.Disconnect(&pahov5.Disconnect{})
	entry, ok := w.Registry().ByName("bqc")
	if !ok {
		t.Fatal("bqc was not registered")
	}
	waitMetric(t, w, offlineSessionsLine, 1)

	c := dialService(t, w.LocalAddr())
	subscribeService(t, c, retiredFilter)
	if err := announce(c, serviceRecord(t, entry.ULID, true)); err != nil {
		t.Fatalf("announce: %v", err)
	}
	if err := announce(c, nil); err != nil {
		t.Fatalf("retire: %v", err)
	}
	if err := c.Disconnect(&pahov5.Disconnect{}); err != nil {
		t.Fatal(err)
	}

	waitMetric(t, w, retiredEndedLine, 1)
	if _, held := w.srv.S.Clients.Get("bqc"); held {
		t.Fatal("the retired service's session is still held")
	}
	if sessionSubscribed(w, "bqc") {
		t.Fatal("the retired service's subscription is still in the topic index")
	}
	if got := scrapeMetric(t, w.m, offlineSessionsLine); got != 0 {
		t.Fatalf("%s = %v, want 0", offlineSessionsLine, got)
	}
}

// A service that disconnects without retiring keeps its session, and one that
// announces itself again after an empty record has not retired.
func TestOnlyARetiredServiceLosesItsSession(t *testing.T) {
	w := startServerWithLocalDoor(t)
	probe := dialService(t, w.LocalAddr())
	_ = probe.Disconnect(&pahov5.Disconnect{})
	entry, ok := w.Registry().ByName("bqc")
	if !ok {
		t.Fatal("bqc was not registered")
	}
	active := serviceRecord(t, entry.ULID, true)

	for name, again := range map[string]bool{"never retired": false, "announced again": true} {
		c := dialService(t, w.LocalAddr())
		subscribeService(t, c, retiredFilter)
		if err := announce(c, active); err != nil {
			t.Fatalf("%s: announce: %v", name, err)
		}
		if again {
			if err := announce(c, nil); err != nil {
				t.Fatalf("%s: retire: %v", name, err)
			}
			if err := announce(c, active); err != nil {
				t.Fatalf("%s: announce again: %v", name, err)
			}
		}
		if err := c.Disconnect(&pahov5.Disconnect{}); err != nil {
			t.Fatal(err)
		}
		waitMetric(t, w, offlineSessionsLine, 1)
		if _, held := w.srv.S.Clients.Get("bqc"); !held || !sessionSubscribed(w, "bqc") {
			t.Fatalf("%s: the session of a service that did not retire was removed", name)
		}
	}
	if got := scrapeMetric(t, w.m, retiredEndedLine); got != 0 {
		t.Fatalf("%s = %v, want 0", retiredEndedLine, got)
	}
}
