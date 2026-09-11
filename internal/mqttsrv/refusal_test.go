package mqttsrv

import (
	"context"
	"sync"
	"testing"
	"time"

	paho5 "github.com/eclipse/paho.golang/paho"
	paho "github.com/eclipse/paho.mqtt.golang"
)

// mochi acts on a reason code returned from OnPublish only for an MQTT 5 client at
// QoS 1 or 2. For any other publish it goes on to retain and fan out the packet the
// hook refused, so those have to be dropped instead.
func TestARefusedPublishReachesNoSubscriberAtAnyVersionOrQoS(t *testing.T) {
	w := newWorld(t)
	addr := w.srv.Addr()

	var mu sync.Mutex
	seen := map[string]int{}
	obs := connect(t, addr, "obs-refusals", w.obs)
	tok := obs.Subscribe("#", 1, func(_ paho.Client, m paho.Message) {
		mu.Lock()
		seen[m.Topic()]++
		mu.Unlock()
	})
	if !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("subscribe: %v", tok.Error())
	}

	c5 := connect5(t, addr, w.m1)
	publishV5 := func(topic, payload string, qos byte, retain bool) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := c5.Publish(ctx, &paho5.Publish{Topic: topic, QoS: qos, Retain: retain, Payload: []byte(payload)}); err != nil {
			t.Fatalf("mqtt5 publish %s: %v", topic, err)
		}
	}
	c3 := connect(t, addr, "m1-v3-refusals", w.m1)
	publishV3 := func(topic, payload string, qos byte, retain bool) {
		t.Helper()
		if tok := c3.Publish(topic, qos, retain, []byte(payload)); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
			t.Fatalf("mqtt3 publish %s: %v", topic, tok.Error())
		}
	}

	refused := []string{
		"colca/v1/_Metric/m2/temp",
		"colca/v1/_Metric/n1/m1/pressure",
		"colca/v1/_CmdParam/m1/m1/go",
		"factory/raw/retained-v5-qos0",
		"factory/raw/retained-v3-qos0",
		"factory/raw/retained-v3-qos1",
	}
	publishV5(refused[0], `{"v": 1}`, 0, false)                                          // another node's level-4
	publishV5(refused[1], `{"nope": 1}`, 0, false)                                       // schema invalid
	publishV5(refused[2], `{"correlation_id":"c","expires_at":9000000000000}`, 0, false) // command without a grant
	publishV5(refused[3], "value", 0, true)                                              // retained outside the UNS
	publishV3(refused[4], "value", 0, true)
	// MQTT 3.1.1 has no negative ack, so a refused QoS 1 publish gets no PUBACK.
	c3.Publish(refused[5], 1, true, []byte("value"))

	// Plain broker traffic still flows. Once it has arrived, a refused publish sent
	// before it would have arrived too.
	publishV5("factory/raw/after-v5", "x", 0, false)
	publishV3("factory/raw/after-v3", "x", 0, false)
	deadline := time.Now().Add(5 * time.Second)
	for {
		mu.Lock()
		arrived := seen["factory/raw/after-v5"] > 0 && seen["factory/raw/after-v3"] > 0
		mu.Unlock()
		if arrived {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("plain broker traffic never reached the observer")
		}
		time.Sleep(20 * time.Millisecond)
	}
	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	for _, topic := range refused {
		if seen[topic] > 0 {
			t.Errorf("refused publish on %s reached a subscriber", topic)
		}
		if _, ok := w.srv.S.Topics.Retained.Get(topic); ok {
			t.Errorf("refused publish on %s was retained", topic)
		}
	}
}
