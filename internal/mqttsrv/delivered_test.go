package mqttsrv

import (
	"testing"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"
)

// Every PUBLISH written to a subscriber is counted by door with its payload
// bytes; the series exist at 0 before anything is delivered.
func TestDeliveredPublishesAreCountedByDoor(t *testing.T) {
	w := newWorld(t)
	for _, line := range []string{
		`colca_mqtt_delivered_messages_total{door="mqtt"}`,
		`colca_mqtt_delivered_messages_total{door="local"}`,
		`colca_mqtt_delivered_messages_total{door="human"}`,
		`colca_mqtt_delivered_payload_bytes_total{door="mqtt"}`,
	} {
		if v := scrapeMetric(t, w.m, line); v != 0 {
			t.Fatalf("%s = %v before any delivery, want 0", line, v)
		}
	}

	obs := connect(t, w.srv.Addr(), "obs", w.obs)
	got := make(chan struct{}, 4)
	tok := obs.Subscribe("colca/v1/_Metric/n1/m1/#", 1, func(paho.Client, paho.Message) { got <- struct{}{} })
	if !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("subscribe: %v", tok.Error())
	}
	payload := []byte(`{"v":42}`)
	w.srv.DeliverLocal("colca/v1/_Metric/n1/m1/sig", payload, false)
	w.srv.DeliverLocal("colca/v1/_Metric/n1/m1/sig", payload, false)
	for range 2 {
		select {
		case <-got:
		case <-time.After(5 * time.Second):
			t.Fatal("the subscriber did not receive both publishes")
		}
	}

	// The counter is bumped after the write, which may land just after the
	// client's callback ran.
	deadline := time.Now().Add(5 * time.Second)
	for scrapeMetric(t, w.m, `colca_mqtt_delivered_messages_total{door="mqtt"}`) < 2 {
		if time.Now().After(deadline) {
			t.Fatal("delivered publishes were not counted on the mqtt door")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if v := scrapeMetric(t, w.m, `colca_mqtt_delivered_payload_bytes_total{door="mqtt"}`); v != float64(2*len(payload)) {
		t.Fatalf("delivered payload bytes = %v, want %d", v, 2*len(payload))
	}
	if v := scrapeMetric(t, w.m, `colca_mqtt_delivered_messages_total{door="local"}`); v != 0 {
		t.Fatalf("a delivery on the TLS door was counted on the local door: %v", v)
	}
}
