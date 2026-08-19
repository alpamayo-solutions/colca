package bench

import (
	"encoding/json"
	"testing"
	"time"

	pahomqtt "github.com/eclipse/paho.mqtt.golang"
)

// TestPairUplinkAndHubRestart pins everything the scenarios rely on: machines
// can publish, records replicate to the hub under the mount, the hub observer
// sees the canonical topic, and the hub restarts on the same repl address so
// catch-up scenarios can take it down and bring it back.
func TestPairUplinkAndHubRestart(t *testing.T) {
	p, err := StartPair(t.TempDir(), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Stop()

	obs, err := p.Observer("bench-obs")
	if err != nil {
		t.Fatal(err)
	}
	got := make(chan string, 16)
	if tk := obs.Subscribe("colca/#", 1, func(_ pahomqtt.Client, m pahomqtt.Message) {
		got <- m.Topic()
	}); !tk.WaitTimeout(5*time.Second) || tk.Error() != nil {
		t.Fatalf("subscribe: %v", tk.Error())
	}

	m, err := p.Machine(1)
	if err != nil {
		t.Fatal(err)
	}
	payload, _ := json.Marshal(map[string]any{"v": 1.5, "value": 1.5, "signal_id": "bench"})
	if tk := m.Publish("colca/v1/_Metric/n-edge/m1/temp", 1, false, payload); !tk.WaitTimeout(5*time.Second) || tk.Error() != nil {
		t.Fatalf("publish: %v", tk.Error())
	}

	deadline := time.After(10 * time.Second)
	for {
		select {
		case topic := <-got:
			if topic == "colca/v1/_Metric/n-edge/edge1/m1/temp" {
				goto restart
			}
		case <-deadline:
			t.Fatal("hub observer never saw colca/v1/_Metric/n-edge/edge1/m1/temp")
		}
	}

restart:
	before := NextOffset(p.Hub, "metrics")
	p.StopHub()
	if tk := m.Publish("colca/v1/_Metric/n-edge/m1/temp", 1, false, payload); !tk.WaitTimeout(5*time.Second) || tk.Error() != nil {
		t.Fatalf("publish while hub down: %v", tk.Error())
	}
	if err := p.StartHub(); err != nil {
		t.Fatalf("hub restart: %v", err)
	}
	waitDeadline := time.Now().Add(15 * time.Second)
	for NextOffset(p.Hub, "metrics") <= before {
		if time.Now().After(waitDeadline) {
			t.Fatalf("hub metrics offset stuck at %d after restart", NextOffset(p.Hub, "metrics"))
		}
		time.Sleep(50 * time.Millisecond)
	}
}
