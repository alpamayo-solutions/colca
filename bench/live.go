package bench

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	pahomqtt "github.com/eclipse/paho.mqtt.golang"
)

// RunLive measures end-to-end freshness: machine → edge broker → engine →
// fsync → uplink cursor → mTLS → hub apply → hub bus. Machines publish at
// RateHz with sent_ns embedded; an observer on the HUB bus timestamps receipt.
// Both ends run in one process on one host, so one clock rules.
func RunLive(p Params) (*Report, error) {
	pair, err := StartPair(p.WorkDir, p.Machines)
	if err != nil {
		return nil, err
	}
	defer pair.Stop()

	r := NewReport("live", p.Storage, map[string]any{
		"machines": p.Machines, "rate_hz": p.RateHz, "duration": p.Duration.String(),
	})

	obs, err := pair.Observer("bench-live-obs")
	if err != nil {
		return nil, err
	}
	defer obs.Disconnect(250)
	var (
		mu   sync.Mutex
		lats []float64
	)
	tk := obs.Subscribe("colca/v1/_Metric/#", 1, func(_ pahomqtt.Client, m pahomqtt.Message) {
		now := time.Now().UnixNano()
		if !strings.Contains(m.Topic(), "/edge1/") {
			return // only records that crossed the replication hop count
		}
		var body struct {
			SentNS int64 `json:"sent_ns"`
		}
		if json.Unmarshal(m.Payload(), &body) != nil || body.SentNS == 0 {
			return
		}
		mu.Lock()
		lats = append(lats, float64(now-body.SentNS)/1e6)
		mu.Unlock()
	})
	if !tk.WaitTimeout(10*time.Second) || tk.Error() != nil {
		return nil, fmt.Errorf("observer subscribe: %w", tk.Error())
	}

	var (
		wg      sync.WaitGroup
		clients []pahomqtt.Client
	)
	stopAt := time.Now().Add(p.Duration)
	interval := time.Second / time.Duration(p.RateHz)
	for i := 1; i <= p.Machines; i++ {
		m, err := pair.Machine(i)
		if err != nil {
			return nil, err
		}
		clients = append(clients, m)
		topic := fmt.Sprintf("colca/v1/_Metric/m%d/temp", i)
		wg.Add(1)
		go func(m pahomqtt.Client, topic string) {
			defer wg.Done()
			tick := time.NewTicker(interval)
			defer tick.Stop()
			seq := 0
			for time.Now().Before(stopAt) {
				<-tick.C
				seq++
				payload, _ := json.Marshal(map[string]any{"v": float64(seq), "sent_ns": time.Now().UnixNano()})
				m.Publish(topic, 1, false, payload) // fire; ticker paces, PUBACK not awaited
			}
		}(m, topic)
	}
	wg.Wait()
	for _, c := range clients {
		c.Disconnect(250)
	}
	time.Sleep(2 * time.Second) // drain in-flight records across the hop

	mu.Lock()
	defer mu.Unlock()
	if len(lats) == 0 {
		return nil, fmt.Errorf("live: hub observer received nothing")
	}
	sort.Float64s(lats)
	r.Metrics["live_received_total"] = float64(len(lats))
	r.Metrics["live_achieved_rate_hz"] = float64(len(lats)) / p.Duration.Seconds()
	r.Metrics["live_p50_ms"] = Percentile(lats, 50)
	r.Metrics["live_p95_ms"] = Percentile(lats, 95)
	r.Metrics["live_p99_ms"] = Percentile(lats, 99)
	r.Metrics["live_max_ms"] = lats[len(lats)-1]
	return r, nil
}
