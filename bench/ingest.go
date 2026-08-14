package bench

import (
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	pahomqtt "github.com/eclipse/paho.mqtt.golang"
)

// Params configures every scenario; each scenario documents the fields it uses.
type Params struct {
	Machines   int           // concurrent MQTT publishers at the edge
	RateHz     int           // per-machine publish rate (live scenario)
	Records    int           // records to buffer (catchup scenario)
	Paths      int           // distinct signal paths (cardinality scenario)
	Duration   time.Duration // measurement window (ingest, live, footprint)
	ColcadPath string        // path to the colcad binary (footprint scenario)
	Storage    string        // operator note for the report ("emmc-eg300")
	WorkDir    string        // scratch dir; caller owns cleanup
}

// RunIngest measures the MQTT→engine→fsync path flat out: every machine
// publishes QoS-1 as fast as its PUBACKs come back for Duration. Each PUBLISH
// is one synced Pebble batch today, so ingest_msgs_per_sec IS the per-record
// fsync rate of the storage device, and disk_bytes_per_record ×
// write_amplification is the flash-endurance input.
func RunIngest(p Params) (*Report, error) {
	pair, err := StartPair(p.WorkDir, p.Machines)
	if err != nil {
		return nil, err
	}
	defer pair.Stop()

	r := NewReport("ingest", p.Storage, map[string]any{
		"machines": p.Machines, "duration": p.Duration.String(),
	})
	diskBefore := pair.Edge.Store.DiskMetrics()

	var (
		wg         sync.WaitGroup
		mu         sync.Mutex
		count      int
		payloadLen int
		lats       []float64
		clients    []pahomqtt.Client
	)
	stopAt := time.Now().Add(p.Duration)
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
			seq := 0
			for time.Now().Before(stopAt) {
				seq++
				payload, _ := json.Marshal(map[string]any{"v": float64(seq), "ts": time.Now().UnixMilli()})
				t0 := time.Now()
				tk := m.Publish(topic, 1, false, payload)
				if !tk.WaitTimeout(10*time.Second) || tk.Error() != nil {
					return // broker gone or stalled; the achieved count still gets reported
				}
				mu.Lock()
				count++
				payloadLen += len(payload)
				lats = append(lats, float64(time.Since(t0).Microseconds())/1000.0)
				mu.Unlock()
			}
		}(m, topic)
	}
	wg.Wait()
	for _, c := range clients {
		c.Disconnect(250)
	}

	if count == 0 {
		return nil, fmt.Errorf("ingest: no message was ever acknowledged")
	}
	diskAfter := pair.Edge.Store.DiskMetrics()
	walDelta := float64(diskAfter.WALBytesWritten - diskBefore.WALBytesWritten)
	levelDelta := float64(diskAfter.LevelBytesWritten - diskBefore.LevelBytesWritten)
	sort.Float64s(lats)
	r.Metrics["ingest_msgs_per_sec"] = float64(count) / p.Duration.Seconds()
	r.Metrics["ingest_p95_publish_ms"] = Percentile(lats, 95)
	r.Metrics["disk_bytes_per_record"] = (walDelta + levelDelta) / float64(count)
	r.Metrics["write_amplification"] = (walDelta + levelDelta) / float64(payloadLen)
	return r, nil
}
