package bench

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// RunCatchup measures how fast a parent absorbs a child's offline backlog: the
// hub goes down, records buffer at the edge, the hub comes back, and the clock
// runs until its metrics stream holds everything. Prefill uses the edge's HTTP
// /publish, so the measurement covers the drain rather than MQTT overhead.
func RunCatchup(p Params) (*Report, error) {
	pair, err := StartPair(p.WorkDir, 1)
	if err != nil {
		return nil, err
	}
	defer pair.Stop()

	r := NewReport("catchup", p.Storage, map[string]any{"records": p.Records})

	hubTarget := NextOffset(pair.Hub, "metrics") + uint64(p.Records) //nolint:gosec // Records is a positive flag value
	pair.StopHub()

	client := &http.Client{Timeout: 10 * time.Second, Transport: apiTransport()}
	for i := 0; i < p.Records; i++ {
		payload := map[string]any{
			"topic":   uns.Prefix() + "_Metric/n-edge/m1/temp",
			"payload": map[string]any{"v": float64(i), "value": float64(i), "signal_id": "bench", "timestamp": float64(time.Now().UnixNano()) / 1e9},
		}
		body, _ := json.Marshal(payload)
		if err := postAdmin(client, pair.Edge.APIAddr, "/publish", body); err != nil {
			return nil, fmt.Errorf("prefill publish %d: %w", i, err)
		}
	}

	t0 := time.Now()
	if err := pair.StartHub(); err != nil {
		return nil, err
	}
	deadline := time.Now().Add(10 * time.Minute)
	for NextOffset(pair.Hub, "metrics") < hubTarget {
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("catchup: hub stuck at offset %d, want %d", NextOffset(pair.Hub, "metrics"), hubTarget)
		}
		time.Sleep(20 * time.Millisecond)
	}
	drain := time.Since(t0)

	r.Metrics["catchup_total_records"] = float64(p.Records)
	r.Metrics["catchup_drain_seconds"] = drain.Seconds()
	r.Metrics["catchup_recs_per_sec"] = float64(p.Records) / drain.Seconds()
	return r, nil
}
