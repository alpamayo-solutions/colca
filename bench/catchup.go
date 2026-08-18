package bench

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// RunCatchup measures how fast a parent absorbs a child's offline backlog:
// hub down → Records buffered at the edge → hub up → time until the hub's
// metrics stream contains everything. This is the "outage recovery" number
// that retention sizing divides outage duration by.
//
// Prefill goes through the edge's HTTP /publish (admin ingest) instead of
// paho: it exercises the same engine+fsync path without MQTT round-trip
// overhead, so the measurement isolates the DRAIN (uplink read → mTLS push →
// hub apply), not the fill.
func RunCatchup(p Params) (*Report, error) {
	pair, err := StartPair(p.WorkDir, 1)
	if err != nil {
		return nil, err
	}
	defer pair.Stop()

	r := NewReport("catchup", p.Storage, map[string]any{"records": p.Records})

	hubTarget := NextOffset(pair.Hub, "metrics") + uint64(p.Records)
	pair.StopHub()

	client := &http.Client{Timeout: 10 * time.Second, Transport: apiTransport()}
	for i := 0; i < p.Records; i++ {
		payload := map[string]any{
			"topic":   "colca/v1/_Metric/n-edge/m1/temp",
			"payload": map[string]any{"v": float64(i), "value": float64(i), "signal_id": "bench"},
		}
		body, _ := json.Marshal(payload)
		req, err := http.NewRequest("POST", "https://"+pair.Edge.APIAddr+"/publish", bytes.NewReader(body))
		if err != nil {
			return nil, fmt.Errorf("prefill publish %d: %w", i, err)
		}
		req.Header.Set("X-Colca-Token", BenchToken)
		resp, err := client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("prefill publish %d: %w", i, err)
		}
		resp.Body.Close()
		if resp.StatusCode >= 300 {
			return nil, fmt.Errorf("prefill publish %d: HTTP %d", i, resp.StatusCode)
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
