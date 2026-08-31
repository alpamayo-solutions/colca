package bench

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	pahomqtt "github.com/eclipse/paho.mqtt.golang"
)

// RunCardinality seeds Paths distinct signal paths and measures what a big
// namespace costs: full KV scan time (the /kv endpoint a UI would hit),
// retained-set replay time for a fresh subscriber (the bus's "current state on
// connect" contract), and process RSS growth. Replication runs during seeding,
// so the hub-side numbers include the mount-rewritten copies.
func RunCardinality(p Params) (*Report, error) {
	pair, err := StartPair(p.WorkDir, 1)
	if err != nil {
		return nil, err
	}
	defer pair.Stop()

	r := NewReport("cardinality", p.Storage, map[string]any{"paths": p.Paths})
	rssBefore, err := RSSBytes(os.Getpid())
	if err != nil {
		return nil, err
	}

	client := &http.Client{Timeout: 10 * time.Second, Transport: apiTransport()}
	seedStart := time.Now()
	for i := 0; i < p.Paths; i++ {
		body, _ := json.Marshal(map[string]any{
			"topic":   fmt.Sprintf("colca/v1/_Metric/n-edge/line%d/sig%d", i/100, i%100),
			"payload": map[string]any{"v": float64(i), "value": float64(i), "signal_id": "bench", "timestamp": float64(time.Now().UnixNano()) / 1e9},
		})
		if err := postAdmin(client, pair.Edge.APIAddr, "/publish", body); err != nil {
			return nil, fmt.Errorf("seed %d: %w", i, err)
		}
	}
	r.Metrics["cardinality_seed_seconds"] = time.Since(seedStart).Seconds()

	// Full KV scan at the edge, timed over 5 runs, best run reported (cold
	// caches are a separate scenario — this is steady-state read cost).
	var bestScan time.Duration
	for run := 0; run < 5; run++ {
		t0 := time.Now()
		req, _ := http.NewRequest("GET", "https://"+pair.Edge.APIAddr+"/kv?prefix=", nil)
		req.Header.Set("X-Colca-Token", BenchToken)
		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		var out struct {
			Entries []json.RawMessage `json:"entries"`
		}
		if err := json.Unmarshal(raw, &out); err != nil {
			return nil, fmt.Errorf("kv scan response: %w", err)
		}
		if len(out.Entries) < p.Paths {
			return nil, fmt.Errorf("kv scan returned %d entries, want >= %d", len(out.Entries), p.Paths)
		}
		d := time.Since(t0)
		if bestScan == 0 || d < bestScan {
			bestScan = d
		}
	}
	r.Metrics["cardinality_kv_scan_ms"] = float64(bestScan.Microseconds()) / 1000.0

	// Retained replay on the HUB: wait until replication has carried every
	// path across, then time a fresh subscriber receiving the full retained set.
	// Each wait phase gets its own deadline so a slow replication phase can't
	// silently eat the budget the replay-wait phase needs, which would surface
	// as a misleading "retained replay delivered X of Y" error.
	replicationDeadline := time.Now().Add(5 * time.Minute)
	for NextOffset(pair.Hub, "metrics") < uint64(p.Paths)+1 {
		if time.Now().After(replicationDeadline) {
			return nil, fmt.Errorf("hub never received all %d paths (offset %d)", p.Paths, NextOffset(pair.Hub, "metrics"))
		}
		time.Sleep(20 * time.Millisecond)
	}
	obs, err := pair.Observer("bench-card-obs")
	if err != nil {
		return nil, err
	}
	defer obs.Disconnect(250)
	var got atomic.Int64
	t0 := time.Now()
	tk := obs.Subscribe("colca/v1/_Metric/#", 1, func(_ pahomqtt.Client, m pahomqtt.Message) {
		if m.Retained() {
			got.Add(1)
		}
	})
	if !tk.WaitTimeout(10*time.Second) || tk.Error() != nil {
		return nil, fmt.Errorf("subscribe: %w", tk.Error())
	}
	replayDeadline := time.Now().Add(5 * time.Minute)
	for got.Load() < int64(p.Paths) {
		if time.Now().After(replayDeadline) {
			return nil, fmt.Errorf("retained replay delivered %d of %d", got.Load(), p.Paths)
		}
		time.Sleep(10 * time.Millisecond)
	}
	r.Metrics["cardinality_retained_replay_seconds"] = time.Since(t0).Seconds()

	rssAfter, err := RSSBytes(os.Getpid())
	if err != nil {
		return nil, err
	}
	r.Metrics["cardinality_paths"] = float64(p.Paths)
	var rssDelta uint64
	if rssAfter > rssBefore {
		rssDelta = rssAfter - rssBefore
	}
	r.Metrics["cardinality_rss_delta_mb"] = float64(rssDelta) / (1 << 20)
	return r, nil
}
