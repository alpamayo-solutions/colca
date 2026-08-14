package bench

import (
	"testing"
	"time"
)

func TestRunLiveSmall(t *testing.T) {
	r, err := RunLive(Params{Machines: 2, RateHz: 20, Duration: 2 * time.Second, WorkDir: t.TempDir(), Storage: "test"})
	if err != nil {
		t.Fatal(err)
	}
	if r.Metrics["live_received_total"] < 10 {
		t.Fatalf("only %v messages measured — pipeline broken", r.Metrics["live_received_total"])
	}
	if r.Metrics["live_p50_ms"] <= 0 || r.Metrics["live_p99_ms"] < r.Metrics["live_p50_ms"] {
		t.Fatalf("implausible latency distribution: %+v", r.Metrics)
	}
}
