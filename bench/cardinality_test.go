package bench

import "testing"

func TestRunCardinalitySmall(t *testing.T) {
	r, err := RunCardinality(Params{Paths: 300, WorkDir: t.TempDir(), Storage: "test"})
	if err != nil {
		t.Fatal(err)
	}
	if r.Metrics["cardinality_paths"] != 300 {
		t.Fatalf("paths = %v, want 300", r.Metrics["cardinality_paths"])
	}
	if r.Metrics["cardinality_kv_scan_ms"] <= 0 || r.Metrics["cardinality_retained_replay_seconds"] <= 0 {
		t.Fatalf("scan/replay not measured: %+v", r.Metrics)
	}
}
