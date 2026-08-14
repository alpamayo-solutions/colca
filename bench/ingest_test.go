package bench

import (
	"testing"
	"time"
)

// TestRunIngestSmall runs the ingest scenario at smoke scale and checks the
// report is structurally sound — real numbers come from make bench-scenarios.
func TestRunIngestSmall(t *testing.T) {
	r, err := RunIngest(Params{Machines: 2, Duration: 1 * time.Second, WorkDir: t.TempDir(), Storage: "test"})
	if err != nil {
		t.Fatal(err)
	}
	if r.Scenario != "ingest" {
		t.Fatalf("scenario = %q", r.Scenario)
	}
	for _, k := range []string{"ingest_msgs_per_sec", "ingest_p95_publish_ms", "disk_bytes_per_record", "write_amplification"} {
		if r.Metrics[k] <= 0 {
			t.Fatalf("metric %s = %v, want > 0 (report: %+v)", k, r.Metrics[k], r.Metrics)
		}
	}
}
