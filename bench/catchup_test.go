package bench

import (
	"testing"
)

func TestRunCatchupSmall(t *testing.T) {
	r, err := RunCatchup(Params{Records: 500, WorkDir: t.TempDir(), Storage: "test"})
	if err != nil {
		t.Fatal(err)
	}
	if r.Metrics["catchup_total_records"] != 500 {
		t.Fatalf("drained %v records, want 500", r.Metrics["catchup_total_records"])
	}
	if r.Metrics["catchup_recs_per_sec"] <= 0 {
		t.Fatalf("rate not measured: %+v", r.Metrics)
	}
}
