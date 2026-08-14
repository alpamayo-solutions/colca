package bench

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

func TestPercentile(t *testing.T) {
	sorted := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	for _, tc := range []struct {
		p    float64
		want float64
	}{{0, 1}, {50, 5}, {95, 10}, {100, 10}} {
		if got := Percentile(sorted, tc.p); got != tc.want {
			t.Errorf("Percentile(%v) = %v, want %v", tc.p, got, tc.want)
		}
	}
	if got := Percentile(nil, 50); got != 0 {
		t.Errorf("Percentile(nil) = %v, want 0", got)
	}
}

func TestReportJSONAndTable(t *testing.T) {
	r := NewReport("ingest", "laptop-nvme", map[string]any{"machines": 4})
	r.Metrics["ingest_msgs_per_sec"] = 1234.5
	b, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	var back Report
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatal(err)
	}
	if back.Scenario != "ingest" || back.Host.Storage != "laptop-nvme" || back.Metrics["ingest_msgs_per_sec"] != 1234.5 {
		t.Fatalf("roundtrip lost data: %+v", back)
	}
	if back.Host.OS == "" || back.Host.NumCPU == 0 || back.StartedAt == "" {
		t.Fatalf("host/started not populated: %+v", back)
	}
	tab := r.Table()
	if !strings.Contains(tab, "ingest_msgs_per_sec") || !strings.Contains(tab, "1234.5") {
		t.Fatalf("table missing metric: %s", tab)
	}
}

func TestRSSBytesSelf(t *testing.T) {
	rss, err := RSSBytes(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	if rss < 1<<20 {
		t.Fatalf("own RSS %d bytes — implausibly small", rss)
	}
}
