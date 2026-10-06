package engine

import (
	"testing"

	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/store"
)

func TestOnlyChildMetricsLeaveTheRetainedSet(t *testing.T) {
	for _, keep := range []bool{false, true} {
		s, err := store.Open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		e := New(s, &config.Config{ULID: "n-hub", Bus: config.Bus{RetainChildMetrics: keep}}, testIDs(), nil, nil, nil)
		for topic, want := range map[string]bool{
			"colca/v1/_Metric/n-hub/hub/svc/load":            true,  // the node's own metric
			"colca/v1/_Metric/n-edge1/site1/edge1/m1/temp":   keep,  // a child's metric
			"colca/v1/_ClockProgress/n-edge1/site1/edge1/c":  true,  // data, but not a metric
			"colca/v1/_Signal/n-edge1/site1/edge1/m1/temp":   true,  // a child's entity
			"colca/v1/_CmdParam/n-edge1/site1/edge1/m1/stop": false, // an event
		} {
			if got := e.RetainOnBus(topic); got != want {
				t.Errorf("retain_child_metrics=%v: %s retained %v, want %v", keep, topic, got, want)
			}
		}
		s.Close()
	}
}
