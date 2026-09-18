package repl

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/engine"
	"github.com/alpamayo-solutions/colca/internal/metrics"
	"github.com/alpamayo-solutions/colca/internal/metrics/metricstest"
	"github.com/alpamayo-solutions/colca/internal/store"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

func TestUplinkPolicyKeepsQueuedSamplesAndNeverBackfillsLocalHistory(t *testing.T) {
	f := newParentFixture(t)
	dir := filepath.Join(t.TempDir(), "child")
	cs, err := store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{ULID: "n-child"}
	_, eng := nodeParts(t, cs, cfg, nil, nil, nil)
	setPolicy := func(policy string) {
		t.Helper()
		_, err := eng.IngestAdmin("colca/v1/_Signal/n-child/temp", []byte(fmt.Sprintf(`{"id":"s1","name":"Temp","replication_policy":%q}`, policy)))
		if err != nil {
			t.Fatal(err)
		}
	}
	sample := func(value int) {
		t.Helper()
		_, err := eng.IngestAdmin("colca/v1/_Metric/n-child/temp", []byte(fmt.Sprintf(`{"signal_id":"s1","v":%d,"timestamp":1}`, value)))
		if err != nil {
			t.Fatal(err)
		}
	}
	// No uplink runs yet: this is a never-connected source's backlog.
	setPolicy("replicate_to_parents")
	sample(11)
	setPolicy("source_local_only")
	for i := 0; i < 450; i++ {
		sample(99)
	} // Several wholly local physical pages.
	setPolicy("replicate_to_parents")
	sample(22)
	setPolicy("source_local_only")
	sample(99)
	if err := cs.Close(); err != nil {
		t.Fatal(err)
	}
	cs, err = store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer cs.Close()
	_, eng = nodeParts(t, cs, cfg, nil, nil, nil)
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() { defer close(done); RunUplink(f.cl, eng, nil, nil, stop) }()
	defer func() { close(stop); waitForClosed(t, "uplink", done, 5*time.Second) }()
	waitFor(t, "all physical offsets acknowledged", 5*time.Second, func() bool { return cs.CursorGet(uns.UplinkCursor(f.pid.PublicHex()), "metrics") == 454 })
	got, _, err := f.ps.Read("metrics", 1, 100, nil)
	if err != nil || len(got) != 2 {
		t.Fatalf("hub metrics: %+v %v", got, err)
	}
	for i, want := range []int{11, 22} {
		if string(got[i].Payload) != fmt.Sprintf(`{"signal_id":"s1","v":%d,"timestamp":1}`, want) {
			t.Fatalf("sample: %+v", got[i])
		}
	}
	if cs.NextOffset("metrics") != 454 {
		t.Fatal("local history lost")
	}
	// Parent forwards the received records normally, independent of the source's
	// latest descriptor (which now says local-only).
	grand := newParentFixture(t)
	grandStop := make(chan struct{})
	grandDone := make(chan struct{})
	go func() { defer close(grandDone); RunUplink(grand.cl, f.peng, nil, nil, grandStop) }()
	defer func() { close(grandStop); waitForClosed(t, "parent uplink", grandDone, 5*time.Second) }()
	waitFor(t, "eligible samples at grandparent", 5*time.Second, func() bool { return grand.ps.NextOffset("metrics") == 3 })

}

func TestMetricSkipProtocolPreservesLossDetectionAndRejectsMalformedRanges(t *testing.T) {
	f := newParentFixture(t)
	// Wire validation, no payload or topic can be hidden inside a skip entry.
	for _, batch := range [][]store.ReplRecord{
		{{SkipFrom: 3, ChildOffset: 2}},
		{{SkipFrom: 1, ChildOffset: 2, Topic: "colca/v1/_Metric/n-child/temp"}},
		{{SkipFrom: 1, ChildOffset: 2, Payload: []byte("secret")}},
		{{ChildOffset: 2, Topic: "colca/v1/_Metric/n-child/temp"}, {SkipFrom: 2, ChildOffset: 4}},
	} {
		if _, err := f.cl.Replicate("metrics", batch); err == nil {
			t.Fatalf("accepted %+v", batch)
		}
	}
	if _, err := f.cl.Replicate("entities", []store.ReplRecord{{SkipFrom: 1, ChildOffset: 2}}); err == nil {
		t.Fatal("accepted skip on entities")
	}
	if _, err := f.cl.Replicate("metrics", []store.ReplRecord{{SkipFrom: 1, ChildOffset: 500}}); err != nil {
		t.Fatal(err)
	}
	if f.ps.NextOffset("metrics") != 1 || f.ps.HWMGet("n-child", "metrics") != 500 {
		t.Fatal("skip became data or lost progress")
	}
	// Exercise engine loss reporting with its own metrics registry.
	st := mustStore(t, filepath.Join(t.TempDir(), "loss"))
	m := metrics.New(st, config.Retention{}, nil)
	e := engine.New(st, &config.Config{ULID: "root"}, nil, nil, m, nil)
	batch := []store.ReplRecord{{SkipFrom: 1, ChildOffset: 200}, {ChildOffset: 201, Topic: "colca/v1/_Metric/n-child/temp", Payload: []byte(`{"value":1}`)}}
	if _, _, err := e.IngestReplicated("child", "metrics", batch); err != nil {
		t.Fatal(err)
	}

	// Missing 202..209 is real loss, even though 210..220 was intentionally skipped.
	if _, _, err := e.IngestReplicated("child", "metrics", []store.ReplRecord{{SkipFrom: 210, ChildOffset: 220}}); err != nil {
		t.Fatal(err)
	}
	if v := metricstest.Value(t, m, `colca_repl_gap_applied_total{child="child",stream="metrics"}`); v != 1 {
		t.Fatalf("real gap hidden: %v", v)
	}
}
