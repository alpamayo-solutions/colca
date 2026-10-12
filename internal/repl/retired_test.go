package repl

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/metrics"
	"github.com/alpamayo-solutions/colca/internal/store"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// A _ClockProgress marker written before the upgrade stays in the child's
// metrics stream until retention. The uplink does not push it; the samples
// around it rise and the cursor reaches the head. The parent would skip the
// marker too; counting what was sent shows the child kept it.
func TestTheUplinkLeavesRetiredRecordsHome(t *testing.T) {
	cs, ps, parentPub, cl, start := uplinkPairWithClient(t)
	pushes := watchPushes(cl)
	if _, _, err := cs.Append("metrics", []store.Record{
		{Topic: "colca/v1/_Metric/n-child/press/temp", Payload: []byte(`{"signal_id":"temp","value":1}`), TS: 1},
		{Topic: "colca/v1/_ClockProgress/n-child/historian", Payload: []byte(`{"run_id":"r","processed_at":1,"ready":true,"observed_at":1}`), TS: 2},
		{Topic: "colca/v1/_Metric/n-child/press/speed", Payload: []byte(`{"signal_id":"speed","value":2}`), TS: 3},
	}); err != nil {
		t.Fatal(err)
	}
	head := cs.NextOffset("metrics")
	start()
	waitFor(t, "the child's metrics uplink cursor to reach its head", 20*time.Second, func() bool {
		return cs.CursorGet(uns.UplinkCursor(parentPub), "metrics") == head
	})
	recs, _, err := ps.Read("metrics", 1, 100, nil)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, r := range recs {
		if p, perr := uns.Parse(r.Topic); perr == nil && p.NodeID == "n-child" {
			got = append(got, p.Contract)
		}
	}
	if len(got) != 2 || got[0] != "_Metric" || got[1] != "_Metric" {
		t.Fatalf("parent metrics from the child: %v, want the two samples only", got)
	}
	pushes.mu.Lock()
	defer pushes.mu.Unlock()
	sent := 0
	for _, p := range pushes.log {
		if p.stream == "metrics" {
			sent += p.records
		}
	}
	if sent != 2 {
		t.Fatalf("the child sent %d metrics records, want 2: the marker stays home", sent)
	}
}

// An older parent still hands down a _ClockDefinition. The child refuses it,
// applies the definition after it and moves its cursor past both, so the lane
// does not stall on a contract it no longer knows.
func TestARetiredDefinitionFromTheParentIsSkipped(t *testing.T) {
	dir := t.TempDir()
	cs := mustStore(t, filepath.Join(dir, "cdata"))
	_, ceng := nodeParts(t, cs, &config.Config{ULID: "n-child"}, nil, nil, nil)
	parent := mustIdentity(t, filepath.Join(dir, "p.key"))
	cl := mustClient(t, "127.0.0.1:1", parent.PublicHex(), mustIdentity(t, filepath.Join(dir, "c.key")))
	m := metrics.New(cs, config.Retention{}, nil)

	moved := applyDefinitions(cl, ceng, m, downResult{
		Definitions: []DownRec{
			{ParentOffset: 1, Topic: "colca/v1/_ClockDefinition/n-parent/factory", TS: 1,
				Payload: []byte(`{"id":"factory","run_id":"r","revision":1,"real_anchor":0,"factory_anchor":0,"rate":1}`)},
			{ParentOffset: 2, Topic: "colca/v1/_Group/n-parent/01HGRP-OPS", TS: 2,
				Payload: []byte(`{"id":"01HGRP-OPS","name":"Ops"}`)},
		},
		DefNext: 3,
	})
	if !moved {
		t.Fatal("the definitions cursor did not move")
	}
	if pos := cs.CursorGet(uns.DownlinkDefCursor(cl.ParentPub()), downlinkDefStream); pos != 3 {
		t.Fatalf("definitions cursor = %d, want 3: past the retired definition and the group", pos)
	}
	if entries := mustKVScan(t, cs, "01HGRP-OPS"); len(entries) != 1 {
		t.Fatalf("child KV = %+v, want the group after the retired definition", entries)
	}
	if entries := mustKVScan(t, cs, "factory"); len(entries) != 0 {
		t.Fatalf("child KV = %+v, want no clock definition", entries)
	}
}
