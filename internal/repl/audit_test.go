package repl

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/store"
)

func TestAuditStreamRisesWithMountAndNeverCarriesDefinitions(t *testing.T) {
	dir := t.TempDir()
	parentID := mustIdentity(t, filepath.Join(dir, "p.key"))
	childID := mustIdentity(t, filepath.Join(dir, "c.key"))

	ps := mustStore(t, filepath.Join(dir, "pdata"))
	pcfg := &config.Config{ULID: "n-parent", Repl: config.Endpoint{Addr: "127.0.0.1:0"}}
	preg, peng := nodeParts(t, ps, pcfg, nil, nil, nil, childSpec{"n-child", childID.PublicHex(), "child1"})
	srv, addr := startServer(t, pcfg, peng, parentID, preg)
	defer srv.Stop()

	cs := mustStore(t, filepath.Join(dir, "cdata"))
	ccfg := &config.Config{ULID: "n-child"}
	_, ceng := nodeParts(t, cs, ccfg, nil, nil, nil)
	payload := []byte(`{"event_id":"evt-1","source":"api","action":"authorize","outcome":"denied","actor_kind":"human","occurred_at":1}`)
	if _, _, err := cs.Append("audit", []store.Record{{
		Topic: "colca/v1/_AuditEvent/n-child/_colca/audit/evt-1", Payload: payload, TS: 1, WrittenBy: "api",
	}}); err != nil {
		t.Fatal(err)
	}

	cl := mustClient(t, addr, parentID.PublicHex(), childID)
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() { defer close(done); RunUplink(cl, ceng, nil, nil, stop) }()
	waitFor(t, "audit record to reach parent", 5*time.Second, func() bool {
		return ps.NextOffset("audit") == 2
	})
	close(stop)
	waitForClosed(t, "audit uplink stop", done, 5*time.Second)

	records, _, err := ps.Read("audit", 1, 10, nil)
	if err != nil || len(records) != 1 {
		t.Fatalf("parent audit records = %+v, err=%v", records, err)
	}
	wantTopic := "colca/v1/_AuditEvent/n-child/child1/_colca/audit/evt-1"
	if records[0].Topic != wantTopic || records[0].WrittenBy != "api" {
		t.Fatalf("replicated audit = %+v, want topic %s and writer api", records[0], wantTopic)
	}

	bad := []store.ReplRecord{{
		ChildOffset: 2,
		Topic:       "colca/v1/_Group/n-child/01HGRP-LOCAL",
		Payload:     []byte(`{"id":"01HGRP-LOCAL","name":"Local"}`),
		TS:          2,
	}}
	if _, err := cl.Replicate("audit", bad); err == nil {
		t.Fatal("a definition may not travel upward disguised as an audit record")
	}
	if ps.NextOffset("audit") != 3 {
		t.Fatal("rejected wrong-direction record did not append exactly one audit denial")
	}
	records, _, err = ps.Read("audit", 2, 10, nil)
	if err != nil || len(records) != 1 || !strings.Contains(string(records[0].Payload), `"reason_code":"direction_denied"`) {
		t.Fatalf("direction denial audit = %+v, err=%v", records, err)
	}
}
