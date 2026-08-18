package repl

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/identity"
	"github.com/alpamayo-solutions/colca/internal/store"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

const groupTopic = "colca/v1/_Group/n-parent/01HGRP-OPS"

// A definition crosses a hop untouched. No mount is inserted and none is
// stripped, because a definition has no position — what the parent holds and
// what the child holds are the same bytes (definition-stream design §2/§3).
func TestDefinitionCrossesAHopByteIdentical(t *testing.T) {
	dir := t.TempDir()
	parentID, _ := identity.Generate(filepath.Join(dir, "p.key"))
	childID, _ := identity.Generate(filepath.Join(dir, "c.key"))

	ps, _ := store.Open(filepath.Join(dir, "pdata"))
	defer ps.Close()
	pcfg := &config.Config{ULID: "n-parent", Repl: config.Endpoint{Addr: "127.0.0.1:0"}}
	preg, peng := nodeParts(t, ps, pcfg, nil, nil, nil, childSpec{"n-child", childID.PublicHex(), "child1"})
	srv, addr := startServer(t, pcfg, peng, parentID, preg)
	defer srv.Stop()

	if _, err := peng.IngestAdmin(groupTopic,
		[]byte(`{"id":"01HGRP-OPS","name":"Ops","grants":["read:01HLINE1/#"]}`)); err != nil {
		t.Fatal(err)
	}

	cl := mustClient(t, addr, parentID.PublicHex(), childID)
	defs, next, err := cl.DownlinkDefinitions(1, 10, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(defs) != 1 {
		t.Fatalf("definitions = %+v, want the one authored at the parent", defs)
	}
	if defs[0].Topic != groupTopic {
		t.Fatalf("topic = %q, want %q — a definition has no position, so nothing rewrites it",
			defs[0].Topic, groupTopic)
	}
	if next <= 1 {
		t.Fatalf("def_next = %d, want it to have advanced", next)
	}
}

// Nothing addresses a definition at one child: every child below the author
// gets every definition (design §10.1, decided: broadcast).
func TestEveryChildReceivesEveryDefinition(t *testing.T) {
	dir := t.TempDir()
	parentID := mustIdentity(t, filepath.Join(dir, "p.key"))
	child1ID := mustIdentity(t, filepath.Join(dir, "c1.key"))
	child2ID := mustIdentity(t, filepath.Join(dir, "c2.key"))

	ps := mustStore(t, filepath.Join(dir, "pdata"))
	pcfg := &config.Config{ULID: "n-parent", Repl: config.Endpoint{Addr: "127.0.0.1:0"}}
	preg, peng := nodeParts(t, ps, pcfg, nil, nil, nil,
		childSpec{"n-child1", child1ID.PublicHex(), "child1"},
		childSpec{"n-child2", child2ID.PublicHex(), "child2"})
	srv, addr := startServer(t, pcfg, peng, parentID, preg)
	defer srv.Stop()

	mustIngestAdmin(t, peng, groupTopic, `{"id":"01HGRP-OPS","name":"Ops"}`)

	for _, id := range []*identity.Identity{child1ID, child2ID} {
		cl := mustClient(t, addr, parentID.PublicHex(), id)
		defs, _, err := cl.DownlinkDefinitions(1, 10, 5*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if len(defs) != 1 || defs[0].Topic != groupTopic {
			t.Fatalf("child received %+v, want the definition — it is addressed at no child in particular", defs)
		}
	}
}

// The two streams advance independently: a child caught up on commands may
// still be behind on definitions, and neither cursor may drag the other.
func TestDefinitionCursorIsIndependentOfTheCommandCursor(t *testing.T) {
	dir := t.TempDir()
	parentID := mustIdentity(t, filepath.Join(dir, "p.key"))
	childID := mustIdentity(t, filepath.Join(dir, "c.key"))

	ps := mustStore(t, filepath.Join(dir, "pdata"))
	pcfg := &config.Config{ULID: "n-parent", Repl: config.Endpoint{Addr: "127.0.0.1:0"}}
	preg, peng := nodeParts(t, ps, pcfg, nil, nil, nil, childSpec{"n-child", childID.PublicHex(), "child1"})
	srv, addr := startServer(t, pcfg, peng, parentID, preg)
	defer srv.Stop()

	mustIngestAdmin(t, peng, groupTopic, `{"id":"01HGRP-OPS","name":"Ops"}`)
	mustIngestAdmin(t, peng, "colca/v1/_CmdParam/m1/child1/m1/go",
		`{"correlation_id":"c1","expires_at":99999999999}`)

	cl := mustClient(t, addr, parentID.PublicHex(), childID)
	// Read the definition, leave the command alone. Two polls: the cursor is a
	// DELIVERY FLOOR — it records the position the child reports, so it only
	// moves once the child comes back having consumed the first batch. Same
	// semantics as the command cursor, deliberately.
	_, next, err := cl.DownlinkDefinitions(1, 10, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := cl.DownlinkDefinitions(next, 10, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	defCursor := ps.CursorGet(uns.DownlinkDefCursorPrefix+"n-child", "definitions")
	cmdCursor := ps.CursorGet(uns.DownlinkCursorPrefix+"n-child", "commands")
	if defCursor <= 1 {
		t.Fatalf("definition cursor = %d, want it advanced by the read", defCursor)
	}
	if cmdCursor > 1 {
		t.Fatalf("command cursor = %d — reading definitions must not move it", cmdCursor)
	}
}

// Definitions descend. A child that authors one must never push it upward: the
// uplink carries metrics, entities and acks, and a leaf that could send policy
// up would be authoring for the whole tree (design §4).
func TestTheUplinkNeverCarriesDefinitions(t *testing.T) {
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
	// The child authors a definition of its own, plus a metric that MUST rise.
	mustIngestAdmin(t, ceng, "colca/v1/_Group/n-child/01HGRP-LOCAL", `{"id":"01HGRP-LOCAL","name":"Local"}`)
	mustIngestAdmin(t, ceng, "colca/v1/_Metric/n-child/temp", `{"v":1}`)

	cl := mustClient(t, addr, parentID.PublicHex(), childID)
	stop, done := make(chan struct{}), make(chan struct{})
	go func() { defer close(done); RunUplink(cl, ceng, nil, stop) }()
	waitFor(t, "the child's metric to reach the parent", 5*time.Second, func() bool {
		return ps.NextOffset("metrics") > 1
	})
	close(stop)
	waitForClosed(t, "the uplink to stop", done, 5*time.Second)

	if got := ps.NextOffset("definitions"); got != 1 {
		t.Fatalf("the parent's definitions stream is at %d — a child pushed definitions upward, "+
			"which would let a leaf author policy for the tree", got)
	}
}

// A definition that arrives is APPLIED, not executed: it lands in the store, in
// the KV view, and retained on the local bus, so a consumer that subscribes
// later still sees it.
func TestAnArrivingDefinitionIsAppliedAsRetainedState(t *testing.T) {
	dir := t.TempDir()
	parentID := mustIdentity(t, filepath.Join(dir, "p.key"))
	childID := mustIdentity(t, filepath.Join(dir, "c.key"))

	ps := mustStore(t, filepath.Join(dir, "pdata"))
	pcfg := &config.Config{ULID: "n-parent", Repl: config.Endpoint{Addr: "127.0.0.1:0"}}
	preg, peng := nodeParts(t, ps, pcfg, nil, nil, nil, childSpec{"n-child", childID.PublicHex(), "child1"})
	srv, addr := startServer(t, pcfg, peng, parentID, preg)
	defer srv.Stop()
	mustIngestAdmin(t, peng, groupTopic, `{"id":"01HGRP-OPS","name":"Ops"}`)

	cs := mustStore(t, filepath.Join(dir, "cdata"))
	ccfg := &config.Config{ULID: "n-child"}
	type delivery struct {
		topic  string
		retain bool
	}
	delivered := make(chan delivery, 8)
	_, ceng := nodeParts(t, cs, ccfg, func(topic string, payload []byte, retain bool) {
		delivered <- delivery{topic, retain}
	}, nil, nil)

	cl := mustClient(t, addr, parentID.PublicHex(), childID)
	stop, done := make(chan struct{}), make(chan struct{})
	go func() { defer close(done); RunDownlink(cl, ceng, nil, stop) }()
	t.Cleanup(func() { close(stop); waitForClosed(t, "RunDownlink to stop", done, 5*time.Second) })

	waitFor(t, "the definition to be applied at the child", 5*time.Second, func() bool {
		return len(cs.KVScan("01HGRP-OPS")) == 1
	})
	// Retained on the bus: a consumer subscribing later still learns of it.
	deadline := time.After(2 * time.Second)
	for {
		select {
		case d := <-delivered:
			if d.topic == groupTopic {
				if !d.retain {
					t.Fatal("a definition must be delivered retained — it is state, not an event")
				}
				return
			}
		case <-deadline:
			t.Fatal("the definition never reached the child's local bus")
		}
	}
}
