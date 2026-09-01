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
	go func() { defer close(done); RunUplink(cl, ceng, nil, nil, stop) }()
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
		return len(mustKVScan(t, cs, "01HGRP-OPS")) == 1
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

// A definition this node's contracts cannot apply is skipped, and the ones
// behind it still arrive.
//
// A hub upgraded before its edges authors a definition contract the older
// bundle downstream does not know — this branch added _DataModel and PAT
// records exactly that way. The child classifies it as unknown and refuses
// it; re-offering it forever parked the channel there, so every later
// definition (a new group, a type, a revoked group's tombstone) stopped
// arriving at that node until someone upgraded it.
func TestADefinitionThisNodeCannotApplyDoesNotBlockTheOnesBehindIt(t *testing.T) {
	dir := t.TempDir()
	parentID := mustIdentity(t, filepath.Join(dir, "p.key"))
	childID := mustIdentity(t, filepath.Join(dir, "c.key"))

	ps := mustStore(t, filepath.Join(dir, "pdata"))
	pcfg := &config.Config{ULID: "n-parent", Repl: config.Endpoint{Addr: "127.0.0.1:0"}}
	preg, peng := nodeParts(t, ps, pcfg, nil, nil, nil, childSpec{"n-child", childID.PublicHex(), "child1"})
	srv, addr := startServer(t, pcfg, peng, parentID, preg)
	defer srv.Stop()

	// The newer hub's definition, written straight into the stream: its
	// contract is one this binary has no class for, so no door here would
	// author it either.
	if _, _, err := ps.Append("definitions", []store.Record{
		{Topic: "colca/v1/_FutureThing/n-parent/01HFUT", Payload: []byte(`{"id":"01HFUT"}`), TS: 1},
	}); err != nil {
		t.Fatal(err)
	}
	mustIngestAdmin(t, peng, groupTopic, `{"id":"01HGRP-OPS","name":"Ops"}`)

	cs := mustStore(t, filepath.Join(dir, "cdata"))
	_, ceng := nodeParts(t, cs, &config.Config{ULID: "n-child"}, nil, nil, nil)
	cl := mustClient(t, addr, parentID.PublicHex(), childID)
	stop, done := make(chan struct{}), make(chan struct{})
	go func() { defer close(done); RunDownlink(cl, ceng, nil, stop) }()
	t.Cleanup(func() { close(stop); waitForClosed(t, "RunDownlink to stop", done, 5*time.Second) })

	waitFor(t, "the group behind the unapplicable definition to arrive", 5*time.Second, func() bool {
		return len(mustKVScan(t, cs, "01HGRP-OPS")) == 1
	})
	waitFor(t, "the definitions cursor to advance past both records", 5*time.Second, func() bool {
		return cs.CursorGet(uns.DownlinkDefCursor(cl.ParentPub()), downlinkDefStream) == ps.NextOffset("definitions")
	})
	// The unknown one was skipped, not applied: nothing of it is stored here.
	if recs, _, _ := cs.Read("definitions", 1, 10, nil); len(recs) != 1 || recs[0].Topic != groupTopic {
		t.Fatalf("child definitions = %+v, want only the group it understands", recs)
	}
}

// After compaction has emptied the tail of the definitions stream, the poll
// reports the stream's HEAD — not the position the child already holds.
//
// The stream is compacted, not pruned, so a hole in it is not a gap: what
// remains IS the current definition set and a child reading from below the
// hole is caught up. Answering with its own position back left it nothing to
// ack while the poll's wake condition still said a definition was waiting, so
// the poll returned instantly and the child re-polled at once — both nodes
// spinning at the rate limit until someone authored a new definition.
func TestDefinitionsPollReportsTheHeadAfterCompaction(t *testing.T) {
	f := newParentFixture(t)
	// A group and its retraction, both read by another child — which is what
	// lets compaction remove the tombstone too.
	if _, _, err := f.ps.Append("definitions", []store.Record{
		{Topic: groupTopic, Payload: []byte(`{"id":"01HGRP-OPS","name":"Ops"}`), TS: 1,
			KVPath: "01HGRP-OPS", KVNode: "n-parent"},
		{Topic: groupTopic, Payload: nil, TS: 2, KVPath: "01HGRP-OPS", KVNode: "n-parent", Delete: true},
	}); err != nil {
		t.Fatal(err)
	}
	f.ps.CursorAck(uns.DownlinkDefCursorPrefix+"n-other", "definitions", 3)
	if _, err := f.ps.Compact("definitions"); err != nil {
		t.Fatal(err)
	}
	head := f.ps.NextOffset("definitions")

	defs, next, err := f.cl.DownlinkDefinitions(1, 10, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(defs) != 0 {
		t.Fatalf("definitions = %+v, want none: compaction removed the group and its tombstone", defs)
	}
	if next != head {
		t.Fatalf("def_next = %d, want the head %d — with no progress to ack the child re-polls immediately, forever", next, head)
	}

	// The denominator: from that same position a definition authored AFTER
	// the hole is still delivered, so the answer above means "caught up",
	// not "this poll is broken".
	mustIngestAdmin(t, f.peng, groupTopic, `{"id":"01HGRP-OPS","name":"Ops again"}`)
	defs, next, err = f.cl.DownlinkDefinitions(head, 10, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(defs) != 1 || defs[0].Topic != groupTopic {
		t.Fatalf("definitions = %+v, want the newly authored group", defs)
	}
	if next != f.ps.NextOffset("definitions") {
		t.Fatalf("def_next = %d, want the head %d", next, f.ps.NextOffset("definitions"))
	}
}

// A child may not push onto the definitions stream, whatever contract it puts
// in the records. TestTheUplinkNeverCarriesDefinitions pins the pusher's half
// of that rule; this pins the door's, which is the half that has to hold
// against a child that is buggy or hostile rather than merely well-behaved.
//
// The direction check used to run only for a contract this node's bundle
// declares, so an unknown one landed on whichever stream the request named.
// One such record on `definitions` was then broadcast to every child of this
// node, each of which rejected it as "not a definition" and stopped advancing
// its definitions cursor — no group, PAT or type reached any of them again.
func TestChildCannotReplicateOntoTheDefinitionsStream(t *testing.T) {
	f := newParentFixture(t)
	before := f.ps.NextOffset("definitions")

	// The denominator: this child CAN replicate, on a stream that rises.
	if _, err := f.cl.Replicate("metrics", []store.ReplRecord{
		{ChildOffset: 1, Topic: "colca/v1/_Metric/n-child/t", Payload: []byte(`{"v":1}`), TS: 1},
	}); err != nil {
		t.Fatalf("the fixture cannot replicate at all: %v", err)
	}

	for _, topic := range []string{
		"colca/v1/_Bogus/n-child/whatever", // unknown here: used to bypass the check
		groupTopic,                       // known, and a definition: flows down, never up
	} {
		if _, err := f.cl.Replicate("definitions", []store.ReplRecord{
			{ChildOffset: 2, Topic: topic, Payload: []byte(`{"x":1}`), TS: 2},
		}); err == nil {
			t.Fatalf("%s was accepted onto the definitions stream", topic)
		}
	}
	if _, err := f.cl.Replicate("not-a-stream", []store.ReplRecord{
		{ChildOffset: 3, Topic: "colca/v1/_Metric/n-child/t", Payload: []byte(`{"v":1}`), TS: 3},
	}); err == nil {
		t.Fatal("a record was accepted onto a stream no class ever routes to")
	}
	if got := f.ps.NextOffset("definitions"); got != before {
		t.Fatalf("definitions stream grew from %d to %d — a child wrote policy for the subtree", before, got)
	}
}
