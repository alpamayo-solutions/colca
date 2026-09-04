package repl

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/alpamayo-solutions/colca/internal/clock"
	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/engine"
	"github.com/alpamayo-solutions/colca/internal/identity"
	"github.com/alpamayo-solutions/colca/internal/metrics"
	"github.com/alpamayo-solutions/colca/internal/registry"
	"github.com/alpamayo-solutions/colca/internal/store"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

func TestReplicateAndDownlinkOverMTLS(t *testing.T) {
	dir := t.TempDir()
	parentID, _ := identity.Generate(filepath.Join(dir, "p.key"))
	childID, _ := identity.Generate(filepath.Join(dir, "c.key"))

	ps, _ := store.Open(filepath.Join(dir, "pdata"))
	defer ps.Close()
	pcfg := &config.Config{ULID: "n-parent", Repl: config.Endpoint{Addr: "127.0.0.1:0"}}
	preg, peng := nodeParts(t, ps, pcfg, nil, nil, nil, childSpec{"n-child", childID.PublicHex(), "child1"})
	srv, err := NewServer(pcfg, peng, parentID, preg, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	addr, err := srv.Start()
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Stop()

	cl, err := NewClient("https://"+addr, parentID.PublicHex(), childID)
	if err != nil {
		t.Fatal(err)
	}

	// child pushes two metric records (child-local topics, child offsets 1,2)
	recs := []store.ReplRecord{
		{ChildOffset: 1, OriginOffset: 41, Topic: "colca/v1/_Metric/m1/m1/temp", Payload: []byte(`{"v":1}`), TS: 1,
			WrittenBy: "svc-connector", ActorID: "user-anna", ActorLabel: "anna@example.com", ActorKind: "human",
			ActorGroups: []string{"operators"}},
		{ChildOffset: 2, OriginOffset: 42, Topic: "colca/v1/_Metric/m1/m1/temp", Payload: []byte(`{"v":2}`), TS: 2},
	}
	hwm, err := cl.Replicate("metrics", recs)
	if err != nil {
		t.Fatal(err)
	}
	if hwm != 2 {
		t.Fatalf("hwm %d", hwm)
	}

	stored, _, _ := ps.Read("metrics", 1, 10, nil)
	if len(stored) != 2 || stored[0].Topic != "colca/v1/_Metric/m1/child1/m1/temp" {
		t.Fatalf("mount-insert on replicate failed: %+v", stored)
	}
	if stored[0].WrittenBy != "svc-connector" || stored[0].ActorID != "user-anna" ||
		stored[0].ActorLabel != "anna@example.com" || stored[0].ActorKind != "human" ||
		len(stored[0].ActorGroups) != 1 || stored[0].ActorGroups[0] != "operators" {
		t.Fatalf("uplink lost attribution: %+v", stored[0])
	}
	if stored[0].OriginOffset != 41 || stored[1].OriginOffset != 42 {
		t.Fatalf("uplink lost owner offsets: %+v", stored)
	}
	if kv := mustKVScan(t, ps, "child1/m1/temp"); len(kv) != 1 ||
		string(kv[0].Payload) != `{"v":2}` || kv[0].OriginOffset != 42 {
		t.Fatalf("kv on replicate: %+v", kv)
	}

	// idempotent re-push
	hwm, _ = cl.Replicate("metrics", recs)
	if hwm != 2 {
		t.Fatal("dedupe hwm")
	}
	if ps.NextOffset("metrics") != 3 {
		t.Fatal("dedupe append")
	}

	// parent stores a command for the child zone; child fetches → stripped
	peng.IngestAdminAttributed(
		"colca/v1/_CmdParam/m1/child1/m1/go",
		[]byte(`{"correlation_id":"c1","expires_at":99999999999}`),
		engine.Attribution{WrittenBy: "api", ActorID: "user-anna", ActorLabel: "anna@example.com", ActorKind: "human",
			ActorGroups: []string{"operators"}},
	)
	dl, next, gap, err := cl.Downlink(1, 10, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if gap != nil {
		t.Fatalf("no gap expected on an unpruned stream: %+v", gap)
	}
	if len(dl) != 1 || dl[0].Topic != "colca/v1/_CmdParam/m1/m1/go" {
		t.Fatalf("downlink: %+v", dl)
	}
	if dl[0].WrittenBy != "api" || dl[0].ActorID != "user-anna" ||
		dl[0].ActorLabel != "anna@example.com" || dl[0].ActorKind != "human" ||
		len(dl[0].ActorGroups) != 1 || dl[0].ActorGroups[0] != "operators" {
		t.Fatalf("downlink lost attribution — the executing node cannot reconstitute the person: %+v", dl[0])
	}
	if next != 2 {
		t.Fatalf("next %d", next)
	}

	// wrong key must be rejected at TLS layer
	evilID, _ := identity.Generate(filepath.Join(dir, "e.key"))
	evil, err := NewClient("https://"+addr, parentID.PublicHex(), evilID)
	if err == nil {
		if _, err := evil.Replicate("metrics", recs[:1]); err == nil {
			t.Fatal("unpinned child key must be rejected")
		}
	}
}

// TestStopReleasesPortAndKillsLongPoll pins the shutdown contract the 4-node
// integration suite depends on: a node is restarted on the SAME address, so
// Stop() must kill in-flight long polls (Close, never Shutdown), release the
// port before it returns, leave no handler goroutine behind, and be safe to
// call twice or without a preceding Start.
func TestStopReleasesPortAndKillsLongPoll(t *testing.T) {
	dir := t.TempDir()
	parentID := mustIdentity(t, filepath.Join(dir, "p.key"))
	childID := mustIdentity(t, filepath.Join(dir, "c.key"))

	ps := mustStore(t, filepath.Join(dir, "pdata"))
	pcfg := &config.Config{ULID: "n-parent", Repl: config.Endpoint{Addr: "127.0.0.1:0"}}
	preg, peng := nodeParts(t, ps, pcfg, nil, nil, nil, childSpec{"n-child", childID.PublicHex(), "child1"})

	// Stop before Start must not panic.
	unstarted, err := NewServer(pcfg, peng, parentID, preg, nil, nil)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	unstarted.Stop()

	srv, addr := startServer(t, pcfg, peng, parentID, preg)
	cl := mustClient(t, addr, parentID.PublicHex(), childID)

	// Nothing in the commands stream → the handler enters the ~20s long poll.
	done := make(chan error, 1)
	go func() {
		_, _, _, err := cl.Downlink(1, 10, 20*time.Second)
		done <- err
	}()
	waitFor(t, "the long-poll handler to be running", 5*time.Second, func() bool {
		return goroutinesIn("handleDownlink") > 0
	})

	start := time.Now()
	srv.Stop()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("in-flight long poll must fail once the server is stopped")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("in-flight long poll survived Stop() — Stop must Close(), not Shutdown()")
	}
	if el := time.Since(start); el > 5*time.Second {
		t.Fatalf("Stop() took %v — it must not wait for in-flight long polls", el)
	}

	// The port must be free for an immediate re-bind (node restart on same addr).
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("port %s not released by Stop(): %v", addr, err)
	}
	ln.Close()

	// No handler goroutine may outlive Stop() (else -race reports leaks and the
	// 20s poll loop keeps hammering a closed store).
	waitFor(t, "the long-poll handler to exit", 5*time.Second, func() bool {
		return goroutinesIn("handleDownlink") == 0
	})

	// Stop is idempotent.
	srv.Stop()
}

// TestDownlinkOnlyOwnMountCommands: a child sees commands for its own subtree
// only — not a sibling's, not acks — and the returned cursor still advances
// past everything scanned, so it never re-scans foreign records forever.
func TestDownlinkOnlyOwnMountCommands(t *testing.T) {
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

	// commands stream: 1 = sibling's command, 2 = own ack, 3 = own command.
	mustIngestAdmin(t, peng, "colca/v1/_CmdParam/m2/child2/m2/go", `{"correlation_id":"c0","expires_at":99999999999}`)
	mustIngestAdmin(t, peng, "colca/v1/_Ack/m1/child1/m1/go", `{"correlation_id":"c1","result_code":0}`)
	mustIngestAdmin(t, peng, "colca/v1/_CmdParam/m1/child1/m1/go", `{"correlation_id":"c1","expires_at":99999999999}`)

	cl := mustClient(t, addr, parentID.PublicHex(), child1ID)
	dl, next, _, err := cl.Downlink(1, 10, 5*time.Second)
	if err != nil {
		t.Fatalf("downlink: %v", err)
	}
	if len(dl) != 1 {
		t.Fatalf("child1 must receive exactly its own command, got %+v", dl)
	}
	if dl[0].Topic != "colca/v1/_CmdParam/m1/m1/go" {
		t.Fatalf("topic %q — the mount must be stripped to child-local coordinates", dl[0].Topic)
	}
	if dl[0].ParentOffset != 3 {
		t.Fatalf("parent offset %d, want 3", dl[0].ParentOffset)
	}
	// next skips the sibling command and the ack instead of re-scanning them.
	if next != 4 {
		t.Fatalf("next %d, want 4 (cursor must advance past filtered records)", next)
	}
	for _, r := range dl {
		if strings.Contains(r.Topic, "child2") || strings.Contains(r.Topic, "m2") {
			t.Fatalf("foreign-mount record leaked to child1: %+v", r)
		}
	}
}

// TestUplinkOfflineBuffersThenDeliversExactlyOnce is the unit-level proof of
// the offline-buffering mechanism the integration suite relies on: while the
// parent is unreachable the uplink cursor must not move, and once the parent
// comes back on the SAME address every buffered record arrives exactly once.
// It also pins the ack-only filter on the commands stream.
func TestUplinkOfflineBuffersThenDeliversExactlyOnce(t *testing.T) {
	dir := t.TempDir()
	parentID := mustIdentity(t, filepath.Join(dir, "p.key"))
	childID := mustIdentity(t, filepath.Join(dir, "c.key"))

	ps := mustStore(t, filepath.Join(dir, "pdata"))
	pcfg := &config.Config{ULID: "n-parent", Repl: config.Endpoint{Addr: "127.0.0.1:0"}}
	preg, peng := nodeParts(t, ps, pcfg, nil, nil, nil, childSpec{"n-child", childID.PublicHex(), "child1"})

	// Start once only to obtain a real address, then stop: the parent is down.
	srv1, addr := startServer(t, pcfg, peng, parentID, preg)
	srv1.Stop()
	pcfg.Repl.Addr = addr

	cs := mustStore(t, filepath.Join(dir, "cdata"))
	ccfg := &config.Config{ULID: "n-child"}
	_, ceng := nodeParts(t, cs, ccfg, nil, nil, nil)
	mustIngestAdmin(t, ceng, "colca/v1/_Metric/m1/m1/temp", `{"v":1}`)
	mustIngestAdmin(t, ceng, "colca/v1/_Metric/m1/m1/temp", `{"v":2}`)
	// commands stream: 1 = a command (must never be mirrored back up), 2 = an ack.
	mustIngestAdmin(t, ceng, "colca/v1/_CmdParam/m1/m1/go", `{"correlation_id":"c1","expires_at":99999999999}`)
	mustIngestAdmin(t, ceng, "colca/v1/_Ack/m1/m1/go", `{"correlation_id":"c1","result_code":0}`)

	cl := mustClient(t, addr, parentID.PublicHex(), childID)

	// Phase 1: parent unreachable → push fails, cursors must stay put.
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		RunUplink(cl, ceng, nil, nil, stop)
	}()
	time.Sleep(400 * time.Millisecond)
	close(stop)
	waitForClosed(t, "RunUplink to return after stop (offline)", done, 5*time.Second)

	if got := cs.CursorGet(uns.UplinkCursor(cl.ParentPub()), "metrics"); got != 1 {
		t.Fatalf("uplink metrics cursor moved to %d while the parent was down — offline buffering broken", got)
	}
	if got := cs.CursorGet(uns.UplinkCursor(cl.ParentPub()), "commands"); got != 1 {
		t.Fatalf("uplink commands cursor moved to %d while the parent was down", got)
	}
	if got := ps.NextOffset("metrics"); got != 1 {
		t.Fatalf("parent stored %d metric records while it was down", got-1)
	}

	// Phase 2: parent comes back on the same address (proves the port was released).
	srv2, err := NewServer(pcfg, peng, parentID, preg, nil, nil)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	addr2, err := srv2.Start()
	if err != nil {
		t.Fatalf("restart on %s: %v", addr, err)
	}
	defer srv2.Stop()
	if addr2 != addr {
		t.Fatalf("restarted on %s, want %s", addr2, addr)
	}

	stop2 := make(chan struct{})
	done2 := make(chan struct{})
	go func() {
		defer close(done2)
		RunUplink(cl, ceng, nil, nil, stop2)
	}()
	waitFor(t, "the buffered records to reach the parent", 10*time.Second, func() bool {
		return ps.NextOffset("metrics") == 3 && ps.NextOffset("commands") == 2
	})
	// keep the loop running: a second delivery would show up here
	time.Sleep(400 * time.Millisecond)
	close(stop2)
	waitForClosed(t, "RunUplink to return after stop (online)", done2, 5*time.Second)

	if got := ps.NextOffset("metrics"); got != 3 {
		t.Fatalf("parent metrics next offset %d, want 3 — records must arrive exactly once", got)
	}
	if got := ps.NextOffset("commands"); got != 2 {
		t.Fatalf("parent commands next offset %d, want 2 — only the _Ack may travel up, exactly once", got)
	}
	stored, _, err := ps.Read("metrics", 1, 10, nil)
	if err != nil {
		t.Fatalf("read parent metrics: %v", err)
	}
	for _, r := range stored {
		if r.Topic != "colca/v1/_Metric/m1/child1/m1/temp" {
			t.Fatalf("parent stored %q, want the mount-inserted topic", r.Topic)
		}
	}
	cmds, _, err := ps.Read("commands", 1, 10, nil)
	if err != nil {
		t.Fatalf("read parent commands: %v", err)
	}
	if len(cmds) != 1 || cmds[0].Topic != "colca/v1/_Ack/m1/child1/m1/go" {
		t.Fatalf("commands stream at parent: %+v — commands must never be mirrored back up", cmds)
	}
	if got := cs.CursorGet(uns.UplinkCursor(cl.ParentPub()), "metrics"); got != 3 {
		t.Fatalf("uplink metrics cursor %d, want 3", got)
	}
	if got := cs.CursorGet(uns.UplinkCursor(cl.ParentPub()), "commands"); got != 3 {
		t.Fatalf("uplink commands cursor %d, want 3 (it must skip the filtered command)", got)
	}
}

// TestRunDownlinkIngestsAndStopsPromptly covers the child-side loop: parent
// offsets are tracked under the pseudo-stream "commands-parent", each record is
// ingested locally (persist + local delivery), and the loop returns promptly
// when stop is closed even though it is parked in a 20s long poll.
func TestRunDownlinkIngestsAndStopsPromptly(t *testing.T) {
	dir := t.TempDir()
	parentID := mustIdentity(t, filepath.Join(dir, "p.key"))
	childID := mustIdentity(t, filepath.Join(dir, "c.key"))

	ps := mustStore(t, filepath.Join(dir, "pdata"))
	pcfg := &config.Config{ULID: "n-parent", Repl: config.Endpoint{Addr: "127.0.0.1:0"}}
	preg, peng := nodeParts(t, ps, pcfg, nil, nil, nil, childSpec{"n-child", childID.PublicHex(), "child1"})
	srv, addr := startServer(t, pcfg, peng, parentID, preg)
	defer srv.Stop()
	// Two commands: the child is attached at offset 2 below, so the first one
	// only exists to make that position seedable (see attachAt). The second is
	// the one this test follows.
	mustIngestAdmin(t, peng, "colca/v1/_CmdParam/m1/child1/m1/filler", `{"correlation_id":"c0","expires_at":99999999999}`)
	at := ps.NextOffset("commands")
	mustIngestAdmin(t, peng, "colca/v1/_CmdParam/m1/child1/m1/go", `{"correlation_id":"c1","expires_at":99999999999}`)

	cs := mustStore(t, filepath.Join(dir, "cdata"))
	ccfg := &config.Config{ULID: "n-child"}
	delivered := make(chan string, 4)
	_, ceng := nodeParts(t, cs, ccfg, func(topic string, payload []byte, retain bool) {
		if retain {
			t.Errorf("a command must not be retained on the local bus: %s", topic)
		}
		delivered <- topic
	}, nil, nil)
	cl := mustClient(t, addr, parentID.PublicHex(), childID)
	attachAt(t, cs, cl, at)

	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		RunDownlink(cl, ceng, nil, stop)
	}()

	select {
	case topic := <-delivered:
		if topic != "colca/v1/_CmdParam/m1/m1/go" {
			t.Fatalf("delivered %q, want the mount-stripped topic", topic)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("command was never delivered locally")
	}
	waitFor(t, "the downlink cursor to advance", 5*time.Second, func() bool {
		return cs.CursorGet(uns.DownlinkCursor(cl.ParentPub()), "commands-parent") == at+1
	})
	if got := cs.NextOffset("commands"); got != 2 {
		t.Fatalf("child commands next offset %d, want 2", got)
	}
	if got := cs.CursorGet(uns.DownlinkCursor(cl.ParentPub()), "commands"); got != 1 {
		t.Fatal("the downlink cursor must live under \"commands-parent\", not the local commands stream")
	}

	// The loop is now parked in a long poll; stop must still be prompt.
	start := time.Now()
	close(stop)
	waitForClosed(t, "RunDownlink to return after stop", done, 5*time.Second)
	if el := time.Since(start); el > 5*time.Second {
		t.Fatalf("RunDownlink took %v to stop", el)
	}
}

// The lanes RunUplink pushes are exactly the streams the domain says rise,
// and the parent's door accepts exactly those. A class that gains a stream
// must gain a lane here, or its records never leave the node that wrote
// them — silently, because nothing else would notice.
func TestEveryRisingStreamHasAnUplinkLane(t *testing.T) {
	pushed := map[string]bool{}
	for _, stream := range uplinkStreams() {
		pushed[stream] = true
		if !uns.IsUplinkStream(stream) {
			t.Fatalf("the uplink pushes %q, which the parent's door refuses", stream)
		}
	}
	rising := uns.UplinkStreams()
	if len(rising) == 0 {
		t.Fatal("no stream rises: the check above would pass against an empty lane list")
	}
	for _, stream := range rising {
		if !pushed[stream] {
			t.Fatalf("no uplink lane pushes %q — records written there never leave this node", stream)
		}
	}
}

// --- helpers ---------------------------------------------------------------

// A machine-kind key at the repl door is rejected like an unknown one: the
// door is for nodes (auth §6.2 / §2.1 kind rule).
func TestMachineKindRejectedAtReplDoor(t *testing.T) {
	dir := t.TempDir()
	parentID := mustIdentity(t, filepath.Join(dir, "p.key"))
	machineID := mustIdentity(t, filepath.Join(dir, "m.key"))

	ps := mustStore(t, filepath.Join(dir, "pdata"))
	pcfg := &config.Config{ULID: "n-parent", Repl: config.Endpoint{Addr: "127.0.0.1:0"}}
	preg, peng := nodeParts(t, ps, pcfg, nil, nil, nil)
	entry, _ := json.Marshal(uns.Entry{ULID: "m-x", Pubkey: machineID.PublicHex(),
		Kind: uns.KindExternal, Element: placeElement(t, peng, "mx")})
	if _, _, err := preg.Enroll(entry); err != nil {
		t.Fatal(err)
	}
	srv, addr := startServer(t, pcfg, peng, parentID, preg)
	defer srv.Stop()

	cl := mustClient(t, addr, parentID.PublicHex(), machineID)
	if _, err := cl.Replicate("metrics", []store.ReplRecord{
		{ChildOffset: 1, Topic: "colca/v1/_Metric/m-x/mx/t", Payload: []byte(`{"v":1}`), TS: 1},
	}); err == nil {
		t.Fatal("a machine-kind key must be rejected at the repl door")
	}
	if ps.NextOffset("metrics") != 1 {
		t.Fatal("nothing may be applied for a rejected identity")
	}
}

// childSpec is a fixture child: enrolled as kind node at the parent's
// registry (entry-before-connect by construction), at the element sitting at
// Mount.
type childSpec struct{ ULID, Pubkey, Mount string }

// mustKVScan is KVScan with the error handled the only way a test fixture
// can: fail loud (resources design §8).
func mustKVScan(t *testing.T, st *store.Store, prefix string) []store.KVEntry {
	t.Helper()
	entries, err := st.KVScan(prefix)
	if err != nil {
		t.Fatal(err)
	}
	return entries
}

// nodeParts builds a registry and an engine wired to each other exactly the way
// node.Start does — the registry resolving placements through the engine's
// element index — then places each child's element and enrolls it there.
// Placement comes first because an identity binds to an element, never to a
// path (id-grants design §4).
func nodeParts(t *testing.T, st *store.Store, cfg *config.Config, deliver engine.LocalDeliver,
	m *metrics.Metrics, clk *clock.Clock, children ...childSpec) (*registry.Manager, *engine.Engine) {
	t.Helper()
	reg, err := registry.New(st, cfg.ULID)
	if err != nil {
		t.Fatal(err)
	}
	eng := engine.New(st, cfg, reg, deliver, m, clk)
	reg.SetNamespace(eng.Elements())
	for _, ch := range children {
		element := placeElement(t, eng, ch.Mount)
		b, err := json.Marshal(uns.Entry{ULID: ch.ULID, Pubkey: ch.Pubkey, Kind: uns.KindNode, Element: element})
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := reg.Enroll(b); err != nil {
			t.Fatalf("enroll child %s: %v", ch.ULID, err)
		}
	}
	return reg, eng
}

// placeElement authors a system element at path in the node's own namespace and
// returns its id.
func placeElement(t *testing.T, eng *engine.Engine, path string) string {
	t.Helper()
	id := "el-" + strings.ReplaceAll(path, "/", "-")
	payload, err := json.Marshal(map[string]string{"id": id, "name": path})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := eng.IngestAdmin("colca/v1/_SystemElement/"+eng.NodeID()+"/"+path, payload); err != nil {
		t.Fatalf("place element at %s: %v", path, err)
	}
	return id
}

// uplinkPair brings up a live parent and a child engine wired to it, and
// returns the two stores, the parent's pinned pubkey hex (the scope key for
// this child's cursors — parent-scoped-cursors design §3.1), plus a started
// RunUplink whose stop is registered as cleanup. Seeding goes straight to the
// child store: these tests are about the ORDER lanes drain in, not about
// contract validation at a door.
func uplinkPair(t *testing.T) (child, parent *store.Store, parentPub string, start func()) {
	t.Helper()
	dir := t.TempDir()
	parentID := mustIdentity(t, filepath.Join(dir, "p.key"))
	childID := mustIdentity(t, filepath.Join(dir, "c.key"))

	ps := mustStore(t, filepath.Join(dir, "pdata"))
	pcfg := &config.Config{ULID: "n-parent", Repl: config.Endpoint{Addr: "127.0.0.1:0"}}
	preg, peng := nodeParts(t, ps, pcfg, nil, nil, nil, childSpec{"n-child", childID.PublicHex(), "child1"})
	_, addr := startServer(t, pcfg, peng, parentID, preg)

	cs := mustStore(t, filepath.Join(dir, "cdata"))
	ccfg := &config.Config{ULID: "n-child"}
	_, ceng := nodeParts(t, cs, ccfg, nil, nil, nil)
	cl := mustClient(t, addr, parentID.PublicHex(), childID)

	return cs, ps, parentID.PublicHex(), func() {
		stop := make(chan struct{})
		done := make(chan struct{})
		go func() {
			defer close(done)
			RunUplink(cl, ceng, nil, nil, stop)
		}()
		t.Cleanup(func() {
			close(stop)
			waitForClosed(t, "RunUplink to return after stop", done, 5*time.Second)
		})
	}
}

func seed(t *testing.T, s *store.Store, stream, topic string, n int) {
	t.Helper()
	recs := make([]store.Record, n)
	for i := range recs {
		recs[i] = store.Record{
			Topic:   fmt.Sprintf(topic, i),
			Payload: []byte(`{"v":1}`),
			TS:      int64(1000 + i),
		}
	}
	if _, _, err := s.Append(stream, recs); err != nil {
		t.Fatalf("seed %s: %v", stream, err)
	}
}

// Design §4. After an outage the small lanes drain BEFORE the metrics
// backlog. Order inside a stream never changes, so this — not a place in the
// metrics queue — is the only thing that lets an alarm arrive promptly.
func TestUplinkDrainsSmallLanesBeforeTheMetricsBacklog(t *testing.T) {
	cs, ps, _, start := uplinkPair(t)
	// The alarm lane is deliberately several batches deep. With a single
	// record any ordering that merely puts metrics last would pass, and this
	// test would not distinguish lanes from a reshuffled slice. At three
	// batches the claim bites: the lane must drain ACROSS passes before
	// metrics moves at all.
	const alarmBacklog = 3 * replBatch
	seed(t, cs, "metrics", "colca/v1/_Metric/m1/m1/temp%d", 5*replBatch)
	seed(t, cs, "alarms", "colca/v1/_AlarmStateChange/m1/m1/alarm-events/a1/e%d", alarmBacklog)

	start()
	waitFor(t, "the alarm backlog to reach the parent", 20*time.Second, func() bool {
		return ps.NextOffset("alarms") == uint64(alarmBacklog)+1
	})

	// Flat round-robin would have pushed one metrics batch per pass alongside
	// each alarm batch, so metrics would sit at ~alarmBacklog by now. Lanes
	// mean at most the single floor batch has gone.
	if got := ps.NextOffset("metrics") - 1; got > uint64(replBatch) {
		t.Fatalf("%d metric records reached the parent before the %d-record alarm "+
			"backlog finished, want at most one %d-record floor batch — the alarm "+
			"lane is not draining ahead of metrics", got, alarmBacklog, replBatch)
	}
}

// Dataops-evaluator design §8. Annotations get the same promptness guarantee
// as alarms: the lane drains ahead of the metrics backlog, so a producer's
// output reaches the root without waiting behind a large sample catch-up.
func TestUplinkDrainsAnnotationsBeforeTheMetricsBacklog(t *testing.T) {
	cs, ps, _, start := uplinkPair(t)
	const annotationBacklog = 3 * replBatch
	seed(t, cs, "metrics", "colca/v1/_Metric/m1/m1/temp%d", 5*replBatch)
	seed(t, cs, "annotations", "colca/v1/_Annotation/m1/m1/press1/a%d", annotationBacklog)

	start()
	waitFor(t, "the annotation backlog to reach the parent", 20*time.Second, func() bool {
		return ps.NextOffset("annotations") == uint64(annotationBacklog)+1
	})

	if got := ps.NextOffset("metrics") - 1; got > uint64(replBatch) {
		t.Fatalf("%d metric records reached the parent before the %d-record annotation "+
			"backlog finished, want at most one %d-record floor batch — the annotation "+
			"lane is not draining ahead of metrics", got, annotationBacklog, replBatch)
	}
}

// Design §4, the floor. A lane that never empties must not hold the metrics
// cursor still: once the local pruner passes it the backlog is gone and only
// a gap marker remains, so lane pressure would become silent data loss.
func TestMetricsAdvancesWhileAPriorityLaneStaysHot(t *testing.T) {
	cs, ps, _, start := uplinkPair(t)
	seed(t, cs, "metrics", "colca/v1/_Metric/m1/m1/temp%d", 3*replBatch)

	hot, hotDone := make(chan struct{}), make(chan struct{})
	go func() { // keep the alarms lane permanently non-empty
		defer close(hotDone)
		for i := 0; ; i++ {
			select {
			case <-hot:
				return
			default:
			}
			// Append directly, errors ignored: this goroutine outlives nothing
			// and must not call t.Fatalf, which is illegal off the test
			// goroutine and races the store's own cleanup.
			_, _, _ = cs.Append("alarms", []store.Record{{
				Topic:   fmt.Sprintf("colca/v1/_AlarmStateChange/m1/m1/alarm-events/a1/e%d", i),
				Payload: []byte(`{"v":1}`),
				TS:      int64(1000 + i),
			}})
			time.Sleep(time.Millisecond)
		}
	}()
	// Registered BEFORE start(), so cleanup (LIFO) stops the uplink first and
	// this producer second — neither touches a closed store.
	t.Cleanup(func() {
		close(hot)
		<-hotDone
	})

	start()
	waitFor(t, "metrics to advance while the alarms lane stays hot", 20*time.Second, func() bool {
		return ps.NextOffset("metrics") > 1
	})
}

// Design §3.1: two parents, two sets of cursors. A child that changes parents
// must not resume against the new one at the old one's offsets — and a child
// that RETURNS to a former parent finds its old position intact.
func TestCursorsAreScopedPerParent(t *testing.T) {
	cs, _, parentPub, start := uplinkPair(t)
	seed(t, cs, "metrics", "colca/v1/_Metric/m1/m1/temp%d", 3)
	start()

	// The cursor that moved is the one named for THIS parent.
	waitFor(t, "the uplink cursor for this parent to advance", 20*time.Second, func() bool {
		return cs.CursorGet(uns.UplinkCursor(parentPub), "metrics") > 1
	})
	if got := cs.CursorGet(uns.UplinkCursor("ffff"), "metrics"); got != 1 {
		t.Fatalf("a DIFFERENT parent's uplink cursor = %d, want the untouched default 1 — "+
			"cursors are not scoped and a reparent would resume at the wrong offsets", got)
	}
	if got := cs.CursorGet("uplink", "metrics"); got != 1 {
		t.Fatalf("the legacy un-scoped cursor advanced to %d — the loops still write it", got)
	}
}

// Design §3.3: hello exists to teach a (re)connecting child its position in
// one RTT, and the parent's stream head is part of that position — it is what
// a child with no cursor for THIS parent must start from.
func TestHelloCarriesTheParentsCommandHead(t *testing.T) {
	dir := t.TempDir()
	parentID := mustIdentity(t, filepath.Join(dir, "p.key"))
	childID := mustIdentity(t, filepath.Join(dir, "c.key"))

	ps := mustStore(t, filepath.Join(dir, "pdata"))
	pcfg := &config.Config{ULID: "n-parent", Repl: config.Endpoint{Addr: "127.0.0.1:0"}}
	preg, peng := nodeParts(t, ps, pcfg, nil, nil, nil,
		childSpec{"n-child", childID.PublicHex(), "child1"})
	_, addr := startServer(t, pcfg, peng, parentID, preg)

	mustIngestAdmin(t, peng, "colca/v1/_CmdParam/n-parent/child1/go",
		`{"correlation_id":"c1","expires_at":99999999999}`)
	want := ps.NextOffset("commands")
	if want < 2 {
		t.Fatalf("parent commands head = %d, want >= 2 after seeding a command", want)
	}

	cl := mustClient(t, addr, parentID.PublicHex(), childID)
	res, err := cl.hello(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if res.Head != want {
		t.Fatalf("hello head = %d, want %d (the parent's own commands NextOffset)", res.Head, want)
	}
}

func mustIdentity(t *testing.T, path string) *identity.Identity {
	t.Helper()
	id, err := identity.Generate(path)
	if err != nil {
		t.Fatalf("generate identity %s: %v", path, err)
	}
	return id
}

func mustStore(t *testing.T, dir string) *store.Store {
	t.Helper()
	s, err := store.Open(dir)
	if err != nil {
		t.Fatalf("open store %s: %v", dir, err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func mustIngestAdmin(t *testing.T, eng *engine.Engine, topic, payload string) {
	t.Helper()
	if _, err := eng.IngestAdmin(topic, []byte(payload)); err != nil {
		t.Fatalf("ingest %s: %v", topic, err)
	}
}

func startServer(t *testing.T, cfg *config.Config, eng *engine.Engine, id *identity.Identity, reg *registry.Manager) (*Server, string) {
	t.Helper()
	return startServerWithMetrics(t, cfg, eng, id, reg, nil)
}

// startServerWithMetrics is startServer for tests that assert on the repl
// server's own counters (design §8, colca_gap_served_total{surface="downlink"}).
func startServerWithMetrics(t *testing.T, cfg *config.Config, eng *engine.Engine, id *identity.Identity, reg *registry.Manager, m *metrics.Metrics) (*Server, string) {
	t.Helper()
	srv, err := NewServer(cfg, eng, id, reg, nil, m)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	addr, err := srv.Start()
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	return srv, addr
}

func mustClient(t *testing.T, addr, parentPubHex string, id *identity.Identity) *Client {
	t.Helper()
	c, err := NewClient("https://"+addr, parentPubHex, id)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	return c
}

// attachAt makes st look like a node that is ALREADY attached to c's parent and
// positioned at off on that parent's commands stream.
//
// Since parent-scoped cursors, a child with no cursor for a parent adopts that
// parent's head on first contact and never hears what predates it
// (parent-scoped-cursors design §3.2) — so a test that wants a command seeded
// BEFORE the loop starts to be delivered has to place the child in front of it
// instead of relying on a fresh child reading from 1.
//
// off must be above 1: 1 is the default position and CursorAck is forward-only,
// so "seeding" it there would be a silent no-op and the test would pass for the
// wrong reason. Both that and a refused ack fail loudly here.
func attachAt(t *testing.T, st *store.Store, c *Client, off uint64) {
	t.Helper()
	if off <= 1 {
		t.Fatalf("attachAt(%d): position 1 is the default and cannot be seeded — "+
			"give the parent's stream a record to sit in front of first", off)
	}
	if !st.CursorAck(uns.DownlinkCursor(c.ParentPub()), downlinkStream, off) {
		t.Fatalf("attachAt(%d): the commands cursor did not move — the precondition is a no-op", off)
	}
}

// waitForAttached blocks until c's downlink loop has completed first contact
// with its parent, which since parent-scoped cursors is the moment it becomes
// safe to issue a command to it: whatever is already in the parent's commands
// stream when that handshake runs is pre-attachment and deliberately not
// delivered (design §3.2). A test that seeds a command after starting the loop
// without this races the handshake and passes or fails on scheduling.
//
// The observable is the cursor KEY, not its position: first contact against a
// parent whose commands stream is empty records position 1, which CursorGet
// cannot tell apart from no cursor at all.
func waitForAttached(t *testing.T, st *store.Store, c *Client) {
	t.Helper()
	waitFor(t, "the downlink loop to complete first contact with its parent", 20*time.Second, func() bool {
		for _, cur := range st.Cursors() {
			if cur.Name == uns.DownlinkCursor(c.ParentPub()) && cur.Stream == downlinkStream {
				return true
			}
		}
		return false
	})
}

func waitFor(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timeout after %v waiting for %s", timeout, what)
}

func waitForClosed(t *testing.T, what string, done <-chan struct{}, timeout time.Duration) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(timeout):
		t.Fatalf("timeout after %v waiting for %s", timeout, what)
	}
}

// goroutinesIn counts goroutines whose stack mentions fn — used to prove that
// no request handler outlives the server it belongs to.
func goroutinesIn(fn string) int {
	buf := make([]byte, 1<<20)
	n := runtime.Stack(buf, true)
	count := 0
	for _, g := range strings.Split(string(buf[:n]), "\n\n") {
		if strings.Contains(g, fn) {
			count++
		}
	}
	return count
}
