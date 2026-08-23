package repl

import (
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/identity"
	"github.com/alpamayo-solutions/colca/internal/metrics"
	"github.com/alpamayo-solutions/colca/internal/store"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// Design §3.2, uplink half: first contact with a parent this child has no
// cursor for offers everything the child still RETAINS, starting at the
// stream's LWM.
//
// Reaching the survivors is not the whole claim — the loop's §6.3 clamp would
// reach them from the default position 1 too. It would get there by reporting a
// gap, and that report would be false: a gap says records were lost between a
// position this parent held and the one it holds now, and this parent never
// held one. Seeding the cursor at the LWM up front is what tells the two
// situations apart, so the metric is the assertion that bites.
func TestFirstContactUplinkStartsAtTheLWMWithoutReportingAGap(t *testing.T) {
	dir := t.TempDir()
	parentID := mustIdentity(t, filepath.Join(dir, "p.key"))
	childID := mustIdentity(t, filepath.Join(dir, "c.key"))

	ps := mustStore(t, filepath.Join(dir, "pdata"))
	pcfg := &config.Config{ULID: "n-parent", Repl: config.Endpoint{Addr: "127.0.0.1:0"}}
	preg, peng := nodeParts(t, ps, pcfg, nil, nil, nil, childSpec{"n-child", childID.PublicHex(), "child1"})
	srv, addr := startServer(t, pcfg, peng, parentID, preg)
	defer srv.Stop()

	cs := mustStore(t, filepath.Join(dir, "cdata"))
	cm := metrics.New(cs, config.Retention{}, nil)
	_, ceng := nodeParts(t, cs, &config.Config{ULID: "n-child"}, nil, nil, nil)
	seed(t, cs, "metrics", "colca/v1/_Metric/m1/m1/temp%d", 5)
	cl := mustClient(t, addr, parentID.PublicHex(), childID)

	// Retention already removed offsets 1..3 while this node was attached
	// elsewhere: the survivors are temp3 and temp4, and no cursor exists for
	// the parent it is about to meet.
	if n, err := cs.Prune("metrics", 4, []string{uns.UplinkCursor(cl.ParentPub())}, nil); err != nil || n != 3 {
		t.Fatalf("prune: removed %d records, err %v — want 3 removed", n, err)
	}
	if got := cs.LWM("metrics"); got != 4 {
		t.Fatalf("child metrics LWM = %d, want 4 — the pruned precondition did not take", got)
	}
	if got := cs.CursorGet(uns.UplinkCursor(cl.ParentPub()), "metrics"); got != 1 {
		t.Fatalf("the scoped uplink cursor is already at %d — this test only means something on FIRST contact", got)
	}

	stop := make(chan struct{})
	done := make(chan struct{})
	go func() { defer close(done); RunUplink(cl, ceng, nil, cm, stop) }()
	t.Cleanup(func() {
		close(stop)
		waitForClosed(t, "RunUplink to return after stop", done, 5*time.Second)
	})

	waitFor(t, "the retained survivors to reach the new parent", 20*time.Second, func() bool {
		return ps.NextOffset("metrics") == 3
	})
	recs, _, err := ps.Read("metrics", 1, 10, nil)
	if err != nil {
		t.Fatalf("read parent metrics: %v", err)
	}
	if len(recs) != 2 ||
		recs[0].Topic != "colca/v1/_Metric/m1/child1/m1/temp3" ||
		recs[1].Topic != "colca/v1/_Metric/m1/child1/m1/temp4" {
		t.Fatalf("parent holds %+v — first contact must offer exactly the retained survivors", recs)
	}
	const gapReceived = `colca_gap_received_total{stream="metrics"}`
	if v := scrapeMetric(t, cm, gapReceived); v != 0 {
		t.Fatalf("%s = %v — first contact reported a gap it never observed: this parent held no "+
			"position to lose records against, so the cursor must be seeded at the LWM instead of "+
			"being clamped there by the §6.3 jump", gapReceived, v)
	}
}

// Design §3.2, downlink half: commands are the opposite of uplink. An
// instruction issued before this child attached was addressed to whatever
// occupied the mount then, so handing it to a newcomer would execute a command
// its author never meant for it. First contact therefore adopts the parent's
// head and starts listening from there.
//
// The post-attachment command is what makes the silence a decision rather than
// a dead loop: the parent's stream is ordered and the child reads it in order,
// so a delivery of "post" with no prior delivery of "pre" can only mean "pre"
// was deliberately skipped.
func TestFirstContactCommandsStartAtTheParentsHead(t *testing.T) {
	dir := t.TempDir()
	parentID := mustIdentity(t, filepath.Join(dir, "p.key"))
	childID := mustIdentity(t, filepath.Join(dir, "c.key"))

	ps := mustStore(t, filepath.Join(dir, "pdata"))
	pcfg := &config.Config{ULID: "n-parent", Repl: config.Endpoint{Addr: "127.0.0.1:0"}}
	preg, peng := nodeParts(t, ps, pcfg, nil, nil, nil, childSpec{"n-child", childID.PublicHex(), "child1"})
	srv, addr := startServer(t, pcfg, peng, parentID, preg)
	defer srv.Stop()

	// Issued before this child ever connected.
	mustIngestAdmin(t, peng, "colca/v1/_CmdParam/m1/child1/m1/pre",
		`{"correlation_id":"pre","expires_at":99999999999}`)
	head := ps.NextOffset("commands")
	if head < 2 {
		t.Fatalf("parent commands head = %d, want >= 2 after seeding a command", head)
	}

	cs := mustStore(t, filepath.Join(dir, "cdata"))
	delivered := make(chan string, 8)
	_, ceng := nodeParts(t, cs, &config.Config{ULID: "n-child"},
		func(topic string, _ []byte, _ bool) { delivered <- topic }, nil, nil)
	cl := mustClient(t, addr, parentID.PublicHex(), childID)

	stop := make(chan struct{})
	done := make(chan struct{})
	go func() { defer close(done); RunDownlink(cl, ceng, nil, stop) }()
	t.Cleanup(func() {
		close(stop)
		waitForClosed(t, "RunDownlink to return after stop", done, 5*time.Second)
	})

	waitFor(t, "the commands cursor to adopt the parent's head", 20*time.Second, func() bool {
		return cs.CursorGet(uns.DownlinkCursor(cl.ParentPub()), downlinkStream) == head
	})

	mustIngestAdmin(t, peng, "colca/v1/_CmdParam/m1/child1/m1/post",
		`{"correlation_id":"post","expires_at":99999999999}`)
	select {
	case topic := <-delivered:
		if topic != "colca/v1/_CmdParam/m1/m1/post" {
			t.Fatalf("the first command delivered locally was %q — a command issued BEFORE this "+
				"child attached was handed to it", topic)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("the post-attachment command was never delivered — head adoption swallowed the live stream too")
	}

	recs, _, err := cs.Read("commands", 1, 100, nil)
	if err != nil {
		t.Fatalf("read child commands: %v", err)
	}
	for _, r := range recs {
		if strings.HasSuffix(r.Topic, "/pre") {
			t.Fatalf("a command issued before this child attached was ingested locally: %s", r.Topic)
		}
	}
}

// The head-adoption rule has to survive a parent that is not there yet, which
// is the ordinary case for a node that boots before (or faster than) its
// parent. hello is where the head comes from, so a hello that fails must be
// retried rather than shrugged off: polling without it would read from position
// 1 and hand this child every instruction issued before it attached — one
// transient error defeating §3.2 outright.
//
// Reuses the offline pattern from the uplink suite: allocate an address, take
// the parent down, start the loop against it, bring the parent back on the same
// address with a command already waiting.
func TestFirstContactSurvivesAParentThatIsNotUpYet(t *testing.T) {
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
	delivered := make(chan string, 8)
	_, ceng := nodeParts(t, cs, &config.Config{ULID: "n-child"},
		func(topic string, _ []byte, _ bool) { delivered <- topic }, nil, nil)
	cl := mustClient(t, addr, parentID.PublicHex(), childID)

	stop := make(chan struct{})
	done := make(chan struct{})
	go func() { defer close(done); RunDownlink(cl, ceng, nil, stop) }()
	t.Cleanup(func() {
		close(stop)
		waitForClosed(t, "RunDownlink to return after stop", done, 5*time.Second)
	})

	// Queued while the child could not possibly have been attached.
	mustIngestAdmin(t, peng, "colca/v1/_CmdParam/m1/child1/m1/pre",
		`{"correlation_id":"pre","expires_at":99999999999}`)
	head := ps.NextOffset("commands")

	srv2, err := NewServer(pcfg, peng, parentID, preg, nil, nil)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	if _, err := srv2.Start(); err != nil {
		t.Fatalf("restart on %s: %v", addr, err)
	}
	defer srv2.Stop()

	waitFor(t, "the commands cursor to adopt the head of the parent that finally answered",
		20*time.Second, func() bool {
			return cs.CursorGet(uns.DownlinkCursor(cl.ParentPub()), downlinkStream) == head
		})

	mustIngestAdmin(t, peng, "colca/v1/_CmdParam/m1/child1/m1/post",
		`{"correlation_id":"post","expires_at":99999999999}`)
	select {
	case topic := <-delivered:
		if topic != "colca/v1/_CmdParam/m1/m1/post" {
			t.Fatalf("the first command delivered locally was %q — the hello that would have taught "+
				"this child its position failed once and the loop polled from 1 anyway", topic)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("the post-attachment command was never delivered")
	}
}

// Head adoption applies to a parent this child has NEVER met — not to one it
// met when that parent's commands stream happened to be empty. The two look
// identical through CursorGet, which answers 1 for "absent" and for "at the
// first offset" alike, and they demand opposite behaviour: the second must
// still receive what was queued while it was down, which is the offline
// catch-up contract (cmdadmin design §10).
//
// The sequence is a node's ordinary life: attach to a fresh parent, stop, have
// a command issued in the meantime, come back.
func TestARestartAfterMeetingAnEmptyParentStillGetsWhatWasQueued(t *testing.T) {
	dir := t.TempDir()
	parentID := mustIdentity(t, filepath.Join(dir, "p.key"))
	childID := mustIdentity(t, filepath.Join(dir, "c.key"))

	ps := mustStore(t, filepath.Join(dir, "pdata"))
	pcfg := &config.Config{ULID: "n-parent", Repl: config.Endpoint{Addr: "127.0.0.1:0"}}
	preg, peng := nodeParts(t, ps, pcfg, nil, nil, nil, childSpec{"n-child", childID.PublicHex(), "child1"})
	srv, addr := startServer(t, pcfg, peng, parentID, preg)
	defer srv.Stop()
	if head := ps.NextOffset("commands"); head != 1 {
		t.Fatalf("parent commands head = %d, want the empty-stream 1 — this test is about meeting "+
			"a parent that has nothing to say yet", head)
	}

	// The child's store outlives the loop, exactly as a restart on the same
	// data directory does.
	cs := mustStore(t, filepath.Join(dir, "cdata"))
	delivered := make(chan string, 8)
	deliver := func(topic string, _ []byte, _ bool) { delivered <- topic }
	cl := mustClient(t, addr, parentID.PublicHex(), childID)

	// Run 1: attach, learn there is nothing here, shut down.
	_, ceng1 := nodeParts(t, cs, &config.Config{ULID: "n-child"}, deliver, nil, nil)
	stop1, done1 := make(chan struct{}), make(chan struct{})
	go func() { defer close(done1); RunDownlink(cl, ceng1, nil, stop1) }()
	waitForAttached(t, cs, cl)
	close(stop1)
	waitForClosed(t, "the first RunDownlink to stop", done1, 5*time.Second)

	// Issued while the node is down: it waits durably in the parent's stream.
	mustIngestAdmin(t, peng, "colca/v1/_CmdParam/m1/child1/m1/queued",
		`{"correlation_id":"queued","expires_at":99999999999}`)

	// Run 2: the same store, a fresh loop.
	_, ceng2 := nodeParts(t, cs, &config.Config{ULID: "n-child"}, deliver, nil, nil)
	stop2, done2 := make(chan struct{}), make(chan struct{})
	go func() { defer close(done2); RunDownlink(cl, ceng2, nil, stop2) }()
	t.Cleanup(func() {
		close(stop2)
		waitForClosed(t, "the second RunDownlink to stop", done2, 5*time.Second)
	})

	select {
	case topic := <-delivered:
		if topic != "colca/v1/_CmdParam/m1/m1/queued" {
			t.Fatalf("delivered %q, want the queued command", topic)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("the command queued while this node was down was never delivered — its restart was " +
			"mistaken for first contact and adopted the parent's head")
	}
}

// Design §3.5: an existing deployment's un-scoped cursors are adopted ONCE
// under the scoped names and then deleted.
//
// Two halves, both load-bearing. Adoption must carry the VALUE — a node that
// already offered its first records must not re-offer them, which is why the
// parent's contents are asserted and not just the cursor. And no compatibility
// path may stay behind, which is why the legacy key must be gone afterwards.
func TestLegacyCursorsAreAdoptedOnceThenGone(t *testing.T) {
	cs, ps, parentPub, start := uplinkPair(t)
	seed(t, cs, "metrics", "colca/v1/_Metric/m1/m1/temp%d", 5)

	// The position this node held before cursors were scoped: temp0..temp2
	// (offsets 1..3) already went up to this same parent.
	if !cs.CursorAck(legacyUplinkCursor, "metrics", 4) {
		t.Fatal("seeding the legacy uplink cursor did not move it — the precondition is a no-op " +
			"and everything below would pass for the wrong reason")
	}

	start()
	waitFor(t, "the records above the adopted position to reach the parent", 20*time.Second, func() bool {
		return ps.NextOffset("metrics") == 3
	})
	recs, _, err := ps.Read("metrics", 1, 10, nil)
	if err != nil {
		t.Fatalf("read parent metrics: %v", err)
	}
	if len(recs) != 2 ||
		recs[0].Topic != "colca/v1/_Metric/m1/child1/m1/temp3" ||
		recs[1].Topic != "colca/v1/_Metric/m1/child1/m1/temp4" {
		t.Fatalf("parent holds %+v — adoption must carry the legacy POSITION, not just the name: "+
			"temp0..temp2 had already been offered", recs)
	}
	waitFor(t, "the scoped cursor to carry the adopted position forward", 20*time.Second, func() bool {
		return cs.CursorGet(uns.UplinkCursor(parentPub), "metrics") == 6
	})

	for _, c := range cs.Cursors() {
		if c.Name == legacyUplinkCursor {
			t.Fatalf("the legacy cursor survived adoption (%s × %s = %d) — no compatibility path "+
				"may stay behind", c.Name, c.Stream, c.Position)
		}
	}
}

// Design §3.5, the commands half of adoption — the path EVERY existing
// deployment traverses exactly once, on its first start after this change.
//
// Control flow is what makes this one different from the uplink half: the
// commands branch reads `if !adoptLegacy(...) { SetIfAbsent(head) }`, so the
// two outcomes are not "adopted" and "adopted a bit later" but "resume where
// this node was" and "jump to the parent's head". The second silently drops
// every command that queued while the node was down for the upgrade, which is
// the ordinary shape of an upgrade: stop the node, replace the binary, start
// it. So the position is asserted, not just the name — reaching cmd2 at all is
// the claim, and it is unreachable from the head.
func TestALegacyCommandsCursorIsAdoptedInsteadOfTheParentsHead(t *testing.T) {
	dir := t.TempDir()
	parentID := mustIdentity(t, filepath.Join(dir, "p.key"))
	childID := mustIdentity(t, filepath.Join(dir, "c.key"))

	ps := mustStore(t, filepath.Join(dir, "pdata"))
	pcfg := &config.Config{ULID: "n-parent", Repl: config.Endpoint{Addr: "127.0.0.1:0"}}
	preg, peng := nodeParts(t, ps, pcfg, nil, nil, nil, childSpec{"n-child", childID.PublicHex(), "child1"})
	srv, addr := startServer(t, pcfg, peng, parentID, preg)
	defer srv.Stop()

	// Three commands, all issued before this loop ever runs. cmd1 was already
	// delivered under the pre-scoping cursor; cmd2 and cmd3 queued while the
	// node was down being upgraded.
	mustIngestAdmin(t, peng, "colca/v1/_CmdParam/m1/child1/m1/cmd1",
		`{"correlation_id":"c1","expires_at":99999999999}`)
	afterCmd1 := ps.NextOffset("commands")
	mustIngestAdmin(t, peng, "colca/v1/_CmdParam/m1/child1/m1/cmd2",
		`{"correlation_id":"c2","expires_at":99999999999}`)
	mustIngestAdmin(t, peng, "colca/v1/_CmdParam/m1/child1/m1/cmd3",
		`{"correlation_id":"c3","expires_at":99999999999}`)
	head := ps.NextOffset("commands")
	if afterCmd1 <= 1 || head <= afterCmd1 {
		t.Fatalf("parent commands stream: position after cmd1 = %d, head = %d — the legacy position "+
			"must sit strictly between the default and the head or this test proves nothing",
			afterCmd1, head)
	}

	cs := mustStore(t, filepath.Join(dir, "cdata"))
	delivered := make(chan string, 8)
	_, ceng := nodeParts(t, cs, &config.Config{ULID: "n-child"},
		func(topic string, _ []byte, _ bool) { delivered <- topic }, nil, nil)
	cl := mustClient(t, addr, parentID.PublicHex(), childID)

	// The position this node held under the pre-scoping name: past cmd1, in
	// front of cmd2. CursorAck is forward-only and answers false without moving,
	// so the seed is asserted rather than assumed.
	if !cs.CursorAck(legacyDownlinkCursor, downlinkStream, afterCmd1) {
		t.Fatal("seeding the legacy commands cursor did not move it — the precondition is a no-op " +
			"and everything below would pass for the wrong reason")
	}

	stop := make(chan struct{})
	done := make(chan struct{})
	go func() { defer close(done); RunDownlink(cl, ceng, nil, stop) }()
	t.Cleanup(func() {
		close(stop)
		waitForClosed(t, "RunDownlink to return after stop", done, 5*time.Second)
	})

	for _, want := range []string{"colca/v1/_CmdParam/m1/m1/cmd2", "colca/v1/_CmdParam/m1/m1/cmd3"} {
		select {
		case topic := <-delivered:
			if topic != want {
				t.Fatalf("delivered %q, want %q — the legacy position was not adopted, so this start "+
					"took the parent's head (%d) instead and dropped every command that queued while "+
					"the node was down for its upgrade", topic, want, head)
			}
		case <-time.After(20 * time.Second):
			t.Fatalf("%s was never delivered — the legacy position was not adopted, so this start took "+
				"the parent's head (%d) instead and dropped every command that queued while the node "+
				"was down for its upgrade", want, head)
		}
	}

	waitFor(t, "the scoped commands cursor to carry the adopted position forward", 20*time.Second, func() bool {
		return cs.CursorGet(uns.DownlinkCursor(cl.ParentPub()), downlinkStream) == head
	})
	waitFor(t, "the legacy commands cursor to be gone — no compatibility path may stay behind",
		20*time.Second, func() bool { return !cursorPresent(cs, legacyDownlinkCursor, downlinkStream) })
}

// Design §3.5, the definitions half — the third cursor, and the one neither of
// its siblings speaks for: definitions ride their own name under a first-contact
// rule of their own (start at 1, never at a head), so an adoption that works for
// commands says nothing about it.
//
// The parent's definitions stream is deliberately EMPTY, which is what makes the
// adopted position mean something: nothing this child could consume can explain
// a cursor at 4, so the value can only have come from the legacy key.
func TestALegacyDefinitionsCursorIsAdoptedOnceThenGone(t *testing.T) {
	dir := t.TempDir()
	parentID := mustIdentity(t, filepath.Join(dir, "p.key"))
	childID := mustIdentity(t, filepath.Join(dir, "c.key"))

	ps := mustStore(t, filepath.Join(dir, "pdata"))
	pcfg := &config.Config{ULID: "n-parent", Repl: config.Endpoint{Addr: "127.0.0.1:0"}}
	preg, peng := nodeParts(t, ps, pcfg, nil, nil, nil, childSpec{"n-child", childID.PublicHex(), "child1"})
	srv, addr := startServer(t, pcfg, peng, parentID, preg)
	defer srv.Stop()
	if got := ps.NextOffset("definitions"); got != 1 {
		t.Fatalf("parent definitions head = %d, want the empty-stream 1 — with definitions waiting, a "+
			"cursor above 1 could be explained by consumption instead of by adoption", got)
	}

	cs := mustStore(t, filepath.Join(dir, "cdata"))
	_, ceng := nodeParts(t, cs, &config.Config{ULID: "n-child"}, nil, nil, nil)
	cl := mustClient(t, addr, parentID.PublicHex(), childID)

	const legacyPos = 4
	if !cs.CursorAck(legacyDownlinkDefCursor, downlinkDefStream, legacyPos) {
		t.Fatal("seeding the legacy definitions cursor did not move it — the precondition is a no-op")
	}

	stop := make(chan struct{})
	done := make(chan struct{})
	go func() { defer close(done); RunDownlink(cl, ceng, nil, stop) }()
	t.Cleanup(func() {
		close(stop)
		waitForClosed(t, "RunDownlink to return after stop", done, 5*time.Second)
	})

	waitFor(t, "the legacy definitions position to appear under the scoped name", 20*time.Second, func() bool {
		return cs.CursorGet(uns.DownlinkDefCursor(cl.ParentPub()), downlinkDefStream) == legacyPos
	})
	// Waited for, not asserted once: adoption writes the scoped position and
	// deletes the legacy key as two store calls, so the value arriving under
	// the new name says nothing yet about the old one being gone.
	waitFor(t, "the legacy definitions cursor to be gone — no compatibility path may stay behind",
		20*time.Second, func() bool { return !cursorPresent(cs, legacyDownlinkDefCursor, downlinkDefStream) })
}

// cursorPresent reports whether a cursor KEY exists, which CursorGet cannot
// answer: it returns 1 both for a cursor at the first offset and for no cursor
// at all.
func cursorPresent(st *store.Store, name, stream string) bool {
	for _, c := range st.Cursors() {
		if c.Name == name && c.Stream == stream {
			return true
		}
	}
	return false
}

// Design §3.3, the mixed-version case: a parent that predates the `head` field
// answers hello without one. That is what a leaf-first rolling upgrade produces,
// and the child cannot repair it — with no head there is nothing to adopt, so
// its command cursor stays at the default 1 and the first poll hands it the
// pre-attachment commands §3.2 refuses.
//
// Behaviour is deliberately unchanged; what is pinned here is that the
// degradation is VISIBLE. The §7 diagnostic cannot speak for this case (it needs
// a head of its own to compare against) and hello runs once per process, so
// without this report the node is silently wrong forever.
func TestAParentThatAnswersHelloWithoutAHeadIsReported(t *testing.T) {
	dir := t.TempDir()
	parentID := mustIdentity(t, filepath.Join(dir, "p.key"))
	childID := mustIdentity(t, filepath.Join(dir, "c.key"))
	addr := preScopedParent(t, parentID)

	cs := mustStore(t, filepath.Join(dir, "cdata"))
	cm := metrics.New(cs, config.Retention{}, nil)
	_, ceng := nodeParts(t, cs, &config.Config{ULID: "n-child"}, nil, nil, nil)
	cl := mustClient(t, addr, parentID.PublicHex(), childID)

	stop := make(chan struct{})
	done := make(chan struct{})
	go func() { defer close(done); RunDownlink(cl, ceng, cm, stop) }()
	t.Cleanup(func() {
		close(stop)
		waitForClosed(t, "RunDownlink to return after stop", done, 5*time.Second)
	})

	const absent = `colca_downlink_head_absent_total`
	waitFor(t, "the missing head to be reported", 20*time.Second, func() bool {
		return scrapeMetric(t, cm, absent) == 1
	})

	// And the cursor is left exactly as it was found. Both halves matter: the
	// POSITION is the default 1 (so the poll reads from the start, which is the
	// unchanged behaviour), and no KEY exists (so nothing was claimed from a head
	// this parent never sent, and a later parent that does send one still counts
	// as first contact).
	if got := cs.CursorGet(uns.DownlinkCursor(cl.ParentPub()), downlinkStream); got != 1 {
		t.Fatalf("commands cursor = %d, want the default 1 — a parent that sent no head cannot have "+
			"taught this node a position", got)
	}
	for _, c := range cs.Cursors() {
		if c.Name == uns.DownlinkCursor(cl.ParentPub()) && c.Stream == downlinkStream {
			t.Fatalf("a commands cursor was recorded (%s × %s = %d) against a parent that sent no head",
				c.Name, c.Stream, c.Position)
		}
	}
}

// preScopedParent is a parent from BEFORE this design: it speaks the downlink
// wire protocol exactly as it did then, which is to say without `head`. A real
// server cannot stand in for it — NextOffset never answers 0, so the field is
// always present — and the point of the test is the field's ABSENCE.
//
// Nothing but TLS identity is borrowed from the real thing: the client pins the
// parent's public key and verifies no CA, so presenting that identity's own
// self-signed certificate is the whole handshake.
func preScopedParent(t *testing.T, id *identity.Identity) string {
	t.Helper()
	cert, err := id.SelfSignedCert("colca-parent")
	if err != nil {
		t.Fatalf("parent cert: %v", err)
	}
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// An ordinary poll answers nothing, at a long poll's pace: without the
		// delay the loop under test would spin on an empty answer for the whole
		// test.
		if r.URL.Query().Get("hello") != "1" {
			time.Sleep(200 * time.Millisecond)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"records":[],"next":1,"now_ms":` +
			strconv.FormatInt(time.Now().UnixMilli(), 10) + `}`))
	}))
	srv.TLS = &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS13}
	srv.StartTLS()
	t.Cleanup(srv.Close)
	return strings.TrimPrefix(srv.URL, "https://")
}

// Design §7, the one diagnostic head buys on an EXISTING cursor. A scoped
// commands cursor sitting past the parent's head is a node that will hear
// nothing until the parent's stream grows past it — the parent pruned past this
// position, or was rebuilt from empty. Today that node waits silently forever.
//
// Detection only. The cursor must NOT be rewound: a shorter parent stream is
// also exactly what a legitimately pruned parent looks like, and rewinding
// would re-deliver commands that already ran.
func TestExistingCommandsCursorPastTheParentsHeadIsReported(t *testing.T) {
	dir := t.TempDir()
	parentID := mustIdentity(t, filepath.Join(dir, "p.key"))
	childID := mustIdentity(t, filepath.Join(dir, "c.key"))

	ps := mustStore(t, filepath.Join(dir, "pdata"))
	pcfg := &config.Config{ULID: "n-parent", Repl: config.Endpoint{Addr: "127.0.0.1:0"}}
	preg, peng := nodeParts(t, ps, pcfg, nil, nil, nil, childSpec{"n-child", childID.PublicHex(), "child1"})
	srv, addr := startServer(t, pcfg, peng, parentID, preg)
	defer srv.Stop()
	if head := ps.NextOffset("commands"); head != 1 {
		t.Fatalf("parent commands head = %d, want the empty-stream 1 — this test needs a parent "+
			"the child's cursor can be past", head)
	}

	cs := mustStore(t, filepath.Join(dir, "cdata"))
	cm := metrics.New(cs, config.Retention{}, nil)
	_, ceng := nodeParts(t, cs, &config.Config{ULID: "n-child"}, nil, nil, nil)
	cl := mustClient(t, addr, parentID.PublicHex(), childID)

	const stranded = 5
	if !cs.CursorAck(uns.DownlinkCursor(cl.ParentPub()), downlinkStream, stranded) {
		t.Fatal("seeding the stranded commands cursor did not move it — the precondition is a no-op")
	}

	stop := make(chan struct{})
	done := make(chan struct{})
	go func() { defer close(done); RunDownlink(cl, ceng, cm, stop) }()
	t.Cleanup(func() {
		close(stop)
		waitForClosed(t, "RunDownlink to return after stop", done, 5*time.Second)
	})

	const beyond = `colca_downlink_cursor_beyond_head_total`
	waitFor(t, "the stranded cursor to be reported", 20*time.Second, func() bool {
		return scrapeMetric(t, cm, beyond) == 1
	})
	if got := cs.CursorGet(uns.DownlinkCursor(cl.ParentPub()), downlinkStream); got != stranded {
		t.Fatalf("the stranded cursor moved to %d — detection must never rewind it, because a "+
			"shorter parent stream is also what a legitimately pruned parent looks like", got)
	}
}
