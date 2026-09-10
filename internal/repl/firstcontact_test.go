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

// On first contact with a parent it has no cursor for, the child offers
// everything it still retains, starting at each stream's LWM. The loop's LWM
// clamp would reach the same records from position 1 but would report a gap
// that never happened, so the metric is the real assertion.
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

	// Retention removed offsets 1..3 while this node was attached elsewhere: temp3
	// and temp4 survive, and there is no cursor for the new parent.
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

// Commands are the opposite of uplink: one issued before this child attached
// was meant for whatever held the mount then, so first contact adopts the
// parent's head. The post-attachment command shows this is a decision, not a
// dead loop: the stream is read in order, so "post" arriving without "pre"
// means "pre" was skipped on purpose.
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

// Head adoption must survive a parent that is not up yet, the normal case for a
// node that boots first. A failed hello is retried: polling without the head
// would read from 1 and deliver every command issued before the child attached.
// The test starts the loop against a stopped parent and brings it back on the
// same address with a command waiting.
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

// Head adoption is for a parent this child never met, not one it met while its
// commands stream was empty. CursorGet returns 1 for both, but the second must
// still receive what was queued while the child was down. The test attaches to
// a fresh parent, stops, issues a command and comes back.
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

// Existing unscoped cursors are adopted once under the scoped names and then
// deleted. Adoption must carry the value, so the parent's contents are checked,
// and the legacy key must be gone afterwards.
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

// The commands half of adoption, which every existing deployment goes through
// once. If adoption fails the code jumps to the parent's head, silently dropping
// commands queued during the upgrade, so the test asserts the position: cmd2 is
// unreachable from the head.
func TestALegacyCommandsCursorIsAdoptedInsteadOfTheParentsHead(t *testing.T) {
	dir := t.TempDir()
	parentID := mustIdentity(t, filepath.Join(dir, "p.key"))
	childID := mustIdentity(t, filepath.Join(dir, "c.key"))

	ps := mustStore(t, filepath.Join(dir, "pdata"))
	pcfg := &config.Config{ULID: "n-parent", Repl: config.Endpoint{Addr: "127.0.0.1:0"}}
	preg, peng := nodeParts(t, ps, pcfg, nil, nil, nil, childSpec{"n-child", childID.PublicHex(), "child1"})
	srv, addr := startServer(t, pcfg, peng, parentID, preg)
	defer srv.Stop()

	// Three commands issued before the loop runs: cmd1 was delivered under the old
	// cursor, cmd2 and cmd3 queued while the node was being upgraded.
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

	// The position under the old name: past cmd1, before cmd2. CursorAck is
	// forward-only and returns false without moving, so the seed is checked.
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

// The definitions half: definitions have their own name and first-contact rule
// (start at 1), so the commands case says nothing about them. The parent's
// definitions stream is empty, so a cursor at 4 can only come from the legacy
// key.
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
	// Wait rather than assert once: adoption writes the new position and deletes
	// the old key in two store calls.
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

// A parent older than the head field answers hello without one, as during a
// leaf-first rolling upgrade. The child cannot fix that: its command cursor
// stays at 1 and the first poll delivers pre-attachment commands. Behaviour is
// unchanged; the test pins that the problem is logged, since nothing else would
// show it.
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

	// The cursor is left as found: position 1, so the poll reads from the start, and
	// no key, so a later parent with a head still counts as first contact.
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

// preScopedParent speaks the downlink protocol without head, as parents did
// before the field existed. A real server always sends head, so it cannot stand
// in. Only the TLS identity is borrowed: the client pins the parent's key and
// checks no CA.
func preScopedParent(t *testing.T, id *identity.Identity) string {
	t.Helper()
	cert, err := id.SelfSignedCert("colca-parent")
	if err != nil {
		t.Fatalf("parent cert: %v", err)
	}
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// An ordinary poll answers nothing at a long poll's pace, so the loop under
		// test does not spin.
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

// A commands cursor past the parent's head (the parent pruned past it or was
// rebuilt empty) would leave the node hearing nothing, silently. It must be
// reported, and not rewound: a legitimately pruned parent looks the same, and
// rewinding would re-deliver commands that already ran.
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
