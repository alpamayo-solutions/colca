package repl

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/engine"
	"github.com/alpamayo-solutions/colca/internal/identity"
	"github.com/alpamayo-solutions/colca/internal/metrics"
	"github.com/alpamayo-solutions/colca/internal/metrics/metricstest"
	"github.com/alpamayo-solutions/colca/internal/store"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// parentFixture is the standard mTLS parent + one registered child. pm is the
// parent's own metrics registry (design §8, e.g. colca_gap_served_total —
// wired for every fixture since it is pure observability and changes no
// behavior the other gap_test.go tests assert on).
type parentFixture struct {
	ps      *store.Store
	pcfg    *config.Config
	peng    *engine.Engine
	pid     *identity.Identity
	cid     *identity.Identity
	pm      *metrics.Metrics
	srv     *Server
	addr    string
	cl      *Client
	childID string
}

func newParentFixture(t *testing.T) *parentFixture {
	t.Helper()
	dir := t.TempDir()
	parentID := mustIdentity(t, filepath.Join(dir, "p.key"))
	childID := mustIdentity(t, filepath.Join(dir, "c.key"))
	ps := mustStore(t, filepath.Join(dir, "pdata"))
	pcfg := &config.Config{ULID: "n-parent", Repl: config.Endpoint{Addr: "127.0.0.1:0"}}
	preg, peng := nodeParts(t, ps, pcfg, nil, nil, nil, childSpec{"n-child", childID.PublicHex(), "child1"})
	pm := metrics.New(ps, config.Retention{}, nil)
	srv, addr := startServerWithMetrics(t, pcfg, peng, parentID, preg, pm)
	t.Cleanup(srv.Stop)
	return &parentFixture{
		ps: ps, pcfg: pcfg, peng: peng, pid: parentID, cid: childID, pm: pm,
		srv: srv, addr: addr,
		cl: mustClient(t, addr, parentID.PublicHex(), childID), childID: "n-child",
	}
}

// seedParentCommands appends n commands for the child's mount with
// deterministic timestamps 10, 20, … directly through the store, so the wire
// test can assert exact JSON.
func seedParentCommands(t *testing.T, ps *store.Store, n int) {
	t.Helper()
	var recs []store.Record
	for i := 1; i <= n; i++ {
		recs = append(recs, store.Record{
			Topic:   "colca/v1/_CmdParam/m1/child1/m1/go",
			Payload: []byte(fmt.Sprintf(`{"correlation_id":"c%d","expires_at":99999999999}`, i)),
			TS:      int64(i) * 10,
		})
	}
	if _, _, err := ps.Append("commands", recs); err != nil {
		t.Fatal(err)
	}
}

// Spec §6.2: GET /downlink gains the identical gap object under the same
// condition — the child's poll position below the LWM of the parent's
// commands stream — with offsets in PARENT coordinates. The response is a
// wire contract, so the assertion is exact-JSON (raw request through the
// client's own mTLS transport).
func TestDownlinkGapExactWireShape(t *testing.T) {
	f := newParentFixture(t)
	seedParentCommands(t, f.ps, 4) // offsets 1..4, TS 10..40
	if n, err := f.ps.Prune("commands", 3, nil, nil); err != nil || n != 2 {
		t.Fatalf("prune: %d %v", n, err)
	}

	before := time.Now().UnixMilli()
	resp, err := f.cl.http.Get("https://" + f.addr + "/downlink?after=1&max=10")
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	after := time.Now().UnixMilli()
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("downlink: %d %v", resp.StatusCode, err)
	}
	b64 := func(i int) string {
		return base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf(`{"correlation_id":"c%d","expires_at":99999999999}`, i)))
	}
	// now_ms (time-sync design §2.1) is a live timestamp — checked
	// separately for plausibility and stripped before the exact-shape
	// comparison of everything else on the wire.
	rest, nowMS := stripNowMS(t, body)
	if nowMS < before || nowMS > after {
		t.Fatalf("now_ms %d outside [%d, %d] — the parent is root here, so it must stamp its own raw wall clock", nowMS, before, after)
	}
	// Mount-stripped topics, parent offsets, records beginning at the LWM.
	// Every response also carries the definitions half (definition-stream
	// design §5) — empty here, and its own cursor, independent of the command
	// one. No `head`: that field belongs to the hello response alone
	// (parent-scoped-cursors design §3.3), and this exact-shape comparison is
	// what keeps it from drifting back onto the ordinary poll.
	want := `{"def_next":1,"definitions":[],"gap":{"stream":"commands","from_offset":1,"to_offset":2,"first_ts":10,"last_ts":20,"approx":false},` +
		`"next":5,"records":[` +
		`{"o":3,"t":"colca/v1/_CmdParam/m1/m1/go","p":"` + b64(3) + `","ts":30},` +
		`{"o":4,"t":"colca/v1/_CmdParam/m1/m1/go","p":"` + b64(4) + `","ts":40}]}`
	if rest != want {
		t.Fatalf("downlink gap wire shape:\n got %s\nwant %s", rest, want)
	}

	// At or past the LWM: no gap key.
	resp, err = f.cl.http.Get("https://" + f.addr + "/downlink?after=3&max=10")
	if err != nil {
		t.Fatal(err)
	}
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	rest, _ = stripNowMS(t, body)
	want = `{"def_next":1,"definitions":[],"next":5,"records":[` +
		`{"o":3,"t":"colca/v1/_CmdParam/m1/m1/go","p":"` + b64(3) + `","ts":30},` +
		`{"o":4,"t":"colca/v1/_CmdParam/m1/m1/go","p":"` + b64(4) + `","ts":40}]}`
	if rest != want {
		t.Fatalf("downlink without gap:\n got %s\nwant %s", rest, want)
	}
}

// stripNowMS decodes body, removes the now_ms key, and re-marshals
// deterministically (encoding/json sorts map keys) so exact-wire-shape
// assertions can check everything EXCEPT the live timestamp, which callers
// verify separately for plausibility.
func stripNowMS(t *testing.T, body []byte) (rest string, nowMS int64) {
	t.Helper()
	var m map[string]json.RawMessage
	if err := json.Unmarshal(body, &m); err != nil {
		t.Fatalf("decode response %s: %v", body, err)
	}
	raw, ok := m["now_ms"]
	if !ok {
		t.Fatalf("response missing now_ms (time-sync design §2.1): %s", body)
	}
	if err := json.Unmarshal(raw, &nowMS); err != nil {
		t.Fatalf("now_ms not an int64: %v", err)
	}
	delete(m, "now_ms")
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("re-marshal without now_ms: %v", err)
	}
	return string(b), nowMS
}

// A gap answers the long poll immediately, and when no records survived the
// prune, next points past the hole (the LWM) — otherwise the child could
// never ack and would re-receive the gap forever.
func TestDownlinkGapOnlyResponseIsPromptAndPointsPastHole(t *testing.T) {
	f := newParentFixture(t)
	seedParentCommands(t, f.ps, 2)
	if n, err := f.ps.Prune("commands", 3, nil, nil); err != nil || n != 2 {
		t.Fatalf("prune: %d %v", n, err)
	}

	start := time.Now()
	recs, next, gap, err := f.cl.Downlink(1, 10, 20*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if el := time.Since(start); el > 5*time.Second {
		t.Fatalf("gap-only downlink took %v — a gap must answer the poll immediately, not ride out the long poll", el)
	}
	if len(recs) != 0 {
		t.Fatalf("no records survived the prune, got %+v", recs)
	}
	if gap == nil || gap.FromOffset != 1 || gap.ToOffset != 2 {
		t.Fatalf("gap = %+v, want [1..2]", gap)
	}
	if next != 3 {
		t.Fatalf("next = %d, want 3 (the LWM): next must point past the hole even with no surviving records", next)
	}
}

// Spec §6.3, uplink half: the local pruner overrode the uplink cursor while
// the parent was down (explicit §5.2 opt-in). The loop must not stall — it
// jumps the cursor to the LWM and delivers everything that survived once the
// parent returns. Reuses the offline-buffering mTLS pattern: prune under a
// live uplink loop with the parent down, restart the parent on the same
// address, assert convergence.
func TestUplinkJumpsPastPrunedCursorAndConverges(t *testing.T) {
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
	cm := metrics.New(cs, config.Retention{}, nil)
	_, ceng := nodeParts(t, cs, &config.Config{ULID: "n-child"}, nil, nil, nil)
	for i := 1; i <= 5; i++ {
		mustIngestAdmin(t, ceng, "colca/v1/_Metric/m1/m1/temp", fmt.Sprintf(`{"v":%d}`, i))
	}
	cl := mustClient(t, addr, parentID.PublicHex(), childID)

	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		RunUplink(cl, ceng, nil, cm, stop)
	}()
	defer func() {
		close(stop)
		waitForClosed(t, "RunUplink to return after stop", done, 5*time.Second)
	}()
	time.Sleep(300 * time.Millisecond) // pushes are failing; cursor pinned at 1

	// Retention overrides the uplink cursor (the §5.2 opt-in already decided):
	// offsets 1..3 are gone, LWM 4.
	if n, err := cs.Prune("metrics", 4, []string{uns.UplinkCursor(parentID.PublicHex())}, nil); err != nil || n != 3 {
		t.Fatalf("prune: %d %v", n, err)
	}

	// design §8: the loop's own jump over the pruned range counts
	// colca_gap_received_total{stream="metrics"} exactly once — independent
	// of the parent being reachable yet.
	const gapReceived = `colca_gap_received_total{stream="metrics"}`
	waitFor(t, "the uplink jump to count colca_gap_received_total", 5*time.Second, func() bool {
		return metricstest.Value(t, cm, gapReceived) == 1
	})

	// Parent returns on the same address; the loop must jump 1 → 4 and push
	// the survivors — never stall on the pruned range.
	srv2, err := NewServer(pcfg, peng, parentID, preg, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := srv2.Start(); err != nil {
		t.Fatalf("restart: %v", err)
	}
	defer srv2.Stop()

	waitFor(t, "the surviving records to reach the parent", 10*time.Second, func() bool {
		return ps.NextOffset("metrics") == 3 // exactly offsets 4 and 5 arrived
	})
	waitFor(t, "the uplink cursor to converge past the head", 5*time.Second, func() bool {
		return cs.CursorGet(uns.UplinkCursor(cl.ParentPub()), "metrics") == 6
	})
	recs, _, err := ps.Read("metrics", 1, 10, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 2 || string(recs[0].Payload) != `{"v":4}` || string(recs[1].Payload) != `{"v":5}` {
		t.Fatalf("parent must hold exactly the survivors {v:4},{v:5}: %+v", recs)
	}
	if hwm := ps.HWMGet("n-child", "metrics"); hwm != 5 {
		t.Fatalf("parent HWM = %d, want 5", hwm)
	}
}

// Spec §6.3, downlink half: on receiving a gap object the child logs,
// continues past the hole, and ingests the surviving commands — nothing
// stalls, nothing propagates further down. Also design §8: the parent's
// gap-carrying /downlink response counts colca_gap_served_total{stream=
// "commands",surface="downlink"} and the child's handling of it counts
// colca_gap_received_total{stream="commands"} — the two ends of the same
// wire event, on two different registries.
func TestRunDownlinkContinuesPastGap(t *testing.T) {
	f := newParentFixture(t)
	seedParentCommands(t, f.ps, 3)
	if n, err := f.ps.Prune("commands", 3, nil, nil); err != nil || n != 2 {
		t.Fatalf("prune: %d %v", n, err)
	}

	dir := t.TempDir()
	cs := mustStore(t, filepath.Join(dir, "cdata"))
	cm := metrics.New(cs, config.Retention{}, nil)
	_, ceng := nodeParts(t, cs, &config.Config{ULID: "n-child"}, nil, nil, nil)
	// The claim is about a child whose POSITION lies inside the hole, which is
	// a child that was already attached here — a first-contact child adopts the
	// parent's head instead and never meets the hole at all (§3.2).
	attachAt(t, cs, f.cl, 2)

	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		RunDownlink(f.cl, ceng, cm, stop)
	}()
	waitFor(t, "the downlink cursor to advance past the hole", 10*time.Second, func() bool {
		return cs.CursorGet(uns.DownlinkCursor(f.cl.ParentPub()), downlinkStream) == 4
	})
	close(stop)
	waitForClosed(t, "RunDownlink to return after stop", done, 5*time.Second)

	// Exactly the surviving command was ingested locally, mount-stripped.
	recs, _, err := cs.Read("commands", 1, 10, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 1 || recs[0].Topic != "colca/v1/_CmdParam/m1/m1/go" {
		t.Fatalf("child commands = %+v, want exactly the one surviving command", recs)
	}

	if v := metricstest.Value(t, f.pm, `colca_gap_served_total{stream="commands",surface="downlink"}`); v != 1 {
		t.Fatalf("parent colca_gap_served_total{stream=commands,surface=downlink} = %v, want 1", v)
	}
	if v := metricstest.Value(t, cm, `colca_gap_received_total{stream="commands"}`); v != 1 {
		t.Fatalf("child colca_gap_received_total{stream=commands} = %v, want 1", v)
	}
}

// RunUplink's commands filter must pass _StreamGap in
// addition to _Ack — the durable §6.4 marker for a pruned commands stream
// travels in that very stream, and dropping it would break the
// replicates-upward guarantee. Ordinary commands still never travel up.
func TestUplinkPassesStreamGapMarkerButNotCommands(t *testing.T) {
	f := newParentFixture(t)

	dir := t.TempDir()
	cs := mustStore(t, filepath.Join(dir, "cdata"))
	_, ceng := nodeParts(t, cs, &config.Config{ULID: "n-child"}, nil, nil, nil)
	// Child's commands stream: a command (must stay), a gap marker and an ack
	// (both must travel). Appended through the store: the marker is written by
	// the pruner's prune batch in production, not through an ingest path.
	if _, _, err := cs.Append("commands", []store.Record{
		{Topic: "colca/v1/_CmdParam/m1/m1/go", Payload: []byte(`{"correlation_id":"c1","expires_at":99999999999}`), TS: 1},
		{Topic: "colca/v1/_StreamGap/n-child/commands", Payload: []byte(`{"stream":"commands","from_offset":1,"to_offset":9,"first_ts":1,"last_ts":9,"overridden_cursors":["downlink:x"]}`), TS: 2},
		{Topic: "colca/v1/_Ack/m1/m1/go", Payload: []byte(`{"correlation_id":"c1","result_code":0}`), TS: 3},
	}); err != nil {
		t.Fatal(err)
	}

	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		RunUplink(f.cl, ceng, nil, nil, stop)
	}()
	waitFor(t, "the marker and the ack to reach the parent", 10*time.Second, func() bool {
		return f.ps.NextOffset("commands") == 3
	})
	// Let the loop idle once more: a leaked _CmdParam would arrive now.
	time.Sleep(300 * time.Millisecond)
	close(stop)
	waitForClosed(t, "RunUplink to return after stop", done, 5*time.Second)

	recs, _, err := f.ps.Read("commands", 1, 10, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 2 {
		t.Fatalf("parent commands = %+v, want exactly marker + ack", recs)
	}
	if recs[0].Topic != "colca/v1/_StreamGap/n-child/child1/commands" {
		t.Fatalf("marker topic = %q, want it mount-inserted", recs[0].Topic)
	}
	if recs[1].Topic != "colca/v1/_Ack/m1/child1/m1/go" {
		t.Fatalf("ack topic = %q", recs[1].Topic)
	}
}

// A marker that already crossed one hop crosses the next one too.
//
// Retention design §9 expects the root's stream to hold the edges' markers
// "with mount-inserted provenance", which means a middle node offers its
// child's marker — stored under that child's mount — to its own parent. The
// door used to bind a marker to its stream by the WHOLE path, which is the
// stream name only at the authoring node: at the grandparent the path reads
// `leaf1/metrics`, the record came back 403, and RunUplink kept re-sending
// the same batch forever with every record behind it, the whole subtree's
// _Acks included.
func TestGapMarkerReplicatesPastTheFirstHop(t *testing.T) {
	f := newParentFixture(t)
	gap := []byte(`{"stream":"metrics","from_offset":1,"to_offset":9,"first_ts":1,"last_ts":9,"overridden_cursors":["uplink"]}`)
	// The shape a middle node holds after storing the marker its own leaf
	// pushed: authored at n-leaf, mounted under leaf1.
	hopped := uns.MountInsert("colca/v1/_StreamGap/n-leaf/metrics", "leaf1")

	if _, err := f.cl.Replicate("metrics", []store.ReplRecord{
		{ChildOffset: 1, Topic: hopped, Payload: gap, TS: 1},
	}); err != nil {
		t.Fatalf("a marker that already crossed one hop was refused: %v", err)
	}
	recs, _, err := f.ps.Read("metrics", 1, 10, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 1 || recs[0].Topic != "colca/v1/_StreamGap/n-leaf/child1/leaf1/metrics" {
		t.Fatalf("parent metrics = %+v, want the marker mount-inserted once more", recs)
	}
	// Still bound to the stream it names: the same marker offered on another
	// stream is refused, at every hop depth.
	if _, err := f.cl.Replicate("alarms", []store.ReplRecord{
		{ChildOffset: 2, Topic: hopped, Payload: gap, TS: 2},
	}); err == nil {
		t.Fatal("a metrics gap marker was accepted onto the alarms stream")
	}
}

// The §6.3 jump is the loop's own act, not a side effect of pushing: even
// with NOTHING left to push (everything pruned) and the parent unreachable,
// the uplink cursor must move to the LWM and the ERROR surface must fire —
// otherwise the override would go unnoticed until new data happens to arrive.
func TestUplinkJumpsEvenWithNothingToPush(t *testing.T) {
	logs := captureLogs(t)
	dir := t.TempDir()
	parentID := mustIdentity(t, filepath.Join(dir, "p.key"))
	childID := mustIdentity(t, filepath.Join(dir, "c.key"))

	cs := mustStore(t, filepath.Join(dir, "cdata"))
	_, ceng := nodeParts(t, cs, &config.Config{ULID: "n-child"}, nil, nil, nil)
	for i := 1; i <= 3; i++ {
		mustIngestAdmin(t, ceng, "colca/v1/_Metric/m1/m1/temp", fmt.Sprintf(`{"v":%d}`, i))
	}
	// The override this pins is the pruner passing a cursor that EXISTS: a node
	// that had already offered {v:1} to this parent. First contact is the other
	// case entirely — it is seeded at the LWM and has nothing to be overridden
	// (§3.2), which is why the position is planted here rather than left at the
	// default.
	if !cs.CursorAck(uns.UplinkCursor(parentID.PublicHex()), "metrics", 2) {
		t.Fatal("seeding the uplink cursor did not move it — the precondition is a no-op")
	}
	if n, err := cs.Prune("metrics", 4, []string{uns.UplinkCursor(parentID.PublicHex())}, nil); err != nil || n != 3 {
		t.Fatalf("prune: %d %v", n, err)
	}

	// Parent never reachable: the port is allocated and released immediately.
	cl := mustClient(t, unreachableAddr(t), parentID.PublicHex(), childID)
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		RunUplink(cl, ceng, nil, nil, stop)
	}()
	waitFor(t, "the uplink cursor to jump to the LWM", 5*time.Second, func() bool {
		return cs.CursorGet(uns.UplinkCursor(cl.ParentPub()), "metrics") == 4
	})
	close(stop)
	waitForClosed(t, "RunUplink to return after stop", done, 5*time.Second)

	if !strings.Contains(logs.String(), "uplink cursor below the stream LWM") {
		t.Fatalf("the §6.3 ERROR surface must fire on a jump:\n%s", logs.String())
	}
}

// captureLogs routes slog.Default through a buffer for the duration of the
// test; clients constructed AFTER the call log into it.
func captureLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return &buf
}

// unreachableAddr returns a loopback address nothing listens on.
func unreachableAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	return addr
}

// Spec §5.1 [delta]: the parent persists each child's downlink progress as an
// ordinary named cursor downlink:{child-ulid} on its commands stream — from
// the AUTHENTICATED identity plus the after parameter, forward-only, stamped
// like every CursorAck — and the store's prune clamp honors it like any other
// cursor.
func TestDownlinkPollPersistsChildCursorAndClampsPrune(t *testing.T) {
	f := newParentFixture(t)
	seedParentCommands(t, f.ps, 2)
	cursorName := uns.DownlinkCursorPrefix + f.childID

	find := func() (store.CursorInfo, bool) {
		for _, c := range f.ps.Cursors() {
			if c.Name == cursorName && c.Stream == "commands" {
				return c, true
			}
		}
		return store.CursorInfo{}, false
	}

	// A poll at the start position (after=1) is no advance: no cursor key is
	// created — same "acking is what buys protection" semantics as /fetch.
	if _, _, _, err := f.cl.Downlink(1, 10, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if c, ok := find(); ok {
		t.Fatalf("poll at position 1 must not create a cursor, got %+v", c)
	}

	// A poll reporting progress persists it immediately — before the long poll
	// parks (the request itself stays parked; only the cursor write matters).
	pollDone := make(chan struct{})
	go func() {
		defer close(pollDone)
		f.cl.Downlink(3, 10, 25*time.Second) //nolint:errcheck // killed by srv.Stop below
	}()
	waitFor(t, "the downlink cursor to be persisted", 5*time.Second, func() bool {
		c, ok := find()
		return ok && c.Position == 3
	})
	c, _ := find()
	if c.LastAdvanceMS == 0 {
		t.Fatal("the downlink cursor must carry the ct/ last-advance stamp (spec §5.2 staleness input)")
	}

	// A later poll with a LOWER after must not move it backwards.
	if _, _, _, err := f.cl.Downlink(2, 10, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if c, _ := find(); c.Position != 3 {
		t.Fatalf("cursor regressed to %d after a lower poll, want 3", c.Position)
	}

	// The pruner's clamp honors it: pruning the whole stream stops at the
	// slowest child's persisted position (store in-batch recheck, spec §5.2).
	seedParentCommands(t, f.ps, 2) // offsets 3..4, so there is something past the cursor
	if n, err := f.ps.Prune("commands", 5, nil, nil); err != nil || n != 2 {
		t.Fatalf("prune removed %d (%v), want 2 — clamped at the downlink cursor", n, err)
	}
	if lwm := f.ps.LWM("commands"); lwm != 3 {
		t.Fatalf("LWM = %d, want 3: the child's downlink cursor must clamp the prune", lwm)
	}

	f.srv.Stop()
	waitForClosed(t, "the parked poll to die with the server", pollDone, 5*time.Second)
}
