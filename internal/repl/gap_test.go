package repl

import (
	"bytes"
	"encoding/base64"
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
	"github.com/alpamayo-solutions/colca/internal/store"
)

// parentFixture is the standard mTLS parent + one registered child.
type parentFixture struct {
	ps      *store.Store
	pcfg    *config.Config
	peng    *engine.Engine
	pid     *identity.Identity
	cid     *identity.Identity
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
	pcfg := &config.Config{ULID: "n-parent", Repl: config.Endpoint{Addr: "127.0.0.1:0"},
		Children: []config.Child{{ULID: "n-child", Pubkey: childID.PublicHex(), Mount: "child1"}}}
	peng := engine.New(ps, pcfg, nil, nil)
	srv, addr := startServer(t, pcfg, peng, parentID)
	t.Cleanup(srv.Stop)
	return &parentFixture{
		ps: ps, pcfg: pcfg, peng: peng, pid: parentID, cid: childID,
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

	resp, err := f.cl.http.Get("https://" + f.addr + "/downlink?after=1&max=10")
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("downlink: %d %v", resp.StatusCode, err)
	}
	b64 := func(i int) string {
		return base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf(`{"correlation_id":"c%d","expires_at":99999999999}`, i)))
	}
	// Mount-stripped topics, parent offsets, records beginning at the LWM.
	want := `{"gap":{"stream":"commands","from_offset":1,"to_offset":2,"first_ts":10,"last_ts":20,"approx":false},` +
		`"next":5,"records":[` +
		`{"o":3,"t":"colca/v1/_CmdParam/m1/m1/go","p":"` + b64(3) + `","ts":30},` +
		`{"o":4,"t":"colca/v1/_CmdParam/m1/m1/go","p":"` + b64(4) + `","ts":40}]}` + "\n"
	if string(body) != want {
		t.Fatalf("downlink gap wire shape:\n got %s\nwant %s", body, want)
	}

	// At or past the LWM: no gap key.
	resp, err = f.cl.http.Get("https://" + f.addr + "/downlink?after=3&max=10")
	if err != nil {
		t.Fatal(err)
	}
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	want = `{"next":5,"records":[` +
		`{"o":3,"t":"colca/v1/_CmdParam/m1/m1/go","p":"` + b64(3) + `","ts":30},` +
		`{"o":4,"t":"colca/v1/_CmdParam/m1/m1/go","p":"` + b64(4) + `","ts":40}]}` + "\n"
	if string(body) != want {
		t.Fatalf("downlink without gap:\n got %s\nwant %s", body, want)
	}
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
	pcfg := &config.Config{ULID: "n-parent", Repl: config.Endpoint{Addr: "127.0.0.1:0"},
		Children: []config.Child{{ULID: "n-child", Pubkey: childID.PublicHex(), Mount: "child1"}}}
	peng := engine.New(ps, pcfg, nil, nil)

	// Start once only to obtain a real address, then stop: the parent is down.
	srv1, addr := startServer(t, pcfg, peng, parentID)
	srv1.Stop()
	pcfg.Repl.Addr = addr

	cs := mustStore(t, filepath.Join(dir, "cdata"))
	ceng := engine.New(cs, &config.Config{ULID: "n-child"}, nil, nil)
	for i := 1; i <= 5; i++ {
		mustIngestAdmin(t, ceng, "colca/v1/_Metric/m1/m1/temp", fmt.Sprintf(`{"v":%d}`, i))
	}
	cl := mustClient(t, addr, parentID.PublicHex(), childID)

	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		RunUplink(cl, ceng, nil, stop)
	}()
	defer func() {
		close(stop)
		waitForClosed(t, "RunUplink to return after stop", done, 5*time.Second)
	}()
	time.Sleep(300 * time.Millisecond) // pushes are failing; cursor pinned at 1

	// Retention overrides the uplink cursor (the §5.2 opt-in already decided):
	// offsets 1..3 are gone, LWM 4.
	if n, err := cs.Prune("metrics", 4, []string{uplinkCursor}, nil); err != nil || n != 3 {
		t.Fatalf("prune: %d %v", n, err)
	}

	// Parent returns on the same address; the loop must jump 1 → 4 and push
	// the survivors — never stall on the pruned range.
	srv2, err := NewServer(pcfg, peng, parentID)
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
		return cs.CursorGet(uplinkCursor, "metrics") == 6
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
// stalls, nothing propagates further down.
func TestRunDownlinkContinuesPastGap(t *testing.T) {
	f := newParentFixture(t)
	seedParentCommands(t, f.ps, 3)
	if n, err := f.ps.Prune("commands", 3, nil, nil); err != nil || n != 2 {
		t.Fatalf("prune: %d %v", n, err)
	}

	dir := t.TempDir()
	cs := mustStore(t, filepath.Join(dir, "cdata"))
	ceng := engine.New(cs, &config.Config{ULID: "n-child"}, nil, nil)

	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		RunDownlink(f.cl, ceng, nil, stop)
	}()
	waitFor(t, "the downlink cursor to advance past the hole", 10*time.Second, func() bool {
		return cs.CursorGet(downlinkCursor, downlinkStream) == 4
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
}

// RunUplink's commands filter must pass _StreamGap in
// addition to _Ack — the durable §6.4 marker for a pruned commands stream
// travels in that very stream, and dropping it would break the
// replicates-upward guarantee. Ordinary commands still never travel up.
func TestUplinkPassesStreamGapMarkerButNotCommands(t *testing.T) {
	f := newParentFixture(t)

	dir := t.TempDir()
	cs := mustStore(t, filepath.Join(dir, "cdata"))
	ceng := engine.New(cs, &config.Config{ULID: "n-child"}, nil, nil)
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
		RunUplink(f.cl, ceng, nil, stop)
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
	ceng := engine.New(cs, &config.Config{ULID: "n-child"}, nil, nil)
	for i := 1; i <= 3; i++ {
		mustIngestAdmin(t, ceng, "colca/v1/_Metric/m1/m1/temp", fmt.Sprintf(`{"v":%d}`, i))
	}
	if n, err := cs.Prune("metrics", 4, []string{uplinkCursor}, nil); err != nil || n != 3 {
		t.Fatalf("prune: %d %v", n, err)
	}

	// Parent never reachable: the port is allocated and released immediately.
	cl := mustClient(t, unreachableAddr(t), parentID.PublicHex(), childID)
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		RunUplink(cl, ceng, nil, stop)
	}()
	waitFor(t, "the uplink cursor to jump to the LWM", 5*time.Second, func() bool {
		return cs.CursorGet(uplinkCursor, "metrics") == 4
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
