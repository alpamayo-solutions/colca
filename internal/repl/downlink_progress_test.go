package repl

import (
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/identity"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// b64Payload encodes a payload the way a wire record carries it.
func b64Payload(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }

// stubParent is a parent whose /downlink answers are written by the test, to pin
// what the child does with an answer from a buggy, older or empty parent.
type stubParent struct {
	polls  atomic.Int64
	answer func(poll int64) string
	addr   string
}

func newStubParent(t *testing.T, id *identity.Identity, answer func(poll int64) string) *stubParent {
	t.Helper()
	sp := &stubParent{answer: answer}
	cert, err := id.SelfSignedCert("stub-parent")
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /downlink", func(w http.ResponseWriter, r *http.Request) {
		n := sp.polls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, sp.answer(n))
	})
	srv := httptest.NewUnstartedServer(mux)
	srv.TLS = &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tls.RequireAnyClientCert,
		MinVersion:   tls.VersionTLS13,
	}
	srv.StartTLS()
	t.Cleanup(srv.Close)
	sp.addr = strings.TrimPrefix(srv.URL, "https://")
	return sp
}

// A poll that answers at once and leaves this node where it was is not repeated
// immediately. Whatever the cause, a position the child already holds or a
// record it could not apply, re-polling at once would spin at the rate limit
// (about 100 requests per second per child) for as long as the condition lasts.
func TestADownlinkPollWithNoProgressIsNotRepeatedImmediately(t *testing.T) {
	dir := t.TempDir()
	parentID := mustIdentity(t, filepath.Join(dir, "p.key"))
	childID := mustIdentity(t, filepath.Join(dir, "c.key"))
	parent := newStubParent(t, parentID, func(int64) string {
		return fmt.Sprintf(`{"records":[],"next":1,"definitions":[],"def_next":1,"head":1,"now_ms":%d}`,
			time.Now().UnixMilli())
	})

	cs := mustStore(t, filepath.Join(dir, "cdata"))
	_, ceng := nodeParts(t, cs, &config.Config{ULID: "n-child"}, nil, nil, nil)
	cl := mustClient(t, parent.addr, parentID.PublicHex(), childID)

	stop, done := make(chan struct{}), make(chan struct{})
	go func() { defer close(done); RunDownlink(cl, ceng, nil, stop) }()
	window := 6 * retryAfter // 3s of polling that can make no progress
	time.Sleep(window)
	close(stop)
	waitForClosed(t, "RunDownlink to stop", done, 5*time.Second)

	polls := parent.polls.Load()
	if polls < 2 {
		t.Fatalf("the child polled %d times in %s — it is not polling at all, so the bound below proves nothing",
			polls, window)
	}
	// One hello plus one poll per retryAfter, with slack for scheduling.
	if limit := int64(window/retryAfter) + 5; polls > limit {
		t.Fatalf("the child polled %d times in %s (at most %d expected) — a fruitless poll is repeated at once, "+
			"which is a busy loop at the parent's rate limit", polls, window, limit)
	}
}

// A command that fails to persist holds the cursor and is offered again on the
// next poll instead of being acked past. Here the parent's max_record_bytes is
// larger than this node's. Retrying is only safe because the loop backs off when
// nothing advanced, as pinned above.
func TestADownlinkCommandThatCannotBePersistedHoldsTheCursor(t *testing.T) {
	dir := t.TempDir()
	parentID := mustIdentity(t, filepath.Join(dir, "p.key"))
	childID := mustIdentity(t, filepath.Join(dir, "c.key"))

	big := strings.Repeat("x", 400)
	oversized := fmt.Sprintf(`{"correlation_id":"c1","expires_at":99999999999,"pad":%q}`, big)
	small := `{"correlation_id":"c2","expires_at":99999999999}`
	parent := newStubParent(t, parentID, func(int64) string {
		return fmt.Sprintf(`{"records":[`+
			`{"o":1,"t":"colca/v1/_CmdParam/n-child/m1/go","p":%q,"ts":10},`+
			`{"o":2,"t":"colca/v1/_CmdParam/n-child/m1/stop","p":%q,"ts":20}`+
			`],"next":3,"definitions":[],"def_next":1,"head":1,"now_ms":%d}`,
			b64Payload(oversized), b64Payload(small), time.Now().UnixMilli())
	})

	cs := mustStore(t, filepath.Join(dir, "cdata"))
	_, ceng := nodeParts(t, cs, &config.Config{ULID: "n-child"}, nil, nil, nil)
	// After the node's own startup writes: the first command does not fit
	// under this cap, the second does.
	cs.SetMaxRecordBytes(uint64(len(small) + 100))
	cl := mustClient(t, parent.addr, parentID.PublicHex(), childID)

	stop, done := make(chan struct{}), make(chan struct{})
	go func() { defer close(done); RunDownlink(cl, ceng, nil, stop) }()
	waitFor(t, "the child to poll at least twice", 5*time.Second, func() bool {
		return parent.polls.Load() >= 2
	})
	close(stop)
	waitForClosed(t, "RunDownlink to stop", done, 5*time.Second)

	if pos := cs.CursorGet(uns.DownlinkCursor(cl.ParentPub()), downlinkStream); pos > 1 {
		t.Fatalf("commands cursor = %d, want it held at 1 — the parent will never offer the unwritten command again", pos)
	}
	// Nothing landed, and the record behind the failing one did not overtake it: a
	// commands stream that skipped ahead would run commands out of order.
	recs, _, err := cs.Read("commands", 1, 10, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 0 {
		t.Fatalf("child commands = %+v, want none: the first record could not be written", recs)
	}
	// The same child does persist a command that fits, so the assertion above
	// measures the size failure and not a broken downlink.
	if _, err := ceng.IngestDownlink("colca/v1/_CmdParam/n-child/m1/stop", []byte(small), 20); err != nil {
		t.Fatalf("a command within the limit was refused too: %v", err)
	}
	if recs, _, _ := cs.Read("commands", 1, 10, nil); len(recs) != 1 {
		t.Fatalf("child commands = %+v, want the one that fits", recs)
	}
}

// A definition that fails to persist holds its cursor too. Only definitions this
// node refuses are skipped: a definition lost to a transient write error could
// never be recovered.
func TestADownlinkDefinitionThatCannotBePersistedHoldsTheCursor(t *testing.T) {
	dir := t.TempDir()
	parentID := mustIdentity(t, filepath.Join(dir, "p.key"))
	childID := mustIdentity(t, filepath.Join(dir, "c.key"))

	small := `{"id":"01HGRP-OPS","name":"Ops"}`
	oversized := fmt.Sprintf(`{"id":"01HGRP-OPS","name":%q}`, strings.Repeat("x", 400))
	parent := newStubParent(t, parentID, func(int64) string {
		return fmt.Sprintf(`{"records":[],"next":1,"head":1,"definitions":[`+
			`{"o":1,"t":"colca/v1/_Group/n-parent/01HGRP-OPS","p":%q,"ts":10}`+
			`],"def_next":2,"now_ms":%d}`, b64Payload(oversized), time.Now().UnixMilli())
	})

	cs := mustStore(t, filepath.Join(dir, "cdata"))
	_, ceng := nodeParts(t, cs, &config.Config{ULID: "n-child"}, nil, nil, nil)
	cs.SetMaxRecordBytes(uint64(len(small) + 100))
	cl := mustClient(t, parent.addr, parentID.PublicHex(), childID)

	stop, done := make(chan struct{}), make(chan struct{})
	go func() { defer close(done); RunDownlink(cl, ceng, nil, stop) }()
	waitFor(t, "the child to poll at least twice", 5*time.Second, func() bool {
		return parent.polls.Load() >= 2
	})
	close(stop)
	waitForClosed(t, "RunDownlink to stop", done, 5*time.Second)

	if pos := cs.CursorGet(uns.DownlinkDefCursor(cl.ParentPub()), downlinkDefStream); pos > 1 {
		t.Fatalf("definitions cursor = %d, want it held at 1 — the definition is gone and nothing re-sends it", pos)
	}
	if entries := mustKVScan(t, cs, "01HGRP-OPS"); len(entries) != 0 {
		t.Fatalf("child KV = %+v, want nothing: the definition could not be written", entries)
	}
	// The denominator: the same child applies a definition that fits.
	if _, err := ceng.IngestDownlinkDefinition("colca/v1/_Group/n-parent/01HGRP-OPS", []byte(small), 10); err != nil {
		t.Fatalf("a definition within the limit was refused too: %v", err)
	}
	if entries := mustKVScan(t, cs, "01HGRP-OPS"); len(entries) != 1 {
		t.Fatalf("child KV = %+v, want the group that fits", entries)
	}
}
