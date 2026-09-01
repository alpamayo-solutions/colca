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

// stubParent is a parent whose /downlink answers the test writes itself. It
// exists to pin what the CHILD does with an answer, independent of any parent
// implementation: the client must be well behaved against a parent that is
// buggy, older, or simply has nothing to give it.
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

// A poll that answers instantly and leaves this node exactly where it was is
// not repeated immediately.
//
// Both cursors staying put is the condition, whatever caused it: a parent
// answering with a position the child already holds, or a record the child
// could not apply. Without the wait, the loop re-polls at once and the pair
// spins at the replication rate limit (~100 req/s per child) for as long as
// the condition lasts — the state a compacted definitions tail used to leave
// every freshly enrolled child in, indefinitely.
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
	if max := int64(window/retryAfter) + 5; polls > max {
		t.Fatalf("the child polled %d times in %s (at most %d expected) — a fruitless poll is repeated at once, "+
			"which is a busy loop at the parent's rate limit", polls, window, max)
	}
}

// A command that fails to PERSIST holds the cursor: it is offered again on
// the next poll rather than acked past.
//
// The cursor used to advance whatever happened, so a record that could not be
// written — a full disk, an I/O error, or (as here) a parent whose
// max_record_bytes is larger than this node's — was dropped at the last hop
// with one log line and no ack. The issuer sees "target slow" forever.
// Definitions on the same response always had the opposite, correct handling.
//
// Retrying is only safe because the loop backs off when nothing advanced
// (pinned above); together they retry the record without spinning.
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
	// The denominator: nothing landed, and in particular the record BEHIND
	// the failing one did not overtake it. A command stream that skipped
	// ahead would execute out of order.
	recs, _, err := cs.Read("commands", 1, 10, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 0 {
		t.Fatalf("child commands = %+v, want none: the first record could not be written", recs)
	}
	// And the same child DOES persist a command that fits, so the assertion
	// above measures the size failure and not a broken downlink.
	if _, err := ceng.IngestDownlink("colca/v1/_CmdParam/n-child/m1/stop", []byte(small), 20); err != nil {
		t.Fatalf("a command within the limit was refused too: %v", err)
	}
	if recs, _, _ := cs.Read("commands", 1, 10, nil); len(recs) != 1 {
		t.Fatalf("child commands = %+v, want the one that fits", recs)
	}
}
