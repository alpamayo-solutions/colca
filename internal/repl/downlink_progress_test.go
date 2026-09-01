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
