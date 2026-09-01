package repl

import (
	"io"
	"net/http"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/store"
)

// The repl door must run every request inside the in-flight tracker its owner
// installs, because Stop closes connections instead of draining them: without
// this, a handler is still inside ApplyReplicated (or writing an audit denial)
// when the node closes Pebble, and Pebble panics on use after close. A hub
// applies child batches continuously, so the window is not theoretical.
//
// The tracker is the node's WaitGroup middleware (node.trackInflight), handed
// down through SetInflightTracker. What this test pins is the contract that
// makes that wiring worth anything: the wrap is OUTSIDE both the rate limiter
// and authentication, so a request is counted for its whole life at this door
// — including a rejected one, which still writes an audit denial to the store
// and is therefore exactly as dangerous to close underneath.
//
// Ordering is what is asserted, not just a count: the tracker records "before"
// on the way in and "after" on the way out, and a real response body proves
// the handler ran between them.
func TestEveryReplRequestRunsInsideTheInstalledInflightTracker(t *testing.T) {
	dir := t.TempDir()
	parentID := mustIdentity(t, filepath.Join(dir, "p.key"))
	childID := mustIdentity(t, filepath.Join(dir, "c.key"))
	strangerID := mustIdentity(t, filepath.Join(dir, "s.key"))
	ps := mustStore(t, filepath.Join(dir, "pdata"))
	pcfg := &config.Config{ULID: "n-parent", Repl: config.Endpoint{Addr: "127.0.0.1:0"}}
	preg, peng := nodeParts(t, ps, pcfg, nil, nil, nil,
		childSpec{"n-child", childID.PublicHex(), "child1"})

	srv, err := NewServer(pcfg, peng, parentID, preg, nil, nil)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	var entered, left, inside, maxInside atomic.Int64
	srv.SetInflightTracker(func(h http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			entered.Add(1)
			if n := inside.Add(1); n > maxInside.Load() {
				maxInside.Store(n)
			}
			defer func() {
				inside.Add(-1)
				left.Add(1)
			}()
			h.ServeHTTP(w, r)
		})
	})
	addr, err := srv.Start()
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(srv.Stop)

	// 1. An authorized write. This is the handler that reaches ApplyReplicated.
	cl := mustClient(t, addr, parentID.PublicHex(), childID)
	if _, err := cl.Replicate("metrics", []store.ReplRecord{
		{ChildOffset: 1, Topic: "colca/v1/_Metric/n-child/m1/t", Payload: []byte(`{"v":1}`), TS: 1},
	}); err != nil {
		t.Fatalf("replicate: %v", err)
	}
	if got := ps.NextOffset("metrics"); got != 2 {
		t.Fatalf("replicated record did not land (metrics next = %d) — the rest proves nothing", got)
	}

	// 2. An authorized read. Seeded first so the long poll has something to
	//    answer with and returns at once.
	seedParentCommands(t, ps, 1)
	resp, err := cl.http.Get("https://" + addr + "/downlink?after=1&def_after=1&max=10")
	if err != nil {
		t.Fatalf("downlink: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || len(body) == 0 {
		t.Fatalf("downlink: status %d body %q", resp.StatusCode, body)
	}

	// 3. A valid certificate this parent has never enrolled: refused at
	//    childFromReq, and the refusal itself appends an audit denial. A
	//    request that is rejected still touches the store.
	auditBefore := ps.NextOffset("audit")
	stranger := mustClient(t, addr, parentID.PublicHex(), strangerID)
	if _, err := stranger.Replicate("metrics", []store.ReplRecord{
		{ChildOffset: 1, Topic: "colca/v1/_Metric/n-stranger/m1/t", Payload: []byte(`{"v":1}`), TS: 1},
	}); err == nil {
		t.Fatal("an unenrolled key was accepted at the repl door")
	}
	if got := ps.NextOffset("audit"); got == auditBefore {
		t.Fatalf("the refused request appended no audit record (audit next stayed %d) — it would "+
			"be safe to close the store under it, so this case proves nothing about tracking", got)
	}

	if entered.Load() != 3 {
		t.Fatalf("the tracker saw %d of the 3 requests — a repl handler that is not wrapped can "+
			"run inside Pebble after the node closed it", entered.Load())
	}
	if left.Load() != 3 || inside.Load() != 0 {
		t.Fatalf("tracker left=%d inside=%d, want 3 and 0 — the wrap must span the whole handler, "+
			"not just its entry", left.Load(), inside.Load())
	}
	if maxInside.Load() < 1 {
		t.Fatal("no request was ever observed inside the tracker")
	}
}
