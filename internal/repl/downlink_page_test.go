package repl

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/engine"
	"github.com/alpamayo-solutions/colca/internal/store"
)

// linkTap sees every downlink request of a child and may act between the
// parent writing a response and the child reading it.
type linkTap struct {
	next http.RoundTripper
	// onPage runs once, on the first poll response (not a hello, not a confirm
	// request): the moment a page is on the link.
	onPage   func()
	paged    atomic.Bool
	confirms atomic.Int32
	// failConfirm makes every confirm request fail as a dead link would.
	failConfirm bool
	// oldParent removes "confirms" from the responses: a parent that predates it.
	oldParent bool
}

type linkDown struct{}

func (linkDown) Error() string { return "link down" }

func (l *linkTap) RoundTrip(req *http.Request) (*http.Response, error) {
	q := req.URL.Query()
	poll := strings.HasSuffix(req.URL.Path, "/downlink") && q.Get("hello") == ""
	if poll && q.Get("confirm") == "1" {
		l.confirms.Add(1)
		if l.failConfirm {
			return nil, linkDown{}
		}
	}
	resp, err := l.next.RoundTrip(req)
	if err != nil || !poll {
		return resp, err
	}
	if l.oldParent {
		resp.Body = withoutConfirms(resp.Body)
	}
	if q.Get("confirm") == "" && l.onPage != nil && l.paged.CompareAndSwap(false, true) {
		l.onPage()
	}
	return resp, err
}

// withoutConfirms rewrites a downlink response as a parent that predates
// confirm requests sends it.
func withoutConfirms(body io.ReadCloser) io.ReadCloser {
	defer body.Close()
	raw, err := io.ReadAll(body)
	if err != nil {
		return io.NopCloser(bytes.NewReader(raw))
	}
	var m map[string]json.RawMessage
	if json.Unmarshal(raw, &m) != nil {
		return io.NopCloser(bytes.NewReader(raw))
	}
	delete(m, "confirms")
	out, err := json.Marshal(m)
	if err != nil {
		return io.NopCloser(bytes.NewReader(raw))
	}
	return io.NopCloser(bytes.NewReader(out))
}

// pageWorld is a parent and a child whose downlink runs through a linkTap. The
// child records, for every command delivered locally, which commands its own
// stream held at that moment: what a consumer reading ahead would find.
type pageWorld struct {
	peng *engine.Engine
	cs   *store.Store
	tap  *linkTap

	mu        sync.Mutex
	delivered []delivery
}

type delivery struct {
	topic  string
	stored []string // the child's commands stream when this one was delivered
}

func (w *pageWorld) deliveries() []delivery {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]delivery(nil), w.delivered...)
}

const (
	proposeTopic  = "colca/v1/_CmdParam/m1/m1/propose"
	withdrawTopic = "colca/v1/_CmdParam/m1/m1/withdraw"
)

func newPageWorld(t *testing.T, tap *linkTap) *pageWorld {
	t.Helper()
	dir := t.TempDir()
	w := &pageWorld{tap: tap}
	parentID := mustIdentity(t, filepath.Join(dir, "p.key"))
	childID := mustIdentity(t, filepath.Join(dir, "c.key"))
	ps := mustStore(t, filepath.Join(dir, "pdata"))
	pcfg := &config.Config{ULID: "n-parent", Repl: config.Endpoint{Addr: "127.0.0.1:0"}}
	preg, peng := nodeParts(t, ps, pcfg, nil, nil, nil, childSpec{"n-child", childID.PublicHex(), "child1"})
	w.peng = peng
	srv, addr := startServer(t, pcfg, w.peng, parentID, preg)
	t.Cleanup(srv.Stop)

	w.cs = mustStore(t, filepath.Join(dir, "cdata"))
	deliver := func(topic string, _ []byte, _ bool) {
		if !strings.Contains(topic, "/_CmdParam/") {
			return
		}
		recs, _, err := w.cs.Read("commands", 1, 100, nil)
		if err != nil {
			t.Errorf("read the child's commands stream: %v", err)
		}
		d := delivery{topic: topic}
		for _, r := range recs {
			d.stored = append(d.stored, r.Topic)
		}
		w.mu.Lock()
		w.delivered = append(w.delivered, d)
		w.mu.Unlock()
	}
	_, ceng := nodeParts(t, w.cs, &config.Config{ULID: "n-child"}, deliver, nil, nil)

	cl := mustClient(t, addr, parentID.PublicHex(), childID)
	tap.next = cl.http.Transport
	cl.http.Transport = tap
	stop, done := make(chan struct{}), make(chan struct{})
	go func() { defer close(done); RunDownlink(cl, ceng, nil, stop) }()
	t.Cleanup(func() { close(stop); waitForClosed(t, "RunDownlink to stop", done, 5*time.Second) })
	waitForAttached(t, w.cs, cl)
	return w
}

func (w *pageWorld) issue(t *testing.T, topic, correlation string) {
	t.Helper()
	mustIngestAdmin(t, w.peng, strings.Replace(topic, "/m1/m1/", "/m1/child1/m1/", 1),
		`{"correlation_id":"`+correlation+`","expires_at":99999999999999}`)
}

// The split seen on a link that stalled: a proposal reaches the parent while
// the child's poll is open, the parent writes its response at once, and the
// response sits on the link. Its withdrawal is issued while it sits there. When
// the link returns the child receives a page with the proposal alone.
//
// The child confirms the page before it stores it, so the withdrawal is stored
// in the same write: when the proposal is delivered locally, the withdrawal is
// already behind it in the child's stream, where a consumer that reads ahead
// before it acts finds it.
func TestCommandsThatWaitedAtTheParentArriveInOneWrite(t *testing.T) {
	tap := &linkTap{}
	w := newPageWorld(t, tap)
	// The response with the proposal is on the link; the withdrawal is issued now.
	tap.onPage = func() { w.issue(t, withdrawTopic, "withdraw-1") }
	w.issue(t, proposeTopic, "propose-1")

	waitFor(t, "both commands to be delivered at the child", 5*time.Second, func() bool {
		return len(w.deliveries()) == 2
	})
	got := w.deliveries()
	if got[0].topic != proposeTopic || got[1].topic != withdrawTopic {
		t.Fatalf("delivered %q then %q, want the proposal then its withdrawal", got[0].topic, got[1].topic)
	}
	if len(got[0].stored) != 2 || got[0].stored[0] != proposeTopic || got[0].stored[1] != withdrawTopic {
		t.Fatalf("when the proposal was delivered the child's commands stream held %q, want the proposal and its withdrawal: "+
			"both waited at the parent when the child was next in contact", got[0].stored)
	}
	if n := tap.confirms.Load(); n != 1 {
		t.Fatalf("the child sent %d confirm requests for one page, want 1", n)
	}
}

// A parent that predates confirm requests would hold one as a long poll, with
// the page waiting in the child's hands for as long. It is never asked: its
// pages are stored as they arrive.
func TestAParentThatCannotConfirmIsNotAsked(t *testing.T) {
	tap := &linkTap{oldParent: true}
	w := newPageWorld(t, tap)
	w.issue(t, proposeTopic, "propose-1")
	w.issue(t, withdrawTopic, "withdraw-1")

	waitFor(t, "both commands to be delivered at the child", 5*time.Second, func() bool {
		return len(w.deliveries()) == 2
	})
	if n := tap.confirms.Load(); n != 0 {
		t.Fatalf("the child sent %d confirm requests to a parent that does not say it answers them, want 0", n)
	}
}

// A confirm request that fails does not keep commands back that the child
// already has: the page in hand is stored as it is.
func TestAFailedConfirmStoresThePageInHand(t *testing.T) {
	tap := &linkTap{failConfirm: true}
	w := newPageWorld(t, tap)
	w.issue(t, proposeTopic, "propose-1")

	waitFor(t, "the proposal to be delivered although the confirm request failed", 5*time.Second, func() bool {
		return len(w.deliveries()) == 1
	})
	if got := w.deliveries()[0]; got.topic != proposeTopic {
		t.Fatalf("delivered %q, want the proposal", got.topic)
	}
	if tap.confirms.Load() == 0 {
		t.Fatal("the child never tried to confirm the page: the test proves nothing about a failed confirm")
	}
}

// A confirm request is answered at once, also when nothing waits, and takes no
// receipt: the child's position at the parent is where it was.
func TestAConfirmRequestIsAnsweredAtOnce(t *testing.T) {
	dir := t.TempDir()
	parentID := mustIdentity(t, filepath.Join(dir, "p.key"))
	childID := mustIdentity(t, filepath.Join(dir, "c.key"))
	ps := mustStore(t, filepath.Join(dir, "pdata"))
	pcfg := &config.Config{ULID: "n-parent", Repl: config.Endpoint{Addr: "127.0.0.1:0"}}
	preg, peng := nodeParts(t, ps, pcfg, nil, nil, nil, childSpec{"n-child", childID.PublicHex(), "child1"})
	srv, addr := startServer(t, pcfg, peng, parentID, preg)
	defer srv.Stop()
	cl := mustClient(t, addr, parentID.PublicHex(), childID)
	hello, err := cl.hello(t.Context(), greeting{})
	if err != nil {
		t.Fatal(err)
	}

	began := time.Now()
	res, err := cl.poll(t.Context(), hello.Head, hello.DefNext, true)
	if err != nil {
		t.Fatal(err)
	}
	if took := time.Since(began); took > longPollFor/4 {
		t.Fatalf("a confirm request with nothing waiting took %v, want an answer at once (a long poll holds %v)", took, longPollFor)
	}
	if len(res.Records) != 0 || res.Next != hello.Head || !res.Confirms {
		t.Fatalf("confirm with nothing waiting = %d records, next %d, confirms %v; want none, %d, true",
			len(res.Records), res.Next, res.Confirms, hello.Head)
	}

	mustIngestAdmin(t, peng, "colca/v1/_CmdParam/m1/child1/m1/go", `{"correlation_id":"c1","expires_at":99999999999999}`)
	res, err = cl.poll(t.Context(), hello.Head, hello.DefNext, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Records) != 1 || res.Records[0].Topic != "colca/v1/_CmdParam/m1/m1/go" {
		t.Fatalf("confirm with one command waiting = %+v, want that command", res.Records)
	}
}
