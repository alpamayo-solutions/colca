package repl

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/httplimit"
	"github.com/alpamayo-solutions/colca/internal/identity"
	"github.com/alpamayo-solutions/colca/internal/store"
)

// fanoutParent starts a parent with n enrolled children mounted at child1..childN
// and returns the server and a client per child.
func fanoutParent(t *testing.T, n int) (*Server, *store.Store, []*Client, []*identity.Identity) {
	t.Helper()
	dir := t.TempDir()
	parentID := mustIdentity(t, filepath.Join(dir, "p.key"))
	ps := mustStore(t, filepath.Join(dir, "pdata"))
	pcfg := &config.Config{ULID: "n-parent", Repl: config.Endpoint{Addr: "127.0.0.1:0"}}
	var specs []childSpec
	var ids []*identity.Identity
	for i := 1; i <= n; i++ {
		id := mustIdentity(t, filepath.Join(dir, fmt.Sprintf("c%d.key", i)))
		ids = append(ids, id)
		specs = append(specs, childSpec{fmt.Sprintf("n-child%d", i), id.PublicHex(), fmt.Sprintf("child%d", i)})
	}
	preg, peng := nodeParts(t, ps, pcfg, nil, nil, nil, specs...)
	srv, addr := startServer(t, pcfg, peng, parentID, preg)
	t.Cleanup(srv.Stop)
	clients := make([]*Client, n)
	for i, id := range ids {
		clients[i] = mustClient(t, addr, parentID.PublicHex(), id)
	}
	return srv, ps, clients, ids
}

func certOf(t *testing.T, id *identity.Identity) *x509.Certificate {
	t.Helper()
	cert, err := id.SelfSignedCert("test")
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	return parsed
}

type pollResult struct {
	next uint64
	err  error
}

// openPoll makes first contact and opens a long poll at the parent's head.
func openPoll(ctx context.Context, t *testing.T, c *Client) (after uint64, done <-chan pollResult) {
	t.Helper()
	hello, err := c.hello(ctx, 1)
	if err != nil {
		t.Fatalf("hello: %v", err)
	}
	ch := make(chan pollResult, 1)
	go func() {
		next, _, err := c.Poll(ctx, hello.Head, hello.DefNext, 10, longPollFor)
		ch <- pollResult{next, err}
	}()
	return hello.Head, ch
}

// A parent commits replicated batches all the time. Polls waiting for commands
// or definitions must sleep through them: waking every poll on every commit made
// the wakeups grow with children × commits, each taking the store lock. Only a
// change to their own streams wakes them, and a command still answers at once.
func TestWaitingDownlinkPollsSleepThroughOtherStreams(t *testing.T) {
	srv, ps, clients, _ := fanoutParent(t, 3)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var polls []<-chan pollResult
	var afters []uint64
	for _, c := range clients {
		after, done := openPoll(ctx, t, c)
		afters = append(afters, after)
		polls = append(polls, done)
	}
	waitFor(t, "three polls waiting at the parent", 5*time.Second, func() bool {
		return goroutinesIn("(*Server).handleDownlink") == 3
	})

	for i := uint64(1); i <= 50; i++ {
		if _, err := clients[0].Replicate("metrics", []store.ReplRecord{
			{ChildOffset: i, Topic: "colca/v1/_Metric/n-child1/m1/t", Payload: []byte(`{"v":1}`), TS: int64(i)},
		}); err != nil {
			t.Fatalf("replicate %d: %v", i, err)
		}
	}
	if got := ps.NextOffset("metrics"); got != 51 {
		t.Fatalf("metrics next = %d, want 51", got)
	}
	if got := srv.downlinkWakes.Load(); got != 0 {
		t.Fatalf("50 metrics commits woke waiting downlink polls %d times, want 0", got)
	}
	for i, done := range polls {
		select {
		case res := <-done:
			t.Fatalf("poll %d answered (%+v) though nothing arrived for it", i, res)
		default:
		}
	}

	// A command for child2 answers child2's poll at once.
	seedCommandFor(t, ps, "child2")
	select {
	case res := <-polls[1]:
		if res.err != nil || res.next <= afters[1] {
			t.Fatalf("child2's poll: next %d err %v, want past %d", res.next, res.err, afters[1])
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a command did not wake its child's waiting poll")
	}
	if srv.downlinkWakes.Load() == 0 {
		t.Fatal("the command was delivered without a wakeup")
	}
}

func seedCommandFor(t *testing.T, ps *store.Store, mount string) {
	t.Helper()
	if _, _, err := ps.Append("commands", []store.Record{{
		Topic:   "colca/v1/_CmdParam/m1/" + mount + "/m1/go",
		Payload: []byte(`{"correlation_id":"c1","expires_at":99999999999}`),
		TS:      1,
	}}); err != nil {
		t.Fatal(err)
	}
}

// Children behind one site router or carrier NAT share a source address. The
// door limits an enrolled node per node, and its waiting downlink polls never
// take the slots its pushes need. Both bounds are shrunk here so three children
// show what used to take dozens.
func TestChildrenBehindOneAddressAreLimitedPerNode(t *testing.T) {
	auth, replication := replAuthPolicy, replicationPolicy
	t.Cleanup(func() { replAuthPolicy, replicationPolicy = auth, replication })
	replAuthPolicy = httplimit.Policy{PerCallerConcurrent: 2, GlobalConcurrent: 2}
	replicationPolicy = httplimit.Policy{PerCallerConcurrent: 2, GlobalConcurrent: 2}

	_, ps, clients, _ := fanoutParent(t, 3)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var polls []<-chan pollResult
	for _, c := range clients {
		_, done := openPoll(ctx, t, c)
		polls = append(polls, done)
	}
	waitFor(t, "three polls waiting at the parent", 5*time.Second, func() bool {
		return goroutinesIn("(*Server).handleDownlink") == 3
	})
	for i, done := range polls {
		select {
		case res := <-done:
			t.Fatalf("poll %d answered at once (%v): it was limited by the shared address", i, res.err)
		default:
		}
	}
	for i, c := range clients {
		if _, err := c.Replicate("metrics", []store.ReplRecord{
			{ChildOffset: 1, Topic: fmt.Sprintf("colca/v1/_Metric/n-child%d/m1/t", i+1), Payload: []byte(`{"v":1}`), TS: 1},
		}); err != nil {
			t.Fatalf("child %d push while three polls wait: %v", i+1, err)
		}
	}
	if got := ps.NextOffset("metrics"); got != 4 {
		t.Fatalf("metrics next = %d, want 4", got)
	}
}

// Before authentication a caller is known by its certificate only. An enrolled
// node's key, proven by the TLS handshake, is limited per node; any other key,
// such as a stranger flooding the door with fresh certificates, by its address.
func TestDoorAdmissionKeysEnrolledNodesByIdentity(t *testing.T) {
	srv, _, _, ids := fanoutParent(t, 1)
	childCert := certOf(t, ids[0])
	strangerCert := certOf(t, mustIdentity(t, filepath.Join(t.TempDir(), "s.key")))

	for _, tc := range []struct {
		name        string
		tls         *tls.ConnectionState
		class, from string
	}{
		{"enrolled node", &tls.ConnectionState{PeerCertificates: []*x509.Certificate{childCert}}, limitClassReplNode, "n-child1"},
		{"unknown key", &tls.ConnectionState{PeerCertificates: []*x509.Certificate{strangerCert}}, limitClassReplAuth, "192.0.2.7"},
		{"no certificate", nil, limitClassReplAuth, "192.0.2.7"},
	} {
		r := &http.Request{RemoteAddr: "192.0.2.7:4711", TLS: tc.tls}
		class, caller, _ := srv.admission(r)
		if class != tc.class || caller != tc.from {
			t.Errorf("%s: admitted as %s/%s, want %s/%s", tc.name, class, caller, tc.class, tc.from)
		}
	}
}
