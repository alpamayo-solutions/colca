package repl

import (
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// colca-node design §3.1/§7 gap 4: /healthz's uplink field is read straight
// off Client.Status(), so its three observable transitions — never having
// reached the parent, having been refused, and being current — have to be
// pinned here, at the source, rather than only through the HTTP surface that
// merely relays them.
func TestUplinkStatusTransitionsThroughConnectingUnauthorizedConnected(t *testing.T) {
	dir := t.TempDir()
	parentID := mustIdentity(t, filepath.Join(dir, "p.key"))
	childID := mustIdentity(t, filepath.Join(dir, "c.key"))

	ps := mustStore(t, filepath.Join(dir, "pdata"))
	pcfg := &config.Config{ULID: "n-parent", Repl: config.Endpoint{Addr: "127.0.0.1:0"}}
	// The parent does not know this child yet — first phase is a real 401.
	preg, peng := nodeParts(t, ps, pcfg, nil, nil, nil)
	srv, addr := startServer(t, pcfg, peng, parentID, preg)
	defer srv.Stop()

	cs := mustStore(t, filepath.Join(dir, "cdata"))
	_, ceng := nodeParts(t, cs, &config.Config{ULID: "n-child"}, nil, nil, nil)
	cl := mustClient(t, addr, parentID.PublicHex(), childID)

	if got := cl.Status().State; got != UplinkConnecting {
		t.Fatalf("a freshly built client reports %q, want %q — it has not attempted anything yet",
			got, UplinkConnecting)
	}

	stop := make(chan struct{})
	done := make(chan struct{})
	go func() { defer close(done); RunDownlink(cl, ceng, nil, stop) }()
	t.Cleanup(func() {
		close(stop)
		waitForClosed(t, "RunDownlink to return after stop", done, 5*time.Second)
	})

	waitFor(t, "the client to report unauthorized against a parent that has not enrolled it",
		20*time.Second, func() bool { return cl.Status().State == UplinkUnauthorized })

	// Enroll the child at runtime — exactly what `colca node enroll` does —
	// while the SAME loop keeps retrying. It must observe the transition on
	// its own, with no restart.
	element := placeElement(t, peng, "child1")
	entry, err := json.Marshal(uns.Entry{ULID: "n-child", Pubkey: childID.PublicHex(), Kind: uns.KindNode, Element: element})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := preg.Enroll(entry); err != nil {
		t.Fatalf("enroll child: %v", err)
	}

	waitFor(t, "the client to report connected once the parent enrolls it",
		20*time.Second, func() bool { return cl.Status().State == UplinkConnected })
}

// A Since that moves on every successful poll would be useless to a caller
// trying to tell "just connected" from "connected for a week" apart — it has
// to mark the START of the current state, not the timestamp of the most
// recent read.
func TestUplinkStatusSinceOnlyMovesOnATransition(t *testing.T) {
	dir := t.TempDir()
	parentID := mustIdentity(t, filepath.Join(dir, "p.key"))
	childID := mustIdentity(t, filepath.Join(dir, "c.key"))
	cl := mustClient(t, "127.0.0.1:0", parentID.PublicHex(), childID)

	first := cl.Status()
	if first.State != UplinkConnecting {
		t.Fatalf("initial state = %q, want %q", first.State, UplinkConnecting)
	}
	time.Sleep(5 * time.Millisecond)
	cl.setStatus(UplinkConnecting) // no-op: same state
	second := cl.Status()
	if !second.Since.Equal(first.Since) {
		t.Fatalf("Since moved from %v to %v on a no-op transition", first.Since, second.Since)
	}

	cl.setStatus(UplinkConnected)
	third := cl.Status()
	if !third.Since.After(first.Since) {
		t.Fatalf("Since did not advance on a real transition: %v -> %v", first.Since, third.Since)
	}
}
