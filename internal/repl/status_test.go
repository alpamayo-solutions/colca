package repl

import (
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// /healthz reads the uplink field straight from Client.Status(), so its three
// transitions (never reached the parent, refused, current) are pinned here at
// the source.
func TestUplinkStatusTransitionsThroughConnectingUnauthorizedConnected(t *testing.T) {
	dir := t.TempDir()
	parentID := mustIdentity(t, filepath.Join(dir, "p.key"))
	childID := mustIdentity(t, filepath.Join(dir, "c.key"))

	ps := mustStore(t, filepath.Join(dir, "pdata"))
	pcfg := &config.Config{ULID: "n-parent", Repl: config.Endpoint{Addr: "127.0.0.1:0"}}
	// The parent does not know this child yet, so the first phase is a real 401.
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

	// Enroll the child at runtime, as an operator would, while the same loop keeps
	// retrying; it must see the transition without a restart.
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

// Since must mark the start of the current state, not the latest read, or a
// caller could not tell "just connected" from "connected for a week".
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
