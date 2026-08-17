package repl

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/engine"
	"github.com/alpamayo-solutions/colca/internal/identity"
	"github.com/alpamayo-solutions/colca/internal/store"
)

// Prefix hand-down (cmdadmin design §3): the parent tells the child its
// root-frame prefix in every downlink response — parentPrefix + "/" + mount,
// or just the mount at the root. Nothing is handed down while the parent's
// own prefix is unknown.
func TestDownlinkHandsDownPrefix(t *testing.T) {
	dir := t.TempDir()
	parentID, _ := identity.Generate(filepath.Join(dir, "p.key"))
	childID, _ := identity.Generate(filepath.Join(dir, "c.key"))

	ps, _ := store.Open(filepath.Join(dir, "pdata"))
	defer ps.Close()
	pcfg := &config.Config{ULID: "n-parent", Repl: config.Endpoint{Addr: "127.0.0.1:0"}}
	preg := regWithChildren(t, ps, pcfg.ULID, childSpec{"n-child", childID.PublicHex(), "child1"})
	peng := engine.New(ps, pcfg, preg, nil, nil, nil)
	srv, err := NewServer(pcfg, peng, parentID, preg, nil)
	if err != nil {
		t.Fatal(err)
	}
	addr, err := srv.Start()
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Stop()

	cl, err := NewClient("https://"+addr, parentID.PublicHex(), childID)
	if err != nil {
		t.Fatal(err)
	}

	// One command in the child's zone so the long poll answers immediately.
	if _, err := peng.IngestAdmin("colca/v1/_CmdParam/m1/child1/m1/go",
		[]byte(`{"correlation_id":"c1","expires_at":99999999999999}`)); err != nil {
		t.Fatal(err)
	}

	// Parent prefix unknown → nothing handed down.
	_, _, _, prefix, err := cl.DownlinkWithPrefix(1, 10, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if prefix != nil {
		t.Fatalf("prefix handed down while parent unknown: %q", *prefix)
	}

	// Parent is a mid node at site1 → child prefix site1/child1.
	peng.SetPrefix("site1")
	_, _, _, prefix, err = cl.DownlinkWithPrefix(1, 10, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if prefix == nil || *prefix != "site1/child1" {
		t.Fatalf("prefix = %v, want site1/child1", prefix)
	}

	// Root parent (empty prefix, known) → child prefix is just its mount.
	peng.SetPrefix("")
	_, _, _, prefix, err = cl.DownlinkWithPrefix(1, 10, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if prefix == nil || *prefix != "child1" {
		t.Fatalf("prefix = %v, want child1", prefix)
	}
}

// RunDownlink teaches the child engine: after one poll cycle against a
// prefix-knowing parent, the child's engine knows its prefix.
func TestRunDownlinkTeachesPrefix(t *testing.T) {
	dir := t.TempDir()
	parentID, _ := identity.Generate(filepath.Join(dir, "p.key"))
	childID, _ := identity.Generate(filepath.Join(dir, "c.key"))

	ps, _ := store.Open(filepath.Join(dir, "pdata"))
	defer ps.Close()
	pcfg := &config.Config{ULID: "n-parent", Repl: config.Endpoint{Addr: "127.0.0.1:0"}}
	preg := regWithChildren(t, ps, pcfg.ULID, childSpec{"n-child", childID.PublicHex(), "child1"})
	peng := engine.New(ps, pcfg, preg, nil, nil, nil)
	peng.SetPrefix("")
	srv, err := NewServer(pcfg, peng, parentID, preg, nil)
	if err != nil {
		t.Fatal(err)
	}
	addr, err := srv.Start()
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Stop()

	cs, _ := store.Open(filepath.Join(dir, "cdata"))
	defer cs.Close()
	ccfg := &config.Config{ULID: "n-child"}
	creg := regWithChildren(t, cs, ccfg.ULID)
	ceng := engine.New(cs, ccfg, creg, nil, nil, nil)

	cl, err := NewClient("https://"+addr, parentID.PublicHex(), childID)
	if err != nil {
		t.Fatal(err)
	}
	// A command in the child's zone so the first poll answers immediately.
	if _, err := peng.IngestAdmin("colca/v1/_CmdParam/m1/child1/m1/go",
		[]byte(`{"correlation_id":"c2","expires_at":99999999999999}`)); err != nil {
		t.Fatal(err)
	}
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() { defer close(done); RunDownlink(cl, ceng, nil, stop) }()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if p, ok := ceng.Prefix(); ok {
			if p != "child1" {
				t.Errorf("taught prefix %q, want child1", p)
			}
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	close(stop)
	<-done
	if p, ok := ceng.Prefix(); !ok || p != "child1" {
		t.Fatalf("child engine prefix = (%q, %v), want (child1, true)", p, ok)
	}
}
