package repl

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/identity"
	"github.com/alpamayo-solutions/colca/internal/store"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// Position hand-down (id-grants design §4): the parent tells the child where it
// sits in every downlink response — the parent's own chain extended by the
// child's element. Nothing is handed down while the parent's own position is
// unknown, because a guessed frame is worse than none.
func TestDownlinkHandsDownAncestry(t *testing.T) {
	dir := t.TempDir()
	parentID, _ := identity.Generate(filepath.Join(dir, "p.key"))
	childID, _ := identity.Generate(filepath.Join(dir, "c.key"))

	ps, _ := store.Open(filepath.Join(dir, "pdata"))
	defer ps.Close()
	pcfg := &config.Config{ULID: "n-parent", Repl: config.Endpoint{Addr: "127.0.0.1:0"}}
	preg, peng := nodeParts(t, ps, pcfg, nil, nil, nil, childSpec{"n-child", childID.PublicHex(), "child1"})
	srv, err := NewServer(pcfg, peng, parentID, preg, nil, nil)
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

	// Parent's own position unknown → nothing handed down.
	_, _, _, ancestry, err := cl.DownlinkWithAncestry(1, 10, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if ancestry != nil {
		t.Fatalf("position handed down while the parent's own is unknown: %+v", *ancestry)
	}

	// Parent is a mid node under site1 → the child's chain is site1 + its own
	// element, and the rendered path is what the old prefix string carried.
	peng.SetAncestry(uns.Ancestry{{Element: "01HSITE1", Name: "site1"}})
	_, _, _, ancestry, err = cl.DownlinkWithAncestry(1, 10, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if ancestry == nil {
		t.Fatal("no position handed down by a parent that knows its own")
	}
	if got := ancestry.Prefix(); got != "site1/child1" {
		t.Fatalf("rendered path = %q, want site1/child1", got)
	}
	// The identities, not just the path: the child's own element is the last
	// step, and its ancestor's id is what makes an inherited grant resolvable.
	if len(*ancestry) != 2 || (*ancestry)[0].Element != "01HSITE1" {
		t.Fatalf("chain = %+v, want site1's element at the head", *ancestry)
	}
	if !ancestry.Covers(elementAtChild1) {
		t.Fatalf("chain = %+v, want the child's own element %q as the last step", *ancestry, elementAtChild1)
	}

	// Root parent (empty chain, known) → the child's chain is just its own
	// element.
	peng.SetAncestry(uns.Ancestry{})
	_, _, _, ancestry, err = cl.DownlinkWithAncestry(1, 10, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if ancestry == nil || ancestry.Prefix() != "child1" {
		t.Fatalf("chain under the root = %+v, want the single step child1", ancestry)
	}
}

// elementAtChild1 is the element nodeParts places at "child1" (placeElement's
// naming convention) — the one the fixture child binds to.
const elementAtChild1 = "el-child1"

// RunDownlink teaches the child engine: after one poll cycle against a parent
// that knows where it sits, the child knows too — and it keeps knowing across a
// restart while the parent is unreachable, which is the whole reason the
// position is persisted rather than re-fetched.
func TestRunDownlinkTeachesTheChildItsPosition(t *testing.T) {
	dir := t.TempDir()
	parentID, _ := identity.Generate(filepath.Join(dir, "p.key"))
	childID, _ := identity.Generate(filepath.Join(dir, "c.key"))

	ps, _ := store.Open(filepath.Join(dir, "pdata"))
	defer ps.Close()
	pcfg := &config.Config{ULID: "n-parent", Repl: config.Endpoint{Addr: "127.0.0.1:0"}}
	preg, peng := nodeParts(t, ps, pcfg, nil, nil, nil, childSpec{"n-child", childID.PublicHex(), "child1"})
	peng.SetAncestry(uns.Ancestry{})
	srv, err := NewServer(pcfg, peng, parentID, preg, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	addr, err := srv.Start()
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Stop()

	cdir := filepath.Join(dir, "cdata")
	cs, _ := store.Open(cdir)
	ccfg := &config.Config{ULID: "n-child"}
	_, ceng := nodeParts(t, cs, ccfg, nil, nil, nil)

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
	waitFor(t, "the child to learn its position", 5*time.Second, func() bool {
		_, ok := ceng.Ancestry()
		return ok
	})
	close(stop)
	<-done

	a, ok := ceng.Ancestry()
	if !ok || a.Prefix() != "child1" {
		t.Fatalf("child position = (%+v, %v), want the single step child1", a, ok)
	}
	if !a.Covers(elementAtChild1) {
		t.Fatalf("child position %+v does not carry its own element %q", a, elementAtChild1)
	}

	// Restart the child with its parent gone: it still knows where it sits, so
	// a scoped human grant keeps resolving instead of failing closed.
	cs.Close()
	srv.Stop()
	cs2, err := store.Open(cdir)
	if err != nil {
		t.Fatal(err)
	}
	defer cs2.Close()
	_, ceng2 := nodeParts(t, cs2, ccfg, nil, nil, nil)
	a2, ok := ceng2.Ancestry()
	if !ok || !a2.Covers(elementAtChild1) || a2.Prefix() != "child1" {
		t.Fatalf("position after restart = (%+v, %v), want it intact with its ids", a2, ok)
	}
}
