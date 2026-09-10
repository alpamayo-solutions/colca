package engine

import (
	"testing"

	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/store"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

var siteEdge = uns.Ancestry{
	{Element: "01HSITE1", Name: "site1"},
	{Element: "01HEDGE1", Name: "edge1"},
}

// The node's position is unknown until taught and then persisted, so a restart
// while the parent is unreachable still answers human grants. The prefix is
// rendered from it, never stored.
func TestEngineAncestryLifecycle(t *testing.T) {
	dir := t.TempDir()
	s, err := store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{ULID: "n-edge1"}
	e := New(s, cfg, testIDs(), nil, nil, nil)

	if a, ok := e.Ancestry(); ok || len(a) != 0 {
		t.Fatalf("fresh engine: Ancestry = (%+v, %v), want unknown", a, ok)
	}
	if p, ok := e.Prefix(); ok || p != "" {
		t.Fatalf("fresh engine: Prefix = (%q, %v), want unknown", p, ok)
	}

	e.SetAncestry(siteEdge)
	if p, ok := e.Prefix(); !ok || p != "site1/edge1" {
		t.Fatalf("Prefix = (%q, %v), want (site1/edge1, true)", p, ok)
	}
	e.SetAncestry(siteEdge) // idempotent re-teach: no error, value stable
	if p, ok := e.Prefix(); !ok || p != "site1/edge1" {
		t.Fatalf("re-teach changed value: (%q, %v)", p, ok)
	}

	// A new engine over the same store knows its position, identities included.
	s.Close()
	s2, err := store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	e2 := New(s2, cfg, testIDs(), nil, nil, nil)
	a, ok := e2.Ancestry()
	if !ok || len(a) != 2 || a[0].Element != "01HSITE1" || a[1].Element != "01HEDGE1" {
		t.Fatalf("ancestry must survive restart with its ids, got (%+v, %v)", a, ok)
	}
	if p, _ := e2.Prefix(); p != "site1/edge1" {
		t.Fatalf("prefix after restart = %q, want site1/edge1", p)
	}

	// The root's empty chain is a known value, not absence.
	e2.SetAncestry(uns.Ancestry{})
	if p, ok := e2.Prefix(); !ok || p != "" {
		t.Fatalf("the root's empty position must be known, got (%q, %v)", p, ok)
	}
}

// A node that knows its position can answer whether a grant's element reaches
// it, which a path string cannot.
func TestAncestryAnswersWhetherAnElementReachesThisNode(t *testing.T) {
	s, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	e := New(s, &config.Config{ULID: "n-edge1"}, testIDs(), nil, nil, nil)
	e.SetAncestry(siteEdge)

	a, _ := e.Ancestry()
	if !a.Covers("01HSITE1") {
		t.Error("an ancestor's element must reach this node")
	}
	if a.Covers("01HOTHER") {
		t.Error("an element off this node's chain must not reach it")
	}
}
