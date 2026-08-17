package engine

import (
	"testing"

	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/store"
)

// The node prefix (cmdadmin design §3): unknown until taught, persisted so a
// restart while the parent is unreachable keeps translating human grants.
func TestEnginePrefixLifecycle(t *testing.T) {
	dir := t.TempDir()
	s, err := store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{ULID: "n-edge1"}
	e := New(s, cfg, testIDs(), nil, nil, nil)

	if p, ok := e.Prefix(); ok || p != "" {
		t.Fatalf("fresh engine: Prefix = (%q, %v), want unknown", p, ok)
	}
	e.SetPrefix("site1/edge1")
	if p, ok := e.Prefix(); !ok || p != "site1/edge1" {
		t.Fatalf("Prefix = (%q, %v), want (site1/edge1, true)", p, ok)
	}
	e.SetPrefix("site1/edge1") // idempotent re-teach: no error, value stable
	if p, ok := e.Prefix(); !ok || p != "site1/edge1" {
		t.Fatalf("re-teach changed value: (%q, %v)", p, ok)
	}

	// Persistence: a new engine over the SAME store knows the prefix.
	s.Close()
	s2, err := store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	e2 := New(s2, cfg, testIDs(), nil, nil, nil)
	if p, ok := e2.Prefix(); !ok || p != "site1/edge1" {
		t.Fatalf("prefix must survive restart, got (%q, %v)", p, ok)
	}

	// The root's empty prefix is a KNOWN value.
	e2.SetPrefix("")
	if p, ok := e2.Prefix(); !ok || p != "" {
		t.Fatalf("empty prefix must be known, got (%q, %v)", p, ok)
	}
}
