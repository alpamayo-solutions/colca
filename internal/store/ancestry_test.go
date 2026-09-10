package store

import "testing"

// Key presence means known: a root's empty ancestry reads back as known, and a
// missing key as never learned.
func TestAncestryPutGet(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if b, ok := s.AncestryGet(); ok || b != nil {
		t.Fatalf("fresh store: AncestryGet = (%q, %v), want (nil, false)", b, ok)
	}
	chain := []byte(`[{"element":"01HSITE1","name":"site1"}]`)
	if err := s.AncestryPut(chain); err != nil {
		t.Fatal(err)
	}
	if b, ok := s.AncestryGet(); !ok || string(b) != string(chain) {
		t.Fatalf("AncestryGet = (%q, %v), want (%q, true)", b, ok, chain)
	}
	// The root's empty ancestry is a known value, not absence.
	if err := s.AncestryPut([]byte(`[]`)); err != nil {
		t.Fatal(err)
	}
	if b, ok := s.AncestryGet(); !ok || string(b) != "[]" {
		t.Fatalf("the empty ancestry must round-trip as known, got (%q, %v)", b, ok)
	}
}
