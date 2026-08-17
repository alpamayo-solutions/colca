package store

import "testing"

// Node-prefix persistence (cmdadmin design §3): key presence IS "known" —
// a root's empty prefix round-trips as known, absence means never learned.
func TestPrefixPutGet(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if p, ok := s.PrefixGet(); ok || p != "" {
		t.Fatalf("fresh store: PrefixGet = (%q, %v), want (\"\", false)", p, ok)
	}
	if err := s.PrefixPut("site1/edge1"); err != nil {
		t.Fatal(err)
	}
	if p, ok := s.PrefixGet(); !ok || p != "site1/edge1" {
		t.Fatalf("PrefixGet = (%q, %v), want (\"site1/edge1\", true)", p, ok)
	}
	// The root's empty prefix is a KNOWN value, not absence.
	if err := s.PrefixPut(""); err != nil {
		t.Fatal(err)
	}
	if p, ok := s.PrefixGet(); !ok || p != "" {
		t.Fatalf("empty prefix must round-trip as known, got (%q, %v)", p, ok)
	}
}
