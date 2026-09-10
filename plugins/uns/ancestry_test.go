package uns

import "testing"

// placements is a stub namespace: which element sits at which local path.
type placements map[string]string

func (p placements) IDAt(path string) (string, bool) { id, ok := p[path]; return id, ok }

// The rendered path must equal the prefix string the parent used to send, so
// code that still reads a prefix keeps working.
func TestPrefixRendersTheSamePathTheStringCarried(t *testing.T) {
	cases := []struct {
		name string
		a    Ancestry
		want string
	}{
		{"the root", Ancestry{}, ""},
		{"one hop", Ancestry{{Element: "01HSITE1", Name: "site1"}}, "site1"},
		{"two hops", Ancestry{
			{Element: "01HSITE1", Name: "site1"},
			{Element: "01HEDGE1", Name: "edge1"},
		}, "site1/edge1"},
	}
	for _, c := range cases {
		if got := c.a.Prefix(); got != c.want {
			t.Errorf("%s: Prefix() = %q, want %q", c.name, got, c.want)
		}
	}
}

// The question a grant asks: is the element it names this node or above it? If
// so it reaches everything here.
func TestCoversRecognisesThisNodeAndEveryAncestor(t *testing.T) {
	a := Ancestry{
		{Element: "01HSITE1", Name: "site1"},
		{Element: "01HEDGE1", Name: "edge1"},
	}
	for _, id := range []string{"01HSITE1", "01HEDGE1"} {
		if !a.Covers(id) {
			t.Errorf("Covers(%s) = false — the element is on this node's own chain", id)
		}
	}
	if a.Covers("01HELSEWHERE") {
		t.Error("an element outside the chain must not cover this node")
	}
	// A position nobody placed an element on must never act as a skeleton key.
	unplaced := Ancestry{{Element: "", Name: "line1"}}
	if unplaced.Covers("") {
		t.Error("the empty id covered a node")
	}
}

func TestExtendAddsOnePositionPerMountSegment(t *testing.T) {
	site1 := Ancestry{{Element: "01HSITE1", Name: "site1"}}
	ns := placements{"line1": "01HLINE1", "line1/m6": "01HM6"}

	got := site1.Extend(ns, "line1/m6")

	want := Ancestry{
		{Element: "01HSITE1", Name: "site1"},
		{Element: "01HLINE1", Name: "line1"},
		{Element: "01HM6", Name: "m6"},
	}
	if len(got) != len(want) {
		t.Fatalf("Extend = %+v, want %+v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("Extend[%d] = %+v, want %+v", i, got[i], want[i])
		}
	}
	if p := got.Prefix(); p != "site1/line1/m6" {
		t.Fatalf("derived prefix = %q, want site1/line1/m6", p)
	}
	// Extend must not modify the ancestry it extends: the parent passes its
	// own chain to every child.
	if len(site1) != 1 {
		t.Fatalf("Extend mutated the receiver: %+v", site1)
	}
}

// An element can sit below a segment with no element; that position keeps
// its name in the path but has no identity a grant could name.
func TestExtendKeepsThePathExactAcrossAnUnplacedSegment(t *testing.T) {
	ns := placements{"line1/m6": "01HM6"} // nothing at "line1"

	got := Ancestry{}.Extend(ns, "line1/m6")

	if p := got.Prefix(); p != "line1/m6" {
		t.Fatalf("derived prefix = %q, want line1/m6 — an unplaced segment must not vanish from the path", p)
	}
	if got[0].Element != "" {
		t.Fatalf("unplaced position resolved to %q, want no identity", got[0].Element)
	}
	if got.Covers("") {
		t.Fatal("the unplaced position must not be nameable in a grant")
	}
	if !got.Covers("01HM6") {
		t.Fatal("the placed position must still be nameable")
	}
}
