package uns

import "testing"

func placed(f *fakeStore, path, id, name string) {
	f.records["colca/v1/_SystemElement/"+f.node+"/"+path] = mustJSON(
		map[string]any{"id": id, "name": name})
}

func TestPathOfResolvesAnElementToItsPosition(t *testing.T) {
	f := newStore("n-edge1")
	placed(f, "line1", "01HLINE1", "Linie 1")
	placed(f, "line1/m6", "01HM6", "Maschine 6")
	x := NewElementIndex(f)

	if got, ok := x.PathOf("01HM6"); !ok || got != "line1/m6" {
		t.Fatalf("PathOf(01HM6) = %q %v, want line1/m6", got, ok)
	}
}

// A renamed element resolves at its new path without re-authoring anything.
// The index is loaded first, so this tests the maintained map, not a late load.
func TestARenamedElementMovesInTheLoadedIndex(t *testing.T) {
	f := newStore("n-edge1")
	placed(f, "line1", "01HLINE1", "Linie 1")
	x := NewElementIndex(f)
	if got, _ := x.PathOf("01HLINE1"); got != "line1" {
		t.Fatalf("before rename PathOf = %q, want line1", got)
	}

	// A rename as it arrives: retired at the old path, written at the new
	// one, each announced to the index.
	f.records["colca/v1/_SystemElement/n-edge1/line1"] = nil
	x.Observe("_SystemElement", "colca/v1/_SystemElement/n-edge1/line1", nil)
	placed(f, "line1a", "01HLINE1", "Linie 1a")
	x.Observe("_SystemElement", "colca/v1/_SystemElement/n-edge1/line1a",
		f.records["colca/v1/_SystemElement/n-edge1/line1a"])

	if got, ok := x.PathOf("01HLINE1"); !ok || got != "line1a" {
		t.Fatalf("after rename PathOf = %q %v, want line1a", got, ok)
	}
	if _, ok := x.IDAt("line1"); ok {
		t.Fatal("the old position still resolves to an element")
	}
}

// Once loaded, the index learns only from Observe and never re-reads the
// store, so a record that is stored but not yet announced does not resolve.
// Engine.IngestReplicated commits a batch before calling Observe, which is
// why awaitElement in tests/integration_test.go waits on this index, not on
// the KV. If the index ever re-scans on a miss, revisit awaitElement.
func TestALoadedIndexDoesNotSeeAStoreWriteItWasNotToldAbout(t *testing.T) {
	f := newStore("n-edge1")
	placed(f, "line1", "01HLINE1", "Linie 1")
	x := NewElementIndex(f)
	if _, ok := x.PathOf("01HLINE1"); !ok {
		t.Fatal("the seeded element does not resolve, so nothing below means anything")
	}

	// Stored but not announced: the window between ApplyReplicated and Observe.
	placed(f, "line1/m6", "01HM6", "Maschine 6")
	if _, ok := x.PathOf("01HM6"); ok {
		t.Fatal("a store write the index was never told about resolved anyway — then a KV read " +
			"would be a safe proxy for resolvability, and awaitElement's whole reason is gone")
	}
	if _, ok := x.IDAt("line1/m6"); ok {
		t.Fatal("the reverse lookup saw an unannounced store write")
	}

	// Announcing makes it resolvable; without this the assertions above
	// would also pass for an index that resolves nothing.
	x.Observe("_SystemElement", "colca/v1/_SystemElement/n-edge1/line1/m6",
		f.records["colca/v1/_SystemElement/n-edge1/line1/m6"])
	if got, ok := x.PathOf("01HM6"); !ok || got != "line1/m6" {
		t.Fatalf("after Observe PathOf(01HM6) = %q %v, want line1/m6", got, ok)
	}
}

// When a position changes hands, the previous element must not stay mapped,
// or it would resolve to whatever moved in.
func TestAPositionChangingHandsDropsThePreviousOccupant(t *testing.T) {
	f := newStore("n-edge1")
	placed(f, "line1", "01HOLD", "Linie 1")
	x := NewElementIndex(f)
	x.PathOf("01HOLD") // load

	placed(f, "line1", "01HNEW", "Linie 1 (neu)")
	x.Observe("_SystemElement", "colca/v1/_SystemElement/n-edge1/line1",
		f.records["colca/v1/_SystemElement/n-edge1/line1"])

	if _, ok := x.PathOf("01HOLD"); ok {
		t.Fatal("the previous occupant still resolves to the position it lost")
	}
	if got, _ := x.PathOf("01HNEW"); got != "line1" {
		t.Fatalf("the new occupant resolves to %q, want line1", got)
	}
}

// A child's elements arrive mount-inserted, already at this node's local path.
// Indexing only the node's own records would leave a hub unable to resolve
// most grants.
func TestElementsReplicatedFromBelowResolveAtTheirLocalPath(t *testing.T) {
	f := newStore("n-global")
	placed(f, "site1", "01HSITE1", "Werk 1")
	// As it arrives at the hub: published by n-site1, mount-inserted to
	// site1/edge1 on the way up.
	f.records["colca/v1/_SystemElement/n-site1/site1/edge1"] = mustJSON(
		map[string]any{"id": "01HEDGE1", "name": "Edge 1"})
	x := NewElementIndex(f)

	if got, ok := x.PathOf("01HEDGE1"); !ok || got != "site1/edge1" {
		t.Fatalf("PathOf(01HEDGE1) = %q %v, want site1/edge1 — a hub must place its subtree's elements", got, ok)
	}
	if !x.Covers("01HEDGE1", "site1/edge1/m1/temp") {
		t.Fatal("a grant on a child's element must cover that child's records at the hub")
	}
	if x.Covers("01HEDGE1", "site1/edge2/m2/temp") {
		t.Fatal("it must not cover a sibling's records")
	}
}

func TestAnUnknownElementResolvesToNothing(t *testing.T) {
	f := newStore("n-edge1")
	placed(f, "line1", "01HLINE1", "Linie 1")
	x := NewElementIndex(f)

	// Fail closed: a node that has never heard of an element must not guess.
	if _, ok := x.PathOf("01HSOMEWHERE-ELSE"); ok {
		t.Fatal("an unknown element resolved to a path")
	}
	if _, ok := x.PathOf(""); ok {
		t.Fatal("an empty id resolved to a path")
	}
}

func TestARetiredElementStopsResolving(t *testing.T) {
	f := newStore("n-edge1")
	placed(f, "line1", "01HLINE1", "Linie 1")
	x := NewElementIndex(f)
	f.records["colca/v1/_SystemElement/n-edge1/line1"] = nil

	if _, ok := x.PathOf("01HLINE1"); ok {
		t.Fatal("a tombstoned element still resolves")
	}
}

func TestIDAtAnswersTheReverse(t *testing.T) {
	f := newStore("n-edge1")
	placed(f, "line1/m6", "01HM6", "Maschine 6")
	x := NewElementIndex(f)

	if got, ok := x.IDAt("line1/m6"); !ok || got != "01HM6" {
		t.Fatalf("IDAt(line1/m6) = %q %v, want 01HM6", got, ok)
	}
	if _, ok := x.IDAt("line1/nothing"); ok {
		t.Fatal("an empty position yielded an element")
	}
}

func TestCoversIsBoundedByThePathSeparator(t *testing.T) {
	f := newStore("n-edge1")
	placed(f, "line1", "01HLINE1", "Linie 1")
	x := NewElementIndex(f)

	cases := map[string]bool{
		"line1":          true,  // the element's own position
		"line1/m6":       true,  // below it
		"line1/m6/temp":  true,  // further below
		"line10":         false, // a different element whose name starts the same
		"line10/m6":      false,
		"other/line1/m6": false, // same name, different position
	}
	for path, want := range cases {
		if got := x.Covers("01HLINE1", path); got != want {
			t.Errorf("Covers(01HLINE1, %q) = %v, want %v", path, got, want)
		}
	}
	if x.Covers("01HUNKNOWN", "line1/m6") {
		t.Error("an unknown element covered a path")
	}
}
