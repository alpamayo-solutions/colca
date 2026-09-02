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

// The reason grants and mounts name elements at all: the position changes, the
// identity does not, and nothing has to be re-authored.
//
// The index is loaded FIRST here, so this exercises the maintained map rather
// than a lazy load that happens to run after the change.
func TestARenamedElementMovesInTheLoadedIndex(t *testing.T) {
	f := newStore("n-edge1")
	placed(f, "line1", "01HLINE1", "Linie 1")
	x := NewElementIndex(f)
	if got, _ := x.PathOf("01HLINE1"); got != "line1" {
		t.Fatalf("before rename PathOf = %q, want line1", got)
	}

	// The rename as it actually arrives: retired at the old position, written
	// at the new one, each announced to the index.
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

// Once loaded, the index is maintained by ANNOUNCEMENT and never re-reads the
// store. A record that is durable but not yet announced therefore does not
// resolve — and that is why "the record is in the KV" is not a proxy for "this
// node can resolve the element".
//
// The consequence is not theoretical. Engine.IngestReplicated commits a
// replicated batch to the store and only then loops over the applied records
// calling Observe, so a reader polling /kv sees the element while a grant
// naming it still resolves to nothing. A level-1 test waited on the KV and
// published a command in that window; the hub answered "no cmd grant covers
// …" with PUBACK 0x87, once in about 300 runs under load. `awaitElement`
// (colca/tests/integration_test.go) now waits on this index instead.
//
// If this ever goes red because the index learned to re-scan on a miss, that
// is a design change, not a broken test — but `awaitElement`'s reasoning has
// to be revisited with it.
func TestALoadedIndexDoesNotSeeAStoreWriteItWasNotToldAbout(t *testing.T) {
	f := newStore("n-edge1")
	placed(f, "line1", "01HLINE1", "Linie 1")
	x := NewElementIndex(f)
	if _, ok := x.PathOf("01HLINE1"); !ok {
		t.Fatal("the seeded element does not resolve, so nothing below means anything")
	}

	// Durable, unannounced — the window between ApplyReplicated and Observe.
	placed(f, "line1/m6", "01HM6", "Maschine 6")
	if _, ok := x.PathOf("01HM6"); ok {
		t.Fatal("a store write the index was never told about resolved anyway — then a KV read " +
			"would be a safe proxy for resolvability, and awaitElement's whole reason is gone")
	}
	if _, ok := x.IDAt("line1/m6"); ok {
		t.Fatal("the reverse lookup saw an unannounced store write")
	}

	// The announcement is what makes it resolvable — the denominator: without
	// this the assertions above would also pass against an index that never
	// resolved anything at all.
	x.Observe("_SystemElement", "colca/v1/_SystemElement/n-edge1/line1/m6",
		f.records["colca/v1/_SystemElement/n-edge1/line1/m6"])
	if got, ok := x.PathOf("01HM6"); !ok || got != "line1/m6" {
		t.Fatalf("after Observe PathOf(01HM6) = %q %v, want line1/m6", got, ok)
	}
}

// A position that changes hands must not leave the previous occupant mapped:
// resolving a retired element to a live path would grant access to whatever
// moved in.
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

// A child's elements replicate upward with the mount inserted at each hop, so
// at an ancestor they already sit at that ancestor's own local path. Indexing
// only the node's OWN records would leave a hub unable to answer any grant
// naming an element deeper in its tree — which is most of them.
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
