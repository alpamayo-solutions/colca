package uns

import "testing"

// The predicates carry rules the core used to spell out for itself. What is
// worth pinning here is not that each one returns a bool, but the distinctions
// that are easy to get wrong when a class is added — chiefly that "state" and
// "state the publisher owns" are different sets, and that the refresh set is
// narrower than both.

// A definition is state — it projects into KV and is retained — but it is not
// OWNED state: it descends from the parent, its path is its own identity, and
// no hop rewrites it. Conflating the two would make every hop insert a mount
// into a definition's topic and break the rule that the same definition means
// the same thing at every node.
func TestDefinitionsAreStateButNotOwnedState(t *testing.T) {
	if !IsState(ClassDefinition) {
		t.Error("a definition must be state: it KV-projects, is retained and is tombstonable")
	}
	if IsOwnedState(ClassDefinition) {
		t.Error("a definition must NOT be owned state: it descends and is never mount-rewritten")
	}
	for _, c := range []Class{ClassData, ClassEntity} {
		if !IsState(c) || !IsOwnedState(c) {
			t.Errorf("class %v: data and entities are state authored under the publisher's own mount", c)
		}
	}
}

// Events are neither. Retaining a command would re-deliver stale instructions
// to every new subscriber, which is the whole reason the split exists.
func TestEventsAreNeitherStateNorOwnedState(t *testing.T) {
	for _, c := range []Class{ClassCmd, ClassAck, ClassGap, ClassTimeSync} {
		if IsState(c) || IsOwnedState(c) {
			t.Errorf("class %v is an event and must not be state", c)
		}
	}
}

// Only entities refresh across a retention boundary: nothing else re-supplies
// them. Definitions arrive again on the downlink, and data are samples whose
// ageing out is the point — refreshing either would defeat retention.
func TestOnlyEntitiesNeedStateRefresh(t *testing.T) {
	if !NeedsStateRefresh(ClassEntity) {
		t.Error("entities are authored here and nothing re-supplies them — they must refresh")
	}
	for _, c := range []Class{ClassData, ClassDefinition, ClassCmd, ClassAck, ClassGap, ClassTimeSync} {
		if NeedsStateRefresh(c) {
			t.Errorf("class %v must not refresh: it is either re-supplied or meant to age out", c)
		}
	}
}

// An unknown contract is the one answer that must never be treated as routable:
// ClassOf and the bundle authority both return ClassNone for it.
func TestOnlyClassNoneIsUnknown(t *testing.T) {
	if IsKnown(ClassNone) {
		t.Error("ClassNone is the unknown-contract answer and must not read as known")
	}
	for _, c := range []Class{ClassData, ClassEntity, ClassDefinition, ClassCmd, ClassAck, ClassGap, ClassTimeSync} {
		if !IsKnown(c) {
			t.Errorf("class %v is a real class and must read as known", c)
		}
	}
}

// A bundle may declare the contracts that ride the wire, and only those. Gap
// markers and time beacons are authored by the binary itself, so their class
// names are deliberately absent from the manifest vocabulary — a bundle that
// names one must fail to load rather than silently redefine a builtin.
func TestManifestVocabularyExcludesBuiltinAuthoredClasses(t *testing.T) {
	for name, want := range map[string]Class{
		"data": ClassData, "entity": ClassEntity, "definition": ClassDefinition,
		"cmd": ClassCmd, "ack": ClassAck,
	} {
		got, ok := ClassFromManifest(name)
		if !ok || got != want {
			t.Errorf("manifest class %q: got (%v, %v), want (%v, true)", name, got, ok, want)
		}
	}
	for _, name := range []string{"gap", "timesync", "time_sync", "none", ""} {
		if got, ok := ClassFromManifest(name); ok {
			t.Errorf("manifest class %q must not map (got %v) — builtin-authored classes are not declarable", name, got)
		}
	}
}

// Draining is asked, never spelled out: an entry persisted before the field
// existed carries no status at all and must read as active, which is exactly
// the rule a `Status == "active"` comparison at a call site would get wrong.
func TestAnEntryWithNoStatusIsNotDraining(t *testing.T) {
	if (&Entry{Kind: KindNode}).IsDraining() {
		t.Error("absent status means active — an entry with no status must not read as draining")
	}
	if !(&Entry{Kind: KindNode, Status: StatusDraining}).IsDraining() {
		t.Error("an entry marked draining must read as draining")
	}
	e := &Entry{Kind: KindNode}
	e.MarkDraining()
	if !e.IsDraining() {
		t.Error("MarkDraining must move the entry into the state IsDraining reports")
	}
}

// Only nodes drain. A machine's delivery rides broker QoS-1 session state
// rather than a cursor, so a parent has nothing to drain it against.
func TestOnlyNodesCanDrain(t *testing.T) {
	if !(&Entry{Kind: KindNode}).CanDrain() {
		t.Error("a node must be drainable")
	}
	for _, k := range []Kind{KindMachine, KindHuman} {
		if (&Entry{Kind: k}).CanDrain() {
			t.Errorf("kind %q must not be drainable — there is no cursor to drain against", k)
		}
	}
}

// Doors are answered per kind so no listener re-derives the rule. Humans are
// the case worth pinning: they arrive as tokens and are authorized per publish,
// so they hold no door of their own and must be refused at all three.
func TestEachKindHoldsOnlyItsOwnDoors(t *testing.T) {
	for _, tc := range []struct {
		kind  Kind
		allow []Door
	}{
		{KindMachine, []Door{DoorMQTT, DoorHTTP}},
		{KindNode, []Door{DoorRepl}},
		{KindHuman, nil},
	} {
		allowed := map[Door]bool{}
		for _, d := range tc.allow {
			allowed[d] = true
		}
		for _, d := range []Door{DoorMQTT, DoorHTTP, DoorRepl} {
			got := (&Entry{Kind: tc.kind}).MayUseDoor(d)
			if got != allowed[d] {
				t.Errorf("kind %q at door %v: got %v, want %v", tc.kind, d, got, allowed[d])
			}
		}
	}
}
