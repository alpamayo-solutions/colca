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
	for _, c := range []Class{ClassCmd, ClassAck, ClassGap, ClassTimeSync, ClassAudit} {
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
	for _, c := range []Class{ClassData, ClassDefinition, ClassCmd, ClassAck, ClassGap, ClassTimeSync, ClassAudit} {
		if NeedsStateRefresh(c) {
			t.Errorf("class %v must not refresh: it is either re-supplied or meant to age out", c)
		}
	}
}

// A command authors the entity graph and files definitions; both commit as one
// atomic batch. Nothing else may ride one: a metric is a machine's to publish at
// its own door, and commands, acks, gap markers, audit events and the beacon are
// not state at all.
func TestOnlyEntitiesAndDefinitionsMayBeCommandAuthored(t *testing.T) {
	for _, c := range []Class{ClassEntity, ClassDefinition} {
		if !IsCommandAuthoredState(c) {
			t.Errorf("class %v is authored by a command and must be accepted by the atomic commit port", c)
		}
	}
	for _, c := range []Class{ClassData, ClassCmd, ClassAck, ClassGap, ClassTimeSync, ClassAudit, ClassAlarm, ClassNone} {
		if IsCommandAuthoredState(c) {
			t.Errorf("class %v must not enter a command's atomic state batch", c)
		}
	}
}

// An unknown contract is the one answer that must never be treated as routable:
// ClassOf and the bundle authority both return ClassNone for it.
func TestOnlyClassNoneIsUnknown(t *testing.T) {
	if IsKnown(ClassNone) {
		t.Error("ClassNone is the unknown-contract answer and must not read as known")
	}
	for _, c := range []Class{ClassData, ClassEntity, ClassDefinition, ClassCmd, ClassAck, ClassGap, ClassTimeSync, ClassAudit} {
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
		"cmd": ClassCmd, "ack": ClassAck, "audit": ClassAudit,
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

func TestOnlyOwnedEventsAndStateFlowUp(t *testing.T) {
	for _, c := range []Class{ClassData, ClassEntity, ClassAck, ClassGap, ClassAudit} {
		if !FlowsUp(c) {
			t.Errorf("class %v must flow upward", c)
		}
	}
	for _, c := range []Class{ClassDefinition, ClassCmd, ClassTimeSync, ClassNone} {
		if FlowsUp(c) {
			t.Errorf("class %v must not flow upward", c)
		}
	}
	p, _ := Parse("colca/v1/_AuditEvent/n1/_colca/audit/e1")
	if !MatchesUplinkStream(ClassAudit, p, "audit") || MatchesUplinkStream(ClassAudit, p, "entities") {
		t.Fatal("audit records must replicate only on the audit stream")
	}
	gap, _ := Parse("colca/v1/_StreamGap/n1/audit")
	if !MatchesUplinkStream(ClassGap, gap, "audit") || MatchesUplinkStream(ClassGap, gap, "metrics") {
		t.Fatal("gap markers replicate in the stream their topic names")
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

// A nil entry is "no identity" and gets the same truthful null answer from
// every boolean predicate on *Entry: not draining, not drainable, not admin
// — the same rule TestANilEntryHoldsNoDoor pins for MayUseDoor. Without this
// test the nil guards added alongside MayUseDoor's would be unfalsifiable:
// removing any one of them would still compile and still pass every other
// test in this file.
func TestANilEntryAnswersFalseToEveryBooleanPredicate(t *testing.T) {
	var e *Entry
	if e.IsDraining() {
		t.Error("a nil entry must not read as draining")
	}
	if e.CanDrain() {
		t.Error("a nil entry must not be drainable")
	}
	if e.IsAdmin() {
		t.Error("a nil entry must not read as admin")
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

// A nil entry is "no identity", and no identity holds a door. The predicate
// answers that instead of panicking because every caller reaches it through
// `entry, ok := ids.Get(id); if !ok || !entry.MayUseDoor(…)`, where the right
// half runs on any registry that hands back (nil, true) — a pair the Mounts
// contract forbids and a test fake can still produce.
func TestANilEntryHoldsNoDoor(t *testing.T) {
	var e *Entry
	for _, d := range []Door{DoorMQTT, DoorHTTP, DoorRepl, DoorLocal} {
		if e.MayUseDoor(d) {
			t.Errorf("a nil entry must hold no door, but it claimed door %v", d)
		}
	}
}
