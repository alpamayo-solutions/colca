package uns

import "testing"

// These tests pin the distinctions that are easy to get wrong when a class is
// added: "state" and "state the publisher owns" are different sets, and the
// refresh set is narrower than both.

// A definition is state (projected and retained) but not owned state: it
// descends, its path is its id, and no hop rewrites it.
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

// Events are neither; retaining a command would replay stale instructions to
// every new subscriber.
func TestEventsAreNeitherStateNorOwnedState(t *testing.T) {
	for _, c := range []Class{ClassCmd, ClassAck, ClassGap, ClassTimeSync, ClassAudit} {
		if IsState(c) || IsOwnedState(c) {
			t.Errorf("class %v is an event and must not be state", c)
		}
	}
}

// Only entities refresh across a retention boundary: definitions arrive again
// on the downlink, and samples are meant to age out.
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

// A command may author entities and definitions in its batch, nothing else:
// metrics belong to their machine's door, and events are not state.
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

// A bundle may declare wire contracts only. Gap markers and time beacons are
// authored by the binary, so a bundle naming their class must fail to load.
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

// The Edit receipt is entity-class state, so the authoring node can find it,
// but it never leaves that node, since only that node reads it. "Its class
// flows up" and "this record leaves the node" are different questions.
func TestTheEditReceiptIsNodePrivateStateAndNothingElseIs(t *testing.T) {
	const receipt = "_EditOperation"
	if !IsNodePrivate(receipt) {
		t.Fatalf("IsNodePrivate(%s) = false: every ancestor would hold receipts only the author reads", receipt)
	}
	if c := ClassOf(receipt); !IsState(c) || !IsOwnedState(c) || !FlowsUp(c) {
		t.Fatalf("%s must stay ordinary entity-class state locally (state=%v owned=%v flowsUp=%v) — "+
			"node-private is a per-record question, not a class", receipt, IsState(c), IsOwnedState(c), FlowsUp(c))
	}
	for _, contract := range []string{
		"_Metric", "_Node", "_ServiceDetails", "_SystemElement", "_Signal", "_Constant",
		"_ExternalReference", "_Resource", "_EnrolledIdentity", "_AlarmNotificationConfig",
		"_NotificationConfigStatus", "_Group", "_MetadataType", "_AnnotationType", "_DataModel",
		"_Ack", "_StreamGap", "_AuditEvent", "_AlarmStateChange", "_Annotation", "_Log", "_CmdParam",
	} {
		if IsNodePrivate(contract) {
			t.Errorf("IsNodePrivate(%s) = true: the record would silently stop reaching the tree", contract)
		}
	}
}

// A parent's offset gap detection assumes the uplink carries the whole stream.
// Only commands (only acks and gap markers rise) and entities (the private
// receipt stays home) are filtered, and both are derived, not listed by hand.
func TestOnlyFilteredUplinksAreNotGapless(t *testing.T) {
	for _, stream := range []string{"commands", "entities"} {
		if UplinkCarriesEveryRecord(stream) {
			t.Errorf("UplinkCarriesEveryRecord(%q) = true: a parent would report the uplink filter as data loss", stream)
		}
	}
	for _, stream := range []string{"metrics", "alarms", "audit", "annotations", "logs"} {
		if !UplinkCarriesEveryRecord(stream) {
			t.Errorf("UplinkCarriesEveryRecord(%q) = false: a real child-offset gap there would go unreported", stream)
		}
	}
	if UplinkCarriesEveryRecord(StreamFor(ClassOf("_EditOperation"))) {
		t.Error("the stream a node-private contract lives on must read as partial — the exemption is derived, not listed")
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

// An entry persisted before the status field existed has no status and must
// read as active, which a `Status == "active"` comparison would get wrong.
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

// Only nodes drain; a machine's delivery lives in broker session state, not
// in a cursor.
func TestOnlyNodesCanDrain(t *testing.T) {
	if !(&Entry{Kind: KindNode}).CanDrain() {
		t.Error("a node must be drainable")
	}
	for _, k := range []Kind{KindExternal, KindHuman} {
		if (&Entry{Kind: k}).CanDrain() {
			t.Errorf("kind %q must not be drainable — there is no cursor to drain against", k)
		}
	}
}

// A nil entry answers false from every boolean predicate on *Entry. Without
// this test, removing any nil guard would still pass everything else.
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

// Each kind holds only its own doors. People arrive as tokens and are
// authorized per publish, so they are refused at all three.
func TestEachKindHoldsOnlyItsOwnDoors(t *testing.T) {
	for _, tc := range []struct {
		kind  Kind
		allow []Door
	}{
		{KindExternal, []Door{DoorMQTT, DoorHTTP}},
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

// A nil entry holds no door. Callers use `if !ok || !entry.MayUseDoor(…)`,
// which still reaches the predicate if a registry returns (nil, true).
func TestANilEntryHoldsNoDoor(t *testing.T) {
	var e *Entry
	for _, d := range []Door{DoorMQTT, DoorHTTP, DoorRepl, DoorLocal} {
		if e.MayUseDoor(d) {
			t.Errorf("a nil entry must hold no door, but it claimed door %v", d)
		}
	}
}

func TestCatalogueNameIsTheNameALocalEntryPresentedAndTheULIDOfAKeyedOne(t *testing.T) {
	local := &Entry{ULID: "01LOCAL", Kind: KindLocal, Name: "opcua-1"}
	if got := local.CatalogueName(); got != "opcua-1" {
		t.Fatalf("local entry: CatalogueName() = %q, want the presented name %q", got, "opcua-1")
	}
	external := &Entry{ULID: "01EXT", Kind: KindExternal, Pubkey: "ab", Element: "el"}
	if got := external.CatalogueName(); got != "01EXT" {
		t.Fatalf("external entry with no name: CatalogueName() = %q, want its ULID", got)
	}
}
