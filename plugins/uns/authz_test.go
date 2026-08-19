package uns

import (
	"strings"
	"testing"
)

// nsByPath is the default test scope: an element id that carries the path it
// sits at, with the separators swapped for "~" because an id may not contain
// one. Cases can then name a placement inline, and every element resolves at
// this node — no ancestors. Movement is what mapScope below is for.
type nsByPath struct{}

func (nsByPath) PathOf(id string) (string, bool) {
	encoded, ok := strings.CutPrefix(id, "el-")
	if !ok || encoded == "" {
		return "", false
	}
	return strings.ReplaceAll(encoded, "~", "/"), true
}

func (nsByPath) Reaches(string) bool { return false }

var ns = nsByPath{}

// hex64 is a syntactically valid ed25519 pubkey (64 hex chars) — for tests
// that only need Validate to get past the pubkey shape check.
var hex64 = strings.Repeat("ab", 32)

// mapScope is the explicit test scope: which elements sit where at this node,
// and which ones are this node or above it. Used where the answer has to CHANGE
// — a rename, a reparent, an inherited grant.
type mapScope struct {
	paths map[string]string // element id → local path
	above map[string]bool   // element id is this node or an ancestor
}

func (m mapScope) PathOf(id string) (string, bool) { p, ok := m.paths[id]; return p, ok }
func (m mapScope) Reaches(id string) bool          { return id != "" && m.above[id] }

// testScope builds a Scope from element id → local path, with no ancestors —
// a shorthand for mapScope{paths: ...} where a case only needs placements.
func testScope(paths map[string]string) Scope {
	return mapScope{paths: paths}
}

// elementAt is the id of the element sitting at path.
func elementAt(path string) string {
	if path == "" {
		return ""
	}
	return "el-" + strings.ReplaceAll(path, "/", "~")
}

// readAt / cmdAt author a grant on the element sitting at path — what an
// operator writes once the ids come out of the plant model.
func readAt(path string) string { return "read:" + elementAt(path) + "/#" }

func cmdAt(path, classes string) string { return "cmd:" + elementAt(path) + "/#:" + classes }

// entry is a test helper: a machine entry placed at mount, with grants.
func entry(mount string, grants ...string) *Entry {
	return &Entry{
		ULID:    "01MACHINE0000000000000000A",
		Pubkey:  strings.Repeat("ab", 32),
		Kind:    KindMachine,
		Element: elementAt(mount),
		Grants:  grants,
	}
}

func TestParseGrant(t *testing.T) {
	cases := []struct {
		in      string
		wantErr bool
		verb    string
		element string
		classes []string
	}{
		{in: "read:01HLINE1/#", verb: "read", element: "01HLINE1"},
		{in: "read:01HLINE1", verb: "read", element: "01HLINE1"}, // "/#" is optional: an element grant always covers below
		{in: "read:#", verb: "read", element: "#"},
		{in: "cmd:01HLINE1/#:param,operate", verb: "cmd", element: "01HLINE1", classes: []string{"param", "operate"}},
		{in: "cmd:01HM6:admin", verb: "cmd", element: "01HM6", classes: []string{"admin"}},
		{in: "write:01HLINE1/#", verb: "write", element: "01HLINE1"},
		{in: "write:01HLINE1/#:param", wantErr: true}, // write grants carry no classes
		{in: "read:", wantErr: true},                  // empty zone
		{in: "cmd:01HLINE1/#", wantErr: true},         // cmd without classes
		{in: "cmd:01HLINE1/#:", wantErr: true},        // empty classes
		{in: "cmd:01HLINE1/#:reboot", wantErr: true},  // unknown class
		{in: "grant:01HLINE1/#", wantErr: true},       // unknown verb
		{in: "", wantErr: true},
		// A path-shaped zone is the mistake this design removes: it means
		// different things at different nodes and stops meaning anything at all
		// once somebody renames a position.
		{in: "read:werk1/linie3/#", wantErr: true},
		{in: "cmd:werk1/linie3/#:param", wantErr: true},
	}
	for _, c := range cases {
		g, err := ParseGrant(c.in)
		if c.wantErr {
			if err == nil {
				t.Errorf("ParseGrant(%q): expected error, got %+v", c.in, g)
			}
			continue
		}
		if err != nil {
			t.Errorf("ParseGrant(%q): unexpected error %v", c.in, err)
			continue
		}
		if g.Verb != c.verb || g.Element != c.element {
			t.Errorf("ParseGrant(%q) = %+v, want verb=%q element=%q", c.in, g, c.verb, c.element)
		}
		if len(c.classes) != len(g.Classes) {
			t.Errorf("ParseGrant(%q) classes = %v, want %v", c.in, g.Classes, c.classes)
		}
	}
}

func TestEntryValidate(t *testing.T) {
	good := entry("werk1/linie3/cnc5")
	if err := good.Validate(); err != nil {
		t.Fatalf("valid entry rejected: %v", err)
	}
	cases := []struct {
		name string
		mut  func(e *Entry)
	}{
		{"empty ulid", func(e *Entry) { e.ULID = "" }},
		{"short pubkey", func(e *Entry) { e.Pubkey = strings.Repeat("ab", 31) }},
		{"non-hex pubkey", func(e *Entry) { e.Pubkey = strings.Repeat("zz", 32) }},
		{"bad kind", func(e *Entry) { e.Kind = "gateway" }},
		{"node bound to nothing", func(e *Entry) { e.Kind = KindNode; e.Element = "" }},
		{"element named by a path", func(e *Entry) { e.Element = "werk1/linie3" }},
		{"element with a wildcard", func(e *Entry) { e.Element = "el-werk1#" }},
		// A grant is verb:element:classes — an id with a colon would parse into
		// a different grant than the one authored.
		{"element with a colon", func(e *Entry) { e.Element = "el:werk1" }},
		{"bad grant", func(e *Entry) { e.Grants = []string{"cmd:01HZ/#"} }},
	}
	for _, c := range cases {
		e := entry("werk1/linie3/cnc5")
		c.mut(e)
		if err := e.Validate(); err == nil {
			t.Errorf("%s: expected validation error", c.name)
		}
	}
}

// A local service is the one kind that may be unplaced: the local door
// already proved it belongs to this deployment. A machine or a node must be
// placed explicitly — the element-less read-only observer is gone (design
// §7): bound-to-nothing and bound-to-everything must never be the same
// stored value.
func TestALocalEntryNeedsANameAndNoPubkey(t *testing.T) {
	e := &Entry{ULID: "01J", Kind: KindLocal, Name: "connector-opcua", Element: "el-press3"}
	if err := e.Validate(); err != nil {
		t.Fatalf("a valid local entry was rejected: %v", err)
	}
	nameless := &Entry{ULID: "01J", Kind: KindLocal, Element: "el-press3"}
	if err := nameless.Validate(); err == nil {
		t.Fatal("a nameless local entry validated; the name is how the local door finds it")
	}
	keyed := &Entry{ULID: "01J", Kind: KindLocal, Name: "connector-opcua", Element: "el-press3", Pubkey: hex64}
	if err := keyed.Validate(); err == nil {
		t.Fatal("a local entry carrying a pubkey validated; the door is its proof, it holds no key")
	}
}

// Unplaced is a position, not a missing value: a local service with no element is
// bound to the NODE. A machine's is required — the local door proved a local
// service belongs to this deployment, and nothing proved that about a machine.
func TestALocalServiceMayBeUnplaced(t *testing.T) {
	e := &Entry{ULID: "01J", Kind: KindLocal, Name: "dataops"}
	if err := e.Validate(); err != nil {
		t.Fatalf("an unplaced local service was rejected: %v", err)
	}
}

func TestAMachineOrNodeMustBePlaced(t *testing.T) {
	for _, e := range []*Entry{
		{ULID: "01J", Pubkey: hex64, Kind: KindMachine},
		{ULID: "01J", Pubkey: hex64, Kind: KindNode},
	} {
		if err := e.Validate(); err == nil {
			t.Fatalf("kind %q validated unplaced; only a local service may be", e.Kind)
		}
	}
}

func TestLocalIdentitiesUseOnlyTheLocalDoor(t *testing.T) {
	l := &Entry{Kind: KindLocal}
	if !l.MayUseDoor(DoorLocal) {
		t.Fatal("a local identity was refused the local door")
	}
	for _, d := range []Door{DoorMQTT, DoorHTTP, DoorRepl} {
		if l.MayUseDoor(d) {
			t.Fatalf("a local identity was admitted at door %v; it holds no key to present there", d)
		}
	}
	if (&Entry{Kind: KindMachine}).MayUseDoor(DoorLocal) {
		t.Fatal("a machine was admitted at the local door; that door proves nothing about it")
	}
}

func TestCmdClass(t *testing.T) {
	cases := map[string]string{
		"_CmdParam":     "param",
		"_CmdOperate":   "operate",
		"_CmdMaintain":  "maintain",
		"_CmdConfigure": "configure",
		"_CmdAdmin":     "admin",
		"_CmdFoo":       "admin", // unknown command contracts demand the highest class (§5.1)
	}
	for contract, want := range cases {
		if got := CmdClass(contract); got != want {
			t.Errorf("CmdClass(%s) = %q, want %q", contract, got, want)
		}
	}
}

// Editing the data model is not a point on the equipment-hazard ladder: the
// person who binds a signal must not thereby be able to send maintenance
// commands to a PLC, and the maintenance technician must not silently gain the
// ability to rewrite the model (data-model binding design §3.3).
func TestConfigureAndMaintainDoNotImplyEachOther(t *testing.T) {
	const (
		configureCmd = "colca/v1/_CmdConfigure/n1/werk1/signal/upsert"
		maintainCmd  = "colca/v1/_CmdMaintain/n1/werk1/cnc5/calibrate"
	)
	cfg := entry("", cmdAt("werk1", "configure"))
	if !Authorize(ns, cfg, ActCmd, configureCmd) {
		t.Error("a configure grant must admit a configure command")
	}
	if Authorize(ns, cfg, ActCmd, maintainCmd) {
		t.Error("a configure grant must not admit equipment maintenance")
	}

	maint := entry("", cmdAt("werk1", "maintain"))
	if !Authorize(ns, maint, ActCmd, maintainCmd) {
		t.Error("a maintain grant must admit a maintain command")
	}
	if Authorize(ns, maint, ActCmd, configureCmd) {
		t.Error("a maintain grant must not admit data-model editing")
	}
}

func TestConfigureIsAGrantableClass(t *testing.T) {
	g, err := ParseGrant(cmdAt("werk1", "configure,param"))
	if err != nil {
		t.Fatalf("cmd:...:configure rejected: %v", err)
	}
	if g.Verb != "cmd" || len(g.Classes) != 2 {
		t.Fatalf("parsed %+v, want a cmd grant with two classes", g)
	}
	if _, err := ParseGrant(cmdAt("werk1", "configur")); err == nil {
		t.Fatal("a misspelled class must be rejected, not silently ignored")
	}
}

func TestAuthorizeReadRecord(t *testing.T) {
	cases := []struct {
		name  string
		e     *Entry
		topic string
		want  bool
	}{
		{"own zone default", entry("werk1/linie3/cnc5"), "colca/v1/_Metric/01X/werk1/linie3/cnc5/temp", true},
		{"own zone root", entry("werk1"), "colca/v1/_Metric/01X/werk1", true},
		{"outside zone", entry("werk1/linie3/cnc5"), "colca/v1/_Metric/01X/werk1/linie4/x", false},
		{"prefix is not a zone boundary", entry("werk1"), "colca/v1/_Metric/01X/werk10/x", false},
		{"explicit wide grant", entry("werk1/linie3/cnc5", readAt("werk2")), "colca/v1/_Metric/01X/werk2/a/b", true},
		{"read all", entry("", "read:#"), "colca/v1/_Metric/01X/anything/at/all", true},
		{"observer without grants", entry(""), "colca/v1/_Metric/01X/werk1/x", false},
		{"non-UNS record", entry("werk1"), "factory/raw", false},
		{"malformed UNS topic", entry("werk1"), "colca/v1/_Metric/01X", false},
	}
	for _, c := range cases {
		if got := Authorize(ns, c.e, ActReadRecord, c.topic); got != c.want {
			t.Errorf("%s: Authorize(ReadRecord, %q) = %v, want %v", c.name, c.topic, got, c.want)
		}
	}
}

func TestAuthorizeSub(t *testing.T) {
	cases := []struct {
		name   string
		e      *Entry
		filter string
		want   bool
	}{
		{"own zone wildcard", entry("werk1/linie3"), "colca/v1/+/+/werk1/linie3/#", true},
		{"own zone deeper", entry("werk1/linie3"), "colca/v1/_Metric/+/werk1/linie3/cnc5/+", true},
		{"exact zone no wildcards", entry("werk1/linie3"), "colca/v1/_Metric/01X/werk1/linie3", true},
		{"sibling zone", entry("werk1/linie3"), "colca/v1/+/+/werk1/linie4/#", false},
		{"parent zone", entry("werk1/linie3"), "colca/v1/+/+/werk1/#", false},
		{"granted wide", entry("werk1/linie3", readAt("werk1")), "colca/v1/+/+/werk1/#", true},
		{"grant narrower than filter", entry("", readAt("werk1/linie3")), "colca/v1/+/+/werk1/#", false},
		{"hash all denied", entry("werk1/linie3"), "#", false},
		{"hash all with read-all", entry("", "read:#"), "#", true},
		{"uns hash denied", entry("werk1/linie3"), "colca/#", false},
		{"header-only filter", entry("werk1/linie3"), "colca/v1", false},
		{"empty fixed prefix via plus", entry("werk1/linie3"), "colca/v1/+/+/#", false},
		{"non-UNS filter", entry("werk1/linie3"), "factory/raw/+", true},
		{"non-UNS exact", entry(""), "local/announce", true},
		{"plus first segment reaches uns rule", entry("werk1/linie3"), "+/v1/+/+/werk1/linie3/#", true},
	}
	for _, c := range cases {
		if got := Authorize(ns, c.e, ActSub, c.filter); got != c.want {
			t.Errorf("%s: Authorize(Sub, %q) = %v, want %v", c.name, c.filter, got, c.want)
		}
	}
}

// Time-sync design §2.2/§4: every authenticated machine session may subscribe
// the beacon filter regardless of its zone grants — an element-less observer
// with NO grants at all still gets it, which a plain zone/read:# check would
// deny.
func TestAuthorizeSubTimeSyncBypassesZoneGrants(t *testing.T) {
	cases := []struct {
		name   string
		e      *Entry
		filter string
		want   bool
	}{
		{"zone-scoped machine, canonical filter", entry("werk1/linie3"), "colca/v1/_TimeSync/+", true},
		{"unplaced machine with zero grants", entry(""), "colca/v1/_TimeSync/+", true},
		{"concrete node ulid, no wildcard", entry("werk1/linie3"), "colca/v1/_TimeSync/n-edge1", true},
		{"bare contract, no trailing segment", entry(""), "colca/v1/_TimeSync", true},
		{"broad colca/# wildcard is judged normally, not bypassed", entry("werk1/linie3"), "colca/#", false},
		{"broad colca/# wildcard WITH read:# still allowed (normal rule, not the bypass)", entry("", "read:#"), "colca/#", true},
		{"different contract at the same position is not the bypass", entry("werk1/linie3"), "colca/v1/_Metric/+", false},
	}
	for _, c := range cases {
		if got := Authorize(ns, c.e, ActSub, c.filter); got != c.want {
			t.Errorf("%s: Authorize(Sub, %q) = %v, want %v", c.name, c.filter, got, c.want)
		}
	}
}

func TestAuthorizeCmd(t *testing.T) {
	withGrant := entry("hmi", cmdAt("werk1/linie3", "param,operate"))
	cases := []struct {
		name  string
		e     *Entry
		topic string
		want  bool
	}{
		{"granted class in zone", withGrant, "colca/v1/_CmdParam/01T/werk1/linie3/cnc5/setpoint", true},
		{"second granted class", withGrant, "colca/v1/_CmdOperate/01T/werk1/linie3/start", true},
		{"ungranted class", withGrant, "colca/v1/_CmdMaintain/01T/werk1/linie3/calibrate", false},
		{"unknown cmd needs admin", withGrant, "colca/v1/_CmdFoo/01T/werk1/linie3/x", false},
		{"outside zone", withGrant, "colca/v1/_CmdParam/01T/werk2/x", false},
		{"no cmd grant at all", entry("werk1/linie3"), "colca/v1/_CmdParam/01T/werk1/linie3/x", false},
		{"not a command", withGrant, "colca/v1/_Metric/01T/werk1/linie3/x", false},
		{"admin class granted", entry("", cmdAt("werk1", "admin")), "colca/v1/_CmdFoo/01T/werk1/x", true},
	}
	for _, c := range cases {
		if got := Authorize(ns, c.e, ActCmd, c.topic); got != c.want {
			t.Errorf("%s: Authorize(Cmd, %q) = %v, want %v", c.name, c.topic, got, c.want)
		}
	}
}

func TestAdminGrantVerb(t *testing.T) {
	if g, err := ParseGrant("admin:#"); err != nil || g.Verb != "admin" || g.Element != "#" {
		t.Fatalf("admin:# must parse: %+v %v", g, err)
	}
	// Zone-scoped admin is RESERVED grammar (§3): it must be rejected with an
	// error that names the reservation, so introducing it later is additive.
	if _, err := ParseGrant("admin:01HWERK1/#"); err == nil || !strings.Contains(err.Error(), "reserved") {
		t.Fatalf("admin:01HWERK1/# must be rejected as reserved, got %v", err)
	}
	if _, err := ParseGrant("admin:"); err == nil {
		t.Fatal("admin: with an empty zone must be rejected")
	}
}

func TestTokenEntry(t *testing.T) {
	e, err := TokenEntry("kc-sub-1", []string{readAt("werk1"), cmdAt("werk1", "param"), "admin:#"})
	if err != nil {
		t.Fatal(err)
	}
	if e.Kind != KindHuman || e.ULID != "kc-sub-1" || e.Element != "" {
		t.Fatalf("TokenEntry shape: %+v", e)
	}
	if !e.IsAdmin() {
		t.Fatal("IsAdmin must be true with admin:#")
	}
	if _, err := TokenEntry("", nil); err == nil {
		t.Fatal("empty sub must be rejected")
	}
	if _, err := TokenEntry("s", []string{"cmd:01HZ/#"}); err == nil {
		t.Fatal("bad grant must be rejected")
	}
	noAdmin, err := TokenEntry("s2", []string{readAt("z")})
	if err != nil || noAdmin.IsAdmin() {
		t.Fatalf("IsAdmin must be false without admin:#: %v %v", noAdmin, err)
	}
	// Grants absent = read nothing (spec §2.1): no default zone for humans.
	bare, err := TokenEntry("s3", nil)
	if err != nil {
		t.Fatal(err)
	}
	if Authorize(ns, bare, ActReadRecord, "colca/v1/_Metric/x/anything") {
		t.Fatal("a human with no grants must read nothing")
	}
}

// admin unlocks ROUTES, never data: it does not widen read or cmd (§3).
func TestAdminDoesNotImplyReadOrCmd(t *testing.T) {
	e, err := TokenEntry("boss", []string{"admin:#"})
	if err != nil {
		t.Fatal(err)
	}
	if Authorize(ns, e, ActReadRecord, "colca/v1/_Metric/x/werk1/temp") {
		t.Fatal("admin:# must not grant record reads")
	}
	if Authorize(ns, e, ActSub, "colca/#") {
		t.Fatal("admin:# must not grant subscriptions")
	}
	if Authorize(ns, e, ActCmd, "colca/v1/_CmdParam/m1/werk1/go") {
		t.Fatal("admin:# must not grant commands")
	}
}

// Enrollment stays a machine/node affair: humans are tokens, and registry
// identities may not hold admin (provisioning is the _CmdAdmin flow, §3).
func TestEnrollmentRejectsHumanAndAdminGrants(t *testing.T) {
	human := &Entry{ULID: "h1", Pubkey: strings.Repeat("ab", 32), Kind: KindHuman}
	if err := human.Validate(); err == nil {
		t.Fatal("KindHuman must be rejected by enrollment validation")
	}
	m := entry("werk1/cnc5")
	m.Grants = []string{"admin:#"}
	if err := m.Validate(); err == nil {
		t.Fatal("a machine entry holding admin:# must be rejected")
	}
}

// A grant on an element ABOVE this node covers everything here: the node sits
// inside the granted subtree, so there is nothing here that is outside it. This
// is the case a node cannot answer from its own records — the element's record
// lives at the ancestor — and the reason its position is taught to it.
func TestAGrantOnAnAncestorElementCoversThisWholeNode(t *testing.T) {
	// This node is edge1, under site1. It holds m1; site1 it only knows about
	// because its parent told it where it sits.
	sc := mapScope{
		paths: map[string]string{"01HM1": "m1"},
		above: map[string]bool{"01HSITE1": true, "01HEDGE1": true},
	}
	anna, err := TokenEntry("anna", []string{"read:01HSITE1/#"})
	if err != nil {
		t.Fatal(err)
	}
	for _, topic := range []string{
		"colca/v1/_Metric/01X/m1/temp", // inside the node
		"colca/v1/_Metric/01X/anything/at/all",
	} {
		if !Authorize(sc, anna, ActReadRecord, topic) {
			t.Errorf("a grant on an ancestor element must cover %q", topic)
		}
	}
	if !Authorize(sc, anna, ActSub, "colca/#") {
		t.Error("a grant on an ancestor element must cover a whole-node subscription")
	}
}

// A grant on an element this node HOLDS covers that element and below it, and
// nothing else — the same boundary the path grammar enforced, resolved from an
// identity instead of read off a string.
func TestAGrantOnALocalElementCoversOnlyItsSubtree(t *testing.T) {
	sc := mapScope{paths: map[string]string{
		"01HLINE1":  "line1",
		"01HLINE10": "line10",
	}}
	e, err := TokenEntry("anna", []string{"read:01HLINE1/#"})
	if err != nil {
		t.Fatal(err)
	}
	cases := map[string]bool{
		"colca/v1/_Metric/01X/line1":         true,
		"colca/v1/_Metric/01X/line1/m6/temp": true,
		"colca/v1/_Metric/01X/line10/m6":     false, // a sibling that shares a string prefix
		"colca/v1/_Metric/01X/other":         false,
	}
	for topic, want := range cases {
		if got := Authorize(sc, e, ActReadRecord, topic); got != want {
			t.Errorf("Authorize(%q) = %v, want %v", topic, got, want)
		}
	}
}

// The point of the whole design: renaming a position changes where the grant
// applies, and changes nothing about the grant. Nobody re-authors anything.
func TestARenameChangesNothingAboutTheGrant(t *testing.T) {
	sc := mapScope{paths: map[string]string{"01HLINE1": "line1"}}
	e, err := TokenEntry("anna", []string{"read:01HLINE1/#"})
	if err != nil {
		t.Fatal(err)
	}
	if !Authorize(sc, e, ActReadRecord, "colca/v1/_Metric/01X/line1/temp") {
		t.Fatal("setup: the grant must cover the element where it sits")
	}

	sc.paths["01HLINE1"] = "linie-eins" // renamed in the plant model

	if !Authorize(sc, e, ActReadRecord, "colca/v1/_Metric/01X/linie-eins/temp") {
		t.Error("after the rename the same grant must cover the new path")
	}
	if Authorize(sc, e, ActReadRecord, "colca/v1/_Metric/01X/line1/temp") {
		t.Error("after the rename the old path must no longer be covered")
	}
}

// A reparent moves the element's whole subtree, and the verdict moves with it
// on the next decision — no re-authoring, no window in between.
func TestAReparentMovesTheVerdictImmediately(t *testing.T) {
	sc := mapScope{paths: map[string]string{"01HM6": "line1/m6"}}
	e, err := TokenEntry("anna", []string{"read:01HM6/#"})
	if err != nil {
		t.Fatal(err)
	}
	if !Authorize(sc, e, ActReadRecord, "colca/v1/_Metric/01X/line1/m6/temp") {
		t.Fatal("setup: the grant must cover the element where it sits")
	}

	sc.paths["01HM6"] = "line2/m6" // the machine moved to another line

	if !Authorize(sc, e, ActReadRecord, "colca/v1/_Metric/01X/line2/m6/temp") {
		t.Error("the grant must follow the element to its new parent")
	}
	if Authorize(sc, e, ActReadRecord, "colca/v1/_Metric/01X/line1/m6/temp") {
		t.Error("the grant must not linger where the element no longer is")
	}
}

// An element this node has never heard of grants nothing: it is neither above
// the node nor held by it, so there is no position to compare against and the
// answer is no.
func TestAnUnknownElementGrantsNothing(t *testing.T) {
	sc := mapScope{paths: map[string]string{"01HM1": "m1"}}
	e, err := TokenEntry("anna", []string{"read:01HSOMEWHERE-ELSE/#", "cmd:01HSOMEWHERE-ELSE/#:param"})
	if err != nil {
		t.Fatal(err)
	}
	if Authorize(sc, e, ActReadRecord, "colca/v1/_Metric/01X/m1/temp") {
		t.Error("a grant on an unknown element must not read this node's records")
	}
	if Authorize(sc, e, ActSub, "colca/v1/+/+/m1/#") {
		t.Error("a grant on an unknown element must not subscribe here")
	}
	if Authorize(sc, e, ActCmd, "colca/v1/_CmdParam/01T/m1/go") {
		t.Error("a grant on an unknown element must not command here")
	}
}

// Fail closed, and stay useful: a node that has never learned its position
// resolves no scoped grant — it knows of no ancestor and holds no element it
// can place — while the frame-invariant "#" grants keep working exactly as
// before. This replaces the old "prefix never learned" rule and falls out of
// the same lookup instead of being a special case.
func TestANodeThatKnowsNothingFailsClosedOnScopedGrantsOnly(t *testing.T) {
	unpositioned := mapScope{} // no elements, no ancestors

	scoped, err := TokenEntry("anna", []string{"read:01HSITE1/#"})
	if err != nil {
		t.Fatal(err)
	}
	if Authorize(unpositioned, scoped, ActReadRecord, "colca/v1/_Metric/01X/m1/temp") {
		t.Error("a scoped grant must resolve to nothing while the node has no position")
	}

	wide, err := TokenEntry("ops", []string{"read:#", "cmd:#:admin"})
	if err != nil {
		t.Fatal(err)
	}
	if !Authorize(unpositioned, wide, ActReadRecord, "colca/v1/_Metric/01X/m1/temp") {
		t.Error("read:# is frame-invariant and must survive an unknown position")
	}
	if !Authorize(unpositioned, wide, ActCmd, "colca/v1/_CmdFoo/01T/m1/go") {
		t.Error("cmd:#:admin is frame-invariant and must survive an unknown position")
	}
}

// --- FormatGrant: constructing and parsing live in one package ---------------

func TestFormatGrantRoundTripsThroughParseGrant(t *testing.T) {
	// The claim that makes it safe for anything to BUILD grants by asking here
	// instead of writing the grammar down again.
	for _, want := range []string{
		"read:#",
		"read:01HM6/#",
		"admin:#",
		"cmd:#:operate",
		"cmd:01HM6/#:operate",
		"cmd:01HM6/#:configure,maintain,operate,param",
	} {
		parsed, err := ParseGrant(want)
		if err != nil {
			t.Fatalf("ParseGrant(%q): %v", want, err)
		}
		got, err := FormatGrant(parsed)
		if err != nil {
			t.Fatalf("FormatGrant(%+v): %v", parsed, err)
		}
		if got != want {
			t.Errorf("round trip: %q -> %+v -> %q", want, parsed, got)
		}
	}
}

func TestFormatGrantSortsClassesSoTheSameGrantIsTheSameString(t *testing.T) {
	// Callers diff these strings against stored ones; unstable ordering would
	// make every comparison report a change.
	got, err := FormatGrant(Grant{Verb: "cmd", Element: "01HM6",
		Classes: []string{"operate", "configure", "param"}})
	if err != nil {
		t.Fatalf("FormatGrant: %v", err)
	}
	if got != "cmd:01HM6/#:configure,operate,param" {
		t.Fatalf("got %q", got)
	}
}

func TestFormatGrantRefusesWhatParseGrantWouldRefuse(t *testing.T) {
	for name, g := range map[string]Grant{
		"unknown verb":      {Verb: "delete", Element: "01HM6"},
		"unknown class":     {Verb: "cmd", Element: "01HM6", Classes: []string{"sudo"}},
		"cmd with no class": {Verb: "cmd", Element: "01HM6"},
		"zone-scoped admin": {Verb: "admin", Element: "01HM6"},
	} {
		if _, err := FormatGrant(g); err == nil {
			t.Errorf("%s: FormatGrant accepted %+v", name, g)
		}
	}
}

func TestCmdClassesListsExactlyTheClassesParseGrantAccepts(t *testing.T) {
	for _, class := range CmdClasses() {
		if _, err := ParseGrant("cmd:01HM6/#:" + class); err != nil {
			t.Errorf("CmdClasses offers %q which ParseGrant rejects: %v", class, err)
		}
	}
	if _, err := ParseGrant("cmd:01HM6/#:" + strings.Join(CmdClasses(), ",")); err != nil {
		t.Errorf("the full class list does not parse: %v", err)
	}
}

// --- write: a grant verb, not an identity rule ------------------------------

func TestParseGrantReadsTheWriteVerb(t *testing.T) {
	g, err := ParseGrant("write:el-press3/#")
	if err != nil {
		t.Fatalf("ParseGrant: %v", err)
	}
	if g.Verb != "write" || g.Element != "el-press3" || len(g.Classes) != 0 {
		t.Fatalf("got %+v; want {Verb:write Element:el-press3 Classes:[]}", g)
	}
}

func TestWriteGrantRoundTrips(t *testing.T) {
	for _, s := range []string{"write:#", "write:el-press3/#"} {
		g, err := ParseGrant(s)
		if err != nil {
			t.Fatalf("ParseGrant(%q): %v", s, err)
		}
		back, err := FormatGrant(g)
		if err != nil {
			t.Fatalf("FormatGrant(%+v): %v", g, err)
		}
		if back != s {
			t.Fatalf("round-trip %q → %q", s, back)
		}
	}
}

func TestAWriteGrantTakesNoClasses(t *testing.T) {
	if _, err := ParseGrant("write:el-press3/#:param"); err == nil {
		t.Fatal("write:...:param parsed; a write grant carries no hazard classes")
	}
}

// --- ActPub: writing is decided from the identity's binding -----------------

// scope: el-press3 sits at "line1/press3".
func TestALocalServiceWritesItsOwnSubtreeWithNoGrant(t *testing.T) {
	sc := testScope(map[string]string{"el-press3": "line1/press3"})
	svc := &Entry{ULID: "01J", Kind: KindLocal, Name: "conn", Element: "el-press3"}

	if !Authorize(sc, svc, ActPub, "colca/v1/_Metric/n1/line1/press3/temp") {
		t.Fatal("a local service was denied a write inside its own subtree")
	}
	if Authorize(sc, svc, ActPub, "colca/v1/_Metric/n1/line1/press4/temp") {
		t.Fatal("a local service wrote outside its subtree; binding is the scope")
	}
}

func TestAnUnplacedLocalServiceWritesAnywhereOnTheNode(t *testing.T) {
	sc := testScope(map[string]string{"el-press3": "line1/press3"})
	svc := &Entry{ULID: "01J", Kind: KindLocal, Name: "dataops"} // no element: bound to the node

	if !Authorize(sc, svc, ActPub, "colca/v1/_Metric/n1/anywhere/at/all") {
		t.Fatal("an unplaced local service was denied; bound to the node means the whole node")
	}
}

func TestAMachineNeedsAnExplicitWriteGrant(t *testing.T) {
	sc := testScope(map[string]string{"el-press3": "line1/press3"})
	m := &Entry{ULID: "01J", Pubkey: hex64, Kind: KindMachine, Element: "el-press3"}

	if Authorize(sc, m, ActPub, "colca/v1/_Metric/n1/line1/press3/temp") {
		t.Fatal("a machine wrote with no write grant; outside the deployment, position is not permission")
	}
	m.Grants = []string{"write:el-press3/#"}
	if !Authorize(sc, m, ActPub, "colca/v1/_Metric/n1/line1/press3/temp") {
		t.Fatal("a machine with a covering write grant was denied")
	}
}

func TestAGrantNamingAnUnheldElementIsInert(t *testing.T) {
	sc := testScope(map[string]string{"el-press3": "line1/press3"})
	m := &Entry{ULID: "01J", Pubkey: hex64, Kind: KindMachine, Element: "el-press3",
		Grants: []string{"write:el-elsewhere/#"}}

	if Authorize(sc, m, ActPub, "colca/v1/_Metric/n1/other/place") {
		t.Fatal("a grant naming an element this node has never heard of granted something")
	}
}
