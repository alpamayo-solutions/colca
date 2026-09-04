package registry

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// captureLogs routes slog.Default through a buffer for the duration of the
// test; a Manager built AFTER the call logs into it (its own copy of this
// helper — internal/repl and internal/engine each keep one too, since a
// package-private test helper cannot be shared across packages).
func captureLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return &buf
}

// authoredPlacements is the test double for the path→id side of authoring: a
// plain map, grown by the fake author closure below exactly as the real
// ConfigExec.authorElementAt (reached in production through the
// "element/author" verb) grows the namespace when Register authors a missing
// mount. Exposed with the same IDAt orientation the real thing answers, so a
// test can check what got authored without a second resolver of its own.
type authoredPlacements map[string]string

func (p authoredPlacements) IDAt(path string) (string, bool) { id, ok := p[path]; return id, ok }

// newTestManagerWithElements builds a manager wired for self-registration:
// SetNamespace so a bound element's mount resolves (Enroll refuses to bind to
// an element this node cannot resolve), and SetAuthoring so Register can
// resolve or author declared mounts. elements maps path -> element id, the
// same orientation Placements.IDAt answers.
//
// The fake author here does NOT walk missing intermediate segments the way
// ConfigExec.authorElementAt does — that walk is domain knowledge and lives
// in exactly one place (architecture principle 1), pinned where it actually
// runs: exec_configure_test.go's
// TestATagsMetaElementAuthorsMissingSegmentsAndReusesExisting, and
// TestALocalServiceAndACatalogueTagAuthorTheSameElementsThroughOneWalk below,
// which exercises the REAL walk through both callers against one store. This
// fake only has to prove Register calls its authoring dependency and binds to
// whatever it returns — Register's own behavior, not the walk's.
func newTestManagerWithElements(t *testing.T, elements map[string]string) *Manager {
	t.Helper()
	m, err := New(openStore(t, t.TempDir()), "01NODE")
	if err != nil {
		t.Fatal(err)
	}
	place := authoredPlacements{}
	resolver := ns{}
	for path, id := range elements {
		place[path] = id
		resolver[id] = path
	}
	m.SetNamespace(resolver)
	m.SetAuthoring(func(path string) (string, error) {
		if id, ok := place[path]; ok {
			return id, nil
		}
		id := "el-" + strings.ReplaceAll(path, "/", "-")
		place[path] = id
		resolver[id] = path
		return id, nil
	})
	return m
}

// reposition simulates an operator narrowing an already-registered local
// service's placement — the data-model door's job, entirely out of band from
// self-registration. It re-enrolls the same ulid under a new element, exactly
// as an administrative move would.
func reposition(t *testing.T, m *Manager, name, elementID string) {
	t.Helper()
	e, ok := m.ByName(name)
	if !ok {
		t.Fatalf("reposition: %q not registered", name)
	}
	updated := uns.Entry{ULID: e.ULID, Kind: e.Kind, Name: e.Name, Element: elementID}
	if _, _, err := m.Enroll(entryJSON(t, updated)); err != nil {
		t.Fatalf("reposition: %v", err)
	}
}

func TestRegisterBindsToAnExistingDeclaredMount(t *testing.T) {
	m := newTestManagerWithElements(t, map[string]string{"line1/press3": "el-press3"})

	e, err := m.Register("connector-opcua", "line1/press3")
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if e.Element != "el-press3" {
		t.Fatalf("bound to %q; want the element already sitting at line1/press3", e.Element)
	}
}

// A missing declared mount is authored through m.author (SetAuthoring),
// and the entry binds to whatever it returns. The walk that authors every
// missing intermediate segment along the way is domain knowledge, pinned
// once where it actually lives — exec_configure_test.go's
// TestATagsMetaElementAuthorsMissingSegmentsAndReusesExisting — and again,
// through this exact door, by
// TestALocalServiceAndACatalogueTagAuthorTheSameElementsThroughOneWalk below.
func TestRegisterAuthorsAMissingDeclaredMount(t *testing.T) {
	m := newTestManagerWithElements(t, map[string]string{})

	e, err := m.Register("connector-opcua", "line1/press3")
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if e.Element == "" {
		t.Fatal("bound to no element; a declared mount with nothing at it must be authored, not left unplaced")
	}
}

func TestRegisterWithNoDeclarationLeavesTheServiceUnplaced(t *testing.T) {
	m := newTestManagerWithElements(t, map[string]string{})

	e, err := m.Register("dataops", "")
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if e.Element != "" {
		t.Fatalf("bound to %q; an undeclared service is unplaced — bound to the node, "+
			"which needs no element authored anywhere", e.Element)
	}
}

// The rule the whole design rests on: a declaration seeds an entry, it never
// maintains one. Without this an operator's repositioning is undone by the next
// container restart, which makes repositioning pointless.
func TestADeclarationDoesNotMoveAnExistingEntry(t *testing.T) {
	m := newTestManagerWithElements(t, map[string]string{"line1/press3": "el-press3"})
	if _, err := m.Register("conn", ""); err != nil {
		t.Fatalf("Register: %v", err)
	}
	reposition(t, m, "conn", "el-press3") // an operator narrows it

	e, err := m.Register("conn", "") // the container restarts and re-declares
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if e.Element != "el-press3" {
		t.Fatalf("reconnect moved the service back to %q; the declaration maintained the entry", e.Element)
	}
}

// Spec §3.2's second safety rule: a declaration that disagrees with the
// entry's current binding is logged, so drift is visible rather than silent.
// TestADeclarationDoesNotMoveAnExistingEntry above always redeclares "",
// which never even reaches the comparison — this is the test that actually
// exercises it, with a declaration that genuinely disagrees.
func TestADeclarationThatDisagreesWithTheEntryIsLoggedButDoesNotMoveIt(t *testing.T) {
	logs := captureLogs(t) // must run before the Manager is built (see captureLogs)
	m := newTestManagerWithElements(t, map[string]string{
		"line1/press3": "el-press3",
		"line2/press9": "el-press9",
	})
	if _, err := m.Register("conn", "line1/press3"); err != nil {
		t.Fatalf("Register: %v", err)
	}
	reposition(t, m, "conn", "el-press9") // an operator moves it to line2/press9

	e, err := m.Register("conn", "line1/press3") // redeclares the ORIGINAL, now-stale mount
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if e.Element != "el-press9" {
		t.Fatalf("bound to %q; the disagreeing declaration moved the entry", e.Element)
	}
	if !strings.Contains(logs.String(), "declares a mount it is not bound to") {
		t.Fatalf("drift between the declaration and the bound entry must be logged:\n%s", logs.String())
	}

	// A declaration that only differs cosmetically (a leading slash) from the
	// bound path must not be logged as drift: elementFor discards empty
	// segments while authoring a mount, so the drift comparison has to fold
	// the same way.
	before := strings.Count(logs.String(), "declares a mount it is not bound to")
	if _, err := m.Register("conn", "/line2/press9"); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if after := strings.Count(logs.String(), "declares a mount it is not bound to"); after != before {
		t.Fatalf("a cosmetically different but equal declaration logged drift: %d occurrences, want %d", after, before)
	}
}

func TestRegisterIsIdempotentAcrossReconnects(t *testing.T) {
	m := newTestManagerWithElements(t, map[string]string{})
	first, _ := m.Register("conn", "")
	second, _ := m.Register("conn", "")

	if first.ULID != second.ULID {
		t.Fatalf("reconnect minted a second identity: %q then %q", first.ULID, second.ULID)
	}
	if n := len(m.List()); n != 1 {
		t.Fatalf("registry holds %d entries after two connects of one service; want 1", n)
	}
}

// NewULID mints via oklog/ulid/v2 (architecture principle 2: a 130-bit
// Crockford base32 text form is a specification, so this package generates it
// through the canonical library rather than a second implementation of its
// own). The shape (26 characters) and the freshness (two calls never
// collide) are ours to pin; the bit layout inside that shape is the
// library's business, not this package's.
func TestNewULIDIsA26CharacterStringThatDiffersAcrossCalls(t *testing.T) {
	a, b := NewULID(), NewULID()
	if len(a) != 26 || len(b) != 26 {
		t.Fatalf("NewULID lengths = %d, %d; want 26", len(a), len(b))
	}
	if a == b {
		t.Fatalf("two successive calls minted the same id: %q", a)
	}
}
