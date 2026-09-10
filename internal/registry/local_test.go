package registry

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// captureLogs routes slog.Default through a buffer for the test; a Manager built
// after the call logs into it.
func captureLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return &buf
}

// authoredPlacements records what the fake author authored, keyed by path like
// Placements.IDAt.
type authoredPlacements map[string]string

func (p authoredPlacements) IDAt(path string) (string, bool) { id, ok := p[path]; return id, ok }

// newTestManagerWithElements builds a manager wired for self-registration.
// elements maps path to element id. The fake author does not walk intermediate
// segments; that walk is tested in plugins/uns and in
// TestALocalServiceAndACatalogueTagAuthorTheSameElementsThroughOneWalk.
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

// reposition re-enrolls a registered local service under a new element, as an
// operator move would.
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

// A missing declared mount is authored through the authoring hook, and the entry
// binds to whatever it returns.
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

// A declaration seeds an entry and never moves it; otherwise a container restart
// would undo an operator's repositioning.
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

// A declaration that disagrees with the entry's binding is logged, so drift is
// visible.
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

	// A mount that differs only by a leading slash is not drift.
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

func TestNewULIDIsA26CharacterStringThatDiffersAcrossCalls(t *testing.T) {
	a, b := NewULID(), NewULID()
	if len(a) != 26 || len(b) != 26 {
		t.Fatalf("NewULID lengths = %d, %d; want 26", len(a), len(b))
	}
	if a == b {
		t.Fatalf("two successive calls minted the same id: %q", a)
	}
}
