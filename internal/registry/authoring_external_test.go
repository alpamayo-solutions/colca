// An external test package (registry_test, not registry) because it wires a
// real engine.Engine alongside a real registry.Manager, and internal/engine
// imports internal/registry — an internal test file augmenting registry
// itself cannot also import something that imports registry back (Go's
// import-cycle rule for test binaries), even though production code has no
// such cycle (engine depends on registry; registry never depends on engine).
package registry_test

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/engine"
	"github.com/alpamayo-solutions/colca/internal/registry"
	"github.com/alpamayo-solutions/colca/internal/store"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// TestALocalServiceAndACatalogueTagAuthorTheSameElementsThroughOneWalk pins
// architecture principle 1 against a REAL store: registry.Manager.Register (a
// local service's declared mount) and a catalogue tag's own meta.element
// (ConfigExec.bindCatalogue) must converge on the identical element at a
// path, never mint two. This uses the "element/author" _CmdConfigure verb
// directly to stand in for bindCatalogue's own call — bindCatalogue reaches
// the exact same ConfigExec.authorElementAt this verb calls (proven in
// plugins/uns's exec_configure_test.go
// TestATagsMetaElementAuthorsMissingSegmentsAndReusesExisting); publishing a
// real _DataTags catalogue here would need a loaded schema bundle the floor
// validator does not carry, which is unrelated setup weight for what this
// test exists to pin: the cross-package convergence, not the catalogue
// contract.
//
// Both orders are exercised — a/b/c authored by the registry first and
// reused by the verb, x/y/z authored by the verb first and reused by the
// registry — because "one walk, not two" must hold whichever caller gets
// there first.
func TestALocalServiceAndACatalogueTagAuthorTheSameElementsThroughOneWalk(t *testing.T) {
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	reg, err := registry.New(st, "n1")
	if err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{ULID: "n1", DataDir: t.TempDir()}
	eng := engine.New(st, cfg, reg, func(string, []byte, bool) {}, nil, nil)
	domain := uns.NewConfigExec(eng.EntityStore(), reg, eng.Elements(), nil, registry.NewULID, cfg.Plugin)
	reg.SetNamespace(eng.Elements())
	reg.SetAuthoring(func(path string) (string, error) {
		payload, err := json.Marshal(map[string]string{"path": path})
		if err != nil {
			return "", err
		}
		code, msg, _ := domain.Execute(uns.CommandContext{}, "_CmdConfigure", "element/author", payload)
		if code != 200 {
			return "", fmt.Errorf("author element at %s: %s", path, msg)
		}
		return msg, nil
	})
	author := func(path string) string {
		t.Helper()
		payload, err := json.Marshal(map[string]string{"path": path})
		if err != nil {
			t.Fatal(err)
		}
		code, msg, _ := domain.Execute(uns.CommandContext{}, "_CmdConfigure", "element/author", payload)
		if code != 200 {
			t.Fatalf("element/author %s: %d %s", path, code, msg)
		}
		return msg
	}

	// The registry authors a/b/c first (a local service's declared mount);
	// the verb (standing in for a catalogue tag's meta.element) must reuse it.
	registryEntry, err := reg.Register("svc-registry-first", "a/b/c")
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if registryEntry.Element == "" {
		t.Fatal("Register did not author an element for a/b/c")
	}
	if verbID := author("a/b/c"); verbID != registryEntry.Element {
		t.Fatalf("element/author a/b/c = %q, want the registry's own %q — two elements for one path",
			verbID, registryEntry.Element)
	}

	// The verb authors x/y/z first (standing in for a catalogue tag's own
	// meta.element); a local service later declaring the SAME mount must
	// reuse it, not mint a second one.
	verbFirstID := author("x/y/z")
	catalogueFirstEntry, err := reg.Register("svc-catalogue-first", "x/y/z")
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if catalogueFirstEntry.Element != verbFirstID {
		t.Fatalf("Register bound to %q, want the already-authored x/y/z element %q — two elements for one path",
			catalogueFirstEntry.Element, verbFirstID)
	}

	// Neither branch left a hole: the intermediate segments of both paths
	// were authored too, each exactly once.
	for _, path := range []string{"a", "a/b", "x", "x/y"} {
		if _, ok := eng.Elements().IDAt(path); !ok {
			t.Fatalf("intermediate segment %s was not authored; the path has a hole in it", path)
		}
	}
}
