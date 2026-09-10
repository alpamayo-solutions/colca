// An external test package, because it wires a real engine next to a real
// registry and engine imports registry.
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

// A local service's declared mount and a catalogue tag's meta.element must
// resolve to the same element at a path, never two. The "element/author" verb
// stands in for bindCatalogue, which calls the same authorElementAt. Both orders
// are covered, since either caller may get there first.
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

	// The verb authors x/y/z first; a service declaring the same mount must reuse it.
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
