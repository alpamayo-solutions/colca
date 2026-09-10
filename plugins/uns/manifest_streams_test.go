package uns

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

// manifestStreamsVector is the shared vector, read from the contracts package
// rather than copied.
const manifestStreamsVector = "../../contracts/src/colca_data_contracts/vectors/manifest_streams.json"

// TestManifestStreamsVectorMatchesStreamFor checks which stream each class
// routes to, in both directions: every vector entry agrees with StreamFor, and
// every class Go knows has an entry, so a class added on one side only fails.
func TestManifestStreamsVectorMatchesStreamFor(t *testing.T) {
	raw, err := os.ReadFile(filepath.FromSlash(manifestStreamsVector))
	if err != nil {
		t.Fatalf("reading the shared vector: %v", err)
	}
	var vector struct {
		Streams map[string]string `json:"streams"`
	}
	if err := json.Unmarshal(raw, &vector); err != nil {
		t.Fatalf("parsing the shared vector: %v", err)
	}

	known := ManifestClassNames()
	if len(known) == 0 {
		t.Fatal("no manifest classes at all — the vector below would then assert nothing")
	}

	for _, name := range known {
		class, ok := ClassFromManifest(name)
		if !ok {
			t.Fatalf("ManifestClassNames lists %q but ClassFromManifest does not map it", name)
		}
		want := StreamFor(class)
		got, present := vector.Streams[name]
		if !present {
			t.Errorf("class %q routes to stream %q in Go and is missing from %s — "+
				"every Python consumer of that mapping is now one class behind",
				name, want, manifestStreamsVector)
			continue
		}
		if got != want {
			t.Errorf("class %q: vector says stream %q, StreamFor says %q", name, got, want)
		}
	}

	// The other direction: an entry Go does not know routes nowhere.
	surplus := []string{}
	for name := range vector.Streams {
		if _, ok := ClassFromManifest(name); !ok {
			surplus = append(surplus, name)
		}
	}
	sort.Strings(surplus)
	for _, name := range surplus {
		t.Errorf("vector names class %q, which colca does not know", name)
	}
}
