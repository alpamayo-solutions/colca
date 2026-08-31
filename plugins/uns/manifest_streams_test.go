package uns

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

// The shared vector, relative to this package. Deliberately a path into the
// contracts package rather than a copy: a copy is the thing this test exists
// to prevent.
const manifestStreamsVector = "../../contracts/src/colca_data_contracts/vectors/manifest_streams.json"

// TestManifestStreamsVectorMatchesStreamFor pins the one fact two languages
// both need: which stream a class routes to.
//
// Both halves matter, and only one of them is obvious. Checking that every
// entry in the vector agrees with StreamFor catches a WRONG entry. Checking
// that every class Go knows HAS an entry catches a missing one — which is the
// failure that actually happened: _Log was added to the Go side, the Python
// consumer's hand-written table never learned about it, and nothing anywhere
// could go red about that.
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

	// The other direction: an entry Go does not recognise is a stream nothing
	// routes to, which reads as coverage and is not.
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
