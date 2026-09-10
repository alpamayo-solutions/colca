package uns

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// sanitizeVectorPath is the golden sanitize dataset that every copy of the
// sanitize rule is tested against.
const sanitizeVectorPath = "../../contracts/src/colca_data_contracts/vectors/sanitize.json"

type sanitizeVectorFile struct {
	Cases []struct {
		Name string `json:"name"`
		Out  string `json:"out"`
	} `json:"cases"`
}

func loadSanitizeVectors(t *testing.T) sanitizeVectorFile {
	t.Helper()
	raw, err := os.ReadFile(filepath.Clean(sanitizeVectorPath))
	if err != nil {
		t.Fatalf("golden sanitize vectors missing: %v", err)
	}
	var v sanitizeVectorFile
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatalf("golden sanitize vectors unparseable: %v", err)
	}
	if len(v.Cases) == 0 {
		t.Fatal("golden sanitize vectors carry no cases")
	}
	return v
}

// TestSanitizeMatchesTheGoldenVectors checks sanitize (exec_configure.go)
// against the shared dataset.
func TestSanitizeMatchesTheGoldenVectors(t *testing.T) {
	for _, c := range loadSanitizeVectors(t).Cases {
		if got := sanitize(c.Name); got != c.Out {
			t.Errorf("sanitize(%q) = %q, vectors want %q", c.Name, got, c.Out)
		}
	}
}
