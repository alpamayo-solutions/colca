package uns

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// sanitizeVectorPath is the golden `sanitize` dataset (task-3, path-collision
// parity), the same mechanism vectors_test.go and
// exec_edit_model_test.go's vocabulary pin use: one checked-in file both
// native copies of `sanitize` answer to -- this Go suite, and the api
// Python suite (edge/tests/test_model_rules.py) that pins
// edge/edit/model_rules.py's `sanitize_topic_segment`.
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

// TestSanitizeMatchesTheGoldenVectors pins colca's own `sanitize`
// (exec_configure.go) against the shared dataset. A change to `sanitize`'s
// behavior that is not also reflected in the vectors (and in the api
// Python copy the vectors also judge) fails here.
func TestSanitizeMatchesTheGoldenVectors(t *testing.T) {
	for _, c := range loadSanitizeVectors(t).Cases {
		if got := sanitize(c.Name); got != c.Out {
			t.Errorf("sanitize(%q) = %q, vectors want %q", c.Name, got, c.Out)
		}
	}
}
