// Package contractstest locates the generator's REAL bundle for the level-1
// tests that must judge the schemas the doors actually run — not a fixture
// mirroring them. It is the one lookup both `internal/contracts` (digest
// parity) and `internal/engine` (door refusal) use, so the two cannot drift
// onto different files.
package contractstest

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

// GeneratedBundlePath returns the bundle `scripts/dev.py test core` generated
// and named in COLCA_GENERATED_BUNDLE. A bare `go test` falls back to whatever
// `scripts/dev.py bundle` last wrote under test-results/bundle/, and skips
// with the command to run when there is nothing — the level-2 parity gate
// covers generation itself.
func GeneratedBundlePath(t testing.TB) string {
	t.Helper()
	if p := os.Getenv("COLCA_GENERATED_BUNDLE"); p != "" {
		return p
	}
	_, here, _, _ := runtime.Caller(0) // colca/internal/contracts/contractstest/path.go
	repo := filepath.Join(filepath.Dir(here), "..", "..", "..", "..")
	candidates, _ := filepath.Glob(filepath.Join(repo, "test-results", "bundle", "*.json"))
	if len(candidates) == 0 {
		t.Skip("no generated bundle present — run `uv run scripts/dev.py bundle`, or `scripts/dev.py test core` which generates it")
	}
	return candidates[len(candidates)-1]
}
