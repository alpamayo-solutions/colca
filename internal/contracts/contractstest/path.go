// Package contractstest finds the real generated bundle for tests that must
// check the schemas the doors run. internal/contracts and internal/engine both
// use it, so they read the same file.
package contractstest

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

// GeneratedBundlePath returns the bundle named in COLCA_GENERATED_BUNDLE, or
// the one `make bundle` last wrote under build/bundle/. It skips with the
// command to run when there is neither; the contracts' own tests cover
// generation itself.
func GeneratedBundlePath(t testing.TB) string {
	t.Helper()
	if p := os.Getenv("COLCA_GENERATED_BUNDLE"); p != "" {
		return p
	}
	_, here, _, _ := runtime.Caller(0) // internal/contracts/contractstest/path.go
	repo := filepath.Join(filepath.Dir(here), "..", "..", "..")
	candidates, _ := filepath.Glob(filepath.Join(repo, "build", "bundle", "*.json"))
	if len(candidates) == 0 {
		t.Skip("no generated bundle present — run `make bundle`")
	}
	return candidates[len(candidates)-1]
}
