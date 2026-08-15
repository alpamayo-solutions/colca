package uns

import (
	"os/exec"
	"strings"
	"testing"
)

// TestPluginDependsOnStdlibOnly enforces the documented dependency boundary
// (README.md "Layout"): plugins/uns depends on
// the Go standard library only — which is what makes it impossible for the
// plugin to import the core. The opposite direction is by design: the core
// (engine, httpapi, repl) imports this package to call Parse/Validate.
//
// The check goes through the Go toolchain itself: `go list -deps` resolves the
// full transitive import graph of the package (test files excluded), so any
// new dependency — direct or transitive — fails here with its import path
// named.
func TestPluginDependsOnStdlibOnly(t *testing.T) {
	out, err := exec.Command("go", "list", "-deps",
		"-f", "{{if not .Standard}}{{.ImportPath}}{{end}}", ".").CombinedOutput()
	if err != nil {
		t.Fatalf("go list -deps ./plugins/uns: %v\n%s", err, out)
	}
	var nonStd []string
	for _, line := range strings.Split(string(out), "\n") {
		if line = strings.TrimSpace(line); line != "" {
			nonStd = append(nonStd, line)
		}
	}
	self := "github.com/alpamayo-solutions/colca/plugins/uns"
	if len(nonStd) != 1 || nonStd[0] != self {
		t.Fatalf("plugins/uns must depend on the standard library only;\n"+
			"got non-stdlib packages %v, want only the package itself [%s]", nonStd, self)
	}
}
