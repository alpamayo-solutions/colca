package uns

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os/exec"
	"path/filepath"
	"regexp"
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

// TestCoreAsksQuestionsRatherThanSwitchingOnVocabulary enforces the other half
// of the boundary: domain knowledge
// lives in exactly one package, so the core may HOLD a domain value but must
// never make a decision by comparing against one.
//
// `class == uns.ClassCmd` inside internal/ means "commands drain, need a cmd
// grant and flow down" is now a rule in two packages, and the next class added
// here has to be chased through every door that spelled it out. A predicate —
// uns.IsCommand, uns.IsOwnedState, Entry.IsDraining, Entry.MayUseDoor — keeps
// the rule where the vocabulary is.
//
// Value positions are deliberately allowed: passing uns.ClassAck to persist, or
// emitting uns.StatusDraining in an HTTP response, carries a domain value
// without duplicating a domain rule. Only comparisons and switch cases fail.
func TestCoreAsksQuestionsRatherThanSwitchingOnVocabulary(t *testing.T) {
	// The core is everything in the module except plugins/uns itself — the
	// domain package legitimately owns the vocabulary it defines. Walking the
	// module root rather than an enumerated list of core directories is the
	// point: internal/, cmd/, and door/ were once named explicitly here, and
	// that list is exactly what let bench/ (which also imports plugins/uns)
	// go unwalked. A directory list drifts the moment a new one is added;
	// "everything but the domain package" cannot.
	const moduleRoot = "../.."
	vocabulary := regexp.MustCompile(`^(Class|Kind|Status)[A-Z]`)

	isVocab := func(n ast.Expr) (string, bool) {
		sel, ok := n.(*ast.SelectorExpr)
		if !ok {
			return "", false
		}
		pkg, ok := sel.X.(*ast.Ident)
		if !ok || pkg.Name != "uns" || !vocabulary.MatchString(sel.Sel.Name) {
			return "", false
		}
		return "uns." + sel.Sel.Name, true
	}

	// domainDir is plugins/uns exactly — the one package that legitimately
	// owns this vocabulary. Excluding all of plugins/ instead would read the
	// same today (uns is its only occupant) and quietly stop reading a second
	// plugin the day one is added: that plugin would be core-side code with
	// respect to the domain, and letting it switch on uns.Class* unwalked is
	// the drift this gate exists to catch. Excluded by path rather than by
	// name, so a "plugins/uns" appearing elsewhere in the tree would still be
	// walked. vendor/ and testdata/ are excluded by name wherever they appear
	// — the standard Go convention for code this test has no business parsing
	// (third-party sources, fixture data).
	domainDir := filepath.Clean(filepath.Join(moduleRoot, "plugins", "uns"))

	var offences []string
	err := filepath.WalkDir(moduleRoot, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if filepath.Clean(path) == domainDir {
				return fs.SkipDir
			}
			switch d.Name() {
			case "vendor", "testdata":
				return fs.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		fset := token.NewFileSet()
		f, perr := parser.ParseFile(fset, path, nil, 0)
		if perr != nil {
			return perr
		}
		report := func(pos token.Pos, name, form string) {
			offences = append(offences, fmt.Sprintf("%s: %s in a %s — ask a predicate instead",
				fset.Position(pos), name, form))
		}
		ast.Inspect(f, func(n ast.Node) bool {
			switch v := n.(type) {
			case *ast.BinaryExpr:
				if v.Op != token.EQL && v.Op != token.NEQ {
					return true
				}
				for _, side := range []ast.Expr{v.X, v.Y} {
					if name, ok := isVocab(side); ok {
						report(v.Pos(), name, "comparison")
					}
				}
			case *ast.CaseClause:
				for _, e := range v.List {
					if name, ok := isVocab(e); ok {
						report(e.Pos(), name, "switch case")
					}
				}
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("walking %s: %v", moduleRoot, err)
	}
	if len(offences) > 0 {
		t.Fatalf("the core decides by comparing against domain vocabulary in %d place(s):\n  %s",
			len(offences), strings.Join(offences, "\n  "))
	}
}
