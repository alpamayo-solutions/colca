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

// TestPluginDependsOnStdlibOnly enforces that plugins/uns depends on the
// standard library only, so it can never import the core. It uses
// `go list -deps`, so any new direct or transitive dependency fails with its
// import path.
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
// of the boundary: the core may hold a domain value but never decide by
// comparing against one. Use a predicate such as uns.IsCommand instead.
// Passing a value along (uns.ClassAck, uns.StatusDraining) is fine; only
// comparisons and switch cases fail.
func TestCoreAsksQuestionsRatherThanSwitchingOnVocabulary(t *testing.T) {
	// The core is everything in the module except plugins/uns. Walking the
	// module root instead of a list of directories means a new directory
	// cannot be missed.
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

	// Exclude plugins/uns exactly, by path, so a future plugin is still
	// checked. vendor/ and testdata/ are skipped wherever they appear.
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
