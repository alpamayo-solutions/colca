package httpapi

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestAdminTokenIsNeverComparedDirectly is a mechanical gate on the fix pinned
// by TestAdminTokenComparisonRejectsSameLengthMismatch. That test drives a
// correct token, a wrong same-length token, and an empty configured token,
// and checks 200/401/401 — but == answers all three identically to
// subtle.ConstantTimeCompare, so a revert from ConstantTimeCompare back to ==
// would leave every assertion in that test green. Nothing in the behavioral
// suite can catch a regression to a timing side channel; only reading the
// comparison itself can.
//
// It walks EVERY non-test file of the package, not just the one that holds the
// comparison today — a gate scoped to one file is escaped by moving the
// comparison to a new file, which is the cheapest possible way to reintroduce
// the very thing it guards. Within those files it refuses the admin token
// field (cfg.API.Token) as an operand of any equality-shaped construct a
// reader would reach for:
//
//   - == and != (except against the empty-string literal, which is the
//     deliberate "no configured token" guard ahead of the compare, not a
//     comparison against attacker-controlled input);
//   - a byte/string equality helper — bytes.Equal, strings.EqualFold,
//     strings.Compare, slices.Equal, reflect.DeepEqual and anything else
//     named Equal/EqualFold/Compare/DeepEqual. The constant-time families
//     (crypto/subtle, crypto/hmac) are the point of the exercise and are
//     allowed;
//   - a switch on the token, or a case naming it — `switch cfg.API.Token {
//     case hdr: }` leaks exactly as much as == does, and no BinaryExpr walk
//     sees it.
func TestAdminTokenIsNeverComparedDirectly(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("listing package files: %v", err)
	}
	var walked int
	var offences []string
	for _, path := range files {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			t.Fatalf("parsing %s: %v", path, err)
		}
		walked++
		offences = append(offences, tokenCompareOffences(fset, f)...)
	}
	// A glob that silently matched nothing would make this gate pass while
	// reading no code at all — the failure mode the branch exists to remove.
	if walked == 0 {
		if wd, wderr := os.Getwd(); wderr == nil {
			t.Fatalf("no non-test .go files found in %s — this gate read nothing", wd)
		}
		t.Fatal("no non-test .go files found — this gate read nothing")
	}
	if len(offences) > 0 {
		t.Fatalf("admin token compared directly in %d place(s):\n  %s",
			len(offences), strings.Join(offences, "\n  "))
	}
}

// tokenCompareOffences reports every equality-shaped use of the admin token
// field in one parsed file. Split out from the test so the three constructs
// read as three rules rather than one nested walk.
func tokenCompareOffences(fset *token.FileSet, f *ast.File) []string {
	var offences []string
	report := func(pos token.Pos, what string) {
		offences = append(offences, fmt.Sprintf(
			"%s: the admin token is compared with %s instead of subtle.ConstantTimeCompare — "+
				"any variable-time equality on the unscoped admin credential is a timing side channel",
			fset.Position(pos), what))
	}

	ast.Inspect(f, func(n ast.Node) bool {
		switch node := n.(type) {
		case *ast.BinaryExpr:
			if node.Op != token.EQL && node.Op != token.NEQ {
				return true
			}
			var other ast.Expr
			switch {
			case isAdminTokenField(node.X):
				other = node.Y
			case isAdminTokenField(node.Y):
				other = node.X
			default:
				return true
			}
			if isEmptyStringLiteral(other) {
				return true // the "no configured token" guard, not a compare
			}
			report(node.Pos(), node.Op.String())

		case *ast.CallExpr:
			name, constantTime := equalityHelper(node.Fun)
			if name == "" || constantTime {
				return true
			}
			for _, arg := range node.Args {
				if mentionsAdminTokenField(arg) {
					report(node.Pos(), name)
					break
				}
			}

		case *ast.SwitchStmt:
			if node.Tag != nil && mentionsAdminTokenField(node.Tag) {
				report(node.Pos(), "a switch on it")
				return true
			}
			for _, stmt := range node.Body.List {
				clause, ok := stmt.(*ast.CaseClause)
				if !ok {
					continue
				}
				for _, expr := range clause.List {
					// A `case` expression under a non-token tag is an implicit
					// ==; under `switch {` it is a plain boolean and the
					// BinaryExpr rule above already saw it.
					if node.Tag != nil && mentionsAdminTokenField(expr) {
						report(expr.Pos(), "a switch case naming it")
					}
				}
			}
		}
		return true
	})
	return offences
}

// isAdminTokenField matches the cfg.API.Token selector itself.
func isAdminTokenField(n ast.Expr) bool {
	sel, ok := n.(*ast.SelectorExpr)
	return ok && sel.Sel.Name == "Token"
}

// mentionsAdminTokenField matches it anywhere inside an expression, so a
// conversion or a helper wrapper — []byte(cfg.API.Token) — does not hide it.
func mentionsAdminTokenField(n ast.Expr) bool {
	found := false
	ast.Inspect(n, func(inner ast.Node) bool {
		if expr, ok := inner.(ast.Expr); ok && isAdminTokenField(expr) {
			found = true
		}
		return !found
	})
	return found
}

func isEmptyStringLiteral(n ast.Expr) bool {
	lit, ok := n.(*ast.BasicLit)
	return ok && lit.Kind == token.STRING && (lit.Value == `""` || lit.Value == "``")
}

// equalityHelper classifies a called function by name alone. name is the
// rendered call ("bytes.Equal") when the function is equality-shaped, and ""
// otherwise; constantTime marks the two families that are the intended answer
// rather than the problem. Name-based on purpose: the gate must hold with no
// type information, and a variable-time Equal from any package leaks the same
// way bytes.Equal does.
func equalityHelper(fun ast.Expr) (name string, constantTime bool) {
	var pkg, fn string
	switch f := fun.(type) {
	case *ast.SelectorExpr:
		fn = f.Sel.Name
		if ident, ok := f.X.(*ast.Ident); ok {
			pkg = ident.Name
		}
	case *ast.Ident:
		fn = f.Name
	default:
		return "", false
	}
	switch fn {
	case "Equal", "EqualFold", "Compare", "DeepEqual":
	default:
		return "", false
	}
	if pkg != "" {
		name = pkg + "." + fn
	} else {
		name = fn
	}
	return name, pkg == "subtle" || pkg == "hmac"
}
