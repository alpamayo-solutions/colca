package httpapi

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// adminTokenField is the field name the gate treats as the admin credential. It
// matches by name, without type information.
const adminTokenField = "Token"

// TestAdminTokenIsOnlyUsedInAConstantTimeCompare guards the constant-time
// comparison of the admin token. Behavioural tests cannot tell == from
// subtle.ConstantTimeCompare, so this reads the code.
//
// It walks every non-test .go file under this package and allows exactly two uses
// of the field: a comparison with "" (the no-token guard), and an argument, bare or
// as []byte, to subtle.ConstantTimeCompare or hmac.Equal, with the package name
// checked against the file's imports. Anything else fails, including copying the
// value or passing it to a helper. It also fails when it finds no mention of the
// field at all.
//
// Limits: it reads only this package, matches the field by name, and flags
// len(cfg.API.Token) == 0 even though that is safe.
func TestAdminTokenIsOnlyUsedInAConstantTimeCompare(t *testing.T) {
	files, err := goFilesUnder(".")
	if err != nil {
		t.Fatalf("listing package files: %v", err)
	}
	var walked, matched int
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
		fileOffences, fileMatches := tokenUseOffences(fset, f)
		matched += fileMatches
		offences = append(offences, fileOffences...)
	}
	// A walk that matched no files would pass while reading nothing.
	if walked == 0 {
		if wd, wderr := os.Getwd(); wderr == nil {
			t.Fatalf("no non-test .go files found in %s — this gate read nothing", wd)
		}
		t.Fatal("no non-test .go files found — this gate read nothing")
	}
	// Parsing files says nothing about finding the field. If it was renamed or moved,
	// the gate would pass guarding nothing, so fail.
	if matched == 0 {
		t.Fatalf("walked %d non-test .go file(s) under %q but never saw a %q selector — "+
			"this gate is not reading the admin token at all, so it is protecting nothing. "+
			"If the field was renamed, update adminTokenField to match. If it moved outside "+
			"this package's directory tree, point this gate at its new location — do not "+
			"delete or weaken it.", walked, ".", adminTokenField)
	}
	if len(offences) > 0 {
		t.Fatalf("admin token used outside a constant-time compare in %d place(s):\n  %s",
			len(offences), strings.Join(offences, "\n  "))
	}
}

// goFilesUnder collects every .go file at or below root, so moving the comparison
// into a subpackage keeps it covered.
func goFilesUnder(root string) ([]string, error) {
	var files []string
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if strings.HasSuffix(path, ".go") {
			files = append(files, path)
		}
		return nil
	})
	return files, err
}

// tokenUseOffences reports every disallowed use of the admin token field in a file
// and how often the field appears at all. It tracks parents because the context of
// a mention decides.
func tokenUseOffences(fset *token.FileSet, f *ast.File) ([]string, int) {
	imports := importNames(f)
	rebound := reboundNames(f)

	var offences []string
	var matched int
	var stack []ast.Node
	ast.Inspect(f, func(n ast.Node) bool {
		if n == nil {
			stack = stack[:len(stack)-1] // leaving a node
			return true
		}
		stack = append(stack, n)
		sel, ok := n.(*ast.SelectorExpr)
		if !ok || sel.Sel.Name != adminTokenField {
			return true
		}
		matched++
		if what := disallowedUse(stack, imports, rebound); what != "" {
			offences = append(offences, fmt.Sprintf(
				"%s: the admin token is %s — it may only be compared against \"\" (the "+
					"unconfigured guard) or handed to subtle.ConstantTimeCompare / hmac.Equal; "+
					"any other route carries the unscoped admin credential somewhere this gate "+
					"cannot follow, and variable-time equality on it is a timing side channel",
				fset.Position(sel.Pos()), what))
		}
		return true
	})
	return offences, matched
}

// disallowedUse classifies one mention by its context: "" for an allowed use,
// otherwise a short description. stack ends with the selector, so stack[len-2] is
// its parent.
func disallowedUse(stack []ast.Node, imports map[string]string, rebound map[string]bool) string {
	parent := ancestor(stack, 1)
	switch p := parent.(type) {
	case *ast.BinaryExpr:
		if (p.Op == token.EQL || p.Op == token.NEQ) && (isEmptyStringLiteral(p.X) || isEmptyStringLiteral(p.Y)) {
			return "" // the "no configured token" guard, not a compare against input
		}
		return "compared with " + p.Op.String()

	case *ast.CallExpr:
		// The call is an allowed sink or the []byte conversion in front of one; anything
		// else carries the value away.
		if isConstantTimeCompare(p.Fun, imports, rebound) {
			return ""
		}
		if isByteSliceConversion(p.Fun) {
			if outer, ok := ancestor(stack, 2).(*ast.CallExpr); ok && isConstantTimeCompare(outer.Fun, imports, rebound) {
				return ""
			}
			return "converted to []byte outside a constant-time compare"
		}
		return "passed to " + render(p.Fun)
	}
	return "used in " + describe(parent)
}

// ancestor returns the node n levels above the selector at the top of stack,
// or nil when the stack is shorter than that.
func ancestor(stack []ast.Node, n int) ast.Node {
	if len(stack) <= n {
		return nil
	}
	return stack[len(stack)-1-n]
}

// importNames maps each import's local name to its path, so the gate can check that
// subtle really is crypto/subtle.
func importNames(f *ast.File) map[string]string {
	names := map[string]string{}
	for _, spec := range f.Imports {
		path, err := strconv.Unquote(spec.Path.Value)
		if err != nil {
			continue
		}
		name := path[strings.LastIndex(path, "/")+1:]
		if spec.Name != nil {
			name = spec.Name.Name
		}
		names[name] = path
	}
	return names
}

// reboundNames collects every identifier bound anywhere in the file. Scope-accurate
// resolution would need go/types; a name rebound anywhere in the file is reason
// enough to deny the exemption.
func reboundNames(f *ast.File) map[string]bool {
	bound := map[string]bool{}
	add := func(idents ...*ast.Ident) {
		for _, id := range idents {
			if id != nil && id.Name != "_" {
				bound[id.Name] = true
			}
		}
	}
	addFields := func(fl *ast.FieldList) {
		if fl == nil {
			return
		}
		for _, field := range fl.List {
			add(field.Names...)
		}
	}
	ast.Inspect(f, func(n ast.Node) bool {
		switch node := n.(type) {
		case *ast.AssignStmt:
			if node.Tok == token.DEFINE {
				for _, lhs := range node.Lhs {
					if id, ok := lhs.(*ast.Ident); ok {
						add(id)
					}
				}
			}
		case *ast.ValueSpec:
			add(node.Names...)
		case *ast.TypeSpec:
			add(node.Name)
		case *ast.FuncDecl:
			add(node.Name)
			addFields(node.Recv)
		case *ast.FuncType:
			addFields(node.Params)
			addFields(node.Results)
		case *ast.RangeStmt:
			if node.Tok == token.DEFINE {
				for _, e := range []ast.Expr{node.Key, node.Value} {
					if id, ok := e.(*ast.Ident); ok {
						add(id)
					}
				}
			}
		}
		return true
	})
	return bound
}

// isConstantTimeCompare reports whether fun is subtle.ConstantTimeCompare or
// hmac.Equal, with the package name resolved against the imports and not rebound
// in the file.
func isConstantTimeCompare(fun ast.Expr, imports map[string]string, rebound map[string]bool) bool {
	sel, ok := fun.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	pkg, ok := sel.X.(*ast.Ident)
	if !ok || rebound[pkg.Name] {
		return false // a local of that name shadows the package: not the real sink
	}
	switch imports[pkg.Name] {
	case "crypto/subtle":
		return sel.Sel.Name == "ConstantTimeCompare"
	case "crypto/hmac":
		return sel.Sel.Name == "Equal"
	}
	return false
}

// isByteSliceConversion matches the []byte(...) conversion, the one wrapper allowed
// between the field and its sink.
func isByteSliceConversion(fun ast.Expr) bool {
	arr, ok := fun.(*ast.ArrayType)
	if !ok || arr.Len != nil {
		return false
	}
	elt, ok := arr.Elt.(*ast.Ident)
	return ok && elt.Name == "byte"
}

func isEmptyStringLiteral(n ast.Expr) bool {
	lit, ok := n.(*ast.BasicLit)
	return ok && lit.Kind == token.STRING && (lit.Value == `""` || lit.Value == "``")
}

// render prints a called function as the source reads, for the offence message
// only.
func render(fun ast.Expr) string {
	switch f := fun.(type) {
	case *ast.SelectorExpr:
		if id, ok := f.X.(*ast.Ident); ok {
			return id.Name + "." + f.Sel.Name
		}
		return f.Sel.Name
	case *ast.Ident:
		return f.Name
	}
	return "a call"
}

// describe names the offending context in the message for everything that is
// neither a comparison nor a call — assignment, switch, case, composite
// literal, return, and so on.
func describe(n ast.Node) string {
	switch n.(type) {
	case *ast.AssignStmt:
		return "an assignment (the value would leave this gate's reach under another name)"
	case *ast.ValueSpec:
		return "a variable declaration (the value would leave this gate's reach under another name)"
	case *ast.SwitchStmt:
		return "a switch tag (which leaks exactly as much as ==)"
	case *ast.CaseClause:
		return "a switch case (an implicit ==)"
	case *ast.KeyValueExpr, *ast.CompositeLit:
		return "a composite literal"
	case *ast.ReturnStmt:
		return "a return value"
	case nil:
		return "an expression with no enclosing node"
	}
	return fmt.Sprintf("a %T", n)
}
