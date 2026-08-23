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

// adminTokenField is the field name this gate treats as the admin credential.
// Name-matched, with no type information: see the "What this gate cannot see"
// list on the test below for what that costs and why it is still the right
// trade here.
const adminTokenField = "Token"

// TestAdminTokenIsOnlyUsedInAConstantTimeCompare is a mechanical gate on the
// fix pinned by TestAdminTokenComparisonRejectsSameLengthMismatch. That test
// drives a correct token, a wrong same-length token, and an empty configured
// token, and checks 200/401/401 — but == answers all three identically to
// subtle.ConstantTimeCompare, so a revert from ConstantTimeCompare back to ==
// would leave every assertion in that test green. Nothing in the behavioral
// suite can catch a regression to a timing side channel; only reading the
// comparison itself can.
//
// It walks EVERY non-test .go file under this package's directory tree, not
// just the one file that holds the comparison today — a gate scoped to one
// file is escaped by moving the comparison to a new file, which is the
// cheapest possible way to reintroduce the very thing it guards. The walk
// recurses (rather than reading one directory) so moving the token-bearing
// route into a subpackage of httpapi does not silently drop it from
// coverage either.
//
// The gate also refuses to pass for free: it counts every mention of
// adminTokenField it saw, allowed or not, and fails if that count is zero.
// filepath matched > 0 only proves it parsed some Go source — renaming the
// field, or moving it to a struct this package never touches, leaves that
// count positive while the gate reads real code and matches nothing, passing
// forever while guarding nothing. That is the same failure shape the
// walked > 0 check below exists to close, one level down: a check that
// cannot go red.
//
// The rule is an ALLOWLIST over every mention of the field, not a blacklist of
// equality-shaped constructs. The admin token has exactly two permitted uses:
//
//   - an == / != against the empty-string literal — the deliberate "no
//     configured token" guard ahead of the compare, which is a test for
//     configuration rather than a comparison against attacker-controlled input;
//   - an argument (bare, or under a []byte conversion) of
//     subtle.ConstantTimeCompare or hmac.Equal.
//
// Every other appearance is an offence, including ones no blacklist would have
// thought to name: `tok := cfg.API.Token` (which would carry the value beyond
// this gate's reach under a different name), `switch cfg.API.Token {`, a
// `case` naming it, a struct literal field, a log argument, or a call to any
// other function — which is what closes the same-package wrapper escape
// (`equalTokens(hdr, cfg.API.Token)` with `func equalTokens(a, b string) bool
// { return a == b }`), since the wrapper is simply not one of the two allowed
// sinks. A gate that reads one expression cannot follow an arbitrary call
// graph, so it refuses to let the value leave instead.
//
// The two allowed sinks are resolved, not name-matched: a call qualifies only
// if the file imports crypto/subtle (resp. crypto/hmac) under that name AND
// that name is not re-bound anywhere in the file. Without that,
// `subtle := subtleFake{}; subtle.Equal(hdr, cfg.API.Token)` shadowed the
// package and exempted itself — verified green against the earlier
// name-matching version of this gate.
//
// What this gate cannot see, stated so no reader assumes it is airtight:
//
//   - It reads this package only. cfg is a *config.Config and any other
//     package holding one could compare the field itself; nothing here would
//     know. Today no other package mentions it (`grep -rn "API.Token"`), which
//     is the property that makes a package-local gate sufficient rather than a
//     package-local gate that merely feels sufficient.
//   - It matches the field by NAME, so it guards routes that name the token
//     and nothing else. Handing the enclosing struct to a helper
//     (`authorize(cfg.API, hdr)`), a method that returns the token, or
//     reflection all carry the value past it. Over-matching is the deliberate
//     direction of that trade: any `.Token` selector in this package is
//     treated as the admin token, so a false positive costs an argument with
//     this test and a false negative costs a side channel.
//   - Known over-match: `len(cfg.API.Token) == 0` is exactly as safe as the
//     allowed `cfg.API.Token == ""` (the "no configured token" guard is a
//     length/emptiness check either way, not a compare against attacker
//     input), but this gate flags it — the selector's immediate parent is
//     the `len(...)` CallExpr, not the outer `== 0` BinaryExpr, so it is
//     classified as "passed to len" rather than recognized as the guard.
//     Deliberately left unfixed: nothing in this package writes the guard
//     that way today (`grep -rn "len(cfg.API.Token)"` finds nothing), and
//     correlating two AST levels — the call and its enclosing comparison
//     against a zero literal — to special-case it would add real complexity
//     for a form nobody uses. If that changes, prefer rewriting the call
//     site to `== ""` over teaching the gate a second accepted spelling of
//     the same guard (one way to do one thing).
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
	// A walk that silently matched nothing would make this gate pass while
	// reading no code at all — the failure mode the branch exists to remove.
	if walked == 0 {
		if wd, wderr := os.Getwd(); wderr == nil {
			t.Fatalf("no non-test .go files found in %s — this gate read nothing", wd)
		}
		t.Fatal("no non-test .go files found — this gate read nothing")
	}
	// walked > 0 only proves the gate parsed source; it says nothing about
	// whether that source still contains the field this gate exists to
	// guard. Renaming adminTokenField, or moving the admin-token comparison
	// to a struct/package this gate never reads, leaves walked positive and
	// matched at zero — the gate would keep passing while protecting
	// nothing. Fail loud and say what to do about it.
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

// goFilesUnder recursively collects every .go file at or below root, so
// moving the admin-token comparison into a subpackage of httpapi (e.g.
// httpapi/routes) does not silently fall out of this gate's reach. A single
// filepath.Glob("*.go") only reads one directory — recursion closes exactly
// that escape while staying inside the httpapi package tree.
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

// tokenUseOffences reports every disallowed appearance of the admin token
// field in one parsed file, plus how many times the field was mentioned at
// all (allowed or not) — the count TestAdminTokenIsOnlyUsedInAConstantTimeCompare
// sums across files to make sure the gate is reading the field it claims to
// guard, not silently matching nothing. The walk carries a parent stack
// because the rule is about the CONTEXT a mention sits in, not about the
// mention itself.
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

// disallowedUse classifies one mention of the admin token by the context it
// sits in. It returns "" when the use is one of the two allowed ones, and a
// short description of the offending context otherwise. stack ends with the
// selector itself, so stack[len-2] is its parent.
func disallowedUse(stack []ast.Node, imports map[string]string, rebound map[string]bool) string {
	parent := ancestor(stack, 1)
	switch p := parent.(type) {
	case *ast.BinaryExpr:
		if (p.Op == token.EQL || p.Op == token.NEQ) && (isEmptyStringLiteral(p.X) || isEmptyStringLiteral(p.Y)) {
			return "" // the "no configured token" guard, not a compare against input
		}
		return "compared with " + p.Op.String()

	case *ast.CallExpr:
		// Either the call IS an allowed sink, or it is the []byte conversion
		// in front of one — anything else (a same-package wrapper, a logger,
		// a variable-time Equal from any package) carries the value away.
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

// importNames maps each import's local name in this file to its path, so the
// gate can ask whether the `subtle` in `subtle.ConstantTimeCompare` is really
// crypto/subtle rather than something that merely renders that way.
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

// reboundNames collects every identifier this file BINDS anywhere: short
// declarations, var/const/type specs, function names, receivers, parameters,
// results, and range variables.
//
// File-wide rather than scope-accurate on purpose. Resolving scopes properly
// means go/types and a full package load; the question this gate actually
// needs answered is narrower — "could the `subtle` in this call be anything
// other than the imported package?" — and a name that is bound ANYWHERE in a
// file is already reason enough to refuse the exemption there. The cost is a
// false positive if somebody names an unrelated local `subtle` or `hmac`,
// which is a trade the gate takes gladly in this direction.
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

// isConstantTimeCompare reports whether fun is one of the two constant-time
// sinks the admin token may reach, with the package qualifier RESOLVED against
// the file's imports and refused if that name is re-bound in the file.
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

// isByteSliceConversion matches the `[]byte(…)` in
// subtle.ConstantTimeCompare([]byte(a), []byte(b)) — the one wrapper allowed
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

// render prints a called function the way the source reads it — "bytes.Equal",
// "equalTokens" — for the offence message, which is the only place it is used:
// nothing is DECIDED by a rendered name (that is isConstantTimeCompare's job,
// and it resolves).
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
