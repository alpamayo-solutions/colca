package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

func TestRunMigratesVolumeAndStagesTLSMaterial(t *testing.T) {
	root := t.TempDir()
	volume := filepath.Join(root, "volume")
	inputs := filepath.Join(root, "inputs")
	tlsTarget := filepath.Join(root, "tls")
	treeTarget := filepath.Join(root, "tree-target")
	if err := os.MkdirAll(filepath.Join(volume, "nested"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(inputs, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(volume, "nested", "state"), []byte("state"), 0o600); err != nil {
		t.Fatal(err)
	}
	certSource := filepath.Join(inputs, "node.crt")
	keySource := filepath.Join(inputs, "node.key")
	if err := os.WriteFile(certSource, []byte("certificate"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keySource, []byte("private-key"), 0o600); err != nil {
		t.Fatal(err)
	}

	env := map[string]string{
		uidEnv:       strconv.Itoa(os.Getuid()),
		gidEnv:       strconv.Itoa(os.Getgid()),
		tlsCertEnv:   certSource,
		tlsKeyEnv:    keySource,
		tlsTargetEnv: tlsTarget,
		copyTreeSrc:  inputs,
		copyTreeDst:  treeTarget,
	}
	if err := run([]string{volume}, func(key string) string { return env[key] }); err != nil {
		t.Fatal(err)
	}

	assertFile := func(name, content string, mode os.FileMode) {
		t.Helper()
		path := filepath.Join(tlsTarget, name)
		got, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != content {
			t.Fatalf("%s content = %q, want %q", name, got, content)
		}
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != mode {
			t.Fatalf("%s mode = %o, want %o", name, info.Mode().Perm(), mode)
		}
	}
	assertFile("node.crt", "certificate", 0o644)
	assertFile("node.key", "private-key", 0o600)

	copiedKey, err := os.ReadFile(filepath.Join(treeTarget, "node.key"))
	if err != nil {
		t.Fatal(err)
	}
	if string(copiedKey) != "private-key" {
		t.Fatalf("copied key = %q", copiedKey)
	}
}

func TestRunRejectsPartialTLSConfiguration(t *testing.T) {
	env := map[string]string{
		uidEnv:     strconv.Itoa(os.Getuid()),
		gidEnv:     strconv.Itoa(os.Getgid()),
		tlsCertEnv: "/input/node.crt",
	}
	if err := run([]string{t.TempDir()}, func(key string) string { return env[key] }); err == nil {
		t.Fatal("partial TLS configuration was accepted")
	}
}

// TestCopyHappensBeforeOwnershipChanges: once chownTree hands a path to the
// target uid, a process without CAP_FOWNER cannot chmod inside it. A test cannot
// drop capabilities, so it checks on the syntax tree that run() does all
// copying before any chowning.
func TestCopyHappensBeforeOwnershipChanges(t *testing.T) {
	calls := callOrderIn(t, "run")

	first := func(name string) int {
		for i, call := range calls {
			if call == name {
				return i
			}
		}
		t.Fatalf("run() no longer calls %s — this test can no longer see what it checks", name)
		return -1
	}

	chown := first("chownTree")
	for _, copier := range []string{"copyTree", "stageTLS"} {
		if first(copier) > chown {
			t.Errorf("run() calls chownTree before %s: once a path is owned by the "+
				"target uid, this process cannot chmod inside it (no CAP_FOWNER)", copier)
		}
	}
}

// TestAnAlreadyCorrectModeIsNotReapplied: on a second run the tree belongs to
// the target uid, so skipping an unchanged mode is what makes the initializer
// idempotent.
func TestAnAlreadyCorrectModeIsNotReapplied(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dir")
	if err := os.Mkdir(path, 0o750); err != nil {
		t.Fatal(err)
	}

	if err := chmodIfDifferent(path, 0o750); err != nil {
		t.Fatalf("re-applying the mode it already has: %v", err)
	}

	// The denominator: a mode that DOES differ is still applied, so the check
	// above means "skipped", not "does nothing at all".
	if err := chmodIfDifferent(path, 0o700); err != nil {
		t.Fatalf("applying a different mode: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o700 {
		t.Fatalf("mode is %v, want 0700", info.Mode().Perm())
	}
}

// callOrderIn lists the function calls in one top-level function, in source
// order.
func callOrderIn(t *testing.T, function string) []string {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), "main.go", nil, 0)
	if err != nil {
		t.Fatalf("parsing main.go: %v", err)
	}
	for _, declaration := range file.Decls {
		fn, ok := declaration.(*ast.FuncDecl)
		if !ok || fn.Name.Name != function {
			continue
		}
		calls := []string{}
		ast.Inspect(fn.Body, func(node ast.Node) bool {
			if call, ok := node.(*ast.CallExpr); ok {
				if name, ok := call.Fun.(*ast.Ident); ok {
					calls = append(calls, name.Name)
				}
			}
			return true
		})
		return calls
	}
	t.Fatalf("no function %q in main.go", function)
	return nil
}

// TestTheCopyRootKeepsItsOwnPermissions: the real target is a volume created
// 0750 and owned by the runtime user, the source a 0755 directory, and the
// root's mode must not be copied over. The target starts with a different mode,
// since a fresh directory would already match and prove nothing.
func TestTheCopyRootKeepsItsOwnPermissions(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "source")
	target := filepath.Join(root, "target")
	if err := os.MkdirAll(filepath.Join(source, "nested"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "edge1.key"), []byte("k"), 0o600); err != nil {
		t.Fatal(err)
	}
	// The mount point, as the image leaves it.
	if err := os.Mkdir(target, 0o750); err != nil {
		t.Fatal(err)
	}

	if err := copyTree(source, target, os.Getuid(), os.Getgid()); err != nil {
		t.Fatalf("copy tree: %v", err)
	}

	info, err := os.Stat(target)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o750 {
		t.Errorf("the copy root's mode became %v; the source's 0755 was applied over "+
			"the mount point the deployment created", info.Mode().Perm())
	}

	// The copy still happened, and a directory below the root does take the
	// source's mode.
	if _, err := os.Stat(filepath.Join(target, "edge1.key")); err != nil {
		t.Fatalf("the copy did not land: %v", err)
	}
	nested, err := os.Stat(filepath.Join(target, "nested"))
	if err != nil {
		t.Fatal(err)
	}
	if nested.Mode().Perm() != 0o755 {
		t.Errorf("nested directory mode is %v, want 0755 from the source", nested.Mode().Perm())
	}
}
