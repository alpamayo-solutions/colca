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

// TestCopyHappensBeforeOwnershipChanges pins the ordering that broke every
// world bring-up.
//
// The process runs as uid 0 with all capabilities dropped but CHOWN and
// DAC_OVERRIDE, and chmod'ing a file it does not own needs CAP_FOWNER
// specifically. So the moment chownTree hands a path to the target uid,
// copyTree can no longer set a mode inside it. The initializer died on exactly
// that — "chmod /volumes/keys: operation not permitted" — and every service
// waiting on it never started.
//
// A test process cannot drop a capability, so it cannot feel that failure. It
// asserts the property that makes the capability unnecessary instead: run()
// reaches all of its copying before any of its chowning. That is a claim about
// call order, so it is checked on the syntax tree, the same way the plugin
// boundary is.
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

// TestAnAlreadyCorrectModeIsNotReapplied covers the second run.
//
// By then the tree belongs to the target uid, so a chmod would fail for the
// same reason — and there is nothing to change. The guard is what makes the
// initializer idempotent rather than working exactly once.
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

// TestTheCopyRootKeepsItsOwnPermissions is the case a fresh temp dir hides.
//
// The real target is a named volume the image created 0750 and owned by the
// runtime user; the real source is a directory on a developer's machine, 0755
// by umask. Applying the source's mode to that mount point is a downgrade, and
// it is a chmod on a path this process no longer owns — which without
// CAP_FOWNER can only fail. Every world bring-up died there.
//
// Verifying this on a freshly created directory proves nothing: a fresh one
// already matches, so the chmod is skipped for the wrong reason. The target
// here deliberately starts with a DIFFERENT mode from the source.
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

	// The denominator: the copy still happened, and a directory BELOW the root
	// does take the source's mode. Otherwise "unchanged" could mean "did
	// nothing at all".
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
