package main

import (
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
