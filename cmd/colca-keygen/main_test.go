package main

import (
	"path/filepath"
	"testing"
)

func TestIfMissingPreservesProvisionedIdentity(t *testing.T) {
	path := filepath.Join(t.TempDir(), "service.key")
	first, err := generate(path, true)
	if err != nil {
		t.Fatal(err)
	}
	second, err := generate(path, true)
	if err != nil {
		t.Fatal(err)
	}
	if first.PublicHex() != second.PublicHex() {
		t.Fatal("-if-missing rotated an already provisioned service identity")
	}
}
