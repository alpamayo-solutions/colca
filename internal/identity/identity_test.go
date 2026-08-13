package identity

import (
	"os"
	"path/filepath"
	"testing"
)

func TestGenerateLoadAndCert(t *testing.T) {
	dir := t.TempDir()
	keyPath := filepath.Join(dir, "node.key")
	id, err := Generate(keyPath) // writes PEM, returns Identity
	if err != nil {
		t.Fatal(err)
	}
	if len(id.PublicHex()) != 64 {
		t.Fatalf("want 64 hex chars, got %d", len(id.PublicHex()))
	}

	id2, err := Load(keyPath)
	if err != nil {
		t.Fatal(err)
	}
	if id2.PublicHex() != id.PublicHex() {
		t.Fatal("reload mismatch")
	}

	cert, err := id2.SelfSignedCert("n-test")
	if err != nil {
		t.Fatal(err)
	}
	if len(cert.Certificate) == 0 {
		t.Fatal("empty cert")
	}

	if _, err := os.Stat(keyPath); err != nil {
		t.Fatal(err)
	}
}
