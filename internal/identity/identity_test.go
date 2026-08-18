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

// --- LoadOrGenerate: first boot mints, every later boot loads ---------------

func TestLoadOrGenerateMintsOnceThenLoads(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.key")

	first, minted, err := LoadOrGenerate(path)
	if err != nil || !minted {
		t.Fatalf("first call: minted=%v err=%v", minted, err)
	}
	second, minted, err := LoadOrGenerate(path)
	if err != nil || minted {
		t.Fatalf("second call: minted=%v err=%v", minted, err)
	}
	if first.PublicHex() != second.PublicHex() {
		t.Fatal("a node changed identity between boots — the parent that pinned " +
			"the old key would reject it, and the node would be orphaned")
	}
}

func TestLoadOrGenerateRefusesAnUnreadableKeyRatherThanMintingOver(t *testing.T) {
	// Minting over a key that exists but cannot be parsed would silently change
	// the node's identity. Refusing keeps a recoverable situation recoverable.
	path := filepath.Join(t.TempDir(), "node.key")
	if err := os.WriteFile(path, []byte("not a key"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := LoadOrGenerate(path); err == nil {
		t.Fatal("a corrupt key file was silently replaced")
	}
}

func TestLoadOrGenerateCreatesTheDirectory(t *testing.T) {
	// The key lives on a volume that is empty on a device's first boot.
	path := filepath.Join(t.TempDir(), "keys", "node.key")
	if _, minted, err := LoadOrGenerate(path); err != nil || !minted {
		t.Fatalf("LoadOrGenerate: minted=%v err=%v", minted, err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("key not written: %v", err)
	}
}

func TestAMintedKeyIsNotWorldReadable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "keys", "node.key")
	if _, _, err := LoadOrGenerate(path); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Fatalf("key mode %o, want 600", perm)
	}
}
