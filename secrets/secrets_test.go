package secrets

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestKeyringPersistsWithOwnerOnlyPermissions(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "keys")
	first, err := OpenKeyring(directory)
	if err != nil {
		t.Fatal(err)
	}
	id := first.ActiveKeyID()
	second, err := OpenKeyring(directory)
	if err != nil {
		t.Fatal(err)
	}
	if second.ActiveKeyID() != id {
		t.Fatal("active key changed across restart")
	}
	info, err := os.Stat(filepath.Join(directory, "keyring.json"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("keyring mode = %o, want 600", info.Mode().Perm())
	}
}

func TestOnlyRecipientServiceCanOpenEnvelope(t *testing.T) {
	recipient, err := OpenKeyring(filepath.Join(t.TempDir(), "recipient"))
	if err != nil {
		t.Fatal(err)
	}
	other, err := OpenKeyring(filepath.Join(t.TempDir(), "other"))
	if err != nil {
		t.Fatal(err)
	}
	metadata := recipient.Metadata(time.Now(), 0)
	envelope, err := SealForPublic([]byte("provider-api-key"), metadata.Keys[0])
	if err != nil {
		t.Fatal(err)
	}
	plain, err := recipient.Open(envelope)
	if err != nil || string(plain) != "provider-api-key" {
		t.Fatalf("recipient open = %q, %v", plain, err)
	}
	if _, err := other.Open(envelope); err == nil {
		t.Fatal("another service opened the recipient's envelope")
	}
}

func TestPublicKeyFingerprintIsCheckedBeforeSealing(t *testing.T) {
	ring, err := OpenKeyring(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	key := ring.Metadata(time.Now(), 0).Keys[0]
	key.KeyID = "v1:substituted"
	if _, err := SealForPublic([]byte("secret"), key); err == nil {
		t.Fatal("public key with mismatched fingerprint was accepted")
	}
}

func TestRotationKeepsGraceKeyUntilItIsUnused(t *testing.T) {
	ring, err := OpenKeyring(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	oldEnvelope, err := ring.SealForActive([]byte("old"))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if _, err := ring.Rotate(now); err != nil {
		t.Fatal(err)
	}
	if _, err := ring.Open(oldEnvelope); err != nil {
		t.Fatalf("grace key could not open existing envelope: %v", err)
	}
	if err := ring.Prune(now.Add(2*time.Hour), time.Hour, map[string]struct{}{oldEnvelope.KeyID: {}}); err == nil {
		t.Fatal("pruned a key still referenced by stored ciphertext")
	}
	if err := ring.Prune(now.Add(2*time.Hour), time.Hour, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := ring.Open(oldEnvelope); err == nil {
		t.Fatal("pruned key still opened an envelope")
	}
}

func TestFailedRotationDoesNotChangeTheInMemoryActiveKey(t *testing.T) {
	ring, err := OpenKeyring(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	originalID := ring.ActiveKeyID()
	ring.path = filepath.Join(t.TempDir(), "missing", "keyring.json")
	if _, err := ring.Rotate(time.Now()); err == nil {
		t.Fatal("rotation unexpectedly persisted into a missing directory")
	}
	if ring.ActiveKeyID() != originalID {
		t.Fatalf("failed rotation changed active key from %s to %s", originalID, ring.ActiveKeyID())
	}
	if _, err := ring.SealForActive([]byte("still-restart-safe")); err != nil {
		t.Fatalf("old active key became unusable after failed rotation: %v", err)
	}
}
