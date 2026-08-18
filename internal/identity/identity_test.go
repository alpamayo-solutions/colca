package identity

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"
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

// --- ServerCert: a supplied certificate, or the key container ---------------

// writeSuppliedPair mints a throwaway self-signed pair whose CommonName is
// "supplied", so a test can tell it apart from the node's own key container.
func writeSuppliedPair(t *testing.T) (certFile, keyFile string) {
	t.Helper()
	dir := t.TempDir()
	certFile = filepath.Join(dir, "node.crt")
	keyFile = filepath.Join(dir, "node.key")

	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "supplied"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, pub, priv)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		t.Fatal(err)
	}
	write := func(path, blockType string, der []byte) {
		if err := os.WriteFile(path, pem.EncodeToMemory(
			&pem.Block{Type: blockType, Bytes: der}), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write(certFile, "CERTIFICATE", der)
	write(keyFile, "PRIVATE KEY", keyDER)
	return certFile, keyFile
}

func commonName(t *testing.T, cert tls.Certificate) string {
	t.Helper()
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	return leaf.Subject.CommonName
}

func TestServerCertPrefersTheSuppliedPair(t *testing.T) {
	id, _, err := LoadOrGenerate(filepath.Join(t.TempDir(), "n.key"))
	if err != nil {
		t.Fatal(err)
	}
	certFile, keyFile := writeSuppliedPair(t)

	cert, err := ServerCert(id, "n-a", certFile, keyFile)
	if err != nil {
		t.Fatal(err)
	}
	if got := commonName(t, cert); got != "supplied" {
		t.Fatalf("served %q, want the supplied certificate", got)
	}
}

func TestServerCertFallsBackToTheKeyContainerWhenNoneIsConfigured(t *testing.T) {
	// Absent configuration must keep today's behaviour exactly: the self-signed
	// cert that carries this node's ed25519 key, which is what peers pin.
	id, _, err := LoadOrGenerate(filepath.Join(t.TempDir(), "n.key"))
	if err != nil {
		t.Fatal(err)
	}
	cert, err := ServerCert(id, "n-a", "", "")
	if err != nil {
		t.Fatal(err)
	}
	if got := commonName(t, cert); got != "n-a" {
		t.Fatalf("served %q, want the node's own key container", got)
	}
}

func TestServerCertRefusesAnUnreadablePair(t *testing.T) {
	// Falling back to self-signed would leave the node quietly serving exactly
	// what the operator configured it not to serve.
	id, _, err := LoadOrGenerate(filepath.Join(t.TempDir(), "n.key"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ServerCert(id, "n-a", "/nonexistent/x.crt", "/nonexistent/x.key"); err == nil {
		t.Fatal("an unreadable certificate silently fell back to self-signed")
	}
}

func TestServerCertRefusesHalfAPair(t *testing.T) {
	id, _, err := LoadOrGenerate(filepath.Join(t.TempDir(), "n.key"))
	if err != nil {
		t.Fatal(err)
	}
	certFile, keyFile := writeSuppliedPair(t)
	if _, err := ServerCert(id, "n-a", certFile, ""); err == nil {
		t.Fatal("cert_file without key_file was accepted")
	}
	if _, err := ServerCert(id, "n-a", "", keyFile); err == nil {
		t.Fatal("key_file without cert_file was accepted")
	}
}
