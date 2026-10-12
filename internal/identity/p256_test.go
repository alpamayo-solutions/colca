package identity

import (
	"path/filepath"
	"testing"
)

func TestP256FileKeyLoadsAndCarriesItsSPKI(t *testing.T) {
	path := filepath.Join(t.TempDir(), "n.key")
	id, err := GenerateP256(path)
	if err != nil {
		t.Fatal(err)
	}
	again, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if again.PublicHex() != id.PublicHex() || again.Algorithm() != "ecdsa-p256" || again.Store != StoreFile {
		t.Fatalf("reload: %s %s %s", again.PublicHex(), again.Algorithm(), again.Store)
	}
	cert, err := again.SelfSignedCert("n")
	if err != nil {
		t.Fatal(err)
	}
	pub, err := PeerPubHex(cert.Certificate[0])
	if err != nil || pub != id.PublicHex() {
		t.Fatalf("PeerPubHex = %s, %v", pub, err)
	}
}
