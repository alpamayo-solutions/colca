package tpmkey_test

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/sha256"
	"crypto/sha512"
	"testing"

	"github.com/alpamayo-solutions/colca/internal/identity/tpmkey"
	"github.com/alpamayo-solutions/colca/internal/identity/tpmtest"
)

func open(t *testing.T) *tpmkey.Device {
	t.Helper()
	dev, err := tpmkey.Open(tpmtest.Start(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = dev.Close() })
	return dev
}

func TestAKeyCreatedInTheTPMSignsAndReloadsFromItsBlob(t *testing.T) {
	dev := open(t)
	k, err := dev.CreateKey()
	if err != nil {
		t.Fatal(err)
	}
	for _, h := range []crypto.Hash{crypto.SHA256, crypto.SHA384} {
		var digest []byte
		if h == crypto.SHA256 {
			d := sha256.Sum256([]byte("colca"))
			digest = d[:]
		} else {
			d := sha512.Sum384([]byte("colca"))
			digest = d[:]
		}
		sig, err := k.Sign(rand.Reader, digest, h)
		if err != nil {
			t.Fatalf("%v: %v", h, err)
		}
		if !ecdsa.VerifyASN1(k.Public().(*ecdsa.PublicKey), digest, sig) {
			t.Fatalf("%v: signature does not verify", h)
		}
	}
	a := k.TPMPublic().ObjectAttributes
	if !a.FixedTPM || !a.FixedParent {
		t.Fatal("node key may leave the TPM")
	}
	if err := k.Close(); err != nil {
		t.Fatal(err)
	}

	again, err := dev.LoadKey(k.Blob())
	if err != nil {
		t.Fatal(err)
	}
	if !again.Public().(*ecdsa.PublicKey).Equal(k.Public()) {
		t.Fatal("reloaded blob is a different key")
	}
	d := sha256.Sum256([]byte("after reload"))
	sig, err := again.Sign(rand.Reader, d[:], crypto.SHA256)
	if err != nil || !ecdsa.VerifyASN1(again.Public().(*ecdsa.PublicKey), d[:], sig) {
		t.Fatalf("reloaded key cannot sign: %v", err)
	}
}

func TestABlobFromAnotherTPMDoesNotLoad(t *testing.T) {
	k, err := open(t).CreateKey()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := open(t).LoadKey(k.Blob()); err == nil {
		t.Fatal("a key blob loaded into a different TPM")
	}
}

func TestParseBlobRefusesGarbage(t *testing.T) {
	for _, raw := range [][]byte{nil, []byte("not pem"), []byte("-----BEGIN COLCA TPM2 KEY-----\nAAAA\n-----END COLCA TPM2 KEY-----\n")} {
		if _, _, err := tpmkey.ParseBlob(raw); err == nil {
			t.Fatalf("parsed %q", raw)
		}
	}
	if tpmkey.IsBlob([]byte("-----BEGIN PRIVATE KEY-----\nAAAA\n-----END PRIVATE KEY-----\n")) {
		t.Fatal("a PKCS#8 file is not a TPM blob")
	}
}
