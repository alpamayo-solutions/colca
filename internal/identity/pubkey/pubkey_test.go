package pubkey

import (
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/hex"
	"math/big"
	"strings"
	"testing"

	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// Golden vectors. The expected values were computed outside Go:
//
//	echo -n <spki hex> | xxd -r -p | shasum -a 256
const (
	// RFC 8032 §7.1 test 1 public key.
	ed25519Raw  = "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a"
	ed25519SPKI = "302a300506032b6570032100" + ed25519Raw
	ed25519FP   = "SHA256:06:E3:FD:8F:DA:29:BB:60:AB:59:55:7D:E6:1E:DB:0A:EC:DB:23:11:34:BE:30:E7:5B:45:5F:8E:1B:79:2F:A9"

	// The P-256 base point as a public key.
	p256X    = "6b17d1f2e12c4247f8bce6e563a440f277037d812deb33a0f4a13945d898c296"
	p256Y    = "4fe342e2fe1a7f9b8ee7eb4a7c0f9e162bce33576b315ececbb6406837bf51f5"
	p256SPKI = "3059301306072a8648ce3d020106082a8648ce3d03010703420004" + p256X + p256Y
	p256FP   = "SHA256:5C:D2:52:FB:0C:E8:93:24:36:FA:F8:CC:D1:04:09:81:B8:9E:E4:AD:6B:9F:E9:E2:A2:B7:E7:1A:AC:B2:7C:D3"
)

func TestGoldenVectorsEd25519(t *testing.T) {
	raw, _ := hex.DecodeString(ed25519Raw)
	got, err := Hex(ed25519.PublicKey(raw))
	if err != nil || got != ed25519SPKI {
		t.Fatalf("SPKI hex = %s, %v; want %s", got, err, ed25519SPKI)
	}
	spki, _ := hex.DecodeString(ed25519SPKI)
	if fp := Fingerprint(spki); fp != ed25519FP {
		t.Fatalf("fingerprint = %s, want %s", fp, ed25519FP)
	}
	if s := Short(ed25519FP); s != "SHA256:06:E3:FD:8F" {
		t.Fatalf("short = %s", s)
	}
	// The legacy raw form has the fingerprint of its SPKI form.
	for _, in := range []string{ed25519Raw, ed25519SPKI} {
		if fp, err := FingerprintHex(in); err != nil || fp != ed25519FP {
			t.Fatalf("FingerprintHex(%s) = %s, %v", in, fp, err)
		}
	}
}

func TestGoldenVectorsP256(t *testing.T) {
	x, _ := new(big.Int).SetString(p256X, 16)
	y, _ := new(big.Int).SetString(p256Y, 16)
	got, err := Hex(&ecdsa.PublicKey{Curve: elliptic.P256(), X: x, Y: y})
	if err != nil || got != p256SPKI {
		t.Fatalf("SPKI hex = %s, %v; want %s", got, err, p256SPKI)
	}
	if fp, err := FingerprintHex(p256SPKI); err != nil || fp != p256FP {
		t.Fatalf("fingerprint = %s, %v; want %s", fp, err, p256FP)
	}
	if s := Short(p256FP); s != "SHA256:5C:D2:52:FB" {
		t.Fatalf("short = %s", s)
	}
}

func TestLegacyRawEd25519NormalisesToSPKI(t *testing.T) {
	n, err := NormalizeHex(ed25519Raw)
	if err != nil || n != ed25519SPKI {
		t.Fatalf("normalised %s, %v", n, err)
	}
	if n, err := NormalizeHex(p256SPKI); err != nil || n != p256SPKI {
		t.Fatalf("SPKI must normalise to itself: %s, %v", n, err)
	}
	if !Same(ed25519Raw, ed25519SPKI) {
		t.Fatal("the raw and SPKI forms of one key compare unequal")
	}
	if Same(ed25519Raw, p256SPKI) || Same("zz", "zz") {
		t.Fatal("different or unparseable keys compare equal")
	}
}

func TestUnsupportedKeysAreRefused(t *testing.T) {
	p384, err := ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Hex(p384.Public()); err == nil {
		t.Fatal("a P-384 key was accepted as a node key")
	}
	for _, in := range []string{"", "abcd", "not hex", ed25519Raw[:62]} {
		if _, err := ParseHex(in); err == nil {
			t.Fatalf("parsed %q", in)
		}
	}
}

// plugins/uns validates entry keys without importing this package; both must
// agree on what a key is.
func TestEntryValidationAgreesWithParseHex(t *testing.T) {
	p384, err := ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	p384DER, err := x509.MarshalPKIXPublicKey(p384.Public())
	if err != nil {
		t.Fatal(err)
	}
	for _, in := range []string{ed25519Raw, ed25519SPKI, p256SPKI, hex.EncodeToString(p384DER), "", "abcd", "zz", ed25519Raw[:62]} {
		_, perr := ParseHex(in)
		uerr := uns.ValidPubkeyHex(in)
		if (perr == nil) != (uerr == nil) {
			t.Errorf("%q: ParseHex err=%v, uns.ValidPubkeyHex err=%v", in, perr, uerr)
		}
	}
}

func TestFingerprintIDAndParse(t *testing.T) {
	spki, _ := hex.DecodeString(ed25519SPKI)
	id := FingerprintID(spki)
	if id != "06e3fd8fda29bb60ab59557de61edb0aecdb231134be30e75b455f8e1b792fa9" {
		t.Fatalf("id = %s", id)
	}
	for _, in := range []string{ed25519FP, strings.ToLower(ed25519FP[7:]), id, "SHA256:" + id} {
		if in == strings.ToLower(ed25519FP[7:]) {
			in = "SHA256:" + in
		}
		if got, err := ParseFingerprint(in); err != nil || got != id {
			t.Fatalf("ParseFingerprint(%q) = %s, %v", in, got, err)
		}
	}
	for _, in := range []string{"", "SHA256:AB", id[:62], "SHA256:" + id + "00"} {
		if _, err := ParseFingerprint(in); err == nil {
			t.Fatalf("parsed %q", in)
		}
	}
}
