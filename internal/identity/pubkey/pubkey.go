// Package pubkey encodes node public keys and their fingerprints. A public key
// is written as hex of its SubjectPublicKeyInfo DER; the fingerprint is
// SHA-256 over that DER. Node keys are ed25519 or ECDSA P-256.
//
// The package depends on the standard library only, so the registry entry
// shape (plugins/uns) can validate keys without pulling in key stores.
package pubkey

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"fmt"
	"strings"
)

// Marshal returns pub as SubjectPublicKeyInfo DER. Only the key
// algorithms a node key may use are accepted: ed25519 and ECDSA P-256.
func Marshal(pub crypto.PublicKey) ([]byte, error) {
	if Algorithm(pub) == "" {
		return nil, fmt.Errorf("unsupported key %T: want ed25519 or ECDSA P-256", pub)
	}
	return x509.MarshalPKIXPublicKey(pub)
}

// Hex returns pub as SPKI hex, the stored form of every new key.
func Hex(pub crypto.PublicKey) (string, error) {
	spki, err := Marshal(pub)
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(spki), nil
}

// ParseHex reads a public key written as hex: SPKI DER (the current
// form), or the legacy form of 64 hex characters holding a raw ed25519 key.
//
// The legacy form exists because registries and parent.pubkey settings
// written before SPKI hold it. Once the parent's adoption pass rewrites every
// registry entry to SPKI (node enrollment spec §7.1), registry comparisons stop
// needing it; parent.pubkey in a node config keeps being accepted in either
// form.
func ParseHex(s string) (crypto.PublicKey, error) {
	raw, err := hex.DecodeString(strings.TrimSpace(s))
	if err != nil {
		return nil, fmt.Errorf("public key is not hex: %w", err)
	}
	if len(raw) == ed25519.PublicKeySize {
		return ed25519.PublicKey(raw), nil
	}
	pub, err := x509.ParsePKIXPublicKey(raw)
	if err != nil {
		return nil, fmt.Errorf("public key is neither SPKI DER nor a raw ed25519 key: %w", err)
	}
	if Algorithm(pub) == "" {
		return nil, fmt.Errorf("unsupported key %T: want ed25519 or ECDSA P-256", pub)
	}
	return pub, nil
}

// NormalizeHex returns s, in either accepted form, as SPKI hex.
func NormalizeHex(s string) (string, error) {
	pub, err := ParseHex(s)
	if err != nil {
		return "", err
	}
	return Hex(pub)
}

// Same reports whether two hex public keys, in either accepted form,
// name the same key. Unparseable input is never the same as anything.
func Same(a, b string) bool {
	na, err := NormalizeHex(a)
	if err != nil {
		return false
	}
	nb, err := NormalizeHex(b)
	return err == nil && na == nb
}

// Fingerprint is SHA-256 over SPKI DER, written "SHA256:" followed by the
// digest as upper-case hex pairs separated by ":".
func Fingerprint(spki []byte) string {
	sum := sha256.Sum256(spki)
	return "SHA256:" + pairs(sum[:])
}

// Short is the first four pairs of a fingerprint, "SHA256:" kept:
// short enough to read out loud when comparing a device with a dialog.
func Short(fp string) string {
	body, ok := strings.CutPrefix(fp, "SHA256:")
	if !ok {
		return fp
	}
	parts := strings.SplitN(body, ":", 5)
	if len(parts) > 4 {
		parts = parts[:4]
	}
	return "SHA256:" + strings.Join(parts, ":")
}

// FingerprintHex is the fingerprint of a hex public key in either accepted
// form. A legacy raw ed25519 key has the fingerprint of its SPKI form.
func FingerprintHex(s string) (string, error) {
	pub, err := ParseHex(s)
	if err != nil {
		return "", err
	}
	spki, err := Marshal(pub)
	if err != nil {
		return "", err
	}
	return Fingerprint(spki), nil
}

// FingerprintOf is the fingerprint of any public key x509 can marshal. Unlike
// the node key functions it does not restrict the algorithm: a TPM's RSA
// endorsement key has a fingerprint too.
func FingerprintOf(pub crypto.PublicKey) (string, error) {
	spki, err := x509.MarshalPKIXPublicKey(pub)
	if err != nil {
		return "", err
	}
	return Fingerprint(spki), nil
}

// FingerprintID is the fingerprint as 64 lower-case hex characters without
// separators: the form a fingerprint takes in URL paths and record keys. The
// display form is Fingerprint's.
func FingerprintID(spki []byte) string {
	sum := sha256.Sum256(spki)
	return hex.EncodeToString(sum[:])
}

// ParseFingerprint reads a fingerprint in either form, display
// ("SHA256:AB:CD:…", any case) or ID (64 hex characters), and returns its ID
// form.
func ParseFingerprint(s string) (string, error) {
	body := s
	if b, ok := strings.CutPrefix(s, "SHA256:"); ok {
		body = strings.ReplaceAll(b, ":", "")
	}
	raw, err := hex.DecodeString(body)
	if err != nil || len(raw) != sha256.Size {
		return "", fmt.Errorf("fingerprint %q: want SHA256:XX:… or 64 hex characters", s)
	}
	return hex.EncodeToString(raw), nil
}

func pairs(b []byte) string {
	h := strings.ToUpper(hex.EncodeToString(b))
	var sb strings.Builder
	for i := 0; i < len(h); i += 2 {
		if i > 0 {
			sb.WriteByte(':')
		}
		sb.WriteString(h[i : i+2])
	}
	return sb.String()
}

// Algorithm names a supported node key algorithm, "ed25519" or "ecdsa-p256",
// and is empty for anything else.
func Algorithm(pub crypto.PublicKey) string {
	switch k := pub.(type) {
	case ed25519.PublicKey:
		if len(k) == ed25519.PublicKeySize {
			return "ed25519"
		}
	case *ecdsa.PublicKey:
		if k.Curve == elliptic.P256() {
			return "ecdsa-p256"
		}
	}
	return ""
}
