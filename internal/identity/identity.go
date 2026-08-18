// Package identity holds the ed25519 node identity: key generation, loading,
// self-signed certificate creation and peer public-key extraction for pinning.
package identity

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"io/fs"
	"math/big"
	"os"
	"path/filepath"
	"time"
)

type Identity struct{ Priv ed25519.PrivateKey }

func Generate(path string) (*Identity, error) {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}
	der, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return nil, err
	}
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})
	if err := os.WriteFile(path, pemBytes, 0o600); err != nil {
		return nil, err
	}
	return &Identity{Priv: priv}, nil
}

// LoadOrGenerate returns the identity at path, minting one if the file does not
// exist yet. The bool reports whether it minted.
//
// First boot mints; every later boot loads. That is what keeps a node's private
// key out of the generated deployment directory: `colca revision` tars, signs
// and publishes that directory, so a key placed there would become a
// distributed artifact — and one bundle installed on several devices would give
// them all the same identity. Neither `colca deploy` nor the OTA agent removes
// volumes, so a key minted on the device survives every update.
//
// A file that exists but cannot be parsed is an ERROR, never a reason to mint.
// Replacing it would silently change the node's identity and orphan it from the
// parent that pinned the old key — a recoverable situation turned into a
// mysterious one.
func LoadOrGenerate(path string) (*Identity, bool, error) {
	id, err := Load(path)
	if err == nil {
		return id, false, nil
	}
	if !errors.Is(err, fs.ErrNotExist) {
		return nil, false, fmt.Errorf("identity %s exists but is unusable "+
			"(refusing to mint over it — that would change this node's identity): %w", path, err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, false, fmt.Errorf("identity %s: %w", path, err)
	}
	id, err = Generate(path)
	if err != nil {
		return nil, false, fmt.Errorf("identity %s: %w", path, err)
	}
	return id, true, nil
}

func Load(path string) (*Identity, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	block, _ := pem.Decode(raw)
	if block == nil {
		return nil, fmt.Errorf("no PEM block in %s", path)
	}
	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, err
	}
	priv, ok := key.(ed25519.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("key in %s is not ed25519", path)
	}
	return &Identity{Priv: priv}, nil
}

func (i *Identity) PublicHex() string {
	return hex.EncodeToString(i.Priv.Public().(ed25519.PublicKey))
}

// SelfSignedCert returns a TLS cert wrapping the ed25519 key (cert = key container, trust = pinning).
func (i *Identity) SelfSignedCert(cn string) (tls.Certificate, error) {
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(10 * 365 * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, i.Priv.Public(), i.Priv)
	if err != nil {
		return tls.Certificate{}, err
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: i.Priv}, nil
}

// PeerPubHex extracts the hex pubkey from a peer leaf certificate (for pinning).
func PeerPubHex(rawCert []byte) (string, error) {
	c, err := x509.ParseCertificate(rawCert)
	if err != nil {
		return "", err
	}
	pub, ok := c.PublicKey.(ed25519.PublicKey)
	if !ok {
		return "", fmt.Errorf("peer key is not ed25519")
	}
	return hex.EncodeToString(pub), nil
}
