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
	"fmt"
	"math/big"
	"os"
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
