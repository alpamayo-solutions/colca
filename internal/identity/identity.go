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
// exist; the bool reports whether it minted. Minting on the device keeps the
// private key out of installation media. A file that exists but cannot be
// parsed is an error, never a reason to mint a new identity.
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
	raw, err := os.ReadFile(path) //nolint:gosec // the key file named in the node config
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

// ServerCert returns the certificate a listener serves: the given pair when both
// paths are set, otherwise this node's self-signed key container. Only the doors
// people reach may use a supplied certificate; replication and the machine door
// keep the key container, because children pin their parent's key from it. A
// half-configured or unreadable pair is an error, never a silent fallback.
func ServerCert(id *Identity, cn, certFile, keyFile string) (tls.Certificate, error) {
	switch {
	case certFile == "" && keyFile == "":
		return id.SelfSignedCert(cn)
	case certFile == "" || keyFile == "":
		return tls.Certificate{}, fmt.Errorf(
			"tls: cert_file and key_file must be set together (got cert_file=%q key_file=%q)",
			certFile, keyFile)
	}
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("tls: loading %s / %s: %w", certFile, keyFile, err)
	}
	return cert, nil
}

// SelfSignedCert returns a TLS certificate that wraps the ed25519 key. Trust
// comes from pinning the key, not from the certificate.
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

// WriteSelfSignedCert writes the public certificate that wraps this identity's
// key. The private key remains at the separately permissioned key path.
func (i *Identity) WriteSelfSignedCert(path, cn string) error {
	cert, err := i.SelfSignedCert(cn)
	if err != nil {
		return err
	}
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Certificate[0]})
	if err := os.WriteFile(path, pemBytes, 0o644); err != nil { // #nosec G306 -- certificate is public
		return fmt.Errorf("write certificate %s: %w", path, err)
	}
	return nil
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
