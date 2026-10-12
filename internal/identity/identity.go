// Package identity holds a node's key: where it lives (a file or a TPM), how it
// is created and loaded, the self-signed certificate that carries it, and how
// public keys are encoded, compared and shown as fingerprints.
//
// Supported key algorithms are ed25519 (file keys) and ECDSA P-256 (TPM keys,
// also accepted as file keys). Public key encoding and fingerprints are in the
// pubkey subpackage.
package identity

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math/big"
	"os"
	"path/filepath"
	"time"

	"github.com/alpamayo-solutions/colca/internal/identity/pubkey"
)

// Key stores. Store names appear in config (identity.key_store), in /healthz
// and in logs.
const (
	StoreAuto = "auto" // config only: TPM when one works, else file
	StoreFile = "file"
	StoreTPM  = "tpm"
)

// Identity is this node's key. Signer signs with it, wherever it lives; Pub is
// its public half; Store says where it lives (StoreFile or StoreTPM).
type Identity struct {
	Signer crypto.Signer
	Pub    crypto.PublicKey
	Store  string

	spki   []byte
	closer io.Closer
}

// New wraps a signer as an Identity. The key must be ed25519 or ECDSA P-256.
// closer, when not nil, is closed by Close (the TPM behind a TPM key).
func New(signer crypto.Signer, store string, closer io.Closer) (*Identity, error) {
	pub := signer.Public()
	spki, err := pubkey.Marshal(pub)
	if err != nil {
		return nil, err
	}
	return &Identity{Signer: signer, Pub: pub, Store: store, spki: spki, closer: closer}, nil
}

// Close releases what the key holds open (a TPM). File keys hold nothing.
func (i *Identity) Close() error {
	if i.closer == nil {
		return nil
	}
	return i.closer.Close()
}

// SPKI is the public key as SubjectPublicKeyInfo DER.
func (i *Identity) SPKI() []byte { return append([]byte(nil), i.spki...) }

// PublicHex is the public key as hex of its SubjectPublicKeyInfo DER: the form
// a parent's registry pins and a child's parent.pubkey names.
func (i *Identity) PublicHex() string { return hex.EncodeToString(i.spki) }

// Fingerprint is SHA256:XX:XX:… over the SubjectPublicKeyInfo DER.
func (i *Identity) Fingerprint() string { return pubkey.Fingerprint(i.spki) }

// FingerprintID is the fingerprint as 64 lower-case hex characters, the form
// used in URL paths.
func (i *Identity) FingerprintID() string { return pubkey.FingerprintID(i.spki) }

// ShortFingerprint is the first four pairs of the fingerprint.
func (i *Identity) ShortFingerprint() string { return pubkey.Short(pubkey.Fingerprint(i.spki)) }

// Algorithm names the key algorithm: "ed25519" or "ecdsa-p256".
func (i *Identity) Algorithm() string { return pubkey.Algorithm(i.Pub) }

// Generate mints an ed25519 file key at path (PKCS#8 PEM, mode 0600).
func Generate(path string) (*Identity, error) {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}
	return writeFileKey(path, priv)
}

// GenerateP256 mints an ECDSA P-256 file key at path (PKCS#8 PEM, mode 0600).
func GenerateP256(path string) (*Identity, error) {
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	return writeFileKey(path, priv)
}

func writeFileKey(path string, priv crypto.Signer) (*Identity, error) {
	der, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return nil, err
	}
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})
	if err := os.WriteFile(path, pemBytes, 0o600); err != nil {
		return nil, err
	}
	return New(priv, StoreFile, nil)
}

// LoadOrGenerate returns the file key at path, minting an ed25519 key if the
// file does not exist; the bool reports whether it minted. Minting on the
// device keeps the private key out of installation media. A file that exists
// but cannot be parsed is an error, never a reason to mint a new identity.
func LoadOrGenerate(path string) (*Identity, bool, error) {
	id, err := Load(path)
	if err == nil {
		return id, false, nil
	}
	if !errors.Is(err, fs.ErrNotExist) {
		return nil, false, unusable(path, err)
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

func unusable(path string, err error) error {
	return fmt.Errorf("identity %s exists but is unusable "+
		"(refusing to mint over it — that would change this node's identity): %w", path, err)
}

// Load reads a file key: PKCS#8 PEM holding an ed25519 or ECDSA P-256 key.
func Load(path string) (*Identity, error) {
	raw, err := os.ReadFile(path) //nolint:gosec // the key file named in the node config
	if err != nil {
		return nil, err
	}
	return parseFileKey(path, raw)
}

func parseFileKey(path string, raw []byte) (*Identity, error) {
	block, _ := pem.Decode(raw)
	if block == nil {
		return nil, fmt.Errorf("no PEM block in %s", path)
	}
	if block.Type != "PRIVATE KEY" {
		return nil, fmt.Errorf("%s holds a %q PEM block, not a PKCS#8 private key", path, block.Type)
	}
	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, err
	}
	switch k := key.(type) {
	case ed25519.PrivateKey:
		return New(k, StoreFile, nil)
	case *ecdsa.PrivateKey:
		if k.Curve != elliptic.P256() {
			return nil, fmt.Errorf("key in %s is ECDSA on %s; only P-256 is supported", path, k.Curve.Params().Name)
		}
		return New(k, StoreFile, nil)
	}
	return nil, fmt.Errorf("key in %s is %T; want ed25519 or ECDSA P-256", path, key)
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

// SelfSignedCert returns a TLS certificate that wraps the node key. Trust
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
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, i.Pub, i.Signer)
	if err != nil {
		return tls.Certificate{}, err
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: i.Signer}, nil
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

// PeerPubHex returns the public key of a peer's leaf certificate as SPKI hex,
// for pinning. Only ed25519 and ECDSA P-256 keys are accepted.
func PeerPubHex(rawCert []byte) (string, error) {
	c, err := x509.ParseCertificate(rawCert)
	if err != nil {
		return "", err
	}
	h, err := pubkey.Hex(c.PublicKey)
	if err != nil {
		return "", fmt.Errorf("peer key: %w", err)
	}
	return h, nil
}
