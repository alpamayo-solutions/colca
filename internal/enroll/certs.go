package enroll

import (
	"bytes"
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net/url"
	"time"

	"github.com/alpamayo-solutions/colca/internal/identity/pubkey"
)

// CertValidity is the lifetime of an issued certificate (§8). The registry is
// the revocation authority; expiry only makes the child re-assert its key
// store and attestation at least monthly.
const CertValidity = 30 * 24 * time.Hour

// RenewAt is when a child renews its certificate: at two thirds of its
// lifetime.
func RenewAt(leaf *x509.Certificate) time.Time {
	life := leaf.NotAfter.Sub(leaf.NotBefore)
	return leaf.NotBefore.Add(life * 2 / 3)
}

// Issuer issues child certificates with this node's key. Its certificate is a
// CA certificate over the node key, subject CN=<node ULID>; children already
// pin that key, so no new trust anchor exists anywhere.
type Issuer struct {
	signer crypto.Signer
	cert   *x509.Certificate
	der    []byte
	now    func() time.Time
}

// NewIssuer builds the issuer certificate for the node key.
func NewIssuer(signer crypto.Signer, nodeULID string) (*Issuer, error) {
	pub := signer.Public()
	spki, err := pubkey.Marshal(pub)
	if err != nil {
		return nil, err
	}
	ski := sha256.Sum256(spki)
	now := time.Now()
	tmpl := &x509.Certificate{
		SerialNumber:          serial(),
		Subject:               pkix.Name{CommonName: nodeULID},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.Add(20 * 365 * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
		MaxPathLenZero:        true,
		SubjectKeyId:          ski[:20],
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, pub, signer)
	if err != nil {
		return nil, fmt.Errorf("issuer certificate: %w", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, err
	}
	return &Issuer{signer: signer, cert: cert, der: der, now: time.Now}, nil
}

// Leaf is what an issued certificate states about a child.
type Leaf struct {
	ULID    string
	Element string
	// Level is the key store level: file, tpm or tpm-attested.
	Level          string
	EKManufacturer string
	EKSerial       string
	Pub            crypto.PublicKey
}

// SAN URIs of an issued certificate (§8).
func (l Leaf) uris() []*url.URL {
	out := []*url.URL{
		{Scheme: "colca", Opaque: "node:" + l.ULID},
		{Scheme: "colca", Opaque: "element:" + l.Element},
		{Scheme: "colca", Opaque: "keystore:" + l.Level},
	}
	if l.Level == "tpm-attested" && l.EKSerial != "" {
		out = append(out, &url.URL{Scheme: "colca", Opaque: "ek:" + url.PathEscape(l.EKManufacturer) + ":" + l.EKSerial})
	}
	return out
}

// Issue signs a certificate for the child: subject CN=<child ULID>, valid
// CertValidity from now. It returns the chain as PEM (leaf, then the issuer)
// and the end of the validity.
func (i *Issuer) Issue(l Leaf) (chain []byte, notAfter time.Time, err error) {
	if _, err := pubkey.Marshal(l.Pub); err != nil {
		return nil, time.Time{}, err
	}
	now := i.now()
	tmpl := &x509.Certificate{
		SerialNumber:   serial(),
		Subject:        pkix.Name{CommonName: l.ULID},
		NotBefore:      now.Add(-5 * time.Minute),
		NotAfter:       now.Add(CertValidity),
		KeyUsage:       x509.KeyUsageDigitalSignature,
		ExtKeyUsage:    []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		URIs:           l.uris(),
		AuthorityKeyId: i.cert.SubjectKeyId,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, i.cert, l.Pub, i.signer)
	if err != nil {
		return nil, time.Time{}, fmt.Errorf("issue certificate for %s: %w", l.ULID, err)
	}
	var buf bytes.Buffer
	_ = pem.Encode(&buf, &pem.Block{Type: "CERTIFICATE", Bytes: der})
	_ = pem.Encode(&buf, &pem.Block{Type: "CERTIFICATE", Bytes: i.der})
	return buf.Bytes(), tmpl.NotAfter.UTC().Truncate(time.Second), nil
}

// IssuedHere reports whether leaf was signed by this node's key.
func (i *Issuer) IssuedHere(leaf *x509.Certificate) bool {
	return leaf.CheckSignatureFrom(i.cert) == nil
}

// Admission is why a presented certificate does or does not admit a child.
var (
	ErrCertExpired    = errors.New("certificate expired or not yet valid")
	ErrCertForeign    = errors.New("certificate was not issued by this node")
	ErrCertSelfSigned = errors.New("self-signed certificate, but an issued one is required")
	ErrCertWrongNode  = errors.New("certificate names another node")
)

// Check reports whether leaf, which the TLS handshake proved the peer holds
// the key of, is a valid certificate this node issued for ulid at now.
func (i *Issuer) Check(leaf *x509.Certificate, ulid string, now time.Time) error {
	if !i.IssuedHere(leaf) {
		return ErrCertForeign
	}
	if now.Before(leaf.NotBefore) || now.After(leaf.NotAfter) {
		return ErrCertExpired
	}
	if leaf.Subject.CommonName != ulid {
		return ErrCertWrongNode
	}
	return nil
}

// ParseChain reads a PEM chain as Issue writes it.
func ParseChain(chainPEM []byte) ([]*x509.Certificate, error) {
	var out []*x509.Certificate
	rest := chainPEM
	for {
		var block *pem.Block
		block, rest = pem.Decode(rest)
		if block == nil {
			break
		}
		if block.Type != "CERTIFICATE" {
			continue
		}
		c, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	if len(out) == 0 {
		return nil, errors.New("no certificate in the chain")
	}
	return out, nil
}

// VerifyChain is the child's check of an issued chain: the leaf names the
// child's key, the issuer certificate carries the pinned parent key, and the
// leaf is signed by it. It returns the chain as DER for the TLS certificate.
func VerifyChain(chainPEM []byte, childPub crypto.PublicKey, parentPin string) ([]*x509.Certificate, error) {
	certs, err := ParseChain(chainPEM)
	if err != nil {
		return nil, err
	}
	if len(certs) < 2 {
		return nil, errors.New("the chain holds no issuer certificate")
	}
	leaf, issuer := certs[0], certs[1]
	if !samePublic(leaf.PublicKey, childPub) {
		return nil, errors.New("the issued certificate is for another key")
	}
	issuerHex, err := pubkey.Hex(issuer.PublicKey)
	if err != nil {
		return nil, fmt.Errorf("issuer key: %w", err)
	}
	if !pubkey.Same(issuerHex, parentPin) {
		return nil, errors.New("the certificate was not issued by the pinned parent key")
	}
	if err := leaf.CheckSignatureFrom(issuer); err != nil {
		return nil, fmt.Errorf("issued certificate signature: %w", err)
	}
	return certs, nil
}

func samePublic(a, b crypto.PublicKey) bool {
	ah, err := pubkey.Hex(a)
	if err != nil {
		return false
	}
	bh, err := pubkey.Hex(b)
	return err == nil && ah == bh
}

func serial() *big.Int {
	n, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 127))
	if err != nil {
		return big.NewInt(time.Now().UnixNano())
	}
	return n
}

// signMessage signs msg with an ed25519 or ECDSA P-256 key: ed25519 over the
// message, ECDSA over its SHA-256 (ASN.1 signature).
func signMessage(k crypto.Signer, msg []byte) ([]byte, error) {
	switch k.Public().(type) {
	case ed25519.PublicKey:
		return k.Sign(rand.Reader, msg, crypto.Hash(0))
	case *ecdsa.PublicKey:
		d := sha256.Sum256(msg)
		return k.Sign(rand.Reader, d[:], crypto.SHA256)
	}
	return nil, fmt.Errorf("unsupported key %T", k.Public())
}

// verifyMessage checks a signature signMessage made.
func verifyMessage(pub crypto.PublicKey, msg, sig []byte) bool {
	switch k := pub.(type) {
	case ed25519.PublicKey:
		return ed25519.Verify(k, msg, sig)
	case *ecdsa.PublicKey:
		d := sha256.Sum256(msg)
		return ecdsa.VerifyASN1(k, d[:], sig)
	}
	return false
}
