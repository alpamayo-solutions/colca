// Package tpmkey keeps a node key inside a TPM 2.0: an ECDSA P-256 signing key
// that is fixedTPM and fixedParent, created under the owner hierarchy's
// standard ECC storage root key (SRK). The SRK is re-created transient whenever
// a key is created or loaded and flushed right after, so no persistent handle
// has to be managed and steady state holds one transient object: the node key.
//
// What is kept on disk is the key's TPM2B_PUBLIC and TPM2B_PRIVATE as returned
// by TPM2_Create. The private part is wrapped by the chip's storage seed and is
// useless anywhere else.
package tpmkey

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"encoding/asn1"
	"encoding/binary"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"os"
	"sync"

	"github.com/google/go-tpm/tpm2"
	"github.com/google/go-tpm/tpm2/transport"
	"github.com/google/go-tpm/tpm2/transport/linuxtpm"
	"github.com/google/go-tpm/tpm2/transport/linuxudstpm"
)

// PEMType is the PEM block type of a key blob written by this package.
const PEMType = "COLCA TPM2 KEY"

// parentSRK names the parent a blob was created under; it is written into the
// PEM header so a future change of parent template is detectable.
const parentSRK = "owner-ecc-p256-srk"

// Device is an open TPM. Commands are serialised: a TPM handles one command at
// a time, and the socket transport used for simulators is not safe for
// concurrent use.
type Device struct {
	mu  sync.Mutex
	tpm transport.TPMCloser
}

// Open opens the TPM at path: a character device such as /dev/tpmrm0, or the
// Unix socket of a TPM simulator (swtpm --server type=unixio).
func Open(path string) (*Device, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("tpm %s: %w", path, err)
	}
	var t transport.TPMCloser
	if fi.Mode()&os.ModeSocket != 0 {
		t, err = linuxudstpm.Open(path)
	} else {
		t, err = linuxtpm.Open(path)
	}
	if err != nil {
		return nil, fmt.Errorf("tpm %s: %w", path, err)
	}
	return &Device{tpm: t}, nil
}

// FromTransport wraps an already open transport.
func FromTransport(t transport.TPMCloser) *Device { return &Device{tpm: t} }

// Close closes the transport. Keys loaded through this device stop working.
func (d *Device) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.tpm.Close()
}

// Do runs fn with exclusive use of the TPM. Packages that issue their own
// commands (attestation) go through it so they never interleave with a
// signature.
func (d *Device) Do(fn func(t transport.TPM) error) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return fn(d.tpm)
}

// KeyTemplate is the public template of a node key: ECDSA P-256, sign only,
// bound to this TPM and to its parent, scheme chosen per signature so TLS can
// ask for the hash it needs.
func KeyTemplate() tpm2.TPMTPublic {
	return tpm2.TPMTPublic{
		Type:    tpm2.TPMAlgECC,
		NameAlg: tpm2.TPMAlgSHA256,
		ObjectAttributes: tpm2.TPMAObject{
			FixedTPM:            true,
			FixedParent:         true,
			SensitiveDataOrigin: true,
			UserWithAuth:        true,
			NoDA:                true,
			SignEncrypt:         true,
		},
		Parameters: tpm2.NewTPMUPublicParms(tpm2.TPMAlgECC, &tpm2.TPMSECCParms{
			Symmetric: tpm2.TPMTSymDefObject{Algorithm: tpm2.TPMAlgNull},
			Scheme:    tpm2.TPMTECCScheme{Scheme: tpm2.TPMAlgNull},
			CurveID:   tpm2.TPMECCNistP256,
			KDF:       tpm2.TPMTKDFScheme{Scheme: tpm2.TPMAlgNull},
		}),
		Unique: tpm2.NewTPMUPublicID(tpm2.TPMAlgECC, &tpm2.TPMSECCPoint{}),
	}
}

// WithSRK creates the standard ECC SRK, runs fn with it, and flushes it. The
// caller must hold the device (inside Do).
func WithSRK(t transport.TPM, fn func(srk tpm2.NamedHandle) error) error {
	rsp, err := tpm2.CreatePrimary{
		PrimaryHandle: tpm2.TPMRHOwner,
		InPublic:      tpm2.New2B(tpm2.ECCSRKTemplate),
	}.Execute(t)
	if err != nil {
		return fmt.Errorf("create SRK: %w", err)
	}
	defer func() { _, _ = tpm2.FlushContext{FlushHandle: rsp.ObjectHandle}.Execute(t) }()
	return fn(tpm2.NamedHandle{Handle: rsp.ObjectHandle, Name: rsp.Name})
}

// CreateChild creates an object from tmpl under the SRK and loads it. It
// returns the loaded handle and the TPM2B public/private pair.
func CreateChild(t transport.TPM, tmpl tpm2.TPMTPublic) (tpm2.NamedHandle, tpm2.TPM2BPublic, tpm2.TPM2BPrivate, error) {
	var (
		h    tpm2.NamedHandle
		pub  tpm2.TPM2BPublic
		priv tpm2.TPM2BPrivate
	)
	err := WithSRK(t, func(srk tpm2.NamedHandle) error {
		crsp, err := tpm2.Create{ParentHandle: srk, InPublic: tpm2.New2B(tmpl)}.Execute(t)
		if err != nil {
			return fmt.Errorf("create key: %w", err)
		}
		lrsp, err := tpm2.Load{ParentHandle: srk, InPrivate: crsp.OutPrivate, InPublic: crsp.OutPublic}.Execute(t)
		if err != nil {
			return fmt.Errorf("load key: %w", err)
		}
		h = tpm2.NamedHandle{Handle: lrsp.ObjectHandle, Name: lrsp.Name}
		pub, priv = crsp.OutPublic, crsp.OutPrivate
		return nil
	})
	return h, pub, priv, err
}

// LoadChild loads a public/private pair created by CreateChild.
func LoadChild(t transport.TPM, pub tpm2.TPM2BPublic, priv tpm2.TPM2BPrivate) (tpm2.NamedHandle, error) {
	var h tpm2.NamedHandle
	err := WithSRK(t, func(srk tpm2.NamedHandle) error {
		lrsp, err := tpm2.Load{ParentHandle: srk, InPrivate: priv, InPublic: pub}.Execute(t)
		if err != nil {
			return fmt.Errorf("load key: %w", err)
		}
		h = tpm2.NamedHandle{Handle: lrsp.ObjectHandle, Name: lrsp.Name}
		return nil
	})
	return h, err
}

// Key is a node key loaded in the TPM. It implements crypto.Signer.
type Key struct {
	dev    *Device
	handle tpm2.NamedHandle
	public tpm2.TPMTPublic
	pub    *ecdsa.PublicKey
	blob   []byte
}

var _ crypto.Signer = (*Key)(nil)

// CreateKey creates a new node key in the TPM.
func (d *Device) CreateKey() (*Key, error) {
	var k *Key
	err := d.Do(func(t transport.TPM) error {
		h, pub, priv, err := CreateChild(t, KeyTemplate())
		if err != nil {
			return err
		}
		k, err = newKey(d, h, pub, priv)
		if err != nil {
			_, _ = tpm2.FlushContext{FlushHandle: h.Handle}.Execute(t)
		}
		return err
	})
	return k, err
}

// LoadKey loads a key from a blob written by Key.Blob.
func (d *Device) LoadKey(blob []byte) (*Key, error) {
	pub, priv, err := ParseBlob(blob)
	if err != nil {
		return nil, err
	}
	var k *Key
	err = d.Do(func(t transport.TPM) error {
		h, err := LoadChild(t, pub, priv)
		if err != nil {
			return err
		}
		k, err = newKey(d, h, pub, priv)
		if err != nil {
			_, _ = tpm2.FlushContext{FlushHandle: h.Handle}.Execute(t)
		}
		return err
	})
	return k, err
}

func newKey(d *Device, h tpm2.NamedHandle, pub tpm2.TPM2BPublic, priv tpm2.TPM2BPrivate) (*Key, error) {
	public, err := pub.Contents()
	if err != nil {
		return nil, fmt.Errorf("key public area: %w", err)
	}
	if err := checkKeyPublic(public); err != nil {
		return nil, err
	}
	cp, err := tpm2.Pub(*public)
	if err != nil {
		return nil, fmt.Errorf("key public area: %w", err)
	}
	ec, ok := cp.(*ecdsa.PublicKey)
	if !ok || ec.Curve != elliptic.P256() {
		return nil, fmt.Errorf("tpm key is not ECDSA P-256")
	}
	return &Key{dev: d, handle: h, public: *public, pub: ec, blob: encodeBlob(pub, priv)}, nil
}

// checkKeyPublic refuses a blob that is not a node key: whoever can write the
// key file must not be able to swap in a key that can leave the chip.
func checkKeyPublic(p *tpm2.TPMTPublic) error {
	a := p.ObjectAttributes
	if p.Type != tpm2.TPMAlgECC || !a.FixedTPM || !a.FixedParent || !a.SensitiveDataOrigin || !a.SignEncrypt || a.Decrypt || a.Restricted {
		return fmt.Errorf("tpm key blob is not a fixedTPM/fixedParent unrestricted ECC signing key")
	}
	return nil
}

// Public returns the key's public half, an *ecdsa.PublicKey.
func (k *Key) Public() crypto.PublicKey { return k.pub }

// Handle is the key's transient handle and Name, for TPM2_Certify.
func (k *Key) Handle() tpm2.NamedHandle { return k.handle }

// TPMPublic is the key's TPMT_PUBLIC area.
func (k *Key) TPMPublic() tpm2.TPMTPublic { return k.public }

// Device is the TPM the key lives in.
func (k *Key) Device() *Device { return k.dev }

// Blob is the PEM the key is persisted as.
func (k *Key) Blob() []byte { return k.blob }

// Close flushes the key from the TPM. It does not close the device.
func (k *Key) Close() error {
	return k.dev.Do(func(t transport.TPM) error {
		_, err := tpm2.FlushContext{FlushHandle: k.handle.Handle}.Execute(t)
		return err
	})
}

// Sign signs digest with the TPM and returns an ASN.1 ECDSA signature, as
// crypto.Signer requires. rand is unused: the TPM draws its own nonce.
func (k *Key) Sign(_ io.Reader, digest []byte, opts crypto.SignerOpts) ([]byte, error) {
	alg, err := hashAlg(opts.HashFunc())
	if err != nil {
		return nil, err
	}
	if len(digest) != opts.HashFunc().Size() {
		return nil, fmt.Errorf("tpm sign: digest is %d bytes, want %d", len(digest), opts.HashFunc().Size())
	}
	var sig *tpm2.TPMSSignatureECC
	err = k.dev.Do(func(t transport.TPM) error {
		rsp, err := tpm2.Sign{
			KeyHandle: k.handle,
			Digest:    tpm2.TPM2BDigest{Buffer: digest},
			InScheme: tpm2.TPMTSigScheme{
				Scheme:  tpm2.TPMAlgECDSA,
				Details: tpm2.NewTPMUSigScheme(tpm2.TPMAlgECDSA, &tpm2.TPMSSchemeHash{HashAlg: alg}),
			},
			Validation: tpm2.TPMTTKHashCheck{Tag: tpm2.TPMSTHashCheck, Hierarchy: tpm2.TPMRHNull},
		}.Execute(t)
		if err != nil {
			return fmt.Errorf("tpm sign: %w", err)
		}
		sig, err = rsp.Signature.Signature.ECDSA()
		return err
	})
	if err != nil {
		return nil, err
	}
	return asn1.Marshal(struct{ R, S *big.Int }{
		new(big.Int).SetBytes(sig.SignatureR.Buffer),
		new(big.Int).SetBytes(sig.SignatureS.Buffer),
	})
}

func hashAlg(h crypto.Hash) (tpm2.TPMIAlgHash, error) {
	switch h {
	case crypto.SHA256:
		return tpm2.TPMAlgSHA256, nil
	case crypto.SHA384:
		return tpm2.TPMAlgSHA384, nil
	case crypto.SHA512:
		return tpm2.TPMAlgSHA512, nil
	}
	return 0, fmt.Errorf("tpm sign: unsupported hash %v", h)
}

// IsBlob reports whether raw is a PEM key blob written by this package.
func IsBlob(raw []byte) bool {
	b, _ := pem.Decode(raw)
	return b != nil && b.Type == PEMType
}

func encodeBlob(pub tpm2.TPM2BPublic, priv tpm2.TPM2BPrivate) []byte {
	body := append(tpm2.Marshal(pub), tpm2.Marshal(priv)...)
	return pem.EncodeToMemory(&pem.Block{Type: PEMType, Headers: map[string]string{"Parent": parentSRK}, Bytes: body})
}

// ParseBlob splits a key blob into its TPM2B public and private parts.
func ParseBlob(raw []byte) (tpm2.TPM2BPublic, tpm2.TPM2BPrivate, error) {
	b, _ := pem.Decode(raw)
	if b == nil || b.Type != PEMType {
		return tpm2.TPM2BPublic{}, tpm2.TPM2BPrivate{}, errors.New("not a TPM key blob")
	}
	if p := b.Headers["Parent"]; p != parentSRK {
		return tpm2.TPM2BPublic{}, tpm2.TPM2BPrivate{}, fmt.Errorf("TPM key blob made under parent %q, want %q", p, parentSRK)
	}
	pubB, rest, err := split2B(b.Bytes)
	if err != nil {
		return tpm2.TPM2BPublic{}, tpm2.TPM2BPrivate{}, err
	}
	privB, rest, err := split2B(rest)
	if err != nil || len(rest) != 0 {
		return tpm2.TPM2BPublic{}, tpm2.TPM2BPrivate{}, errors.New("TPM key blob: malformed private part")
	}
	pub, err := tpm2.Unmarshal[tpm2.TPM2BPublic](pubB)
	if err != nil {
		return tpm2.TPM2BPublic{}, tpm2.TPM2BPrivate{}, fmt.Errorf("TPM key blob: public part: %w", err)
	}
	priv, err := tpm2.Unmarshal[tpm2.TPM2BPrivate](privB)
	if err != nil {
		return tpm2.TPM2BPublic{}, tpm2.TPM2BPrivate{}, fmt.Errorf("TPM key blob: private part: %w", err)
	}
	return *pub, *priv, nil
}

// split2B returns the leading TPM2B (size prefix included) and the rest.
func split2B(b []byte) ([]byte, []byte, error) {
	if len(b) < 2 {
		return nil, nil, errors.New("TPM key blob: truncated")
	}
	n := int(binary.BigEndian.Uint16(b)) + 2
	if len(b) < n {
		return nil, nil, errors.New("TPM key blob: truncated")
	}
	return b[:n], b[n:], nil
}
