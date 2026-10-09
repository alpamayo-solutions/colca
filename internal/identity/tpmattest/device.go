// Package tpmattest proves that a node key lives in a particular TPM.
//
// Device side (this file): read the endorsement key (EK) and its certificate
// from NV, create an attestation key (AK, restricted signing), have the AK
// certify the node key (TPM2_Certify), and answer the parent's challenge
// (TPM2_ActivateCredential).
//
// Verifier side (verify.go, pure Go, no TPM): check the certification against
// the AK and the node key the TLS peer presented, and make the challenge
// (MakeCredential) only the TPM holding both that EK and that AK can open.
//
// Chain validation of the EK certificate against manufacturer roots is the
// caller's job; this package hands back the parsed certificate.
package tpmattest

import (
	"crypto"
	"crypto/x509"
	"errors"
	"fmt"

	"github.com/google/go-tpm/tpm2"
	"github.com/google/go-tpm/tpm2/transport"

	"github.com/alpamayo-solutions/colca/internal/identity/pubkey"
	"github.com/alpamayo-solutions/colca/internal/identity/tpmkey"
)

// NV indices of the EK certificates for the standard low-range EK templates
// (TCG EK Credential Profile).
const (
	EKCertIndexRSA tpm2.TPMHandle = 0x01c00002
	EKCertIndexECC tpm2.TPMHandle = 0x01c0000a
)

// nvChunk stays below every TPM's TPM_PT_NV_BUFFER_MAX.
const nvChunk = 512

// EK is the TPM's endorsement key as attestation uses it.
type EK struct {
	// Public is the EK's public area, from the standard template.
	Public tpm2.TPMTPublic
	// Key is its public key (*rsa.PublicKey or *ecdsa.PublicKey).
	Key crypto.PublicKey
	// Cert is the EK certificate DER from NV, nil when the TPM holds none.
	Cert []byte
	// Type is "ecc" or "rsa".
	Type string
}

// Fingerprint is SHA256:… over the EK's SubjectPublicKeyInfo DER, the value a
// pre-approval by TPM names.
func (e *EK) Fingerprint() string {
	fp, err := pubkey.FingerprintOf(e.Key)
	if err != nil {
		return ""
	}
	return fp
}

// Certificate parses Cert; nil when there is none.
func (e *EK) Certificate() (*x509.Certificate, error) {
	if e.Cert == nil {
		return nil, nil
	}
	return x509.ParseCertificate(e.Cert)
}

type ekKind struct {
	name     string
	index    tpm2.TPMHandle
	template tpm2.TPMTPublic
}

var ekKinds = []ekKind{
	{"ecc", EKCertIndexECC, tpm2.ECCEKTemplate},
	{"rsa", EKCertIndexRSA, tpm2.RSAEKTemplate},
}

// ReadEK returns the EK whose certificate the TPM holds, ECC first, then RSA.
// A TPM without any EK certificate (most virtual TPMs) yields the ECC EK, or
// the RSA EK where ECC is not supported, with Cert nil. The EK is re-derived
// from the endorsement seed, so the result is the same on every call.
func ReadEK(dev *tpmkey.Device) (*EK, error) {
	var ek *EK
	err := dev.Do(func(t transport.TPM) error {
		for _, k := range ekKinds {
			cert, err := readNV(t, k.index)
			if err != nil {
				continue // no certificate for this kind
			}
			e, err := deriveEK(t, k)
			if err != nil {
				return err
			}
			if c, perr := x509.ParseCertificate(cert); perr == nil && !sameKey(c.PublicKey, e.Key) {
				return fmt.Errorf("EK certificate at %#x is for another key than the %s EK", uint32(k.index), k.name)
			}
			e.Cert = cert
			ek = e
			return nil
		}
		var errs []error
		for _, k := range ekKinds {
			e, err := deriveEK(t, k)
			if err == nil {
				ek = e
				return nil
			}
			errs = append(errs, err)
		}
		return errors.Join(errs...)
	})
	return ek, err
}

func sameKey(a, b crypto.PublicKey) bool {
	ea, ok := a.(interface{ Equal(crypto.PublicKey) bool })
	return ok && ea.Equal(b)
}

// deriveEK creates the EK from the template, reads its public area and
// flushes it.
func deriveEK(t transport.TPM, k ekKind) (*EK, error) {
	h, public, err := loadEK(t, k.template)
	if err != nil {
		return nil, err
	}
	_, _ = tpm2.FlushContext{FlushHandle: h.Handle}.Execute(t)
	key, err := tpm2.Pub(*public)
	if err != nil {
		return nil, err
	}
	return &EK{Public: *public, Key: key, Type: k.name}, nil
}

func loadEK(t transport.TPM, template tpm2.TPMTPublic) (tpm2.NamedHandle, *tpm2.TPMTPublic, error) {
	rsp, err := tpm2.CreatePrimary{
		PrimaryHandle: tpm2.TPMRHEndorsement,
		InPublic:      tpm2.New2B(template),
	}.Execute(t)
	if err != nil {
		return tpm2.NamedHandle{}, nil, fmt.Errorf("create EK: %w", err)
	}
	public, err := rsp.OutPublic.Contents()
	if err != nil {
		_, _ = tpm2.FlushContext{FlushHandle: rsp.ObjectHandle}.Execute(t)
		return tpm2.NamedHandle{}, nil, err
	}
	return tpm2.NamedHandle{Handle: rsp.ObjectHandle, Name: rsp.Name}, public, nil
}

// readNV reads a whole NV index with owner authorization.
func readNV(t transport.TPM, index tpm2.TPMHandle) ([]byte, error) {
	pub, err := tpm2.NVReadPublic{NVIndex: index}.Execute(t)
	if err != nil {
		return nil, err
	}
	nvPub, err := pub.NVPublic.Contents()
	if err != nil {
		return nil, err
	}
	nv := tpm2.NamedHandle{Handle: index, Name: pub.NVName}
	out := make([]byte, 0, nvPub.DataSize)
	for off := uint16(0); off < nvPub.DataSize; {
		n := nvPub.DataSize - off
		if n > nvChunk {
			n = nvChunk
		}
		rsp, err := tpm2.NVRead{
			AuthHandle: tpm2.TPMRHOwner,
			NVIndex:    nv,
			Size:       n,
			Offset:     off,
		}.Execute(t)
		if err != nil {
			return nil, err
		}
		out = append(out, rsp.Data.Buffer...)
		off += n
	}
	return out, nil
}

// AKTemplate is the attestation key: ECDSA P-256 with SHA-256, restricted to
// signing what the TPM itself produced (attestation structures).
func AKTemplate() tpm2.TPMTPublic {
	return tpm2.TPMTPublic{
		Type:    tpm2.TPMAlgECC,
		NameAlg: tpm2.TPMAlgSHA256,
		ObjectAttributes: tpm2.TPMAObject{
			FixedTPM:            true,
			FixedParent:         true,
			SensitiveDataOrigin: true,
			UserWithAuth:        true,
			NoDA:                true,
			Restricted:          true,
			SignEncrypt:         true,
		},
		Parameters: tpm2.NewTPMUPublicParms(tpm2.TPMAlgECC, &tpm2.TPMSECCParms{
			Symmetric: tpm2.TPMTSymDefObject{Algorithm: tpm2.TPMAlgNull},
			Scheme: tpm2.TPMTECCScheme{
				Scheme:  tpm2.TPMAlgECDSA,
				Details: tpm2.NewTPMUAsymScheme(tpm2.TPMAlgECDSA, &tpm2.TPMSSigSchemeECDSA{HashAlg: tpm2.TPMAlgSHA256}),
			},
			CurveID: tpm2.TPMECCNistP256,
			KDF:     tpm2.TPMTKDFScheme{Scheme: tpm2.TPMAlgNull},
		}),
		Unique: tpm2.NewTPMUPublicID(tpm2.TPMAlgECC, &tpm2.TPMSECCPoint{}),
	}
}

// AK is an attestation key loaded in the TPM.
type AK struct {
	dev    *tpmkey.Device
	handle tpm2.NamedHandle
	public tpm2.TPM2BPublic
	blob   []byte
}

// CreateAK creates an attestation key under the SRK and loads it.
func CreateAK(dev *tpmkey.Device) (*AK, error) {
	var ak *AK
	err := dev.Do(func(t transport.TPM) error {
		h, pub, priv, err := tpmkey.CreateChild(t, AKTemplate())
		if err != nil {
			return fmt.Errorf("AK: %w", err)
		}
		ak = &AK{dev: dev, handle: h, public: pub, blob: tpmkey.MarshalPair(pub, priv)}
		return nil
	})
	return ak, err
}

// LoadAK loads an AK from its Blob, so a challenge can be answered after a
// restart.
func LoadAK(dev *tpmkey.Device, blob []byte) (*AK, error) {
	pub, priv, err := tpmkey.ParsePair(blob)
	if err != nil {
		return nil, err
	}
	var ak *AK
	err = dev.Do(func(t transport.TPM) error {
		h, err := tpmkey.LoadChild(t, pub, priv)
		if err != nil {
			return fmt.Errorf("AK: %w", err)
		}
		ak = &AK{dev: dev, handle: h, public: pub, blob: blob}
		return nil
	})
	return ak, err
}

// Blob is the AK's public and wrapped private part, for LoadAK.
func (a *AK) Blob() []byte { return a.blob }

// Public is the AK's TPMT_PUBLIC, marshalled.
func (a *AK) Public() []byte { return a.public.Bytes() }

// Name is the AK's TPM Name, which the challenge is bound to.
func (a *AK) Name() []byte { return a.handle.Name.Buffer }

// Close flushes the AK.
func (a *AK) Close() error {
	return a.dev.Do(func(t transport.TPM) error {
		_, err := tpm2.FlushContext{FlushHandle: a.handle.Handle}.Execute(t)
		return err
	})
}

// Evidence is what a child sends with its enrollment request to prove its
// node key is in a TPM. All TPM structures are in TPM wire format.
type Evidence struct {
	EKCert           []byte `json:"ek_cert,omitempty"` // DER, absent when the TPM holds none
	EKPublic         []byte `json:"ek_public"`         // TPMT_PUBLIC
	AKPublic         []byte `json:"ak_public"`         // TPMT_PUBLIC
	KeyPublic        []byte `json:"key_public"`        // TPMT_PUBLIC of the node key
	CertifyInfo      []byte `json:"certify_info"`      // TPMS_ATTEST
	CertifySignature []byte `json:"certify_signature"` // TPMT_SIGNATURE
}

// Collect has the AK certify the node key and gathers the evidence.
// qualifyingData is echoed inside the signed structure; the verifier passes
// the same bytes to VerifyEvidence.
func Collect(ek *EK, ak *AK, key *tpmkey.Key, qualifyingData []byte) (*Evidence, error) {
	if key.Device() != ak.dev {
		return nil, errors.New("tpmattest: AK and node key are on different TPM connections")
	}
	var ev *Evidence
	err := ak.dev.Do(func(t transport.TPM) error {
		rsp, err := tpm2.Certify{
			ObjectHandle:   key.Handle(),
			SignHandle:     ak.handle,
			QualifyingData: tpm2.TPM2BData{Buffer: qualifyingData},
			InScheme:       tpm2.TPMTSigScheme{Scheme: tpm2.TPMAlgNull},
		}.Execute(t)
		if err != nil {
			return fmt.Errorf("certify: %w", err)
		}
		keyPublic := key.TPMPublic()
		ev = &Evidence{
			EKCert:           ek.Cert,
			EKPublic:         tpm2.Marshal(ek.Public),
			AKPublic:         ak.Public(),
			KeyPublic:        tpm2.Marshal(keyPublic),
			CertifyInfo:      rsp.CertifyInfo.Bytes(),
			CertifySignature: tpm2.Marshal(rsp.Signature),
		}
		return nil
	})
	return ev, err
}

// ActivateCredential answers a challenge from MakeCredential: the TPM opens it
// only if it holds the EK it was made for and the AK it names.
func ActivateCredential(ak *AK, ek *EK, credentialBlob, encryptedSecret []byte) ([]byte, error) {
	var secret []byte
	err := ak.dev.Do(func(t transport.TPM) error {
		// The EK is re-derived from its template: a public area with the key
		// filled in as unique would derive a different key.
		var template *tpm2.TPMTPublic
		for _, k := range ekKinds {
			if k.name == ek.Type {
				template = &k.template
			}
		}
		if template == nil {
			return fmt.Errorf("unknown EK type %q", ek.Type)
		}
		h, _, err := loadEK(t, *template)
		if err != nil {
			return err
		}
		defer func() { _, _ = tpm2.FlushContext{FlushHandle: h.Handle}.Execute(t) }()
		rsp, err := tpm2.ActivateCredential{
			ActivateHandle: ak.handle,
			KeyHandle: tpm2.AuthHandle{
				Handle: h.Handle,
				Name:   h.Name,
				Auth:   tpm2.Policy(tpm2.TPMAlgSHA256, 16, ekPolicy),
			},
			CredentialBlob: tpm2.TPM2BIDObject{Buffer: credentialBlob},
			Secret:         tpm2.TPM2BEncryptedSecret{Buffer: encryptedSecret},
		}.Execute(t)
		if err != nil {
			return fmt.Errorf("activate credential: %w", err)
		}
		secret = rsp.CertInfo.Buffer
		return nil
	})
	return secret, err
}

// ekPolicy satisfies the standard EK's policy: PolicySecret on the
// endorsement hierarchy, whose authorization is empty.
func ekPolicy(t transport.TPM, handle tpm2.TPMISHPolicy, nonceTPM tpm2.TPM2BNonce) error {
	_, err := tpm2.PolicySecret{
		AuthHandle:    tpm2.TPMRHEndorsement,
		PolicySession: handle,
		NonceTPM:      nonceTPM,
	}.Execute(t)
	return err
}
