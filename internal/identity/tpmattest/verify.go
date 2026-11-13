package tpmattest

import (
	"bytes"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"errors"
	"fmt"
	"math/big"

	"github.com/google/go-tpm/tpm2"

	"github.com/alpamayo-solutions/colca/internal/identity/pubkey"
)

// Verified is what VerifyEvidence established: a TPM-resident AK (pending the
// credential challenge) certified the node key, and the EK the challenge must
// be made for.
type Verified struct {
	// EKPublic is the EK's public area, the standard template with its key.
	EKPublic tpm2.TPMTPublic
	// EKKey is the EK public key; EKFingerprint its SPKI fingerprint.
	EKKey         crypto.PublicKey
	EKFingerprint string
	// EKCert is the EK certificate the child sent, nil without one. Its chain
	// is not checked here.
	EKCert *x509.Certificate
	// AKName is the TPM Name of the AK; MakeCredential binds to it.
	AKName []byte
}

// VerifyEvidence checks evidence from Collect against the node key the child
// presented over TLS and the qualifying data the verifier expects. It checks:
//
//   - the EK is a standard low-range EK (template and key), matching the EK
//     certificate's key when one is sent;
//   - the AK is a restricted, fixedTPM/fixedParent ECDSA P-256 signing key;
//   - the AK signed a TPM-generated certification of exactly the node key's
//     public area, carrying qualifyingData;
//   - that node key is fixedTPM/fixedParent and is nodeKey.
//
// What it cannot check is that the AK lives in the TPM of that EK; the
// credential challenge (MakeCredential, ActivateCredential) proves that.
func VerifyEvidence(ev *Evidence, nodeKey crypto.PublicKey, qualifyingData []byte) (*Verified, error) {
	ekPub, err := tpm2.Unmarshal[tpm2.TPMTPublic](ev.EKPublic)
	if err != nil {
		return nil, fmt.Errorf("EK public area: %w", err)
	}
	ekKey, err := tpm2.Pub(*ekPub)
	if err != nil {
		return nil, fmt.Errorf("EK public key: %w", err)
	}
	std, err := EKPublicFromKey(ekKey)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(tpm2.Marshal(std), ev.EKPublic) {
		return nil, errors.New("EK is not a standard low-range EK")
	}
	v := &Verified{EKPublic: *ekPub, EKKey: ekKey}
	if v.EKFingerprint, err = pubkey.FingerprintOf(ekKey); err != nil {
		return nil, err
	}
	if ev.EKCert != nil {
		c, err := x509.ParseCertificate(ev.EKCert)
		if err != nil {
			return nil, fmt.Errorf("EK certificate: %w", err)
		}
		if !sameKey(c.PublicKey, ekKey) {
			return nil, errors.New("EK certificate is for another key")
		}
		v.EKCert = c
	}

	akPub, err := tpm2.Unmarshal[tpm2.TPMTPublic](ev.AKPublic)
	if err != nil {
		return nil, fmt.Errorf("AK public area: %w", err)
	}
	if a := akPub.ObjectAttributes; akPub.Type != tpm2.TPMAlgECC || !a.Restricted || !a.SignEncrypt || a.Decrypt ||
		!a.FixedTPM || !a.FixedParent || !a.SensitiveDataOrigin {
		return nil, errors.New("AK is not a restricted fixedTPM ECC signing key")
	}
	akKey, err := tpm2.Pub(*akPub)
	if err != nil {
		return nil, fmt.Errorf("AK public key: %w", err)
	}
	akEC, ok := akKey.(*ecdsa.PublicKey)
	if !ok || akEC.Curve != elliptic.P256() {
		return nil, errors.New("AK is not ECDSA P-256")
	}
	akName, err := tpm2.ObjectName(akPub)
	if err != nil {
		return nil, err
	}
	v.AKName = akName.Buffer

	sig, err := tpm2.Unmarshal[tpm2.TPMTSignature](ev.CertifySignature)
	if err != nil {
		return nil, fmt.Errorf("certify signature: %w", err)
	}
	ecSig, err := sig.Signature.ECDSA()
	if err != nil || sig.SigAlg != tpm2.TPMAlgECDSA || ecSig.Hash != tpm2.TPMAlgSHA256 {
		return nil, errors.New("certify signature is not ECDSA with SHA-256")
	}
	digest := sha256.Sum256(ev.CertifyInfo)
	r := new(big.Int).SetBytes(ecSig.SignatureR.Buffer)
	s := new(big.Int).SetBytes(ecSig.SignatureS.Buffer)
	if !ecdsa.Verify(akEC, digest[:], r, s) {
		return nil, errors.New("certify signature does not verify against the AK")
	}
	info, err := tpm2.Unmarshal[tpm2.TPMSAttest](ev.CertifyInfo) // checks TPM_GENERATED_VALUE
	if err != nil {
		return nil, fmt.Errorf("certify info: %w", err)
	}
	if info.Type != tpm2.TPMSTAttestCertify {
		return nil, errors.New("certify info is not a certification")
	}
	if !bytes.Equal(info.ExtraData.Buffer, qualifyingData) {
		return nil, errors.New("certify info carries other qualifying data")
	}
	certified, err := info.Attested.Certify()
	if err != nil {
		return nil, err
	}

	keyPub, err := tpm2.Unmarshal[tpm2.TPMTPublic](ev.KeyPublic)
	if err != nil {
		return nil, fmt.Errorf("node key public area: %w", err)
	}
	keyName, err := tpm2.ObjectName(keyPub)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(certified.Name.Buffer, keyName.Buffer) {
		return nil, errors.New("the certified object is not the node key")
	}
	if a := keyPub.ObjectAttributes; !a.FixedTPM || !a.FixedParent || !a.SensitiveDataOrigin || !a.SignEncrypt || a.Decrypt {
		return nil, errors.New("node key is not a fixedTPM/fixedParent signing key")
	}
	k, err := tpm2.Pub(*keyPub)
	if err != nil {
		return nil, fmt.Errorf("node key: %w", err)
	}
	if !sameKey(k, nodeKey) {
		return nil, errors.New("the certified key is not the key the peer presented")
	}
	return v, nil
}

// MakeCredential makes the challenge for v: secret, encrypted to the EK and
// bound to the AK's Name. Only ActivateCredential in the TPM that holds both
// recovers it.
func MakeCredential(v *Verified, secret []byte) (credentialBlob, encryptedSecret []byte, err error) {
	key, err := tpm2.ImportEncapsulationKey(&v.EKPublic)
	if err != nil {
		return nil, nil, err
	}
	return tpm2.CreateCredential(rand.Reader, key, v.AKName, secret)
}

// EKPublicFromKey rebuilds the public area of a standard low-range EK
// (RSA 2048 or ECC P-256 template) from its public key, for instance one taken
// from an EK certificate.
func EKPublicFromKey(key crypto.PublicKey) (tpm2.TPMTPublic, error) {
	switch k := key.(type) {
	case *rsa.PublicKey:
		if k.N.BitLen() != 2048 || k.E != 65537 {
			return tpm2.TPMTPublic{}, errors.New("RSA EK is not 2048-bit with exponent 65537")
		}
		p := tpm2.RSAEKTemplate
		p.Unique = tpm2.NewTPMUPublicID(tpm2.TPMAlgRSA, &tpm2.TPM2BPublicKeyRSA{Buffer: k.N.FillBytes(make([]byte, 256))})
		return p, nil
	case *ecdsa.PublicKey:
		if k.Curve != elliptic.P256() {
			return tpm2.TPMTPublic{}, errors.New("ECC EK is not P-256")
		}
		pt, err := k.Bytes() // 0x04 || X || Y
		if err != nil {
			return tpm2.TPMTPublic{}, err
		}
		p := tpm2.ECCEKTemplate
		p.Unique = tpm2.NewTPMUPublicID(tpm2.TPMAlgECC, &tpm2.TPMSECCPoint{
			X: tpm2.TPM2BECCParameter{Buffer: pt[1:33]},
			Y: tpm2.TPM2BECCParameter{Buffer: pt[33:65]},
		})
		return p, nil
	}
	return tpm2.TPMTPublic{}, fmt.Errorf("EK key %T is neither RSA nor ECDSA", key)
}
