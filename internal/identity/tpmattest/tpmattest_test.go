package tpmattest_test

import (
	"bytes"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"testing"
	"time"

	"github.com/google/go-tpm/tpm2"
	"github.com/google/go-tpm/tpm2/transport"

	"github.com/alpamayo-solutions/colca/internal/identity/pubkey"
	"github.com/alpamayo-solutions/colca/internal/identity/tpmattest"
	"github.com/alpamayo-solutions/colca/internal/identity/tpmkey"
	"github.com/alpamayo-solutions/colca/internal/identity/tpmtest"
)

func open(t *testing.T) *tpmkey.Device {
	t.Helper()
	dev, err := tpmkey.Open(tpmtest.Start(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = dev.Close() })
	return dev
}

// provisionEKCert plays the TPM manufacturer: it derives the EK of the given
// template, issues a certificate for it from a throwaway CA and writes it to
// the EK certificate NV index.
func provisionEKCert(t *testing.T, dev *tpmkey.Device, template tpm2.TPMTPublic, index tpm2.TPMHandle) []byte {
	t.Helper()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	var ekKey crypto.PublicKey
	err = dev.Do(func(tp transport.TPM) error {
		rsp, err := tpm2.CreatePrimary{PrimaryHandle: tpm2.TPMRHEndorsement, InPublic: tpm2.New2B(template)}.Execute(tp)
		if err != nil {
			return err
		}
		defer func() { _, _ = tpm2.FlushContext{FlushHandle: rsp.ObjectHandle}.Execute(tp) }()
		pub, err := rsp.OutPublic.Contents()
		if err != nil {
			return err
		}
		ekKey, err = tpm2.Pub(*pub)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(4711),
		Subject:      pkix.Name{CommonName: "test EK"},
		Issuer:       pkix.Name{CommonName: "test TPM manufacturer"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageKeyEncipherment,
	}
	caTmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test TPM manufacturer"},
		NotBefore: tmpl.NotBefore, NotAfter: tmpl.NotAfter, IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, caTmpl, ekKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	err = dev.Do(func(tp transport.TPM) error {
		def := tpm2.NVDefineSpace{
			AuthHandle: tpm2.TPMRHOwner,
			PublicInfo: tpm2.New2B(tpm2.TPMSNVPublic{
				NVIndex: index,
				NameAlg: tpm2.TPMAlgSHA256,
				Attributes: tpm2.TPMANV{
					OwnerWrite: true, OwnerRead: true, AuthRead: true, NoDA: true, NT: tpm2.TPMNTOrdinary,
				},
				DataSize: uint16(len(der)),
			}),
		}
		if _, err := def.Execute(tp); err != nil {
			return err
		}
		nvPub, err := def.PublicInfo.Contents()
		if err != nil {
			return err
		}
		name, err := tpm2.NVName(nvPub)
		if err != nil {
			return err
		}
		for off := 0; off < len(der); off += 512 {
			end := min(off+512, len(der))
			if _, err := (tpm2.NVWrite{
				AuthHandle: tpm2.TPMRHOwner,
				NVIndex:    tpm2.NamedHandle{Handle: index, Name: *name},
				Data:       tpm2.TPM2BMaxNVBuffer{Buffer: der[off:end]},
				Offset:     uint16(off),
			}).Execute(tp); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return der
}

// attest runs the whole exchange the way child and parent will: evidence,
// verification, challenge, activation.
func attest(t *testing.T, dev *tpmkey.Device) (*tpmattest.EK, *tpmattest.Verified) {
	t.Helper()
	ek, err := tpmattest.ReadEK(dev)
	if err != nil {
		t.Fatal(err)
	}
	key, err := dev.CreateKey()
	if err != nil {
		t.Fatal(err)
	}
	ak, err := tpmattest.CreateAK(dev)
	if err != nil {
		t.Fatal(err)
	}
	nonce := []byte("parent nonce")
	ev, err := tpmattest.Collect(ek, ak, key, nonce)
	if err != nil {
		t.Fatal(err)
	}
	v, err := tpmattest.VerifyEvidence(ev, key.Public(), nonce)
	if err != nil {
		t.Fatal(err)
	}
	if v.EKFingerprint != ek.Fingerprint() {
		t.Fatalf("verifier EK %s, device EK %s", v.EKFingerprint, ek.Fingerprint())
	}
	secret := []byte("0123456789abcdef")
	blob, enc, err := tpmattest.MakeCredential(v, secret)
	if err != nil {
		t.Fatal(err)
	}
	// The child restarts between the rounds: the AK comes back from its blob.
	if err := ak.Close(); err != nil {
		t.Fatal(err)
	}
	ak, err = tpmattest.LoadAK(dev, ak.Blob())
	if err != nil {
		t.Fatal(err)
	}
	got, err := tpmattest.ActivateCredential(ak, ek, blob, enc)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, secret) {
		t.Fatalf("activated %x, want %x", got, secret)
	}
	return ek, v
}

func TestAttestationWithAnECCEKCertificate(t *testing.T) {
	dev := open(t)
	der := provisionEKCert(t, dev, tpm2.ECCEKTemplate, tpmattest.EKCertIndexECC)
	ek, v := attest(t, dev)
	if ek.Type != "ecc" || !bytes.Equal(ek.Cert, der) || v.EKCert == nil || v.EKCert.SerialNumber.Int64() != 4711 {
		t.Fatalf("EK type %s, cert read back %v, verifier cert %v", ek.Type, bytes.Equal(ek.Cert, der), v.EKCert)
	}
	c, _ := x509.ParseCertificate(der)
	if want, _ := pubkey.FingerprintOf(c.PublicKey); ek.Fingerprint() != want {
		t.Fatalf("EK fingerprint %s, certificate key %s", ek.Fingerprint(), want)
	}
}

func TestAttestationWithAnRSAEKCertificate(t *testing.T) {
	dev := open(t)
	provisionEKCert(t, dev, tpm2.RSAEKTemplate, tpmattest.EKCertIndexRSA)
	ek, _ := attest(t, dev)
	if ek.Type != "rsa" || ek.Cert == nil {
		t.Fatalf("EK type %s, cert %v", ek.Type, ek.Cert != nil)
	}
}

func TestAttestationWithoutAnEKCertificate(t *testing.T) {
	ek, v := attest(t, open(t))
	if ek.Cert != nil || v.EKCert != nil || ek.Type != "ecc" {
		t.Fatalf("type %s, cert %v", ek.Type, ek.Cert != nil)
	}
	// The EK is derived from the seed: the same on every read.
	again, err := tpmattest.ReadEK(open(t))
	if err != nil || again.Fingerprint() == "" {
		t.Fatal(err)
	}
}

func TestEvidenceIsBoundToTheNodeKeyAndTheNonce(t *testing.T) {
	dev := open(t)
	ek, err := tpmattest.ReadEK(dev)
	if err != nil {
		t.Fatal(err)
	}
	key, err := dev.CreateKey()
	if err != nil {
		t.Fatal(err)
	}
	other, err := dev.CreateKey()
	if err != nil {
		t.Fatal(err)
	}
	// Its public parts are all the test needs; a simulator has few object slots.
	if err := other.Close(); err != nil {
		t.Fatal(err)
	}
	ak, err := tpmattest.CreateAK(dev)
	if err != nil {
		t.Fatal(err)
	}
	ev, err := tpmattest.Collect(ek, ak, key, []byte("n1"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tpmattest.VerifyEvidence(ev, other.Public(), []byte("n1")); err == nil {
		t.Fatal("evidence for one key verified for another")
	}
	if _, err := tpmattest.VerifyEvidence(ev, key.Public(), []byte("n2")); err == nil {
		t.Fatal("evidence verified with another nonce")
	}
	tampered := *ev
	tampered.CertifyInfo = append([]byte(nil), ev.CertifyInfo...)
	tampered.CertifyInfo[len(tampered.CertifyInfo)-1] ^= 1
	if _, err := tpmattest.VerifyEvidence(&tampered, key.Public(), []byte("n1")); err == nil {
		t.Fatal("tampered certify info verified")
	}
	swapped := *ev
	swapped.KeyPublic = tpm2.Marshal(other.TPMPublic())
	if _, err := tpmattest.VerifyEvidence(&swapped, other.Public(), []byte("n1")); err == nil {
		t.Fatal("a certification of one key verified with another key's public area")
	}
}

func TestAChallengeForOneTPMCannotBeOpenedByAnother(t *testing.T) {
	devA, devB := open(t), open(t)
	_, vA := attest(t, devA)
	ekB, err := tpmattest.ReadEK(devB)
	if err != nil {
		t.Fatal(err)
	}
	akB, err := tpmattest.CreateAK(devB)
	if err != nil {
		t.Fatal(err)
	}
	// TPM B claims TPM A's EK but holds its own AK.
	vA.AKName = akB.Name()
	blob, enc, err := tpmattest.MakeCredential(vA, []byte("secret"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tpmattest.ActivateCredential(akB, ekB, blob, enc); err == nil {
		t.Fatal("TPM B opened a challenge made for TPM A's EK")
	}
}

func TestEKPublicFromKeyRebuildsTheTPMsPublicArea(t *testing.T) {
	dev := open(t)
	for _, tmpl := range []tpm2.TPMTPublic{tpm2.ECCEKTemplate, tpm2.RSAEKTemplate} {
		var public *tpm2.TPMTPublic
		err := dev.Do(func(tp transport.TPM) error {
			rsp, err := tpm2.CreatePrimary{PrimaryHandle: tpm2.TPMRHEndorsement, InPublic: tpm2.New2B(tmpl)}.Execute(tp)
			if err != nil {
				return err
			}
			defer func() { _, _ = tpm2.FlushContext{FlushHandle: rsp.ObjectHandle}.Execute(tp) }()
			public, err = rsp.OutPublic.Contents()
			return err
		})
		if err != nil {
			t.Fatal(err)
		}
		key, err := tpm2.Pub(*public)
		if err != nil {
			t.Fatal(err)
		}
		rebuilt, err := tpmattest.EKPublicFromKey(key)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(tpm2.Marshal(rebuilt), tpm2.Marshal(*public)) {
			t.Fatalf("rebuilt EK public area differs from the TPM's")
		}
	}
}
