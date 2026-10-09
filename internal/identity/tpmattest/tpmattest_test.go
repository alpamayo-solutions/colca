package tpmattest_test

import (
	"bytes"
	"crypto/x509"
	"testing"

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

func provisionEKCert(t *testing.T, dev *tpmkey.Device, template tpm2.TPMTPublic, index tpm2.TPMHandle) []byte {
	t.Helper()
	der, _ := tpmtest.ProvisionEKCert(t, dev, template, index)
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
