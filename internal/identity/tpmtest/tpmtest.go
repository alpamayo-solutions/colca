// Package tpmtest runs a software TPM (swtpm) for tests. Tests that need a TPM
// call Start; without swtpm on PATH they are skipped, unless COLCA_TPM_TESTS is
// "require" (as in CI), where a missing simulator fails the test instead.
package tpmtest

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	"math/big"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/go-tpm/tpm2"
	"github.com/google/go-tpm/tpm2/transport"

	"github.com/alpamayo-solutions/colca/internal/identity/tpmkey"
)

// RequireEnv makes a missing swtpm a failure instead of a skip.
const RequireEnv = "COLCA_TPM_TESTS"

// Start launches swtpm on a Unix socket in a fresh state directory and returns
// the socket path, which tpmkey.Open accepts like a device. The simulator is
// stopped when the test ends.
func Start(t testing.TB) string {
	t.Helper()
	bin, err := exec.LookPath("swtpm")
	if err != nil {
		if os.Getenv(RequireEnv) == "require" {
			t.Fatalf("swtpm not found on PATH and %s=require", RequireEnv)
		}
		t.Skip("swtpm not on PATH; set " + RequireEnv + "=require to fail instead")
	}
	// Unix socket paths are short (104 bytes on macOS); t.TempDir can be longer.
	dir, err := os.MkdirTemp("", "swtpm")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	sock := filepath.Join(dir, "s")
	cmd := exec.CommandContext(context.Background(), bin, "socket", "--tpm2", //nolint:gosec // test helper, fixed arguments
		"--tpmstate", "dir="+dir,
		"--server", "type=unixio,path="+sock,
		"--ctrl", "type=unixio,path="+filepath.Join(dir, "c"),
		"--flags", "not-need-init,startup-clear")
	cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start swtpm: %v", err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})
	deadline := time.Now().Add(10 * time.Second)
	for {
		if fi, err := os.Stat(sock); err == nil && fi.Mode()&os.ModeSocket != 0 {
			return sock
		}
		if time.Now().After(deadline) {
			t.Fatalf("swtpm socket %s did not appear", sock)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// ProvisionEKCert plays the TPM manufacturer: it derives the EK of the given
// template, issues a certificate for it from a throwaway CA and writes it to
// the EK certificate NV index. The certificate names the TPM manufacturer
// Infineon (id:49465800) in a critical subjectAltName, as real ones do. It
// returns the certificate and the CA's, for a verifier's roots.
func ProvisionEKCert(t testing.TB, dev *tpmkey.Device, template tpm2.TPMTPublic, index tpm2.TPMHandle) (ekDER []byte, manufacturer *x509.Certificate) {
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
	nameDER, err := asn1.Marshal(pkix.RDNSequence{
		{{Type: asn1.ObjectIdentifier{2, 23, 133, 2, 1}, Value: "id:49465800"}},
		{{Type: asn1.ObjectIdentifier{2, 23, 133, 2, 2}, Value: "SLB9670"}},
		{{Type: asn1.ObjectIdentifier{2, 23, 133, 2, 3}, Value: "id:000F0001"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	san, err := asn1.Marshal([]asn1.RawValue{{Class: asn1.ClassContextSpecific, Tag: 4, IsCompound: true, Bytes: nameDER}})
	if err != nil {
		t.Fatal(err)
	}
	tmpl.ExtraExtensions = []pkix.Extension{{Id: asn1.ObjectIdentifier{2, 5, 29, 17}, Critical: true, Value: san}}
	caTmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test TPM manufacturer"},
		NotBefore: tmpl.NotBefore, NotAfter: tmpl.NotAfter, IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, caKey.Public(), caKey)
	if err != nil {
		t.Fatal(err)
	}
	ca, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, ekKey, caKey)
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
				DataSize: uint16(len(der)), //nolint:gosec // an EK certificate is far below 64 KiB
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
	return der, ca
}
