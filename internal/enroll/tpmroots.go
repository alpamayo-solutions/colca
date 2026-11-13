package enroll

import (
	"crypto/x509"
	"embed"
	"encoding/asn1"
	"encoding/pem"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"strings"
	"time"
)

// tpmRootFiles are the TPM manufacturer certificates colcad ships: roots and
// the intermediates endorsement key certificates chain through. See
// tpmroots/README.md for where each came from.
//
//go:embed tpmroots
var tpmRootFiles embed.FS

// TPMRoots verifies endorsement key certificates against TPM manufacturer
// certificates.
type TPMRoots struct {
	roots         *x509.CertPool
	intermediates *x509.CertPool
	count         int
}

// LoadTPMRoots builds the pool from the shipped certificates and, when extra
// is set, the PEM file it names (enrollment.tpm_roots). Self-signed
// certificates become roots, everything else an intermediate.
func LoadTPMRoots(extra string) (*TPMRoots, error) {
	t := &TPMRoots{roots: x509.NewCertPool(), intermediates: x509.NewCertPool()}
	err := fs.WalkDir(tpmRootFiles, "tpmroots", func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !certFile(path) {
			return err
		}
		raw, err := tpmRootFiles.ReadFile(path)
		if err != nil {
			return err
		}
		if err := t.add(raw); err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("shipped TPM roots: %w", err)
	}
	if extra != "" {
		raw, err := os.ReadFile(extra) //nolint:gosec // the file named in the node config
		if err != nil {
			return nil, fmt.Errorf("enrollment.tpm_roots: %w", err)
		}
		if err := t.add(raw); err != nil {
			return nil, fmt.Errorf("enrollment.tpm_roots %s: %w", extra, err)
		}
	}
	return t, nil
}

func certFile(path string) bool {
	return strings.HasSuffix(path, ".pem") || strings.HasSuffix(path, ".crt") || strings.HasSuffix(path, ".cer")
}

// AddCert adds one certificate, for tests and callers that hold DER.
func (t *TPMRoots) AddCert(c *x509.Certificate) {
	if isSelfSigned(c) {
		t.roots.AddCert(c)
	} else {
		t.intermediates.AddCert(c)
	}
	t.count++
}

// Count is how many certificates the pool holds.
func (t *TPMRoots) Count() int { return t.count }

func (t *TPMRoots) add(raw []byte) error {
	if !strings.Contains(string(raw), "-----BEGIN") {
		c, err := x509.ParseCertificate(raw)
		if err != nil {
			return err
		}
		t.AddCert(c)
		return nil
	}
	for {
		var block *pem.Block
		block, raw = pem.Decode(raw)
		if block == nil {
			return nil
		}
		if block.Type != "CERTIFICATE" {
			continue
		}
		c, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return err
		}
		t.AddCert(c)
	}
}

func isSelfSigned(c *x509.Certificate) bool {
	return c.CheckSignatureFrom(c) == nil
}

var (
	oidSubjectAltName  = asn1.ObjectIdentifier{2, 5, 29, 17}
	oidTPMManufacturer = asn1.ObjectIdentifier{2, 23, 133, 2, 1}
)

// manufacturers maps TCG vendor IDs (tcg-at-tpmManufacturer, "id:<hex>") to
// names.
var manufacturers = map[string]string{
	"49465800": "Infineon",
	"53544D20": "STMicroelectronics",
	"4E544300": "Nuvoton",
	"4E544320": "Nuvoton",
	"494E5443": "Intel",
	"414D4400": "AMD",
	"4D534654": "Microsoft",
	"474F4F47": "Google",
	"49424D00": "IBM",
}

// Verify checks an endorsement key certificate against the pool at now and
// returns the TPM manufacturer it names and the certificate's serial.
func (t *TPMRoots) Verify(ek *x509.Certificate, now time.Time) (manufacturer, serial string, err error) {
	if t == nil || t.count == 0 {
		return "", "", errors.New("no TPM manufacturer certificates")
	}
	// EK certificates carry the TPM's identity in a critical subjectAltName of
	// directory attributes only, which x509 does not understand; the chain is
	// what is checked here.
	c := *ek
	c.UnhandledCriticalExtensions = nil
	for _, oid := range ek.UnhandledCriticalExtensions {
		if !oid.Equal(oidSubjectAltName) {
			c.UnhandledCriticalExtensions = append(c.UnhandledCriticalExtensions, oid)
		}
	}
	if _, err := c.Verify(x509.VerifyOptions{
		Roots:         t.roots,
		Intermediates: t.intermediates,
		CurrentTime:   now,
		KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageAny},
	}); err != nil {
		return "", "", fmt.Errorf("endorsement key certificate: %w", err)
	}
	return ekManufacturer(ek), strings.ToUpper(ek.SerialNumber.Text(16)), nil
}

// ekManufacturer reads tcg-at-tpmManufacturer from the certificate's
// subjectAltName, falling back to the issuer's organisation.
func ekManufacturer(c *x509.Certificate) string {
	for _, ext := range c.Extensions {
		if !ext.Id.Equal(oidSubjectAltName) {
			continue
		}
		var names []asn1.RawValue
		if _, err := asn1.Unmarshal(ext.Value, &names); err != nil {
			break
		}
		for _, n := range names {
			if n.Tag != 4 { // directoryName
				continue
			}
			var rdn []asn1.RawValue
			if _, err := asn1.Unmarshal(n.Bytes, &rdn); err != nil {
				continue
			}
			for _, set := range rdn {
				var atvs []struct {
					Type  asn1.ObjectIdentifier
					Value string
				}
				if _, err := asn1.UnmarshalWithParams(set.FullBytes, &atvs, "set"); err != nil {
					continue
				}
				for _, atv := range atvs {
					if atv.Type.Equal(oidTPMManufacturer) {
						id := strings.ToUpper(strings.TrimPrefix(atv.Value, "id:"))
						if name, ok := manufacturers[id]; ok {
							return name
						}
						return id
					}
				}
			}
		}
	}
	if len(c.Issuer.Organization) > 0 {
		return c.Issuer.Organization[0]
	}
	return c.Issuer.CommonName
}
