package enroll

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestTheShippedTPMRootsLoad(t *testing.T) {
	r, err := LoadTPMRoots("")
	if err != nil {
		t.Fatal(err)
	}
	if r.Count() < 100 {
		t.Fatalf("%d shipped TPM manufacturer certificates, want the Infineon, STMicro, Nuvoton, AMD and Intel sets", r.Count())
	}
	if _, err := LoadTPMRoots(filepath.Join(t.TempDir(), "missing.pem")); err == nil {
		t.Fatal("a missing enrollment.tpm_roots file loaded")
	}
	bad := filepath.Join(t.TempDir(), "bad.pem")
	if err := os.WriteFile(bad, []byte("-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadTPMRoots(bad); err == nil {
		t.Fatal("a broken enrollment.tpm_roots file loaded")
	}
	var empty TPMRoots
	if _, _, err := empty.Verify(nil, time.Now()); err == nil {
		t.Fatal("an empty pool verified something")
	}
}
