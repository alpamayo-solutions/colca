package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/alpamayo-solutions/colca/internal/identity"
	"github.com/alpamayo-solutions/colca/internal/identity/tpmtest"
)

func writeConfig(t *testing.T, keyFile, extra string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "node.yaml")
	yml := "ulid: n-test\ndata_dir: /tmp/unused\nidentity:\n  key_file: " + keyFile + "\n" + extra
	if err := os.WriteFile(p, []byte(yml), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestIdentityPrintsTheNodeKey(t *testing.T) {
	keyFile := filepath.Join(t.TempDir(), "node.key")
	id, err := identity.Generate(keyFile)
	if err != nil {
		t.Fatal(err)
	}
	var out, errOut bytes.Buffer
	if code := identityCmd([]string{"-json", writeConfig(t, keyFile, "")}, &out, &errOut); code != 0 {
		t.Fatalf("exit %d: %s", code, errOut.String())
	}
	var got map[string]string
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got["ulid"] != "n-test" || got["key_store"] != "file" || got["fingerprint"] != id.Fingerprint() ||
		got["short_fingerprint"] != id.ShortFingerprint() || got["fingerprint_id"] != id.FingerprintID() {
		t.Fatalf("output %v", got)
	}

	out.Reset()
	if code := identityCmd([]string{writeConfig(t, keyFile, "")}, &out, &errOut); code != 0 ||
		!strings.Contains(out.String(), "fingerprint: "+id.Fingerprint()+"\n") {
		t.Fatalf("plain output %q", out.String())
	}
}

func TestIdentityNeverMints(t *testing.T) {
	keyFile := filepath.Join(t.TempDir(), "node.key")
	var out, errOut bytes.Buffer
	if code := identityCmd([]string{writeConfig(t, keyFile, "")}, &out, &errOut); code != 1 {
		t.Fatalf("exit %d", code)
	}
	if _, err := os.Stat(keyFile); !os.IsNotExist(err) {
		t.Fatal("colcad identity minted a key")
	}
}

func TestIdentityAndTPMIdentityWithATPM(t *testing.T) {
	sock := tpmtest.Start(t)
	keyFile := filepath.Join(t.TempDir(), "node.key")
	id, _, err := identity.Open(identity.Options{KeyStore: identity.StoreTPM, KeyFile: keyFile, TPMDevice: sock})
	if err != nil {
		t.Fatal(err)
	}
	_ = id.Close()

	var out, errOut bytes.Buffer
	cfg := writeConfig(t, keyFile, "  key_store: tpm\n  tpm_device: "+sock+"\n")
	if code := identityCmd([]string{"-json", cfg}, &out, &errOut); code != 0 {
		t.Fatalf("exit %d: %s", code, errOut.String())
	}
	var got map[string]string
	_ = json.Unmarshal(out.Bytes(), &got)
	if got["key_store"] != "tpm" || got["fingerprint"] != id.Fingerprint() || got["algorithm"] != "ecdsa-p256" {
		t.Fatalf("output %v", got)
	}

	out.Reset()
	if code := tpmIdentityCmd([]string{"-json", "-device", sock}, &out, &errOut); code != 0 {
		t.Fatalf("exit %d: %s", code, errOut.String())
	}
	got = nil
	_ = json.Unmarshal(out.Bytes(), &got)
	if !strings.HasPrefix(got["ek_fingerprint"], "SHA256:") || len(got["ek_fingerprint_id"]) != 64 || got["ek_certificate"] != "none" {
		t.Fatalf("output %v", got)
	}
}
