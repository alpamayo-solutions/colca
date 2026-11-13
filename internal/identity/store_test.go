package identity

import (
	"crypto/tls"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/alpamayo-solutions/colca/internal/identity/tpmkey"
	"github.com/alpamayo-solutions/colca/internal/identity/tpmtest"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

func opts(t *testing.T, store, device string) Options {
	t.Helper()
	return Options{KeyStore: store, KeyFile: filepath.Join(t.TempDir(), "keys", "node.key"), TPMDevice: device, Log: quiet}
}

func mustOpen(t *testing.T, o Options) (*Identity, bool) {
	t.Helper()
	id, minted, err := Open(o)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = id.Close() })
	return id, minted
}

func TestFileStoreMintsEd25519OnceThenLoads(t *testing.T) {
	o := opts(t, StoreFile, "/nonexistent/tpm")
	id, minted := mustOpen(t, o)
	if !minted || id.Store != StoreFile || id.Algorithm() != "ed25519" {
		t.Fatalf("minted=%v store=%s alg=%s", minted, id.Store, id.Algorithm())
	}
	again, minted := mustOpen(t, o)
	if minted || again.PublicHex() != id.PublicHex() {
		t.Fatal("second start changed identity")
	}
}

func TestAutoWithoutATPMFallsBackToAFileKey(t *testing.T) {
	id, minted := mustOpen(t, opts(t, StoreAuto, "/nonexistent/tpm"))
	if !minted || id.Store != StoreFile {
		t.Fatalf("minted=%v store=%s", minted, id.Store)
	}
	// Empty means auto.
	id, _ = mustOpen(t, opts(t, "", "/nonexistent/tpm"))
	if id.Store != StoreFile {
		t.Fatalf("store=%s", id.Store)
	}
}

func TestTPMStoreWithoutATPMRefusesToStart(t *testing.T) {
	o := opts(t, StoreTPM, "/nonexistent/tpm")
	if _, _, err := Open(o); err == nil {
		t.Fatal("key_store tpm started without a TPM")
	}
	if _, err := os.Stat(o.KeyFile); !os.IsNotExist(err) {
		t.Fatal("a refused start left a key behind")
	}
}

func TestAnUnknownKeyStoreIsRefused(t *testing.T) {
	if _, _, err := Open(opts(t, "hsm", "")); err == nil {
		t.Fatal("key_store hsm accepted")
	}
}

func TestOpenNeverMintsOverAnUnparseableFile(t *testing.T) {
	for _, store := range []string{StoreAuto, StoreFile, StoreTPM} {
		o := opts(t, store, "/nonexistent/tpm")
		_ = os.MkdirAll(filepath.Dir(o.KeyFile), 0o700)
		if err := os.WriteFile(o.KeyFile, []byte("not a key"), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, _, err := Open(o); err == nil {
			t.Fatalf("%s: a corrupt key file was accepted", store)
		}
		if raw, _ := os.ReadFile(o.KeyFile); string(raw) != "not a key" {
			t.Fatalf("%s: the corrupt key file was overwritten", store)
		}
	}
}

func TestATPMBlobWithoutItsTPMIsAnError(t *testing.T) {
	sock := tpmtest.Start(t)
	o := opts(t, StoreTPM, sock)
	id, _ := mustOpen(t, o)
	_ = id.Close()
	// Even with key_store file, a TPM blob is never replaced by a file key.
	for _, store := range []string{StoreFile, StoreAuto} {
		o2 := o
		o2.KeyStore, o2.TPMDevice = store, "/nonexistent/tpm"
		if _, _, err := Open(o2); err == nil {
			t.Fatalf("%s: a TPM blob opened without the TPM", store)
		}
	}
}

func TestTPMStoreMintsInTheTPMAndReloads(t *testing.T) {
	sock := tpmtest.Start(t)
	o := opts(t, StoreTPM, sock)
	id, minted := mustOpen(t, o)
	if !minted || id.Store != StoreTPM || id.Algorithm() != "ecdsa-p256" {
		t.Fatalf("minted=%v store=%s alg=%s", minted, id.Store, id.Algorithm())
	}
	if _, ok := id.TPMKey(); !ok {
		t.Fatal("TPM identity without TPM key")
	}
	raw, err := os.ReadFile(o.KeyFile)
	if err != nil || !tpmkey.IsBlob(raw) {
		t.Fatalf("key_file does not hold a TPM blob: %v", err)
	}
	if fi, _ := os.Stat(o.KeyFile); fi.Mode().Perm() != 0o600 {
		t.Fatalf("blob mode %o", fi.Mode().Perm())
	}
	want := id.PublicHex()
	_ = id.Close()

	for _, store := range []string{StoreTPM, StoreAuto} {
		o.KeyStore = store
		again, minted := mustOpen(t, o)
		if minted || again.PublicHex() != want || again.Store != StoreTPM {
			t.Fatalf("%s: reload minted=%v store=%s", store, minted, again.Store)
		}
		_ = again.Close()
	}
	existing, err := OpenExisting(o)
	if err != nil || existing.PublicHex() != want {
		t.Fatalf("OpenExisting: %v", err)
	}
	_ = existing.Close()
}

func TestAutoPrefersTheTPM(t *testing.T) {
	id, minted := mustOpen(t, opts(t, StoreAuto, tpmtest.Start(t)))
	if !minted || id.Store != StoreTPM {
		t.Fatalf("minted=%v store=%s", minted, id.Store)
	}
}

func TestTPMStoreKeepsAnExistingFileKey(t *testing.T) {
	sock := tpmtest.Start(t)
	o := opts(t, StoreFile, sock)
	file, _ := mustOpen(t, o)
	o.KeyStore = StoreTPM
	id, minted := mustOpen(t, o)
	if minted || id.Store != StoreFile || id.PublicHex() != file.PublicHex() {
		t.Fatalf("an existing file key was replaced: minted=%v store=%s", minted, id.Store)
	}
	o.TPMDevice = "/nonexistent/tpm"
	if _, _, err := Open(o); err == nil {
		t.Fatal("key_store tpm started without a TPM because a file key existed")
	}
}

func TestOpenExistingNeverMints(t *testing.T) {
	o := opts(t, StoreAuto, "/nonexistent/tpm")
	if _, err := OpenExisting(o); !os.IsNotExist(err) {
		t.Fatalf("err = %v, want not-exist", err)
	}
	if _, err := os.Stat(o.KeyFile); !os.IsNotExist(err) {
		t.Fatal("OpenExisting minted a key")
	}
}

// A TPM key must carry TLS 1.3 both ways, the way the replication door uses it.
func TestTPMKeyCompletesAMutualTLSHandshake(t *testing.T) {
	sock := tpmtest.Start(t)
	srv, _ := mustOpen(t, opts(t, StoreTPM, sock))
	cli, _ := mustOpen(t, opts(t, StoreFile, ""))
	srvCert, err := srv.SelfSignedCert("srv")
	if err != nil {
		t.Fatal(err)
	}
	cliCert, err := cli.SelfSignedCert("cli")
	if err != nil {
		t.Fatal(err)
	}
	a, b := net.Pipe()
	done := make(chan error, 1)
	var seenClient string
	go func() {
		s := tls.Server(a, &tls.Config{Certificates: []tls.Certificate{srvCert}, ClientAuth: tls.RequireAnyClientCert, MinVersion: tls.VersionTLS13})
		err := s.Handshake()
		if err == nil {
			seenClient, err = PeerPubHex(s.ConnectionState().PeerCertificates[0].Raw)
		}
		_ = s.Close()
		done <- err
	}()
	c := tls.Client(b, &tls.Config{Certificates: []tls.Certificate{cliCert}, InsecureSkipVerify: true, MinVersion: tls.VersionTLS13}) //nolint:gosec // pinned below
	if err := c.Handshake(); err != nil {
		t.Fatal(err)
	}
	got, err := PeerPubHex(c.ConnectionState().PeerCertificates[0].Raw)
	if err != nil || got != srv.PublicHex() {
		t.Fatalf("server key %s, %v", got, err)
	}
	_ = c.Close()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if seenClient != cli.PublicHex() {
		t.Fatal("client key mismatch")
	}
}
