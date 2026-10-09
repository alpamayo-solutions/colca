package repl

import (
	"crypto/ed25519"
	"crypto/tls"
	"encoding/hex"
	"net/http"
	"path/filepath"
	"testing"

	"github.com/alpamayo-solutions/colca/internal/identity"
)

// handshake runs one TLS handshake from the client's transport config against
// a listener serving the parent's key container.
func handshake(t *testing.T, cl *Client, parent *identity.Identity) error {
	t.Helper()
	cert, err := parent.SelfSignedCert("parent")
	if err != nil {
		t.Fatal(err)
	}
	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{cert}, ClientAuth: tls.RequireAnyClientCert, MinVersion: tls.VersionTLS13})
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		c, err := ln.Accept()
		if err == nil {
			_ = c.(*tls.Conn).Handshake()
			_ = c.Close()
		}
	}()
	conn, err := tls.Dial("tcp", ln.Addr().String(), cl.http.Transport.(*http.Transport).TLSClientConfig)
	if err == nil {
		_ = conn.Close()
	}
	return err
}

func TestTheParentPinMatchesInEitherForm(t *testing.T) {
	dir := t.TempDir()
	parent, err := identity.Generate(filepath.Join(dir, "parent.key"))
	if err != nil {
		t.Fatal(err)
	}
	child, err := identity.Generate(filepath.Join(dir, "child.key"))
	if err != nil {
		t.Fatal(err)
	}
	other, err := identity.GenerateP256(filepath.Join(dir, "other.key"))
	if err != nil {
		t.Fatal(err)
	}
	legacy := hex.EncodeToString(parent.Pub.(ed25519.PublicKey))
	for _, pin := range []string{parent.PublicHex(), legacy} {
		cl, err := NewClient("https://unused", pin, child)
		if err != nil {
			t.Fatal(err)
		}
		if err := handshake(t, cl, parent); err != nil {
			t.Fatalf("pin %s…: %v", pin[:12], err)
		}
		// The configured string, not its normalised form, scopes the cursors:
		// rewriting it would restart replication from zero.
		if cl.ParentPub() != pin {
			t.Fatalf("ParentPub() = %s, want the configured %s", cl.ParentPub(), pin)
		}
	}
	for _, pin := range []string{other.PublicHex(), "not a key"} {
		cl, err := NewClient("https://unused", pin, child)
		if err != nil {
			t.Fatal(err)
		}
		if err := handshake(t, cl, parent); err == nil {
			t.Fatalf("pin %q accepted a different parent", pin)
		}
	}
}
