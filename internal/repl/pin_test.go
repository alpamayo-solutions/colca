package repl

import (
	"crypto/ed25519"
	"crypto/tls"
	"encoding/hex"
	"path/filepath"
	"strings"
	"testing"

	"github.com/alpamayo-solutions/colca/internal/identity"
	"github.com/alpamayo-solutions/colca/internal/store"
	"github.com/alpamayo-solutions/colca/plugins/uns"
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
	conn, err := tls.Dial("tcp", ln.Addr().String(), cl.transport.cur.Load().TLSClientConfig)
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
		// Both spellings of an ed25519 parent key scope the cursors by its raw
		// hex, the form every config held before SPKI: rewriting parent.pubkey
		// into the other form must not restart replication from zero.
		if cl.ParentPub() != legacy {
			t.Fatalf("ParentPub() = %s, want the raw key %s", cl.ParentPub(), legacy)
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

// A child whose parent.pubkey switches from the legacy raw form to the SPKI
// form of the same key (prekit now renders SPKI) keeps every cursor: no reset,
// so no record is sent twice and no queued command is skipped. A P-256 parent
// is scoped by its SPKI hex, and cursors a build left under a literal spelling
// that is not the canonical one are carried over, position 1 included.
func TestCursorsSurviveAChangeOfTheParentKeySpelling(t *testing.T) {
	dir := t.TempDir()
	parent, err := identity.Generate(filepath.Join(dir, "parent.key"))
	if err != nil {
		t.Fatal(err)
	}
	child, err := identity.Generate(filepath.Join(dir, "child.key"))
	if err != nil {
		t.Fatal(err)
	}
	legacy := hex.EncodeToString(parent.Pub.(ed25519.PublicKey))
	st, err := store.Open(filepath.Join(dir, "data"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	// What a node running the old release left: cursors named by the raw form.
	st.CursorAck(uns.UplinkCursor(legacy), "metrics", 7)
	st.CursorAck(uns.UplinkCursor(legacy), "entities", 3)
	if _, err := st.CursorSetIfAbsent(uns.DownlinkCursor(legacy), downlinkStream, 1); err != nil {
		t.Fatal(err)
	}
	st.CursorAck(uns.DownlinkDefCursor(legacy), downlinkDefStream, 5)

	cl, err := NewClient("https://unused", parent.PublicHex(), child) // now configured as SPKI
	if err != nil {
		t.Fatal(err)
	}
	if err := PrepareUplink(cl, st); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		name, stream string
		want         uint64
	}{
		{uns.UplinkCursor(cl.ParentPub()), "metrics", 7},
		{uns.UplinkCursor(cl.ParentPub()), "entities", 3},
		{uns.DownlinkDefCursor(cl.ParentPub()), downlinkDefStream, 5},
	} {
		if got := st.CursorGet(c.name, c.stream); got != c.want {
			t.Errorf("%s/%s = %d, want %d (the old cursor)", c.name, c.stream, got, c.want)
		}
	}
	if pos, known := st.CursorLookup(uns.DownlinkCursor(cl.ParentPub()), downlinkStream); !known || pos != 1 {
		t.Errorf("downlink cursor at 1 not kept as such: %d %v — first contact would re-seat it at the head", pos, known)
	}
	cursors, err := st.CursorSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range cursors {
		if strings.Contains(c.Name, parent.PublicHex()) {
			t.Errorf("a cursor under the SPKI spelling appeared: %s", c.Name)
		}
	}

	// A cursor named under the SPKI spelling (left by a build that used the
	// configured string as it was) is carried to the canonical name.
	st.CursorAck(uns.UplinkCursor(parent.PublicHex()), "alarms", 9)
	if err := PrepareUplink(cl, st); err != nil {
		t.Fatal(err)
	}
	if got := st.CursorGet(uns.UplinkCursor(legacy), "alarms"); got != 9 {
		t.Fatalf("SPKI-named cursor not adopted: %d", got)
	}
	if _, known := st.CursorLookup(uns.UplinkCursor(parent.PublicHex()), "alarms"); known {
		t.Fatal("the adopted cursor was not deleted")
	}

	p256, err := identity.GenerateP256(filepath.Join(dir, "p256.key"))
	if err != nil {
		t.Fatal(err)
	}
	if got := CursorScope(p256.PublicHex()); got != p256.PublicHex() {
		t.Fatalf("P-256 scope %s, want its SPKI hex", got)
	}
}
