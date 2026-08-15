// Package authtest provides shared test helpers for the mTLS + registry auth
// world: machine identities (key + client cert), enrollment JSON, and paho
// TLS configs. It exists so mqttsrv, node and the in-process integration
// suite build their fixtures the same way (same pattern as metricstest).
package authtest

import (
	"crypto/tls"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/alpamayo-solutions/colca/internal/identity"
	"github.com/alpamayo-solutions/colca/internal/registry"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// Machine is a test identity: an ed25519 keypair plus the TLS client cert
// that carries it (cert = key container, trust = registry pinning).
type Machine struct {
	ULID   string
	ID     *identity.Identity
	Cert   tls.Certificate
	Pubkey string // hex
}

// NewMachine generates a fresh identity for ulid (key file in a test tempdir).
func NewMachine(t *testing.T, ulid string) *Machine {
	t.Helper()
	id, err := identity.Generate(filepath.Join(t.TempDir(), ulid+".key"))
	if err != nil {
		t.Fatalf("authtest: generate key for %s: %v", ulid, err)
	}
	cert, err := id.SelfSignedCert(ulid)
	if err != nil {
		t.Fatalf("authtest: cert for %s: %v", ulid, err)
	}
	return &Machine{ULID: ulid, ID: id, Cert: cert, Pubkey: id.PublicHex()}
}

// EntryJSON builds the enrollment wire shape for this machine.
func (m *Machine) EntryJSON(t *testing.T, kind uns.Kind, mount string, grants ...string) []byte {
	t.Helper()
	b, err := json.Marshal(uns.Entry{ULID: m.ULID, Pubkey: m.Pubkey, Kind: kind, Mount: mount, Grants: grants})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// Enroll enrolls the machine (kind machine) at the given registry.
func Enroll(t *testing.T, reg *registry.Manager, m *Machine, mount string, grants ...string) {
	t.Helper()
	if _, _, err := reg.Enroll(m.EntryJSON(t, uns.KindMachine, mount, grants...)); err != nil {
		t.Fatalf("authtest: enroll %s: %v", m.ULID, err)
	}
}

// EnrollNode enrolls a child node's key (kind node) at the parent's registry —
// entry-before-connect for the repl door.
func EnrollNode(t *testing.T, reg *registry.Manager, ulid, pubkeyHex, mount string) {
	t.Helper()
	b, err := json.Marshal(uns.Entry{ULID: ulid, Pubkey: pubkeyHex, Kind: uns.KindNode, Mount: mount})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := reg.Enroll(b); err != nil {
		t.Fatalf("authtest: enroll node %s: %v", ulid, err)
	}
}

// TLSConfig is the machine's client-side TLS config: presents the machine
// key, skips CA verification (trust is the registry's pinning, and in tests
// the server cert is self-signed by construction).
func (m *Machine) TLSConfig() *tls.Config {
	return &tls.Config{
		Certificates:       []tls.Certificate{m.Cert},
		InsecureSkipVerify: true, // #nosec G402 -- pinning model: no CA exists, see package doc
		MinVersion:         tls.VersionTLS13,
	}
}
