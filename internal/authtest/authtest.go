// Package authtest provides shared test helpers for the mTLS + registry auth
// world: machine identities (key + client cert), enrollment JSON, and paho
// TLS configs. It exists so mqttsrv, node and the in-process integration
// suite build their fixtures the same way (same pattern as metricstest).
package authtest

import (
	"crypto/tls"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"github.com/alpamayo-solutions/colca/internal/engine"
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

// ElementID is the element a placement at path gets. Derived from the path so a
// failing assertion names something readable, and stable so two calls for the
// same position agree.
func ElementID(path string) string {
	if path == "" {
		return ""
	}
	return "el-" + strings.ReplaceAll(path, "/", "-")
}

// Place authors a system element at path in the node's own namespace and
// returns its id, so an identity can be enrolled there (id-grants design §4).
// It goes through IngestAdmin — the same door `_CmdConfigure element/upsert`
// publishes through — so the node's element index sees it exactly as it would
// in production.
func Place(t *testing.T, eng *engine.Engine, path string) string {
	t.Helper()
	id := ElementID(path)
	topic := "colca/v1/_SystemElement/" + eng.NodeID() + "/" + path
	payload, err := json.Marshal(map[string]string{"id": id, "name": path})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := eng.IngestAdmin(topic, payload); err != nil {
		t.Fatalf("authtest: place element at %s: %v", path, err)
	}
	return id
}

// EntryJSON builds the enrollment wire shape for this machine.
func (m *Machine) EntryJSON(t *testing.T, kind uns.Kind, element string, grants ...string) []byte {
	t.Helper()
	b, err := json.Marshal(uns.Entry{ULID: m.ULID, Pubkey: m.Pubkey, Kind: kind, Element: element, Grants: grants})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// Enroll enrolls the machine (kind external) at the given registry, bound to
// element.
func Enroll(t *testing.T, reg *registry.Manager, m *Machine, element string, grants ...string) {
	t.Helper()
	if _, _, err := reg.Enroll(m.EntryJSON(t, uns.KindExternal, element, grants...)); err != nil {
		t.Fatalf("authtest: enroll %s: %v", m.ULID, err)
	}
}

// EnrollAt places an element at path and enrolls the machine there — the two
// steps every placed identity needs, in the order a deployment performs them.
func EnrollAt(t *testing.T, reg *registry.Manager, eng *engine.Engine, m *Machine, path string, grants ...string) {
	t.Helper()
	Enroll(t, reg, m, Place(t, eng, path), grants...)
}

// EnrollNode enrolls a child node's key (kind node) at the parent's registry —
// entry-before-connect for the repl door.
func EnrollNode(t *testing.T, reg *registry.Manager, ulid, pubkeyHex, element string) {
	t.Helper()
	b, err := json.Marshal(uns.Entry{ULID: ulid, Pubkey: pubkeyHex, Kind: uns.KindNode, Element: element})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := reg.Enroll(b); err != nil {
		t.Fatalf("authtest: enroll node %s: %v", ulid, err)
	}
}

// EnrollNodeAt places an element at path and enrolls a child node there.
func EnrollNodeAt(t *testing.T, reg *registry.Manager, eng *engine.Engine, ulid, pubkeyHex, path string) {
	t.Helper()
	EnrollNode(t, reg, ulid, pubkeyHex, Place(t, eng, path))
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
