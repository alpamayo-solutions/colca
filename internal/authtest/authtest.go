// Package authtest provides shared test helpers for key-based auth: machine
// identities with client certificates, enrollment JSON and paho TLS configs.
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

// ElementID is the element id a placement at path gets. It is derived from the
// path, so failures stay readable and repeated calls agree.
func ElementID(path string) string {
	if path == "" {
		return ""
	}
	return "el-" + strings.ReplaceAll(path, "/", "-")
}

// Place authors a system element at path through IngestAdmin, the door
// _CmdConfigure uses, and returns its id so an identity can be enrolled there.
func Place(t *testing.T, eng *engine.Engine, path string) string {
	t.Helper()
	id := ElementID(path)
	topic := uns.Prefix() + "_SystemElement/" + eng.NodeID() + "/" + path
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

// EnrollAt places an element at path and enrolls the machine there.
func EnrollAt(t *testing.T, reg *registry.Manager, eng *engine.Engine, m *Machine, path string, grants ...string) {
	t.Helper()
	Enroll(t, reg, m, Place(t, eng, path), grants...)
}

// EnrollNode enrolls a child node's key at the parent's registry, before the
// child connects.
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

// TLSConfig is the machine's client TLS config. It presents the machine key and
// skips CA verification: trust comes from the registry's key pinning.
func (m *Machine) TLSConfig() *tls.Config {
	return &tls.Config{
		Certificates:       []tls.Certificate{m.Cert},
		InsecureSkipVerify: true, // #nosec G402 -- pinning model: no CA exists, see package doc
		MinVersion:         tls.VersionTLS13,
	}
}
