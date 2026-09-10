// Self-registration lives here rather than in registry.go so that file keeps
// one responsibility (the lifecycle of enrolled entries) and this one carries
// the other: how an unprovisioned local service becomes an entry
// (local-service-trust design §3.2).

package registry

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/oklog/ulid/v2"

	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// Register resolves a local service's entry, creating it if this node has
// never seen the name (local-service-trust design §3.2). It is what a local
// door calls on every CONNECT: cheap when the name is already known, and the
// whole reason onboarding a local service costs zero provisioning steps.
//
// The declared mount SEEDS the entry and never maintains it: once an entry
// exists, the entry wins and a differing declaration is only logged. Re-
// applying a declaration on every connect would let a container restart
// silently undo an operator's repositioning, which would make repositioning
// pointless.
func (m *Manager) Register(name, declaredMount string) (*uns.Entry, error) {
	if name == "" {
		return nil, fmt.Errorf("register: a local service needs a name")
	}
	if e, ok := m.ByName(name); ok {
		if declaredMount != "" {
			m.mu.RLock()
			bound, resolved := m.mountOf(e)
			m.mu.RUnlock()
			if resolved && bound != normalizeMount(declaredMount) {
				m.log.Info("local service declares a mount it is not bound to — the entry wins",
					"name", name, "declared", declaredMount, "bound", bound)
			}
		}
		return e, nil
	}

	element, err := m.elementFor(declaredMount)
	if err != nil {
		return nil, err
	}
	entry := uns.Entry{ULID: NewULID(), Kind: uns.KindLocal, Name: name, Element: element}
	raw, err := json.Marshal(&entry)
	if err != nil {
		return nil, err
	}
	if _, _, err := m.Enroll(raw); err != nil {
		return nil, err
	}
	m.log.Info("local service registered", "name", name, "ulid", entry.ULID, "element", element)
	return &entry, nil
}

// elementFor resolves a declared mount to the element a new entry binds to.
// No declaration means unplaced — bound to the node itself — so there is
// nothing to resolve: elementFor("") returns "", and Enroll already treats
// an empty element as a complete, valid placement (KindLocal, id-grants
// design §4).
//
// Authoring the branch when it is absent (§3.2: "path exists? bind to the
// element sitting there. path missing? author the elements along it, bind
// to the leaf.") is domain knowledge this package does not keep a copy of: a
// catalogue tag's own meta.element needs the identical walk
// (exec_configure.go bindCatalogue), so it lives in exactly one place —
// ConfigExec.authorElementAt — and this just calls it, through whatever
// SetAuthoring wired (architecture principle 1: one walk, not two).
func (m *Manager) elementFor(mount string) (string, error) {
	if mount == "" {
		return "", nil
	}
	m.mu.RLock()
	author := m.author
	m.mu.RUnlock()
	if author == nil {
		return "", fmt.Errorf("register: mount authoring is not wired at this node")
	}
	id, err := author(mount)
	if err != nil {
		return "", fmt.Errorf("register: could not author %s: %w", mount, err)
	}
	return id, nil
}

// normalizeMount strips empty segments (a leading/trailing/doubled "/") so
// "line1/press3", "/line1/press3" and "line1/press3/" compare equal.
// elementFor already discards empty segments while authoring a mount, so the
// drift comparison above must fold the same way, or a merely cosmetic
// difference in how the declaration was written reads as drift.
func normalizeMount(mount string) string {
	segs := strings.Split(mount, "/")
	kept := segs[:0]
	for _, s := range segs {
		if s != "" {
			kept = append(kept, s)
		}
	}
	return strings.Join(kept, "/")
}

// NewULID mints a fresh ULID — this system's one identity format (node ids,
// registry entries, elements, and every Signal.id in the data model).
// Exported because it is not only Register's need: a local service
// registering for the first time mints its own entry ULID here, and
// plugins/uns — stdlib-only (arch_test.go) and unable to import a ULID
// library itself — declares minting as a port that the core wires with this
// same function (see NewConfigExec's newID parameter in internal/node).
//
// The encoding is oklog/ulid/v2's, not ours: a 130-bit Crockford base32 text
// form is a specification, and a hand-rolled second implementation of a
// specification is exactly the re-implemented-knowledge case architecture
// principle 2 rules out — generate or use the one definition, never
// reimplement it.
func NewULID() string {
	return ulid.MustNew(ulid.Timestamp(time.Now()), rand.Reader).String()
}
