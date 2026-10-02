// Self-registration: how an unprovisioned local service becomes an entry.

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

// Register returns a local service's entry, creating it the first time this node
// sees the name. Local doors call it on every CONNECT.
//
// The declared mount only seeds a new entry. For an existing entry a differing
// declaration is logged and ignored, so a restart cannot undo an operator's move.
func (m *Manager) Register(name, declaredMount string) (*uns.Entry, error) {
	if name == "" {
		return nil, fmt.Errorf("register: a local service needs a name")
	}
	m.registerMu.Lock()
	defer m.registerMu.Unlock()
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

// elementFor resolves a declared mount to the element a new entry binds to. An
// empty mount means bound to the node itself. Missing elements along the path are
// authored through the authoring hook (ConfigExec.authorElementAt), the same walk
// a catalogue tag's meta.element takes.
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

// normalizeMount drops empty segments, so "line1/press3", "/line1/press3" and
// "line1/press3/" compare equal, matching how elementFor walks a mount.
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

// NewULID mints a ULID, the identity format for nodes, registry entries,
// elements and signals. plugins/uns cannot import a ULID library, so the core
// passes this function in as its id port.
func NewULID() string {
	return ulid.MustNew(ulid.Timestamp(time.Now()), rand.Reader).String()
}
