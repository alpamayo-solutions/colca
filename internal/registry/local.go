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
			if bound, ok := m.MountOf(e.ULID); ok && bound != declaredMount {
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
	entry := uns.Entry{ULID: newULID(), Kind: uns.KindLocal, Name: name, Element: element}
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

// elementFor resolves a declared mount to the element a new entry binds to,
// authoring the branch when it is absent (§3.2: "path exists? bind to the
// element sitting there. path missing? author the elements along it, bind to
// the leaf."). No declaration means unplaced — bound to the node itself —
// so there is nothing to resolve and nothing to author: elementFor("")
// returns "", and Enroll already treats an empty element as a complete,
// valid placement (KindLocal, id-grants design §4).
func (m *Manager) elementFor(mount string) (string, error) {
	if mount == "" {
		return "", nil
	}
	m.mu.RLock()
	place, upsert := m.place, m.upsert
	m.mu.RUnlock()
	if place == nil || upsert == nil {
		return "", fmt.Errorf("register: mount authoring is not wired at this node")
	}

	var local, leaf string
	for _, seg := range strings.Split(mount, "/") {
		if seg == "" {
			continue
		}
		if local == "" {
			local = seg
		} else {
			local += "/" + seg
		}
		id, ok := place.IDAt(local)
		if !ok {
			// Same id convention the bench harness's place() and a parent's
			// enroll-time authoring already use for a path-derived element:
			// "el-" plus the path with "/" flattened to "-".
			id = "el-" + strings.ReplaceAll(local, "/", "-")
			if err := upsert(local, id); err != nil {
				return "", fmt.Errorf("register: could not author %s: %w", local, err)
			}
		}
		leaf = id
	}
	return leaf, nil
}

// newULID mints a fresh identity for a local service registering for the
// first time. Every other kind's ULID arrives from outside this node — a
// machine or child node is enrolled with an identity someone else already
// minted (a human's CLI, or a parent authoring a child at `colca node
// enroll`) — but a local service presents only a name, so Register is the
// first place in this codebase that must mint one itself.
//
// The encoding is oklog/ulid/v2's, not ours: a 130-bit Crockford base32 text
// form is a specification, and a hand-rolled second implementation of a
// specification is exactly the re-implemented-knowledge case architecture
// principle 2 rules out — generate or use the one definition, never
// reimplement it.
func newULID() string {
	return ulid.MustNew(ulid.Timestamp(time.Now()), rand.Reader).String()
}
