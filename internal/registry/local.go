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

// crockfordAlphabet is the ULID text encoding's alphabet (Crockford base32:
// excludes I, L, O, U to avoid transcription confusion with 1, 1, 0, V).
const crockfordAlphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

// newULID mints a fresh identity for a local service registering for the
// first time. Every other kind's ULID arrives from outside this node — a
// machine or child node is enrolled with an identity someone else already
// minted (a human's CLI, or a parent authoring a child at `colca node
// enroll`) — but a local service presents only a name, so Register is the
// first place in this codebase that must mint one itself.
//
// Standard ULID layout: a 48-bit millisecond timestamp followed by 80 bits of
// randomness, rendered as 26 Crockford-base32 characters.
func newULID() string {
	var b [16]byte
	ms := uint64(time.Now().UnixMilli())
	for i := 5; i >= 0; i-- {
		b[i] = byte(ms)
		ms >>= 8
	}
	if _, err := rand.Read(b[6:]); err != nil {
		// crypto/rand failing means the platform's entropy source is broken —
		// there is no meaningful degraded identity to fall back to.
		panic("register: crypto/rand unavailable: " + err.Error())
	}
	return encodeCrockford32(b)
}

// encodeCrockford32 renders 16 bytes (128 bits) as the canonical 26-character
// ULID text form (the well-known ULID bit layout: 130 virtual bits grouped 5
// at a time, most significant first, with the 2 extra bits the split leaves
// over prepended as zeros — so the first character carries only the top 3
// bits of b[0]). Each line below picks out one 5-bit group; there is no
// shorter way to say "these particular bits" than naming them.
func encodeCrockford32(b [16]byte) string {
	e := crockfordAlphabet
	return string([]byte{
		e[(b[0]&224)>>5], e[b[0]&31],
		e[(b[1]&248)>>3], e[((b[1]&7)<<2)|((b[2]&192)>>6)], e[(b[2]&62)>>1],
		e[((b[2]&1)<<4)|((b[3]&240)>>4)], e[((b[3]&15)<<1)|((b[4]&128)>>7)],
		e[(b[4]&124)>>2], e[((b[4]&3)<<3)|((b[5]&224)>>5)], e[b[5]&31],
		e[(b[6]&248)>>3], e[((b[6]&7)<<2)|((b[7]&192)>>6)], e[(b[7]&62)>>1],
		e[((b[7]&1)<<4)|((b[8]&240)>>4)], e[((b[8]&15)<<1)|((b[9]&128)>>7)],
		e[(b[9]&124)>>2], e[((b[9]&3)<<3)|((b[10]&224)>>5)], e[b[10]&31],
		e[(b[11]&248)>>3], e[((b[11]&7)<<2)|((b[12]&192)>>6)], e[(b[12]&62)>>1],
		e[((b[12]&1)<<4)|((b[13]&240)>>4)], e[((b[13]&15)<<1)|((b[14]&128)>>7)],
		e[(b[14]&124)>>2], e[((b[14]&3)<<3)|((b[15]&224)>>5)], e[b[15]&31],
	})
}
