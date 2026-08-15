// Package registry owns the LIFECYCLE of the node's identity registry (auth
// design §2.3, §4, §7): loading the r/ family at start, enrolling and
// revoking entries through the store's atomic batches, serving the in-memory
// map to the doors, and firing the session-kick callback on changes. Every
// SEMANTIC question — entry validation, grant grammar, authorization — is
// answered by plugins/uns; this package never re-implements any of it (§8).
package registry

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/alpamayo-solutions/colca/internal/store"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// ErrNotEnrolled marks a revoke of an identity this node never enrolled (or
// already revoked) — the HTTP layer maps it to 404.
var ErrNotEnrolled = errors.New("identity not enrolled at this node")

// ErrConflict marks an enrollment that collides with an existing entry
// (pubkey or mount uniqueness) — the HTTP layer maps it to 409.
var ErrConflict = errors.New("enrollment conflict")

// observerSegment is the placeholder path segment for mountless observers'
// _EdgeNode topics (the grammar needs a non-empty hierarchy path; "_"-prefixed
// segments are reserved, so no real zone can collide with it).
const observerSegment = "_observer"

type Manager struct {
	st      *store.Store
	log     *slog.Logger
	mu      sync.RWMutex
	byID    uns.Registry      // ulid → entry
	byPK    map[string]string // pubkey hex → ulid
	kick    func(ulid string)
	deliver func(topic string, payload []byte, retain bool)
}

// New loads every locally enrolled entry from the store. A corrupt persisted
// entry is fatal: silently skipping it would revoke an identity by accident.
func New(st *store.Store, nodeULID string) (*Manager, error) {
	m := &Manager{
		st:   st,
		log:  slog.Default().With("node", nodeULID, "comp", "registry"),
		byID: uns.Registry{},
		byPK: map[string]string{},
	}
	for ulid, raw := range st.RegistryScan() {
		var e uns.Entry
		if err := json.Unmarshal(raw, &e); err != nil {
			return nil, fmt.Errorf("registry: corrupt persisted entry %s: %w", ulid, err)
		}
		if err := e.Validate(); err != nil {
			return nil, fmt.Errorf("registry: persisted entry %s invalid: %w", ulid, err)
		}
		m.byID[e.ULID] = &e
		m.byPK[e.Pubkey] = e.ULID
	}
	return m, nil
}

// SetKick late-binds the session-kick callback (the broker exists after the
// registry in node assembly). nil-safe: without a broker nothing is kicked.
func (m *Manager) SetKick(fn func(ulid string)) {
	m.mu.Lock()
	m.kick = fn
	m.mu.Unlock()
}

// SetDeliver late-binds the local-bus mirror (engine.LocalDeliver shape): an
// enrolled entry's _EdgeNode entity is state and appears retained on the bus
// like any engine-persisted entity; a revocation clears the retained copy by
// delivering an empty retained payload (MQTT retained-clear semantics).
func (m *Manager) SetDeliver(fn func(topic string, payload []byte, retain bool)) {
	m.mu.Lock()
	m.deliver = fn
	m.mu.Unlock()
}

// topicFor builds the entry's _EdgeNode entity topic: level 4 = the enrolled
// identity, path = its placement (§2.2).
func topicFor(e *uns.Entry) (topic, kvPath string) {
	p := e.Mount
	if p == "" {
		p = observerSegment
	}
	return "colca/v1/_EdgeNode/" + e.ULID + "/" + p, p
}

// Enroll validates and persists a new or updated entry (§4): entry-shape
// checks via uns, uniqueness of pubkey and (non-empty) mount across OTHER
// entries, then the atomic store batch and the map swap. Re-enrolling an
// existing ULID is an update and kicks the live session.
func (m *Manager) Enroll(entryJSON []byte) (ulid string, offset uint64, err error) {
	var e uns.Entry
	if err := json.Unmarshal(entryJSON, &e); err != nil {
		return "", 0, fmt.Errorf("enroll: not valid JSON: %w", err)
	}
	if err := e.Validate(); err != nil {
		return "", 0, fmt.Errorf("enroll: %w", err)
	}

	m.mu.Lock()
	if other, ok := m.byPK[e.Pubkey]; ok && other != e.ULID {
		m.mu.Unlock()
		return "", 0, fmt.Errorf("enroll %s: pubkey already enrolled for %s: %w", e.ULID, other, ErrConflict)
	}
	if e.Mount != "" {
		for _, ex := range m.byID {
			if ex.ULID != e.ULID && ex.Mount == e.Mount {
				m.mu.Unlock()
				return "", 0, fmt.Errorf("enroll %s: mount %q already held by %s: %w", e.ULID, e.Mount, ex.ULID, ErrConflict)
			}
		}
	}

	canonical, err := json.Marshal(&e)
	if err != nil {
		m.mu.Unlock()
		return "", 0, err
	}
	topic, kvPath := topicFor(&e)
	off, err := m.st.RegistryPut(e.ULID, canonical, "entities", store.Record{
		Topic:   topic,
		Payload: canonical,
		TS:      time.Now().UnixMilli(),
		KVPath:  kvPath,
		KVNode:  e.ULID,
	})
	if err != nil {
		m.mu.Unlock()
		return "", 0, err
	}

	prev, existed := m.byID[e.ULID]
	if existed {
		delete(m.byPK, prev.Pubkey)
	}
	m.byID[e.ULID] = &e
	m.byPK[e.Pubkey] = e.ULID
	kick, deliver := m.kick, m.deliver
	// Callbacks fire OUTSIDE the lock: the broker's delivery path re-enters
	// this registry (per-delivery ACL check) in the same goroutine — invoking
	// it under the write lock would self-deadlock.
	m.mu.Unlock()
	if existed && kick != nil {
		kick(e.ULID)
	}
	if deliver != nil {
		deliver(topic, canonical, true) // entity = state, retained on the bus
	}
	m.log.Info("identity enrolled", "ulid", e.ULID, "kind", e.Kind, "mount", e.Mount, "updated", existed)
	return e.ULID, off, nil
}

// Revoke retires an enrolled identity: tombstone entity + r/ delete + KV
// retirement in one batch, map swap, session kick (§3, §7).
func (m *Manager) Revoke(ulid string) (offset uint64, err error) {
	m.mu.Lock()
	e, ok := m.byID[ulid]
	if !ok {
		m.mu.Unlock()
		return 0, fmt.Errorf("revoke %s: %w", ulid, ErrNotEnrolled)
	}
	topic, kvPath := topicFor(e)
	off, err := m.st.RegistryDelete(ulid, "entities", store.Record{
		Topic:  topic,
		TS:     time.Now().UnixMilli(),
		KVPath: kvPath,
		KVNode: ulid,
	})
	if err != nil {
		m.mu.Unlock()
		return 0, err
	}
	delete(m.byID, ulid)
	delete(m.byPK, e.Pubkey)
	kick, deliver := m.kick, m.deliver
	m.mu.Unlock() // callbacks outside the lock — see Enroll
	if kick != nil {
		kick(ulid)
	}
	if deliver != nil {
		deliver(topic, nil, true) // empty retained payload clears the retained copy
	}
	m.log.Info("identity revoked", "ulid", ulid)
	return off, nil
}

func (m *Manager) Get(ulid string) (*uns.Entry, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	e, ok := m.byID[ulid]
	return e, ok
}

func (m *Manager) ByPubkey(pubkeyHex string) (*uns.Entry, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	ulid, ok := m.byPK[pubkeyHex]
	if !ok {
		return nil, false
	}
	e, ok := m.byID[ulid]
	return e, ok
}

// MountOf is the engine's mount source (engine.Mounts interface).
func (m *Manager) MountOf(ulid string) (string, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	e, ok := m.byID[ulid]
	if !ok || e.Mount == "" {
		// A mountless observer has no place to write into — same contract as
		// the old "client without a mount" rule.
		return "", false
	}
	return e.Mount, true
}

// List returns the locally enrolled entries sorted by ULID (GET /enroll).
func (m *Manager) List() []*uns.Entry {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]*uns.Entry, 0, len(m.byID))
	for _, e := range m.byID {
		out = append(out, e)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ULID < out[j].ULID })
	return out
}
