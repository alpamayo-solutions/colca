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
	"strings"
	"sync"
	"time"

	"github.com/alpamayo-solutions/colca/internal/metrics"
	"github.com/alpamayo-solutions/colca/internal/store"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// ErrNotEnrolled marks a revoke of an identity this node never enrolled (or
// already revoked) — the HTTP layer maps it to 404.
var ErrNotEnrolled = errors.New("identity not enrolled at this node")

// ErrConflict marks an enrollment that collides with an existing entry
// (pubkey or element uniqueness) — the HTTP layer maps it to 409.
var ErrConflict = errors.New("enrollment conflict")

// ErrUnknownElement marks an enrollment that binds to an element this node does
// not hold. Unlike a collision it is not a fight over something that exists —
// the entry names a place that is simply absent — so the HTTP layer answers 422
// rather than 409: author the element, then enroll.
var ErrUnknownElement = errors.New("unknown element")

// ErrNotNode marks a Drain request against an entry that is not kind=node —
// machines are out of scope for move-drain (design §3.2 [delta]) — the HTTP
// layer maps it to 409.
var ErrNotNode = errors.New("move-drain applies only to kind=node entries")

// ErrAlreadyDraining marks a Drain request against an entry already draining
// — the HTTP layer maps it to 409.
var ErrAlreadyDraining = errors.New("already draining")

// observerSegment is the placeholder path segment for element-less observers'
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
	m       *metrics.Metrics // late-bound; every Metrics method is nil-safe, so this may stay unset
	// ns resolves an entry's element to this node's local path for it. Late-
	// bound: the namespace is a projection of records the engine holds, and the
	// engine is built after the registry. Until it is wired nothing resolves,
	// which is the fail-closed answer — an unplaced identity has no place.
	ns uns.Namespace
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

// SetMetrics late-binds the node's metrics registry (move-drain design
// §3.4): Drain owns incrementing colca_drains_active itself, the same way
// Enroll owns firing kick/deliver — a state transition the registry fully
// understands needs no caller to remember a follow-up call. Never required:
// every Metrics method is nil-safe, so an unset m simply means the gauge
// stays unobserved (unit tests that do not wire metrics).
func (m *Manager) SetMetrics(mt *metrics.Metrics) {
	m.mu.Lock()
	m.m = mt
	m.mu.Unlock()
}

// SetNamespace late-binds the element resolver every placement question goes
// through (id-grants design §4). Wiring hands it the node's element index.
func (m *Manager) SetNamespace(ns uns.Namespace) {
	m.mu.Lock()
	m.ns = ns
	m.mu.Unlock()
}

// mountOf resolves an entry's placement at this moment. The caller holds at
// least the read lock. ok=false for an element-less observer (nothing to place)
// and for an element this node cannot resolve (fail closed — never guess a
// place).
func (m *Manager) mountOf(e *uns.Entry) (string, bool) {
	if e.Element == "" || m.ns == nil {
		return "", false
	}
	return m.ns.PathOf(e.Element)
}

// topicFor builds the entry's _EdgeNode entity topic: level 4 = the enrolled
// identity, path = its placement resolved right now (§2.2). ok=false only when
// a bound element does not resolve — an element-less observer files under the
// placeholder segment and is fine.
func (m *Manager) topicFor(e *uns.Entry) (topic, kvPath string, ok bool) {
	p := observerSegment
	if e.Element != "" {
		resolved, found := m.mountOf(e)
		if !found {
			return "", "", false
		}
		p = resolved
	}
	return "colca/v1/_EdgeNode/" + e.ULID + "/" + p, p, true
}

// Enroll validates and persists a new or updated entry (§4): entry-shape
// checks via uns, uniqueness of pubkey and (non-empty) element across OTHER
// entries, then the atomic store batch and the map swap. Re-enrolling an
// existing ULID is an update and kicks the live session.
//
// An entry that binds to an element this node does not hold is refused: the
// element is what gives the identity a place, so enrolling against an unknown
// one would produce an identity that can authenticate and write nowhere. Author
// the element first (`_CmdConfigure element/upsert`), then enroll.
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
	if e.Element != "" {
		for _, ex := range m.byID {
			if ex.ULID != e.ULID && ex.Element == e.Element {
				m.mu.Unlock()
				return "", 0, fmt.Errorf("enroll %s: element %s already bound by %s: %w", e.ULID, e.Element, ex.ULID, ErrConflict)
			}
		}
		if _, ok := m.mountOf(&e); !ok {
			m.mu.Unlock()
			return "", 0, fmt.Errorf("enroll %s: element %s is not placed at this node — author it first: %w",
				e.ULID, e.Element, ErrUnknownElement)
		}
	}

	canonical, err := json.Marshal(&e)
	if err != nil {
		m.mu.Unlock()
		return "", 0, err
	}
	topic, kvPath, ok := m.topicFor(&e)
	if !ok {
		m.mu.Unlock()
		return "", 0, fmt.Errorf("enroll %s: element %s is not placed at this node: %w", e.ULID, e.Element, ErrUnknownElement)
	}
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
	m.log.Info("identity enrolled", "ulid", e.ULID, "kind", e.Kind, "element", e.Element, "mount", kvPath, "updated", existed)
	return e.ULID, off, nil
}

// Revoke retires an enrolled identity: tombstone entity + r/ delete + KV
// retirement in one batch, map swap, session kick (§3, §7). No drain
// precondition here, ever (move-drain design §3.1/§3.2) — DELETE stays the
// immediate kill-switch regardless of status.
//
// wasDraining reports whether the entry's status was "draining" at the exact
// moment of removal (read under the SAME lock as the removal itself, so
// there is no race window between checking and revoking) — the move-drain
// "forced" outcome bookkeeping (httpapi.go) needs this and used to read it
// via a separate, unlocked Get() call before Revoke; folding it into
// Revoke's own return closes that window instead of just narrowing it. Pure
// telemetry: it changes nothing about whether or how Revoke proceeds.
func (m *Manager) Revoke(ulid string) (offset uint64, wasDraining bool, err error) {
	m.mu.Lock()
	e, ok := m.byID[ulid]
	if !ok {
		m.mu.Unlock()
		return 0, false, fmt.Errorf("revoke %s: %w", ulid, ErrNotEnrolled)
	}
	wasDraining = e.Status == uns.StatusDraining
	// Revoke is the kill switch and never waits for the namespace to be
	// healthy: an entry whose element stopped resolving still loses its
	// identity here and now. Its _EdgeNode record cannot be addressed in that
	// case, so it is left behind and logged loudly. Reaching this requires the
	// element to have been deleted out from under a bound entry, which
	// element/delete refuses.
	topic, kvPath, placed := m.topicFor(e)
	if !placed {
		m.log.Error("revoking an identity whose element no longer resolves — its _EdgeNode record is orphaned",
			"ulid", ulid, "element", e.Element)
		topic, kvPath = "colca/v1/_EdgeNode/"+ulid+"/"+observerSegment, observerSegment
	}
	off, err := m.st.RegistryDelete(ulid, "entities", store.Record{
		Topic:  topic,
		TS:     time.Now().UnixMilli(),
		KVPath: kvPath,
		KVNode: ulid,
	})
	if err != nil {
		m.mu.Unlock()
		return 0, false, err
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
	return off, wasDraining, nil
}

// Drain begins a move-drain decommission of an enrolled kind=node child
// (move-drain design §3.1/§3.2): persists status "draining" on its entry —
// its identity stays fully valid (connects, fetches /downlink, acks) — and
// republishes the (now-draining) entity like any entry update. Unlike
// Enroll's update path this does NOT kick the live session: a drain must not
// interrupt the connection its own queue is draining through, and it never
// rewrites pubkey/mount, so there is nothing else to reconcile.
//
// The completion predicate (item 3), the admission-time rejection of new
// commands under a draining mount (item 2, DrainingMount below) and the
// auto-revoke on completion (item 4) all live outside this package —
// registry only owns the identity/lifecycle fact, never stream contents
// (package doc comment).
func (m *Manager) Drain(ulid string) (offset uint64, err error) {
	m.mu.Lock()
	e, ok := m.byID[ulid]
	if !ok {
		m.mu.Unlock()
		return 0, fmt.Errorf("drain %s: %w", ulid, ErrNotEnrolled)
	}
	// Fast, typed gates for the HTTP layer's 404/409/409 mapping — Validate
	// below re-enforces the same kind/status compatibility rule (so this
	// method never drifts from it as Entry.Validate grows), but its generic
	// error can't be matched with errors.Is the way ErrNotNode/
	// ErrAlreadyDraining can.
	if e.Kind != uns.KindNode {
		m.mu.Unlock()
		return 0, fmt.Errorf("drain %s: %w", ulid, ErrNotNode)
	}
	if e.Status == uns.StatusDraining {
		m.mu.Unlock()
		return 0, fmt.Errorf("drain %s: %w", ulid, ErrAlreadyDraining)
	}

	updated := *e
	updated.Status = uns.StatusDraining
	// Same discipline as Enroll: every entry this package ever persists is
	// validated through the one shape-of-truth (uns.Entry.Validate), not
	// re-derived here — the two explicit gates above classify HTTP status
	// codes, they do not replace this.
	if err := updated.Validate(); err != nil {
		m.mu.Unlock()
		return 0, fmt.Errorf("drain %s: %w", ulid, err)
	}
	canonical, err := json.Marshal(&updated)
	if err != nil {
		m.mu.Unlock()
		return 0, err
	}
	topic, kvPath, placed := m.topicFor(&updated)
	if !placed {
		m.mu.Unlock()
		return 0, fmt.Errorf("drain %s: element %s is not placed at this node: %w", ulid, updated.Element, ErrUnknownElement)
	}
	off, err := m.st.RegistryPut(updated.ULID, canonical, "entities", store.Record{
		Topic:   topic,
		Payload: canonical,
		TS:      time.Now().UnixMilli(),
		KVPath:  kvPath,
		KVNode:  updated.ULID,
	})
	if err != nil {
		m.mu.Unlock()
		return 0, err
	}
	m.byID[ulid] = &updated
	deliver, metricsRef := m.deliver, m.m
	m.mu.Unlock() // callbacks outside the lock — see Enroll
	if deliver != nil {
		deliver(topic, canonical, true) // entity = state, retained on the bus
	}
	metricsRef.DrainStarted() // nil-safe
	m.log.Info("move-drain started", "ulid", ulid, "element", updated.Element, "mount", kvPath)
	return off, nil
}

// DrainingMount reports whether path (node-local coordinates, e.g. a
// Parsed.Path) falls under any currently draining kind=node child's mount
// (move-drain design §3.2 item 2). Implements the engine.Mounts extension
// the ClassCmd admission check consults so new commands addressed under a
// draining mount are rejected instead of chasing a moving tail.
func (m *Manager) DrainingMount(path string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, e := range m.byID {
		if e.Status != uns.StatusDraining {
			continue
		}
		if mount, ok := m.mountOf(e); ok && strings.HasPrefix(path, mount+"/") {
			return true
		}
	}
	return false
}

// BoundTo lists the identities bound to an element, sorted. The data-model door
// consults it before retiring a position: an entry whose element disappeared
// would keep authenticating with nowhere to write.
func (m *Manager) BoundTo(elementID string) []string {
	if elementID == "" {
		return nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	var out []string
	for _, e := range m.byID {
		if e.Element == elementID {
			out = append(out, e.ULID)
		}
	}
	sort.Strings(out)
	return out
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

// MountOf is the engine's mount source (engine.Mounts interface): the path the
// identity's element sits at right now. An element-less observer has no place
// to write into, and neither does an identity whose element this node cannot
// resolve — both miss, and the engine rejects the publish.
func (m *Manager) MountOf(ulid string) (string, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	e, ok := m.byID[ulid]
	if !ok {
		return "", false
	}
	return m.mountOf(e)
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
