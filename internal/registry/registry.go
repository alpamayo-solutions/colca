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

type Manager struct {
	st      *store.Store
	log     *slog.Logger
	nodeID  string
	mu      sync.RWMutex
	byID    uns.Registry      // ulid → entry
	byPK    map[string]string // pubkey hex → ulid; KindLocal holds none, so "" is never indexed here
	byName  map[string]string // name → ulid (KindLocal only; a second index, same shape as byPK)
	kick    func(ulid string)
	deliver func(topic string, payload []byte, retain bool)
	m       *metrics.Metrics // late-bound; every Metrics method is nil-safe, so this may stay unset
	// ns resolves an entry's element to this node's local path for it. Late-
	// bound: the namespace is a projection of records the engine holds, and the
	// engine is built after the registry. Until it is wired nothing resolves,
	// which is the fail-closed answer — an unplaced identity has no place.
	ns uns.Namespace
	// author is self-registration's mount-authoring dependency
	// (local-service-trust design §3.2): it resolves a declared mount to its
	// element, authoring any segment along it that is missing and reusing
	// every one that already exists. That walk is domain knowledge shared with
	// a catalogue tag's own meta.element (exec_configure.go bindCatalogue), so
	// it lives in exactly one place — ConfigExec.authorElementAt — reached
	// here through the "element/author" _CmdConfigure verb rather than kept as
	// a second copy (architecture principle 1). Late-bound like ns: nil until
	// SetAuthoring wires it, which is fine — Register is not reachable before
	// the doors that call it are wired either.
	author func(path string) (string, error)
}

// New loads every locally enrolled entry from the store. A corrupt persisted
// entry is fatal: silently skipping it would revoke an identity by accident.
func New(st *store.Store, nodeULID string) (*Manager, error) {
	m := &Manager{
		st:     st,
		log:    slog.Default().With("node", nodeULID, "comp", "registry"),
		nodeID: nodeULID,
		byID:   uns.Registry{},
		byPK:   map[string]string{},
		byName: map[string]string{},
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
		if e.Pubkey != "" {
			m.byPK[e.Pubkey] = e.ULID
		}
		if e.Name != "" {
			m.byName[e.Name] = e.ULID
		}
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
// enrolled entry's _EnrolledIdentity entity is state and appears retained on the bus
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

// SetAuthoring late-binds self-registration's mount-authoring dependency
// (local-service-trust design §3.2): resolve or author the element at a
// declared mount. Late-bound in the same style as SetNamespace: a projection
// of records the engine holds, built after the registry.
func (m *Manager) SetAuthoring(author func(path string) (string, error)) {
	m.mu.Lock()
	m.author = author
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

// topicFor builds the security-inventory entity topic. Level 4 is the node
// that enrolled the identity; the enrolled identity remains the record id in
// the reserved inventory path. Placement controls authorization, not topology.
func (m *Manager) topicFor(e *uns.Entry) (topic, kvPath string) {
	p := "_colca/identities/" + e.ULID
	return "colca/v1/_EnrolledIdentity/" + m.nodeID + "/" + p, p
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
	// KindLocal carries no pubkey (Entry.Validate rejects a non-empty one for
	// it), so "" is not a real key to dedupe on here — every local entry
	// would otherwise collide with the first one enrolled, regardless of
	// name. Uniqueness for KindLocal is byName's job (below).
	if e.Pubkey != "" {
		if other, ok := m.byPK[e.Pubkey]; ok && other != e.ULID {
			m.mu.Unlock()
			return "", 0, fmt.Errorf("enroll %s: pubkey already enrolled for %s: %w", e.ULID, other, ErrConflict)
		}
	}
	if e.Name != "" {
		if other, ok := m.byName[e.Name]; ok && other != e.ULID {
			m.mu.Unlock()
			return "", 0, fmt.Errorf("enroll %s: name %q already enrolled for %s: %w", e.ULID, e.Name, other, ErrConflict)
		}
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
	topic, kvPath := m.topicFor(&e)
	off, err := m.st.RegistryPut(e.ULID, canonical, "entities", store.Record{
		Topic:   topic,
		Payload: canonical,
		TS:      time.Now().UnixMilli(),
		KVPath:  kvPath,
		KVNode:  m.nodeID,
	})
	if err != nil {
		m.mu.Unlock()
		return "", 0, err
	}

	// Seat this child's delivery floor at the CURRENT commands head, if it has
	// none yet. The parent-side mirror of the child-side command rule
	// (parent-scoped-cursors design §3.2): a freshly enrolled child is not
	// charged with commands that predate its enrollment, because those were
	// addressed to whatever occupied its mount before it. Same reasoning, the
	// other end of the wire.
	//
	// Load-bearing, not tidiness. §3.4 deleted these cursors on Revoke on the
	// premise that they are "retention protection only — delivery position
	// rides the child's own `after` parameter". That premise is false: this
	// cursor is ALSO the move-drain completion predicate's floor
	// (repl.drainPendingCommands). Without this seed, re-enrolling a ULID at
	// the same parent leaves CursorGet answering its default 1, so every
	// command already delivered under that mount is re-counted as pending and
	// the next drain of that child can only ever resolve by expiry — a
	// long-TTL command parked below the old floor blocks it for its full TTL.
	//
	// SetIfAbsent, so a re-enroll that is really an UPDATE of a live child
	// (Enroll is idempotent and doubles as edit) never rewinds or advances a
	// position that child is actively using.
	//
	// Gated on the door, never on the kind: only repl children have downlink
	// cursors, and a machine or local service must not get one or the per-
	// device key leak §3.4 removed comes back. Asking the predicate is also
	// what plugins/uns's core-vocabulary check requires.
	//
	// And gated on head > 1, because a cursor AT 1 is not free. CursorGet
	// already answers 1 for an absent key, so writing the key there changes
	// no delivery decision — but it does create a retention floor pinning the
	// commands stream at its very first record, for a child that may never
	// connect. That is the opposite of what §3.4 was cleaning up, and the
	// downlink door pins the same rule from its own side ("a poll at position
	// 1 must not create a cursor"). At head 1 there is nothing older to
	// decline, so there is nothing to record.
	//
	// A failed write is logged for the same reason Revoke logs a failed delete:
	// it is the half of "did not create" that is not the ordinary case. An
	// existing cursor (created false, err nil) is exactly what an Enroll-as-edit
	// is supposed to produce and says nothing; a write error means this child's
	// floor was never recorded, so its next drain re-scans from 1 — the very
	// regression this seed exists to prevent. Never fatal: the enrollment itself
	// is what the caller asked for, and the next Enroll re-attempts the seed.
	if head := m.st.NextOffset("commands"); head > 1 && e.MayUseDoor(uns.DoorRepl) {
		if _, err := m.st.CursorSetIfAbsent(uns.DownlinkCursorPrefix+e.ULID, "commands", head); err != nil {
			m.log.Warn("enroll: downlink floor not seated — this child's next move-drain will count "+
				"already-delivered commands as pending until a later enroll seats it",
				"ulid", e.ULID, "head", head, "err", err)
		}
	}

	// The same seed, for the same reason, on the machine side
	// (command-redelivery design §3): a machine's delivery cursor is where
	// ReplayOwedCommands starts, and an absent cursor answers 1. Without this,
	// enrolling a machine at a node that has been running for a while makes
	// its first subscribe replay every still-live command in the stream —
	// including ones addressed to it before it existed here. A machine is owed
	// what was issued to it since it was enrolled, not since the node was
	// born.
	//
	// Gated on the MQTT door, never on the kind, for the same reason the
	// downlink seed is gated on the repl door: only an identity that can be
	// handed a command over this bus has a delivery cursor. Gated on head > 1
	// so an empty stream creates no retention floor, and SetIfAbsent so an
	// Enroll-as-edit of a live machine never rewinds or skips its position.
	if head := m.st.NextOffset("commands"); head > 1 && e.MayUseDoor(uns.DoorMQTT) {
		if _, err := m.st.CursorSetIfAbsent(e.CommandCursor(), "commands", head); err != nil {
			m.log.Warn("enroll: command delivery floor not seated — this machine's first subscribe "+
				"may replay commands issued before it was enrolled here",
				"ulid", e.ULID, "head", head, "err", err)
		}
	}

	prev, existed := m.byID[e.ULID]
	if existed {
		if prev.Pubkey != "" {
			delete(m.byPK, prev.Pubkey)
		}
		if prev.Name != "" {
			delete(m.byName, prev.Name)
		}
	}
	m.byID[e.ULID] = &e
	if e.Pubkey != "" {
		m.byPK[e.Pubkey] = e.ULID
	}
	if e.Name != "" {
		m.byName[e.Name] = e.ULID
	}
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
	wasDraining = e.IsDraining()
	// Revoke is the kill switch and never waits for the namespace to be
	// healthy: an entry whose element stopped resolving still loses its
	// identity here and now. Security inventory has a stable reserved address,
	// so revocation does not depend on the element still resolving.
	topic, kvPath := m.topicFor(e)
	off, err := m.st.RegistryDelete(ulid, "entities", store.Record{
		Topic:  topic,
		TS:     time.Now().UnixMilli(),
		KVPath: kvPath,
		KVNode: m.nodeID,
	})
	if err != nil {
		m.mu.Unlock()
		return 0, false, err
	}
	delete(m.byID, ulid)
	if e.Pubkey != "" {
		delete(m.byPK, e.Pubkey)
	}
	if e.Name != "" {
		delete(m.byName, e.Name)
	}
	// Design §3.4: the child's parent-side COMMAND cursors die with its
	// identity, because leaving them accumulates one per revoked device
	// forever.
	//
	// Safe because Enroll re-seats the downlink one — NOT because these cursors
	// are retention protection only. That was the original premise and it is
	// false: the commands cursor is also the move-drain delivery floor
	// (repl.drainPendingCommands), so deleting it with nothing to replace it
	// leaves a re-enrolled child's floor at the default 1 and every command
	// already delivered under that mount counted as pending again. See the
	// re-seat in Enroll above, which is the other half of this: neither half is
	// correct alone.
	//
	// Failure is logged, never fatal: a surviving cursor is a retention floor,
	// not a security hole, and the revoke itself has already committed.
	//
	// A machine's command delivery cursor (command-redelivery design §3) dies
	// here for the same reason and is re-seated by the same Enroll: it
	// accumulates one per revoked device forever, and it holds a retention
	// floor on the commands stream on behalf of an identity that no longer
	// exists. Asking the door rather than the kind, as everywhere else.
	//
	// The DEFINITIONS cursor (uns.DownlinkDefCursorPrefix) is deliberately NOT
	// in this set, and that asymmetry is the whole point. A command is
	// EXECUTED: once delivered, nothing at the child depends on the parent
	// remembering where it was, so re-seating at the head is a complete answer.
	// A definition is APPLIED AS STATE (definition-stream design §2): the child
	// still HOLDS every group and type it read, and its own read position is
	// keyed by the PARENT's pubkey, so it survives the revoke untouched and
	// resumes exactly where it stopped when the same node is enrolled again
	// (repl.initCursors case 1).
	//
	// Deleting the parent's copy therefore removed the retention floor while
	// the consumer it protects was still alive and still holding the state.
	// Compaction's tombstone rule is the minimum over the cursors that EXIST
	// (store/compact.go), so the very next pass dropped the retraction of a
	// group this child had not read — and the re-enrolled child then resumed
	// past the hole and kept the withdrawn group, with its grants, forever.
	// That is the one failure compact.go's floor exists to prevent: "a live
	// authorization the operator believes they revoked".
	//
	// Re-seating it at 1 on re-enroll does not substitute for keeping it. The
	// tombstone is already gone by then, and a child applies definitions
	// record by record rather than diffing a snapshot — reading the compacted
	// set from offset 1 re-affirms what still exists and says nothing about
	// what stopped existing.
	//
	// The cost is a definitions floor held by a child that never returns. It is
	// bounded (child NODES only — a machine holds no downlink-def cursor at
	// all, and superseded records still compact regardless of any floor) and it
	// has an explicit operator exit: POST /ack {"cursor":"downlink-def:<ulid>",
	// "stream":"definitions","delete":true} at the admin door, for a node being
	// decommissioned rather than moved.
	dead := map[string]string{
		uns.DownlinkCursorPrefix + ulid: "commands",
	}
	if e.MayUseDoor(uns.DoorMQTT) {
		dead[e.CommandCursor()] = "commands"
	}
	for name, stream := range dead {
		if err := m.st.CursorDelete(name, stream); err != nil {
			m.log.Warn("revoke: cursor not deleted — it will hold a retention floor until the staleness window overrides it",
				"ulid", ulid, "cursor", name, "err", err)
		}
	}
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
	if !e.CanDrain() {
		m.mu.Unlock()
		return 0, fmt.Errorf("drain %s: %w", ulid, ErrNotNode)
	}
	if e.IsDraining() {
		m.mu.Unlock()
		return 0, fmt.Errorf("drain %s: %w", ulid, ErrAlreadyDraining)
	}

	updated := *e
	updated.MarkDraining()
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
	if updated.Element != "" {
		if _, placed := m.mountOf(&updated); !placed {
			m.mu.Unlock()
			return 0, fmt.Errorf("drain %s: element %s is not placed at this node: %w", ulid, updated.Element, ErrUnknownElement)
		}
	}
	topic, kvPath := m.topicFor(&updated)
	off, err := m.st.RegistryPut(updated.ULID, canonical, "entities", store.Record{
		Topic:   topic,
		Payload: canonical,
		TS:      time.Now().UnixMilli(),
		KVPath:  kvPath,
		KVNode:  m.nodeID,
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
		if !e.IsDraining() {
			continue
		}
		if mount, ok := m.mountOf(e); ok && uns.UnderMount(path, mount) {
			return true
		}
	}
	return false
}

// RoutesUnder reports whether some enrolled child NODE's mount covers path —
// i.e. whether a command at that path can be handed to anyone at all.
//
// The same question the downlink filter answers when it decides what to give a
// child, asked at admission instead of at delivery, and asked of every child
// rather than one. It is observability only: nothing is refused on the answer
// (see engine.countIfUnroutable for why refusing would break
// publish-before-enroll and every reparent window).
//
// Draining children count as routable. A drain exists precisely to deliver
// what is already queued for them, so calling their commands unroutable would
// report the mechanism working as a fault.
func (m *Manager) RoutesUnder(path string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, e := range m.byID {
		if !e.MayUseDoor(uns.DoorRepl) {
			continue // only a child node is fed over the downlink
		}
		if mount, ok := m.mountOf(e); ok && uns.UnderMount(path, mount) {
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

// EntryOf answers who an identity is and where it is bound (uns.Bindings port):
// autobind needs both to COMPUTE where that identity's catalogue sits, rather
// than searching for records that look like they might be its
// (local-service-trust design §6).
func (m *Manager) EntryOf(ulid string) (name, element string, ok bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	e, ok := m.byID[ulid]
	if !ok {
		return "", "", false
	}
	return e.CatalogueName(), e.Element, true
}

// Entries lists the identities this node has enrolled (uns.Bindings port):
// the lifecycle trigger asks which of them, if any, an arriving record's
// position belongs to. This package only lists — it has no notion of what a
// catalogue topic looks like; that computation lives in plugins/uns.
func (m *Manager) Entries() []uns.EntryRef {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]uns.EntryRef, 0, len(m.byID))
	for _, e := range m.byID {
		out = append(out, uns.EntryRef{ULID: e.ULID, Name: e.CatalogueName(), Element: e.Element})
	}
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

// ByName resolves a KindLocal entry by the name it presents on connect
// (local-service-trust design §3) — the local door's equivalent of
// ByPubkey, since a local service holds no key for the doors to pin on.
func (m *Manager) ByName(name string) (*uns.Entry, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	ulid, ok := m.byName[name]
	if !ok {
		return nil, false
	}
	e, ok := m.byID[ulid]
	return e, ok
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

// ListPage returns a stable ULID-ordered page. after is the last ULID from the
// preceding page; an empty next value means the registry is exhausted.
func (m *Manager) ListPage(after string, max int) (entries []*uns.Entry, next string) {
	if max <= 0 {
		return nil, ""
	}
	all := m.List()
	start := sort.Search(len(all), func(i int) bool { return all[i].ULID > after })
	end := start + max
	if end > len(all) {
		end = len(all)
	}
	entries = all[start:end]
	if end < len(all) && len(entries) > 0 {
		next = entries[len(entries)-1].ULID
	}
	return entries, next
}
