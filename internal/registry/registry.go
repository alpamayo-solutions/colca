// Package registry manages the lifecycle of the node's identity registry:
// loading entries at start, enrolling and revoking them in atomic store batches,
// serving them to the doors, and kicking sessions on changes. Validation and
// authorization are answered by plugins/uns.
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

// ErrNotEnrolled marks a revoke of an identity this node does not hold. The HTTP
// door answers 404.
var ErrNotEnrolled = errors.New("identity not enrolled at this node")

// ErrConflict marks an enrollment whose pubkey or element is already taken. The
// HTTP door answers 409.
var ErrConflict = errors.New("enrollment conflict")

// ErrUnknownElement marks an enrollment that binds to an element this node does
// not hold. The HTTP door answers 422: author the element, then enroll.
var ErrUnknownElement = errors.New("unknown element")

// ErrNotNode marks a drain of an entry that is not a node. The HTTP door answers
// 409.
var ErrNotNode = errors.New("move-drain applies only to kind=node entries")

// ErrAlreadyDraining marks a drain of an entry that is already draining. The
// HTTP door answers 409.
var ErrAlreadyDraining = errors.New("already draining")

type Manager struct {
	st     *store.Store
	log    *slog.Logger
	nodeID string
	mu     sync.RWMutex
	// registerMu serialises Register, so two first requests under one name
	// (a service's HTTP and MQTT connections starting together) share one entry.
	registerMu sync.Mutex
	byID       uns.Registry      // ulid → entry
	byPK       map[string]string // pubkey hex → ulid; KindLocal holds none, so "" is never indexed here
	byName     map[string]string // name → ulid (KindLocal only; a second index, same shape as byPK)
	kick       func(ulid string)
	deliver    func(topic string, payload []byte, retain bool)
	m          *metrics.Metrics // late-bound; every Metrics method is nil-safe, so this may stay unset
	// ns resolves an entry's element to a local path. It is wired once the engine
	// exists; until then nothing resolves, which fails closed.
	ns uns.Namespace
	// author resolves a declared mount to its element and authors missing segments:
	// ConfigExec.authorElementAt behind the "element/author" verb, wired by
	// SetAuthoring.
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
	persisted, err := st.RegistryScan()
	if err != nil {
		return nil, fmt.Errorf("registry: load persisted entries: %w", err)
	}
	for ulid, raw := range persisted {
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

// SetDeliver wires the local-bus mirror. Enrollment publishes the retained
// _EnrolledIdentity entity; revocation clears it with an empty retained payload.
func (m *Manager) SetDeliver(fn func(topic string, payload []byte, retain bool)) {
	m.mu.Lock()
	m.deliver = fn
	m.mu.Unlock()
}

// SetMetrics wires the node's metrics, so Drain maintains colca_drains_active
// itself. Optional: every Metrics method is nil-safe.
func (m *Manager) SetMetrics(mt *metrics.Metrics) {
	m.mu.Lock()
	m.m = mt
	m.mu.Unlock()
}

// SetNamespace wires the element resolver every placement question goes through.
func (m *Manager) SetNamespace(ns uns.Namespace) {
	m.mu.Lock()
	m.ns = ns
	m.mu.Unlock()
}

// SetAuthoring wires self-registration's mount authoring. Like the namespace it
// depends on the engine, which is built after the registry.
func (m *Manager) SetAuthoring(author func(path string) (string, error)) {
	m.mu.Lock()
	m.author = author
	m.mu.Unlock()
}

// mountOf resolves an entry's placement now. The caller holds at least the read
// lock. ok is false without an element or when it cannot be resolved.
func (m *Manager) mountOf(e *uns.Entry) (string, bool) {
	if e.Element == "" || m.ns == nil {
		return "", false
	}
	return m.ns.PathOf(e.Element)
}

// topicFor builds the inventory topic for an entry: level 4 is this node and the
// entry's ULID is the record id. Placement affects authorization, not the topic.
func (m *Manager) topicFor(e *uns.Entry) (topic, kvPath string) {
	p := "_colca/identities/" + e.ULID
	return uns.Prefix() + "_EnrolledIdentity/" + m.nodeID + "/" + p, p
}

// Enroll validates and persists a new or updated entry: shape checks via uns,
// pubkey and element uniqueness against other entries, then one atomic batch.
// Re-enrolling an existing ULID updates it and kicks its live session.
//
// The element must exist on this node; author it first, then enroll.
func (m *Manager) Enroll(entryJSON []byte) (ulid string, offset uint64, err error) {
	var e uns.Entry
	if err := json.Unmarshal(entryJSON, &e); err != nil {
		return "", 0, fmt.Errorf("enroll: not valid JSON: %w", err)
	}
	if err := e.Validate(); err != nil {
		return "", 0, fmt.Errorf("enroll: %w", err)
	}

	m.mu.Lock()
	// Local entries have no pubkey, so an empty key is not deduplicated; their names
	// are unique instead (below).
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

	// Seat a new repl child's delivery floor at the current commands head: commands
	// issued before its enrollment were meant for the mount's previous occupant, and
	// drain completion reads this cursor. SetIfAbsent keeps an edit of a live child
	// from moving it; an empty stream gets no cursor, which would only pin retention.
	// A failed write is logged, not fatal, and the next Enroll retries.
	if head := m.st.NextOffset("commands"); head > 1 && e.MayUseDoor(uns.DoorRepl) {
		if _, err := m.st.CursorSetIfAbsent(uns.DownlinkCursorPrefix+e.ULID, "commands", head); err != nil {
			m.log.Warn("enroll: downlink floor not seated — this child's next move-drain will count "+
				"already-delivered commands as pending until a later enroll seats it",
				"ulid", e.ULID, "head", head, "err", err)
		}
	}

	// The same seat for a machine's delivery cursor, where command replay starts, so
	// a newly enrolled machine is not replayed commands from before it existed here.
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
	// Callbacks fire outside the lock: the broker's delivery path calls back into
	// the registry on the same goroutine.
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

// Revoke retires an enrolled identity: tombstone entity, registry delete and KV
// retirement in one batch, then the session kick. It never waits for a drain.
//
// wasDraining reports whether the entry was draining at removal, read under the
// same lock, so the "forced" drain outcome is counted correctly.
func (m *Manager) Revoke(ulid string) (offset uint64, wasDraining bool, err error) {
	m.mu.Lock()
	e, ok := m.byID[ulid]
	if !ok {
		m.mu.Unlock()
		return 0, false, fmt.Errorf("revoke %s: %w", ulid, ErrNotEnrolled)
	}
	wasDraining = e.IsDraining()

	// What the identity authored goes with the identity. A service's
	// _ServiceDetails is observed state only that service may write, so one left
	// behind by a revoke can never be retired by anyone: the admin door refuses
	// the contract and the author no longer exists. A live node accumulated three
	// records for one service across two mount moves, two of them under elements
	// deleted since, and the hub above it folded them by name and showed a
	// running service as inactive.
	authored, err := m.authoredBy(ulid)
	if err != nil {
		m.mu.Unlock()
		// Reading nothing is not the same as there being nothing. Revoking on a
		// failed scan would leave exactly the record this retirement exists to
		// remove, with no second chance at it; the operator can retry instead.
		return 0, false, fmt.Errorf("revoke %s: %w", ulid, err)
	}

	// Revoke does not depend on the element still resolving: the inventory record has
	// a fixed address.
	topic, kvPath := m.topicFor(e)
	off, err := m.st.RegistryDelete(ulid, "entities", store.Record{
		Topic:  topic,
		TS:     time.Now().UnixMilli(),
		KVPath: kvPath,
		KVNode: m.nodeID,
	}, authored...)
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
	// A revoked child's command cursors go with it; otherwise one accumulates per
	// revoked device. Enroll re-seats them, which matters because drain completion
	// reads the commands cursor. Failures are logged: a leftover cursor only holds
	// retention.
	//
	// The definitions cursor stays. The child keeps the definitions it applied and
	// resumes from its own position when enrolled again; without this floor,
	// compaction could drop a retraction it has not read and the withdrawn group
	// would keep its grants. To drop it for a decommissioned node, delete
	// downlink-def:<ulid> on the definitions stream through POST /ack.
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
	m.mu.Unlock() // callbacks outside the lock, see Enroll
	if kick != nil {
		kick(ulid)
	}
	if deliver != nil {
		deliver(topic, nil, true) // empty retained payload clears the retained copy
		for _, rec := range authored {
			deliver(rec.Topic, nil, true)
		}
	}
	m.log.Info("identity revoked", "ulid", ulid, "records_retired", len(authored))
	return off, wasDraining, nil
}

// authoredBy builds a retirement tombstone for every record this node holds
// that ulid authored about itself. The caller holds at least the read lock.
//
// It asks each record who wrote it (uns.ServiceRecordAuthor) rather than
// computing the topic the identity would publish to now: a service leaves a
// record standing at every mount it has ever had, and a computed topic finds
// only the last one. Records this node holds for another node's author are a
// child's own state replicated up and are not this node's to retire, so the
// scan keeps only the ones written here.
func (m *Manager) authoredBy(ulid string) ([]store.Record, error) {
	entries, err := m.st.KVScan("")
	if err != nil {
		return nil, fmt.Errorf("scan for the records %s authored: %w", ulid, err)
	}
	ts := time.Now().UnixMilli()
	var out []store.Record
	for _, kv := range entries {
		if kv.NodeID != m.nodeID {
			continue
		}
		p, err := uns.Parse(kv.Topic)
		if err != nil {
			continue
		}
		if uns.ServiceRecordAuthor(p.Contract, kv.Payload) != ulid {
			continue
		}
		out = append(out, store.Record{
			Topic:  kv.Topic,
			TS:     ts,
			KVPath: kv.Path,
			KVNode: kv.NodeID,
			Delete: true,
		})
	}
	return out, nil
}

// Drain starts decommissioning a child node: its entry is persisted as draining
// and republished. The identity stays valid and the session is not kicked, since
// the queue drains through it. Admission refusal, completion and the final revoke
// live outside this package.
func (m *Manager) Drain(ulid string) (offset uint64, err error) {
	m.mu.Lock()
	e, ok := m.byID[ulid]
	if !ok {
		m.mu.Unlock()
		return 0, fmt.Errorf("drain %s: %w", ulid, ErrNotEnrolled)
	}
	// Typed errors for the HTTP status mapping; Validate below enforces the same rule.
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
	// Every persisted entry goes through uns.Entry.Validate.
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
	m.mu.Unlock() // callbacks outside the lock, see Enroll
	if deliver != nil {
		deliver(topic, canonical, true) // entity = state, retained on the bus
	}
	metricsRef.DrainStarted() // nil-safe
	m.log.Info("move-drain started", "ulid", ulid, "element", updated.Element, "mount", kvPath)
	return off, nil
}

// DrainingMount reports whether path lies under the mount of a draining child
// node, so new commands there are refused at admission.
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

// RoutesUnder reports whether some enrolled child node's mount covers path, that
// is, whether a command there can reach anyone. It only feeds a metric. Draining
// children count, because a drain delivers what is already queued.
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

// EntryOf returns an identity's name and bound element (uns.Bindings), which
// autobind needs to compute where that identity's catalogue sits.
func (m *Manager) EntryOf(ulid string) (name, element string, ok bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	e, ok := m.byID[ulid]
	if !ok {
		return "", "", false
	}
	return e.CatalogueName(), e.Element, true
}

// Entries lists the identities enrolled here (uns.Bindings). What a catalogue
// topic looks like is decided in plugins/uns.
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

// ByName resolves a local entry by the name it presents on connect, the local
// door's counterpart to ByPubkey.
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
func (m *Manager) ListPage(after string, limit int) (entries []*uns.Entry, next string) {
	if limit <= 0 {
		return nil, ""
	}
	all := m.List()
	start := sort.Search(len(all), func(i int) bool { return all[i].ULID > after })
	end := start + limit
	if end > len(all) {
		end = len(all)
	}
	entries = all[start:end]
	if end < len(all) && len(entries) > 0 {
		next = entries[len(entries)-1].ULID
	}
	return entries, next
}
