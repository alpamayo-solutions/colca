package registry

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/alpamayo-solutions/colca/internal/store"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

func openStore(t *testing.T, dir string) *store.Store {
	t.Helper()
	st, err := store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}

// mustKVScan is KVScan with the error handled the only way a test fixture
// can: fail loud (resources design §8).
func mustKVScan(t *testing.T, st *store.Store, prefix string) []store.KVEntry {
	t.Helper()
	entries, err := st.KVScan(prefix)
	if err != nil {
		t.Fatal(err)
	}
	return entries
}

func entryJSON(t *testing.T, e uns.Entry) []byte {
	t.Helper()
	b, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func machine(ulid, mount, pub string) uns.Entry {
	return uns.Entry{ULID: ulid, Pubkey: pub, Kind: uns.KindMachine, Element: elementAt(mount)}
}

func pub(seed string) string { return strings.Repeat(seed, 64/len(seed)) }

// elementAt is the element a test places at a path. The registry never derives
// one — it only ever asks the namespace — so the shape is the test's own
// convention and exists purely to keep failures readable.
func elementAt(path string) string {
	if path == "" {
		return ""
	}
	return "el-" + strings.ReplaceAll(path, "/", "-")
}

// ns is the element resolver the manager under test consults: a plain map,
// because one lookup is the whole of what a registry needs from a namespace.
type ns map[string]string // element id → this node's local path

func (n ns) PathOf(id string) (string, bool) { p, ok := n[id]; return p, ok }

// place puts an element at a path, or moves it when it already sits somewhere —
// which is how a rename reaches the registry: the namespace changes underneath,
// nothing is re-enrolled.
func (n ns) place(path string) { n[elementAt(path)] = path }

// newManager builds a manager whose namespace holds an element at each path.
func newManager(t *testing.T, st *store.Store, paths ...string) (*Manager, ns) {
	t.Helper()
	m, err := New(st, "01NODE")
	if err != nil {
		t.Fatal(err)
	}
	n := ns{}
	for _, p := range paths {
		n.place(p)
	}
	m.SetNamespace(n)
	return m, n
}

// newTestManager builds a manager for the name-index tests below: a fresh
// store plus a namespace resolving "el-press3" (elementAt("press3")), the
// element every local-service entry in this file binds to.
func newTestManager(t *testing.T) *Manager {
	t.Helper()
	m, _ := newManager(t, openStore(t, t.TempDir()), "press3")
	return m
}

// mustEnroll enrolls a raw entry JSON string, failing the test on error.
func mustEnroll(t *testing.T, m *Manager, entryJSON string) {
	t.Helper()
	if _, _, err := m.Enroll([]byte(entryJSON)); err != nil {
		t.Fatalf("Enroll: %v", err)
	}
}

func TestEnrollLookupAndPersistence(t *testing.T) {
	dir := t.TempDir()
	st := openStore(t, dir)
	m, _ := newManager(t, st, "z/cnc5")
	ulid, off, err := m.Enroll(entryJSON(t, machine("01M1", "z/cnc5", pub("ab"))))
	if err != nil {
		t.Fatalf("Enroll: %v", err)
	}
	if ulid != "01M1" || off == 0 {
		t.Fatalf("Enroll returned ulid=%q off=%d", ulid, off)
	}
	e, ok := m.Get("01M1")
	if !ok || e.Element != elementAt("z/cnc5") {
		t.Fatalf("Get after enroll: %v %v", e, ok)
	}
	if epk, ok := m.ByPubkey(pub("ab")); !ok || epk.ULID != "01M1" {
		t.Fatalf("ByPubkey after enroll: %v %v", epk, ok)
	}
	if mount, ok := m.mountOf(e); !ok || mount != "z/cnc5" {
		t.Fatalf("mountOf: %q %v", mount, ok)
	}
	if got := m.List(); len(got) != 1 || got[0].ULID != "01M1" {
		t.Fatalf("List: %v", got)
	}

	// The entity record landed in the entities stream under the enrolled topic.
	recs, _, err := st.Read("entities", off, 1, nil)
	if err != nil || len(recs) != 1 || recs[0].Topic != "colca/v1/_EnrolledIdentity/01NODE/_colca/identities/01M1" {
		t.Fatalf("entity record: %v err=%v", recs, err)
	}

	// A fresh manager on the same store sees the enrollment (r/ scan).
	m2, _ := newManager(t, st, "z/cnc5")
	if _, ok := m2.Get("01M1"); !ok {
		t.Fatal("enrollment not persisted")
	}
}

func TestEnrollValidationAndUniqueness(t *testing.T) {
	st := openStore(t, t.TempDir())
	m, _ := newManager(t, st, "z/a", "z/b", "z/c")
	if _, _, err := m.Enroll(entryJSON(t, machine("01M1", "z/a", pub("ab")))); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name string
		e    uns.Entry
	}{
		{"duplicate pubkey", machine("01M2", "z/b", pub("ab"))},
		{"element already bound", machine("01M3", "z/a", pub("cd"))},
		{"invalid entry", machine("", "z/c", pub("ef"))},
		{"element named by a path", uns.Entry{ULID: "01M4", Pubkey: pub("12"), Kind: uns.KindMachine, Element: "z/c"}},
		{"element not placed at this node", uns.Entry{ULID: "01M5", Pubkey: pub("34"), Kind: uns.KindMachine, Element: "el-nowhere"}},
		// The element-less read-only observer is gone (design §7): a machine
		// must be placed, same as a node — nothing proved an unplaced identity
		// belongs to this deployment.
		{"element-less machine", machine("01O1", "", pub("56"))},
	}
	for _, c := range cases {
		if _, _, err := m.Enroll(entryJSON(t, c.e)); err == nil {
			t.Errorf("%s: expected enrollment rejection", c.name)
		}
	}
	if _, _, err := m.Enroll([]byte("{not json")); err == nil {
		t.Error("malformed JSON: expected rejection")
	}
}

func TestReEnrollUpdatesAndKicks(t *testing.T) {
	st := openStore(t, t.TempDir())
	m, _ := newManager(t, st, "z/a", "z/b")
	var kicked []string
	m.SetKick(func(ulid string) { kicked = append(kicked, ulid) })

	if _, _, err := m.Enroll(entryJSON(t, machine("01M1", "z/a", pub("ab")))); err != nil {
		t.Fatal(err)
	}
	if len(kicked) != 0 {
		t.Fatalf("fresh enroll must not kick, got %v", kicked)
	}
	// Re-enroll same ulid with a new element and key: update + kick.
	if _, _, err := m.Enroll(entryJSON(t, machine("01M1", "z/b", pub("cd")))); err != nil {
		t.Fatalf("re-enroll: %v", err)
	}
	if len(kicked) != 1 || kicked[0] != "01M1" {
		t.Fatalf("re-enroll kick: %v", kicked)
	}
	if e, _ := m.Get("01M1"); e.Element != elementAt("z/b") || e.Pubkey != pub("cd") {
		t.Fatalf("entry not updated: %+v", e)
	}
	// The OLD pubkey no longer authenticates.
	if _, ok := m.ByPubkey(pub("ab")); ok {
		t.Fatal("stale pubkey still resolves after re-enroll")
	}
}

func TestRevoke(t *testing.T) {
	dir := t.TempDir()
	st := openStore(t, dir)
	m, _ := newManager(t, st, "z/a")
	var kicked []string
	m.SetKick(func(ulid string) { kicked = append(kicked, ulid) })
	if _, _, err := m.Enroll(entryJSON(t, machine("01M1", "z/a", pub("ab")))); err != nil {
		t.Fatal(err)
	}
	off, wasDraining, err := m.Revoke("01M1")
	if err != nil {
		t.Fatalf("Revoke: %v", err)
	}
	if wasDraining {
		t.Fatal("wasDraining must be false for a plain, never-drained entry")
	}
	if _, ok := m.Get("01M1"); ok {
		t.Fatal("entry survived revoke")
	}
	if _, ok := m.ByPubkey(pub("ab")); ok {
		t.Fatal("pubkey survived revoke")
	}
	if len(kicked) != 1 || kicked[0] != "01M1" {
		t.Fatalf("revoke kick: %v", kicked)
	}
	// Tombstone record: empty payload under the enrolled topic.
	recs, _, err := st.Read("entities", off, 1, nil)
	if err != nil || len(recs) != 1 || len(recs[0].Payload) != 0 {
		t.Fatalf("tombstone: %v err=%v", recs, err)
	}
	// KV projection retired.
	if kv := mustKVScan(t, st, "z/a"); len(kv) != 0 {
		t.Fatalf("KV survived revoke: %v", kv)
	}
	if _, _, err := m.Revoke("01M1"); !errors.Is(err, ErrNotEnrolled) {
		t.Fatalf("second revoke: %v, want ErrNotEnrolled", err)
	}
	// Revocation persists.
	m2, _ := newManager(t, st, "z/a")
	if _, ok := m2.Get("01M1"); ok {
		t.Fatal("revoked entry resurrected")
	}
}

// Registry changes mirror onto the local bus like any entity: enroll delivers
// the retained _EnrolledIdentity, revoke delivers an empty retained payload (the MQTT
// retained-clear), both AFTER the map swap and outside the manager's lock.
func TestEnrollAndRevokeMirrorToBus(t *testing.T) {
	st := openStore(t, t.TempDir())
	m, _ := newManager(t, st, "z/a")
	type msg struct {
		topic   string
		payload int // length; the revoke clear has 0
		retain  bool
	}
	var seen []msg
	m.SetDeliver(func(topic string, payload []byte, retain bool) {
		// Re-enter the manager like the broker's per-delivery ACL check does —
		// this deadlocks if callbacks fire under the write lock.
		m.Get("01M1")
		seen = append(seen, msg{topic, len(payload), retain})
	})
	if _, _, err := m.Enroll(entryJSON(t, machine("01M1", "z/a", pub("ab")))); err != nil {
		t.Fatal(err)
	}
	if _, _, err := m.Revoke("01M1"); err != nil {
		t.Fatal(err)
	}
	if len(seen) != 2 {
		t.Fatalf("deliveries = %v, want enroll + revoke", seen)
	}
	if seen[0].topic != "colca/v1/_EnrolledIdentity/01NODE/_colca/identities/01M1" || !seen[0].retain || seen[0].payload == 0 {
		t.Fatalf("enroll delivery = %+v, want retained entity payload", seen[0])
	}
	if seen[1].topic != seen[0].topic || !seen[1].retain || seen[1].payload != 0 {
		t.Fatalf("revoke delivery = %+v, want empty retained clear on the same topic", seen[1])
	}
}

func TestObserverUsesStableSecurityInventoryTopic(t *testing.T) {
	st := openStore(t, t.TempDir())
	m, _ := newManager(t, st, "z")
	_, off, err := m.Enroll(entryJSON(t, machine("01O1", "z", pub("ab"))))
	if err != nil {
		t.Fatal(err)
	}
	recs, _, err := st.Read("entities", off, 1, nil)
	if err != nil || len(recs) != 1 {
		t.Fatalf("read: %v err=%v", recs, err)
	}
	if recs[0].Topic != "colca/v1/_EnrolledIdentity/01NODE/_colca/identities/01O1" {
		t.Fatalf("observer topic = %q", recs[0].Topic)
	}
	if _, err := uns.Parse(recs[0].Topic); err != nil {
		t.Fatalf("observer topic must satisfy the grammar: %v", err)
	}
}

func node(ulid, mount, pub string) uns.Entry {
	return uns.Entry{ULID: ulid, Pubkey: pub, Kind: uns.KindNode, Element: elementAt(mount)}
}

// Move-drain design §3.1/§3.2: Drain persists status "draining" on a
// kind=node entry, republishes its (now-draining) entity retained like any
// entry update, but — unlike Enroll's re-enroll path — does NOT kick the
// live session: the whole point is to keep the connection the queue drains
// through alive.
func TestDrainPersistsStatusAndDoesNotKick(t *testing.T) {
	st := openStore(t, t.TempDir())
	m, _ := newManager(t, st, "z/child")
	var kicked []string
	m.SetKick(func(ulid string) { kicked = append(kicked, ulid) })
	type delivery struct {
		topic   string
		payload []byte
		retain  bool
	}
	var delivered []delivery
	m.SetDeliver(func(topic string, payload []byte, retain bool) {
		delivered = append(delivered, delivery{topic, payload, retain})
	})

	if _, _, err := m.Enroll(entryJSON(t, node("01N1", "z/child", pub("ab")))); err != nil {
		t.Fatal(err)
	}
	delivered = nil // drop the enroll delivery, only the drain delivery matters below

	off, err := m.Drain("01N1")
	if err != nil {
		t.Fatalf("Drain: %v", err)
	}
	if off == 0 {
		t.Fatal("Drain returned a zero offset")
	}
	if len(kicked) != 0 {
		t.Fatalf("Drain must not kick the live session, got %v", kicked)
	}
	if len(delivered) != 1 {
		t.Fatalf("Drain must deliver exactly one retained entity update, got %v", delivered)
	}
	if !delivered[0].retain {
		t.Fatalf("drain entity update must be retained, topic=%s", delivered[0].topic)
	}
	var deliveredEntry uns.Entry
	if err := json.Unmarshal(delivered[0].payload, &deliveredEntry); err != nil {
		t.Fatalf("drain delivery payload not valid entry JSON: %v", err)
	}
	if deliveredEntry.Status != uns.StatusDraining {
		t.Fatalf("drain delivery status = %q, want %q", deliveredEntry.Status, uns.StatusDraining)
	}
	e, ok := m.Get("01N1")
	if !ok || e.Status != uns.StatusDraining {
		t.Fatalf("Get after Drain: %+v %v, want status=draining", e, ok)
	}
	// Identity stays fully intact: element and pubkey unchanged.
	if e.Element != elementAt("z/child") || e.Pubkey != pub("ab") {
		t.Fatalf("Drain must not touch element/pubkey: %+v", e)
	}
	if _, ok := m.ByPubkey(pub("ab")); !ok {
		t.Fatal("Drain must not invalidate the pubkey — the child must still authenticate")
	}

	// The persisted r/ entry carries the status too (design §3.2: "persisted
	// in the r/ entry; survives restart").
	m2, _ := newManager(t, st, "z/child")
	e2, ok := m2.Get("01N1")
	if !ok || e2.Status != uns.StatusDraining {
		t.Fatalf("status did not survive a fresh load from the store: %+v %v", e2, ok)
	}
}

// Drain's error surface: 404-shaped (ErrNotEnrolled) for an unknown ulid,
// 409-shaped (ErrNotNode) for a machine — move-drain applies only to
// kind=node (design §3.2 [delta]) — and 409-shaped (ErrAlreadyDraining) for
// a second Drain call on the same child.
func TestDrainErrors(t *testing.T) {
	st := openStore(t, t.TempDir())
	m, _ := newManager(t, st, "z/a", "z/child")
	if _, err := m.Drain("nosuch"); !errors.Is(err, ErrNotEnrolled) {
		t.Fatalf("Drain of unknown ulid = %v, want ErrNotEnrolled", err)
	}

	if _, _, err := m.Enroll(entryJSON(t, machine("01M1", "z/a", pub("ab")))); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Drain("01M1"); !errors.Is(err, ErrNotNode) {
		t.Fatalf("Drain of a machine = %v, want ErrNotNode", err)
	}

	if _, _, err := m.Enroll(entryJSON(t, node("01N1", "z/child", pub("cd")))); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Drain("01N1"); err != nil {
		t.Fatalf("first Drain: %v", err)
	}
	if _, err := m.Drain("01N1"); !errors.Is(err, ErrAlreadyDraining) {
		t.Fatalf("second Drain = %v, want ErrAlreadyDraining", err)
	}
}

// DrainingMount is the engine.Mounts extension the ClassCmd admission check
// consults (move-drain design §3.2 item 2): true for any path under a
// draining kind=node child's mount, respecting the path-separator boundary
// (a sibling mount that merely shares a string prefix must not match), and
// false once the drain ends (auto-revoke or DELETE removes the entry
// entirely, so there is nothing left to match).
func TestDrainingMountBoundaryAndClears(t *testing.T) {
	st := openStore(t, t.TempDir())
	m, _ := newManager(t, st, "site1/edge1")
	if _, _, err := m.Enroll(entryJSON(t, node("01N1", "site1/edge1", pub("ab")))); err != nil {
		t.Fatal(err)
	}
	if m.DrainingMount("site1/edge1/x") {
		t.Fatal("not draining yet — DrainingMount must be false")
	}
	if _, err := m.Drain("01N1"); err != nil {
		t.Fatal(err)
	}
	if !m.DrainingMount("site1/edge1/x") {
		t.Fatal("path under the draining mount must report true")
	}
	if m.DrainingMount("site1/edge10/x") {
		t.Fatal("a sibling mount sharing only a string prefix must not match (path-separator boundary)")
	}
	if m.DrainingMount("other/x") {
		t.Fatal("a path outside any draining mount must be false")
	}
	if _, wasDraining, err := m.Revoke("01N1"); err != nil {
		t.Fatal(err)
	} else if !wasDraining {
		t.Fatal("wasDraining must be true — the entry was draining at revoke time")
	}
	if m.DrainingMount("site1/edge1/x") {
		t.Fatal("DrainingMount must be false once the drained child is revoked")
	}
}

func TestCorruptPersistedEntryFailsLoad(t *testing.T) {
	dir := t.TempDir()
	st := openStore(t, dir)
	if _, err := st.RegistryPut("01BAD", []byte("{corrupt"), "entities",
		store.Record{Topic: "colca/v1/_EnrolledIdentity/01NODE/_colca/identities/01BAD", Payload: []byte("{corrupt"), TS: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := New(st, "01NODE"); err == nil {
		t.Fatal("corrupt r/ entry must fail the load, not be skipped")
	}
}

// The whole point of binding to an element instead of asserting a path: the
// element moves, the mount moves with it, and nobody re-enrolls anything. A
// stored mount string would still read "z/a" here.
func TestARenamedElementMovesTheMountWithNoReEnrollment(t *testing.T) {
	st := openStore(t, t.TempDir())
	m, elements := newManager(t, st, "z/a")
	if _, _, err := m.Enroll(entryJSON(t, machine("01M1", "z/a", pub("ab")))); err != nil {
		t.Fatal(err)
	}
	e, ok := m.Get("01M1")
	if !ok {
		t.Fatal("Get after enroll")
	}
	if mount, ok := m.mountOf(e); !ok || mount != "z/a" {
		t.Fatalf("before the rename mountOf = %q %v, want z/a", mount, ok)
	}

	// The element is renamed in the namespace. Nothing touches the registry.
	elements[elementAt("z/a")] = "z/a-neu"

	if mount, ok := m.mountOf(e); !ok || mount != "z/a-neu" {
		t.Fatalf("after the rename mountOf = %q %v, want z/a-neu", mount, ok)
	}
	// The binding is to the element, not to either path: a second identity
	// still cannot take the position, whatever it is currently called.
	if _, _, err := m.Enroll(entryJSON(t, node("01N1", "z/a", pub("cd")))); !errors.Is(err, ErrConflict) {
		t.Fatalf("second binding to the same element = %v, want ErrConflict", err)
	}
}

// A mount that cannot be resolved is not a mount: the identity authenticates but
// has nowhere to write, and the engine rejects its publishes. Placing it
// somewhere by guesswork is the one failure worth avoiding at any cost.
func TestAnIdentityWhoseElementStopsResolvingHasNoMount(t *testing.T) {
	st := openStore(t, t.TempDir())
	m, elements := newManager(t, st, "z/a")
	if _, _, err := m.Enroll(entryJSON(t, machine("01M1", "z/a", pub("ab")))); err != nil {
		t.Fatal(err)
	}
	e, ok := m.Get("01M1")
	if !ok {
		t.Fatal("Get after enroll")
	}
	delete(elements, elementAt("z/a"))
	if mount, ok := m.mountOf(e); ok {
		t.Fatalf("mountOf = %q %v, want a miss — the element is gone", mount, ok)
	}
	// Revoke stays the kill switch regardless: an unresolvable element must
	// never keep an identity alive.
	if _, _, err := m.Revoke("01M1"); err != nil {
		t.Fatalf("Revoke with an unresolvable element: %v", err)
	}
	if _, ok := m.Get("01M1"); ok {
		t.Fatal("the identity survived revoke")
	}
}

// BoundTo is what the data-model door consults before retiring a position.
func TestBoundToNamesTheIdentitiesStandingOnAnElement(t *testing.T) {
	st := openStore(t, t.TempDir())
	m, _ := newManager(t, st, "z/a", "z/b")
	if _, _, err := m.Enroll(entryJSON(t, machine("01M1", "z/a", pub("ab")))); err != nil {
		t.Fatal(err)
	}
	if got := m.BoundTo(elementAt("z/a")); len(got) != 1 || got[0] != "01M1" {
		t.Fatalf("BoundTo(z/a) = %v, want [01M1]", got)
	}
	if got := m.BoundTo(elementAt("z/b")); len(got) != 0 {
		t.Fatalf("BoundTo(z/b) = %v, want nothing", got)
	}
	// "" is not a position query (design §7: an empty element now means
	// bound-to-the-node for the one kind that may go unplaced) — BoundTo
	// short-circuits it rather than answer a question it was never asked.
	if got := m.BoundTo(""); len(got) != 0 {
		t.Fatalf("BoundTo(%q) = %v, want nothing", "", got)
	}
	if _, _, err := m.Revoke("01M1"); err != nil {
		t.Fatal(err)
	}
	if got := m.BoundTo(elementAt("z/a")); len(got) != 0 {
		t.Fatalf("BoundTo after revoke = %v, want nothing", got)
	}
}

// Every entry stays keyed by ULID, local ones included; name is a second
// index, exactly as pubkey is (byPK) — ByName resolves through it, the local
// door's equivalent of ByPubkey (local-service-trust design §3).
func TestByNameFindsALocalEntry(t *testing.T) {
	m := newTestManager(t)
	mustEnroll(t, m, `{"ulid":"01JSVC","kind":"local","name":"connector-opcua","element":"el-press3"}`)

	e, ok := m.ByName("connector-opcua")
	if !ok || e.ULID != "01JSVC" {
		t.Fatalf("ByName = %v, %v; want the entry 01JSVC", e, ok)
	}
	if _, ok := m.ByName("nobody"); ok {
		t.Fatal("ByName resolved a name that was never enrolled")
	}
}

// KindLocal carries no pubkey (Entry.Validate requires it blank), so a bare
// "" must never be dedup-checked as though it were a real key — otherwise the
// FIRST local service ever enrolled at a node permanently blocks every other
// one, regardless of name, since they'd all collide on the shared blank
// pubkey before the name check is even reached. Found by mutation-checking
// TestTwoLocalServicesCannotShareAName: with the name-uniqueness check
// deleted, that test still passed — not via the name-uniqueness code path
// it's meant to pin, but via this pre-existing blank-pubkey collision (dating
// to KindLocal's introduction), which fires first and produces the
// same ErrConflict for the wrong reason.
func TestTwoLocalServicesWithDifferentNamesBothEnroll(t *testing.T) {
	m := newTestManager(t)
	mustEnroll(t, m, `{"ulid":"01JA","kind":"local","name":"connA"}`)
	if _, _, err := m.Enroll([]byte(`{"ulid":"01JB","kind":"local","name":"connB"}`)); err != nil {
		t.Fatalf("second local entry with a different name: err = %v; want success", err)
	}
	if _, ok := m.ByName("connA"); !ok {
		t.Fatal("connA missing after a second local entry enrolled")
	}
	if _, ok := m.ByName("connB"); !ok {
		t.Fatal("connB missing after enrollment")
	}
}

// Name uniqueness is enforced beside pubkey uniqueness: two local services
// cannot both present the same name at the door, or ByName could not tell
// them apart. Both entries are left unplaced (no element) on purpose: Enroll's
// element-uniqueness check is guarded by `if e.Element != ""`, so two unplaced
// entries can never trip it, and placement is optional for KindLocal (Task
// 3) — that leaves exactly one rule able to reject the second entry, the
// name-uniqueness guard this test is named after. (An earlier version of
// this test gave both entries the same element too, which meant the
// pre-existing element-uniqueness check fired first and the test passed even
// with the name check deleted — confirmed by mutation-check.)
func TestTwoLocalServicesCannotShareAName(t *testing.T) {
	m := newTestManager(t)
	mustEnroll(t, m, `{"ulid":"01JA","kind":"local","name":"conn"}`)

	_, _, err := m.Enroll([]byte(`{"ulid":"01JB","kind":"local","name":"conn"}`))
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("second entry with the same name: err = %v; want ErrConflict", err)
	}
}

// A revoked entry's name must not keep resolving — the index must not
// outlive the entry it points at. The direct ByName-after-Revoke assertion
// below is necessary but not sufficient: ByName's second step looks the ulid
// up in byID, which Revoke also deletes, so ByName would report a miss even
// if byName itself were never cleaned up — that mutation was confirmed to
// leave this test green. What isolates a stale byName entry is re-enrolling
// a NEW identity under the freed name: if byName still pointed at the
// revoked ulid, Enroll's uniqueness check would reject the newcomer as a
// conflict with an identity that no longer exists.
func TestByNameForgetsARevokedEntry(t *testing.T) {
	m := newTestManager(t)
	mustEnroll(t, m, `{"ulid":"01JSVC","kind":"local","name":"conn","element":"el-press3"}`)
	if _, _, err := m.Revoke("01JSVC"); err != nil {
		t.Fatalf("Revoke: %v", err)
	}
	if _, ok := m.ByName("conn"); ok {
		t.Fatal("a revoked name still resolves; the index outlived its entry")
	}
	if _, _, err := m.Enroll([]byte(`{"ulid":"01JB","kind":"local","name":"conn"}`)); err != nil {
		t.Fatalf("re-enrolling the freed name: %v; want success — byName must not still point at the revoked ulid", err)
	}
}

// Design §3.4: a revoked child's parent-side downlink cursors die with its
// identity, because they otherwise accumulate one pair per revoked device
// forever.
//
// §3.4's stated REASON for why this is safe — "they protect retention only,
// delivery position rides the child's `after` parameter" — turned out to be
// false: the commands cursor is also the move-drain completion predicate's
// floor. Deleting it is still right, but only because Enroll now re-seats
// that floor at the current head
// (TestEnrollSeatsTheDownlinkFloorAtTheCommandsHead). The two tests are one
// claim in two halves; neither is safe alone. The replication HWM is NOT
// deleted either way: a
// re-enrolled child re-offering from its LWM is deduped by it, which is the
// difference between a cheap reconciliation and duplicate application.
func TestRevokeDeletesDownlinkCursorsAndKeepsTheHWM(t *testing.T) {
	st := openStore(t, t.TempDir())
	m, _ := newManager(t, st, "z/a")
	ulid := "01NCHILD"
	if _, _, err := m.Enroll(entryJSON(t, machine(ulid, "z/a", pub("cd")))); err != nil {
		t.Fatal(err)
	}

	// off=2: CursorAck's monotonic guard treats 1 (the never-acked default)
	// as no movement, so it never persists a key — the cursor must actually
	// advance to exist for this test to prove anything.
	if !st.CursorAck(uns.DownlinkCursorPrefix+ulid, "commands", 2) {
		t.Fatal("seed ack of the downlink cursor did not move it")
	}
	if !st.CursorAck(uns.DownlinkDefCursorPrefix+ulid, "definitions", 2) {
		t.Fatal("seed ack of the downlink-def cursor did not move it")
	}
	if _, _, err := st.ApplyReplicated(ulid, "metrics", []store.ReplRecord{
		{ChildOffset: 7, Topic: "colca/v1/_Metric/m1/m1/t", Payload: []byte(`{"v":1}`), TS: 1},
	}); err != nil {
		t.Fatal(err)
	}
	hwmBefore := st.HWMGet(ulid, "metrics")
	if hwmBefore != 7 {
		t.Fatalf("seeded HWM = %d, want 7", hwmBefore)
	}

	if _, _, err := m.Revoke(ulid); err != nil {
		t.Fatal(err)
	}

	for _, c := range st.Cursors() {
		if c.Name == uns.DownlinkCursorPrefix+ulid || c.Name == uns.DownlinkDefCursorPrefix+ulid {
			t.Fatalf("revoke left cursor %q behind — it accumulates per revoked device forever", c.Name)
		}
	}
	if got := st.HWMGet(ulid, "metrics"); got != hwmBefore {
		t.Fatalf("revoke changed the replication HWM to %d, want %d preserved — a "+
			"re-enrolled child re-offering from LWM would re-apply records", got, hwmBefore)
	}
}

// appendCommands puts n records on the commands stream so NextOffset moves —
// the head this test's claim is about. Contents are irrelevant: the delivery
// floor is a position, not a payload.
func appendCommands(t *testing.T, st *store.Store, n int) uint64 {
	t.Helper()
	recs := make([]store.Record, n)
	for i := range recs {
		recs[i] = store.Record{
			Topic:   "colca/v1/_CmdParam/m1/z/child/m1/c",
			Payload: []byte(`{"correlation_id":"c","expires_at":1}`),
			TS:      1,
		}
	}
	if _, _, err := st.Append("commands", recs); err != nil {
		t.Fatal(err)
	}
	return st.NextOffset("commands")
}

// Parent-scoped-cursors design §3.2, the parent-side mirror: a freshly
// enrolled repl child starts at the parent's CURRENT commands head, never at
// the stream's beginning. Commands issued before it was enrolled were
// addressed to whatever occupied its mount then.
//
// This is also the regression §3.4 shipped, and the seat is its repair. That
// section deletes the child's parent-side cursors on Revoke, justified by
// "they protect retention only — delivery position rides the child's own
// `after`". The cursor is ALSO the move-drain completion predicate's floor
// (repl.drainPendingCommands), so without the seat a re-enrolled ULID gets
// CursorGet's default of 1 back, every command already delivered under its
// mount is scanned as pending again, and the next drain of that child cannot
// complete until the OLDEST of them expires — effectively never, for the
// generous TTLs a delivery test uses. Found at level 4, by a reparent
// scenario whose second test then timed out waiting for a drain outcome it
// could never get.
func TestEnrollSeatsTheDownlinkFloorAtTheCommandsHead(t *testing.T) {
	st := openStore(t, t.TempDir())
	m, _ := newManager(t, st, "z/child")
	ulid := "01NCHILD"
	cursor := uns.DownlinkCursorPrefix + ulid

	// Commands that predate the child. Head > 1 is what makes every
	// assertion below distinguishable from CursorGet's never-acked default.
	head := appendCommands(t, st, 3)
	if head <= 1 {
		t.Fatalf("seeded commands head = %d, want > 1 — the test could not tell a seat from the default", head)
	}

	if _, _, err := m.Enroll(entryJSON(t, node(ulid, "z/child", pub("ab")))); err != nil {
		t.Fatal(err)
	}
	if got := st.CursorGet(cursor, "commands"); got != head {
		t.Fatalf("first enroll seated the delivery floor at %d, want the head %d — a new "+
			"child must not be charged with commands issued before it existed", got, head)
	}

	// It polls, and the floor follows: this is the position the drain
	// predicate treats as "everything below is delivered".
	delivered := appendCommands(t, st, 2)
	if !st.CursorAck(cursor, "commands", delivered) {
		t.Fatalf("ack to %d did not move the floor — the rest of this test would prove nothing", delivered)
	}

	if _, _, err := m.Revoke(ulid); err != nil {
		t.Fatal(err)
	}
	if got := st.CursorGet(cursor, "commands"); got != 1 {
		t.Fatalf("revoke left the floor at %d, want it deleted (CursorGet's default 1) — §3.4's own claim", got)
	}

	// The repair. Without the seat this answers 1, and every command in
	// [1, headAfter) — including the ones acked above — is pending again.
	headAfter := appendCommands(t, st, 4)
	if _, _, err := m.Enroll(entryJSON(t, node(ulid, "z/child", pub("ab")))); err != nil {
		t.Fatal(err)
	}
	if got := st.CursorGet(cursor, "commands"); got != headAfter {
		t.Fatalf("re-enroll left the delivery floor at %d, want the current head %d — at 1 the "+
			"move-drain predicate re-counts every already-delivered command as pending and the "+
			"next drain of this child can only resolve by expiry", got, headAfter)
	}
}

// The seat is gated on the replication DOOR, not on kind: a machine and a
// local service have no downlink at all, so seating one would put back the
// per-device key accumulation §3.4 removed — one dead cursor per enrolled
// identity, forever.
func TestEnrollSeatsNoDownlinkFloorForIdentitiesWithoutAReplDoor(t *testing.T) {
	st := openStore(t, t.TempDir())
	m, _ := newManager(t, st, "z/a")
	if head := appendCommands(t, st, 3); head <= 1 {
		t.Fatalf("seeded commands head = %d, want > 1", head)
	}

	if _, _, err := m.Enroll(entryJSON(t, machine("01NMACHINE", "z/a", pub("cd")))); err != nil {
		t.Fatal(err)
	}
	if _, _, err := m.Enroll([]byte(`{"ulid":"01NLOCAL","kind":"local","name":"conn"}`)); err != nil {
		t.Fatal(err)
	}

	for _, c := range st.Cursors() {
		if strings.HasPrefix(c.Name, uns.DownlinkCursorPrefix) {
			t.Fatalf("enroll seated a delivery floor %q for an identity that has no replication "+
				"door — these accumulate one per device forever", c.Name)
		}
	}
}

// The seat is also skipped when the commands stream is EMPTY. CursorGet
// already answers 1 for an absent key, so a cursor written at 1 decides
// nothing — but it is a live retention floor pinning the stream at its first
// record on behalf of a child that may never connect, which is exactly the
// accumulation §3.4 set out to remove. The downlink door pins the same rule
// from its own side (repl: "a poll at position 1 must not create a cursor");
// this is that claim at the enrollment end.
func TestEnrollSeatsNoDeliveryFloorWhenThereIsNothingOlderToDecline(t *testing.T) {
	st := openStore(t, t.TempDir())
	m, _ := newManager(t, st, "z/child")
	if head := st.NextOffset("commands"); head != 1 {
		t.Fatalf("fresh commands head = %d, want 1 — this test needs an empty stream", head)
	}

	if _, _, err := m.Enroll(entryJSON(t, node("01NCHILD", "z/child", pub("ab")))); err != nil {
		t.Fatal(err)
	}

	for _, c := range st.Cursors() {
		if strings.HasPrefix(c.Name, uns.DownlinkCursorPrefix) {
			t.Fatalf("enroll wrote cursor %q against an empty commands stream — it decides no "+
				"delivery (CursorGet defaults to 1 anyway) and holds retention at offset 1", c.Name)
		}
	}
}

// An Enroll against a LIVE child is an edit (the manager is idempotent and
// doubles as update), and must never move a floor that child is using: the
// seat claims the position only when there is none, so an edit mid-flight
// cannot skip commands the child has not fetched yet.
func TestReEnrollingALiveChildDoesNotMoveItsDeliveryFloor(t *testing.T) {
	st := openStore(t, t.TempDir())
	m, _ := newManager(t, st, "z/child")
	ulid := "01NCHILD"
	cursor := uns.DownlinkCursorPrefix + ulid

	appendCommands(t, st, 2)
	if _, _, err := m.Enroll(entryJSON(t, node(ulid, "z/child", pub("ab")))); err != nil {
		t.Fatal(err)
	}
	floor := st.CursorGet(cursor, "commands")

	// Commands arrive that this child has NOT fetched, then its entry is
	// edited. Its floor must still point at them.
	appendCommands(t, st, 5)
	if _, _, err := m.Enroll(entryJSON(t, node(ulid, "z/child", pub("ab")))); err != nil {
		t.Fatal(err)
	}
	if got := st.CursorGet(cursor, "commands"); got != floor {
		t.Fatalf("re-enrolling a live child moved its delivery floor from %d to %d — the "+
			"commands in between would be treated as delivered without ever being fetched",
			floor, got)
	}
}
