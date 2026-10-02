package registry

import (
	"encoding/json"
	"errors"
	"sort"
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

// mustKVScan is KVScan that fails the test on error.
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
	return uns.Entry{ULID: ulid, Pubkey: pub, Kind: uns.KindExternal, Element: elementAt(mount)}
}

func pub(seed string) string { return strings.Repeat(seed, 64/len(seed)) }

// elementAt is the element id tests place at a path; the shape only keeps
// failures readable.
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

// place puts an element at path or moves it there, which is how a rename reaches
// the registry.
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

// newTestManager builds a manager whose namespace resolves el-press3, the element
// the local entries in this file bind to.
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

func TestListPageUsesStableULIDOrdering(t *testing.T) {
	st := openStore(t, t.TempDir())
	m, _ := newManager(t, st, "a", "b", "c")
	for _, e := range []uns.Entry{
		machine("01M3", "c", pub("cd")),
		machine("01M1", "a", pub("ab")),
		machine("01M2", "b", pub("bc")),
	} {
		if _, _, err := m.Enroll(entryJSON(t, e)); err != nil {
			t.Fatal(err)
		}
	}

	first, next := m.ListPage("", 2)
	if len(first) != 2 || first[0].ULID != "01M1" || first[1].ULID != "01M2" || next != "01M2" {
		t.Fatalf("first page = %+v next=%q", first, next)
	}
	second, final := m.ListPage(next, 2)
	if len(second) != 1 || second[0].ULID != "01M3" || final != "" {
		t.Fatalf("second page = %+v next=%q", second, final)
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
		{"element named by a path", uns.Entry{ULID: "01M4", Pubkey: pub("12"), Kind: uns.KindExternal, Element: "z/c"}},
		{"element not placed at this node", uns.Entry{ULID: "01M5", Pubkey: pub("34"), Kind: uns.KindExternal, Element: "el-nowhere"}},
		// Every machine must be placed.
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
	// The old pubkey no longer authenticates.
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

// Enroll publishes the retained _EnrolledIdentity and revoke clears it, both
// after the map swap and outside the lock.
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
		// Re-enter the manager as the broker's ACL check does; this deadlocks if
		// callbacks run under the write lock.
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

// Drain persists and republishes the draining status but keeps the session,
// since the queue drains through it.
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

	// The persisted entry carries the status too, so it survives a restart.
	m2, _ := newManager(t, st, "z/child")
	e2, ok := m2.Get("01N1")
	if !ok || e2.Status != uns.StatusDraining {
		t.Fatalf("status did not survive a fresh load from the store: %+v %v", e2, ok)
	}
}

// Drain returns ErrNotEnrolled for an unknown ULID, ErrNotNode for a machine and
// ErrAlreadyDraining for a second call.
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

// DrainingMount respects the path-segment boundary and clears once the drained
// entry is gone.
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

// The element moves, the mount moves with it, and nothing is re-enrolled.
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

// An identity whose element no longer resolves has no mount and cannot write;
// the registry never guesses a place.
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
	// An empty element is not a position, so BoundTo answers nothing.
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

// Local entries stay keyed by ULID; the name is a second index, like the pubkey.
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

// Local entries have no pubkey, so the empty key must not count as a duplicate;
// otherwise the first local service would block every other one.
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

// Two local services cannot share a name. Both entries are unplaced, so element
// uniqueness cannot be what rejects the second one.
func TestTwoLocalServicesCannotShareAName(t *testing.T) {
	m := newTestManager(t)
	mustEnroll(t, m, `{"ulid":"01JA","kind":"local","name":"conn"}`)

	_, _, err := m.Enroll([]byte(`{"ulid":"01JB","kind":"local","name":"conn"}`))
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("second entry with the same name: err = %v; want ErrConflict", err)
	}
}

// A revoked entry's name stops resolving. Re-enrolling a new identity under the
// freed name proves the name index was cleaned up; ByName alone would miss
// through byID anyway.
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

// Revoke deletes a child's command cursor (Enroll re-seats it) but keeps the
// replication HWM, which dedupes a re-enrolled child re-offering records. The
// definitions cursor survives, see
// TestRevokeKeepsTheDefinitionsFloorSoARetractionSurvives.
func TestRevokeDeletesTheCommandFloorAndKeepsTheHWM(t *testing.T) {
	st := openStore(t, t.TempDir())
	m, _ := newManager(t, st, "z/a")
	ulid := "01NCHILD"
	if _, _, err := m.Enroll(entryJSON(t, machine(ulid, "z/a", pub("cd")))); err != nil {
		t.Fatal(err)
	}

	// Ack to 2: acking the default of 1 persists nothing.
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
		if c.Name == uns.DownlinkCursorPrefix+ulid {
			t.Fatalf("revoke left cursor %q behind — it accumulates per revoked device forever", c.Name)
		}
	}
	if got := st.CursorGet(uns.DownlinkDefCursorPrefix+ulid, "definitions"); got != 2 {
		t.Fatalf("revoke moved the definitions floor to %d, want the seeded 2 preserved — the "+
			"child still holds every definition it applied", got)
	}
	if got := st.HWMGet(ulid, "metrics"); got != hwmBefore {
		t.Fatalf("revoke changed the replication HWM to %d, want %d preserved — a "+
			"re-enrolled child re-offering from LWM would re-apply records", got, hwmBefore)
	}
}

// A retraction must reach a child that was revoked and enrolled again. The child
// keeps the definitions it applied and resumes from its own position, so the
// parent's definitions cursor must survive the revoke or compaction drops the
// retraction. The positive control at the end shows the floor is what holds the
// tombstone.
func TestRevokeKeepsTheDefinitionsFloorSoARetractionSurvives(t *testing.T) {
	st := openStore(t, t.TempDir())
	m, _ := newManager(t, st, "z/child")
	ulid := "01NCHILD"
	cursor := uns.DownlinkDefCursorPrefix + ulid
	group := "colca/v1/_Group/01NODE/01HGRP-OPS"

	if _, _, err := m.Enroll(entryJSON(t, node(ulid, "z/child", pub("ab")))); err != nil {
		t.Fatal(err)
	}
	// Offset 1: the group. The child reads it and acks to 2 (next unread).
	if _, _, err := st.Append("definitions", []store.Record{
		{Topic: group, Payload: []byte(`{"id":"01HGRP-OPS","grants":["read:el-z-child/#"]}`),
			TS: 1, KVPath: "01HGRP-OPS", KVNode: "01NODE"},
	}); err != nil {
		t.Fatal(err)
	}
	if !st.CursorAck(cursor, "definitions", 2) {
		t.Fatal("seed ack of the definitions cursor did not move it")
	}
	// Offset 2: the retraction the child has not read.
	if _, _, err := st.Append("definitions", []store.Record{
		{Topic: group, Payload: nil, TS: 2, KVPath: "01HGRP-OPS", KVNode: "01NODE", Delete: true},
	}); err != nil {
		t.Fatal(err)
	}

	if _, _, err := m.Revoke(ulid); err != nil {
		t.Fatal(err)
	}
	stats, err := st.Compact("definitions")
	if err != nil {
		t.Fatal(err)
	}
	if stats.Tombstones != 0 {
		t.Fatalf("compaction dropped %d tombstone(s) after the revoke — the child still holds "+
			"group 01HGRP-OPS and nothing will ever tell it the grant was withdrawn", stats.Tombstones)
	}

	// The child comes back at the same parent (a mount move, a key re-pin) and
	// resumes its own surviving position.
	if _, _, err := m.Enroll(entryJSON(t, node(ulid, "z/child", pub("ab")))); err != nil {
		t.Fatal(err)
	}
	recs, _, err := st.Read("definitions", 2, 10, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 1 || recs[0].Topic != group || len(recs[0].Payload) != 0 {
		t.Fatalf("re-enrolled child reading from 2 got %+v, want the retraction of %s", recs, group)
	}

	// Positive control: once the floor passes the tombstone, compaction removes it.
	if !st.CursorAck(cursor, "definitions", st.NextOffset("definitions")) {
		t.Fatal("ack past the tombstone did not move the cursor")
	}
	stats, err = st.Compact("definitions")
	if err != nil {
		t.Fatal(err)
	}
	if stats.Tombstones != 1 {
		t.Fatalf("compaction removed %d tombstone(s) once every cursor had passed it, want 1 — "+
			"the earlier assertion proved nothing about the floor", stats.Tombstones)
	}
}

// appendCommands appends n records to the commands stream to move its head.
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

// A newly enrolled repl child starts at the parent's current commands head.
// Without the seat, a re-enrolled child's drain would treat every delivered
// command as pending until the oldest one expires.
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
		t.Fatalf("revoke left the floor at %d, want it deleted (CursorGet's default 1)", got)
	}

	// Without the seat this is 1, and every command before headAfter is pending again.
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

// The seat depends on the repl door: machines and local services have no
// downlink, so they get no cursor.
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

// No seat while the commands stream is empty: a cursor at 1 decides nothing and
// would only pin retention.
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

// Re-enrolling a live child is an edit and must not move the floor it is using.
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

	// Commands the child has not fetched arrive, then its entry is edited; the floor
	// must still point at them.
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

// RoutesUnder differs from DrainingMount in two ways: it considers every child,
// and a draining child still counts as routable.
func TestRoutesUnderCoversEveryChildNodeAndOnlyChildNodes(t *testing.T) {
	st := openStore(t, t.TempDir())
	m, n := newManager(t, st, "site1/edge1", "site1/edge10", "hall/press3")
	if _, _, err := m.Enroll(entryJSON(t, node("01N1", "site1/edge1", pub("ab")))); err != nil {
		t.Fatal(err)
	}
	// A machine's mount does not make a command routable; machines get no downlink.
	if _, _, err := m.Enroll(entryJSON(t, machine("01M1", "hall/press3", pub("cd")))); err != nil {
		t.Fatal(err)
	}

	if !m.RoutesUnder("site1/edge1/press3/resource/upsert") {
		t.Fatal("a path under an enrolled child node's mount must be routable")
	}
	if m.RoutesUnder("site1/edge10/x") {
		t.Fatal("a sibling mount sharing only a string prefix must not match (path-separator boundary)")
	}
	if m.RoutesUnder("hall/press3/set-speed") {
		t.Fatal("a machine's mount must not make a command routable — machines are fed over the bus, not the downlink")
	}
	if m.RoutesUnder("elsewhere/x") {
		t.Fatal("a path under no enrolled mount at all must not be routable")
	}

	// A drain does not make a child unreachable: its queue is exactly what the
	// drain is waiting to deliver.
	if _, err := m.Drain("01N1"); err != nil {
		t.Fatal(err)
	}
	if !m.RoutesUnder("site1/edge1/press3/resource/upsert") {
		t.Fatal("a draining child is still routable — reporting otherwise would call the mechanism a fault")
	}

	// Routability follows the element when it is renamed.
	delete(n, elementAt("site1/edge1"))
	n["el-site1-edge1"] = "site2/edge1"
	m.SetNamespace(n)
	if m.RoutesUnder("site1/edge1/press3/resource/upsert") {
		t.Fatal("the old path must stop being routable once the element moved")
	}
}

// seedServiceRecord writes the retained _ServiceDetails a service publishes
// about itself, exactly as the engine projects it: the record on the entities
// stream and its KV entry under the topic's path.
func seedServiceRecord(t *testing.T, st *store.Store, nodeID, mount, ulid string) string {
	t.Helper()
	path := mount + "/_service"
	topic := "colca/v1/_ServiceDetails/" + nodeID + "/" + path
	if _, _, err := st.Append("entities", []store.Record{{
		Topic: topic, Payload: []byte(`{"id":"` + ulid + `","name":"svc"}`), TS: 1,
		KVPath: path, KVNode: nodeID,
	}}); err != nil {
		t.Fatal(err)
	}
	return topic
}

// serviceTopics lists the _ServiceDetails records the node still holds.
func serviceTopics(t *testing.T, st *store.Store) []string {
	t.Helper()
	var out []string
	for _, kv := range mustKVScan(t, st, "") {
		if strings.Contains(kv.Topic, "/_ServiceDetails/") {
			out = append(out, kv.Topic)
		}
	}
	sort.Strings(out)
	return out
}

// A _ServiceDetails record is observed state only its own service may write, so
// a revoke that leaves one behind leaves it forever: the admin door refuses the
// contract and the author is gone. Revoke therefore retires every record the
// identity authored, including the ones standing at mounts it has since left —
// a live node had three for one service, two under elements deleted since.
func TestRevokeRetiresEveryRecordTheIdentityAuthored(t *testing.T) {
	st := openStore(t, t.TempDir())
	m, _ := newManager(t, st, "events")
	mustEnroll(t, m, `{"ulid":"01JSVC","kind":"local","name":"tcdb-api","element":"el-events"}`)
	mustEnroll(t, m, `{"ulid":"01JOTHER","kind":"local","name":"other"}`)
	here := seedServiceRecord(t, st, "01NODE", "events/tcdb-api", "01JSVC")
	stale := seedServiceRecord(t, st, "01NODE", "traceability/events/tcdb-api", "01JSVC")
	neighbour := seedServiceRecord(t, st, "01NODE", "events/other", "01JOTHER")
	if got := serviceTopics(t, st); len(got) != 3 {
		t.Fatalf("seeded records = %v, want all three present before the revoke", got)
	}

	off, _, err := m.Revoke("01JSVC")
	if err != nil {
		t.Fatalf("Revoke: %v", err)
	}

	if got := serviceTopics(t, st); len(got) != 1 || got[0] != neighbour {
		t.Fatalf("after the revoke the node holds %v, want only %s — the revoked identity's "+
			"records at every mount it ever had must go with it", got, neighbour)
	}
	// The retirement travels: an ancestor holds its own copy and only a tombstone
	// on the entities stream retires it there.
	recs, _, err := st.Read("entities", off, 10, nil)
	if err != nil {
		t.Fatal(err)
	}
	retired := map[string]bool{}
	for _, rec := range recs {
		if len(rec.Payload) == 0 {
			retired[rec.Topic] = true
		}
	}
	if !retired[here] || !retired[stale] {
		t.Fatalf("tombstones appended from %d = %v, want empty-payload records at %s and %s",
			off, recs, here, stale)
	}
	if retired[neighbour] {
		t.Fatalf("the revoke retired %s, which another identity authored", neighbour)
	}
}

// Revoking an identity that published nothing is an ordinary operator act and
// must not be turned into a failure by having nothing to retire.
func TestRevokeOfAnIdentityThatAuthoredNothingSucceeds(t *testing.T) {
	st := openStore(t, t.TempDir())
	m, _ := newManager(t, st, "events")
	mustEnroll(t, m, `{"ulid":"01JSVC","kind":"local","name":"tcdb-api","element":"el-events"}`)

	if _, _, err := m.Revoke("01JSVC"); err != nil {
		t.Fatalf("Revoke of an identity that never registered: %v", err)
	}
	if _, ok := m.Get("01JSVC"); ok {
		t.Fatal("entry survived a revoke that had nothing to retire")
	}
}

// A record this node holds under another node's authorship is a child's own
// state, replicated up. The child owns it and retires it; revoking an identity
// here must not touch it, however the ulids happen to compare.
func TestRevokeLeavesRecordsAuthoredAtAnotherNodeAlone(t *testing.T) {
	st := openStore(t, t.TempDir())
	m, _ := newManager(t, st, "site1/edge1")
	if _, _, err := m.Enroll(entryJSON(t, node("01NCHILD", "site1/edge1", pub("ab")))); err != nil {
		t.Fatal(err)
	}
	belowChild := seedServiceRecord(t, st, "01NCHILD", "press3/conn", "01NCHILD")

	if _, _, err := m.Revoke("01NCHILD"); err != nil {
		t.Fatalf("Revoke: %v", err)
	}

	if got := serviceTopics(t, st); len(got) != 1 || got[0] != belowChild {
		t.Fatalf("after revoking the child the node holds %v, want its replicated record %s "+
			"untouched — this node does not author or retire it", got, belowChild)
	}
}
