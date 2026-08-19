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
	if e, ok := m.Get("01M1"); !ok || e.Element != elementAt("z/cnc5") {
		t.Fatalf("Get after enroll: %v %v", e, ok)
	}
	if e, ok := m.ByPubkey(pub("ab")); !ok || e.ULID != "01M1" {
		t.Fatalf("ByPubkey after enroll: %v %v", e, ok)
	}
	if mount, ok := m.MountOf("01M1"); !ok || mount != "z/cnc5" {
		t.Fatalf("MountOf: %q %v", mount, ok)
	}
	if got := m.List(); len(got) != 1 || got[0].ULID != "01M1" {
		t.Fatalf("List: %v", got)
	}

	// The entity record landed in the entities stream under the enrolled topic.
	recs, _, err := st.Read("entities", off, 1, nil)
	if err != nil || len(recs) != 1 || recs[0].Topic != "colca/v1/_EdgeNode/01M1/z/cnc5" {
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
	if kv := st.KVScan("z/a"); len(kv) != 0 {
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
// the retained _EdgeNode, revoke delivers an empty retained payload (the MQTT
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
	if seen[0].topic != "colca/v1/_EdgeNode/01M1/z/a" || !seen[0].retain || seen[0].payload == 0 {
		t.Fatalf("enroll delivery = %+v, want retained entity payload", seen[0])
	}
	if seen[1].topic != seen[0].topic || !seen[1].retain || seen[1].payload != 0 {
		t.Fatalf("revoke delivery = %+v, want empty retained clear on the same topic", seen[1])
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
		store.Record{Topic: "colca/v1/_EdgeNode/01BAD/x", Payload: []byte("{corrupt"), TS: 1}); err != nil {
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
	if mount, ok := m.MountOf("01M1"); !ok || mount != "z/a" {
		t.Fatalf("before the rename MountOf = %q %v, want z/a", mount, ok)
	}

	// The element is renamed in the namespace. Nothing touches the registry.
	elements[elementAt("z/a")] = "z/a-neu"

	if mount, ok := m.MountOf("01M1"); !ok || mount != "z/a-neu" {
		t.Fatalf("after the rename MountOf = %q %v, want z/a-neu", mount, ok)
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
	delete(elements, elementAt("z/a"))
	if mount, ok := m.MountOf("01M1"); ok {
		t.Fatalf("MountOf = %q %v, want a miss — the element is gone", mount, ok)
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
