package registry

import (
	"errors"
	"sort"
	"strings"
	"testing"

	"github.com/alpamayo-solutions/colca/internal/store"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// observingNS is ns plus the Observe hook the engine's element index has, so a
// test sees whether retired elements leave the namespace.
type observingNS struct{ ns }

func (o observingNS) Observe(contract, topic string, payload []byte) {
	if contract != "_SystemElement" || len(payload) != 0 {
		return
	}
	p, err := uns.Parse(topic)
	if err != nil {
		return
	}
	for id, path := range o.ns {
		if path == p.Path {
			delete(o.ns, id)
		}
	}
}

// replicated applies one record a child sent, projected into KV the way the
// repl server projects owned state, and returns its topic.
func replicated(t *testing.T, st *store.Store, child, stream string, off uint64, contract, node, path string) string {
	t.Helper()
	topic := "colca/v1/" + contract + "/" + node + "/" + path
	if _, _, err := st.ApplyReplicated(child, stream, []store.ReplRecord{{
		ChildOffset: off, Topic: topic, Payload: []byte(`{"id":"x"}`), TS: 1,
		KVPath: path, KVNode: node,
	}}); err != nil {
		t.Fatal(err)
	}
	return topic
}

func heldTopics(t *testing.T, st *store.Store) []string {
	t.Helper()
	var out []string
	for _, kv := range mustKVScan(t, st, "") {
		out = append(out, kv.Topic)
	}
	sort.Strings(out)
	return out
}

func tombstonesFrom(t *testing.T, st *store.Store, stream string, from uint64) map[string]bool {
	t.Helper()
	recs, _, err := st.Read(stream, from, 1000, nil)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]bool{}
	for _, r := range recs {
		if len(r.Payload) == 0 {
			out[r.Topic] = true
		}
	}
	return out
}

// A node replaced at the same mount left its machines and signals standing on
// the parent, and consumers read them as live: a revoked edge's stale
// machine_state hid the new edge's value. Retiring the child takes everything it
// and the nodes below it replicated up, at its mount and wherever it stood
// before, and leaves this node's own records, a child nested below its mount and
// every other child alone.
func TestRetireTombstonesEverythingTheChildAndItsSubtreeReplicated(t *testing.T) {
	st := openStore(t, t.TempDir())
	m, err := New(st, "01NODE")
	if err != nil {
		t.Fatal(err)
	}
	space := observingNS{ns{}}
	for _, p := range []string{"site/edge1", "site/edge1/cell/edge3", "site/edge2", "site/edge1/press3"} {
		space.place(p)
	}
	m.SetNamespace(space)
	for _, e := range []uns.Entry{
		node("01NCHILD", "site/edge1", pub("ab")),
		node("01NNESTED", "site/edge1/cell/edge3", pub("cd")),
		node("01NOTHER", "site/edge2", pub("ef")),
	} {
		if _, _, err := m.Enroll(entryJSON(t, e)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := st.AdoptChildStore("01NCHILD", "child-store"); err != nil {
		t.Fatal(err)
	}

	gone := []string{
		replicated(t, st, "01NCHILD", "entities", 1, "_SystemElement", "01NCHILD", "site/edge1/press3"),
		replicated(t, st, "01NCHILD", "entities", 2, "_ServiceDetails", "01NCHILD", "site/edge1/_service"),
		replicated(t, st, "01NCHILD", "entities", 3, "_Signal", "01NGRAND", "site/edge1/gc/press9/state"),
		// Where the child stood before a remount: nothing else can retire these.
		replicated(t, st, "01NCHILD", "entities", 4, "_Signal", "01NCHILD", "old/edge1/press3/state"),
	}
	metric := replicated(t, st, "01NCHILD", "metrics", 7, "_Metric", "01NCHILD", "site/edge1/press3/state")
	gone = append(gone, metric)
	nested := replicated(t, st, "01NNESTED", "entities", 1, "_Signal", "01NNESTED", "site/edge1/cell/edge3/x")
	other := replicated(t, st, "01NOTHER", "entities", 1, "_Signal", "01NOTHER", "site/edge2/y")
	own := "colca/v1/_SystemElement/01NODE/site/edge1/local"
	if _, _, err := st.Append("entities", []store.Record{{
		Topic: own, Payload: []byte(`{"id":"el-local"}`), TS: 1, KVPath: "site/edge1/local", KVNode: "01NODE",
	}}); err != nil {
		t.Fatal(err)
	}
	var cleared []string
	m.SetDeliver(func(topic string, payload []byte, _ bool) {
		m.Get("01NCHILD") // re-enter as the broker does; deadlocks under the lock
		if len(payload) == 0 {
			cleared = append(cleared, topic)
		}
	})

	entitiesHead, metricsHead := st.NextOffset("entities"), st.NextOffset("metrics")
	off, _, retired, err := m.Retire("01NCHILD")
	if err != nil {
		t.Fatalf("Retire: %v", err)
	}
	if off != entitiesHead {
		t.Fatalf("offset = %d, want the identity tombstone at %d", off, entitiesHead)
	}
	if retired != len(gone) {
		t.Fatalf("retired = %d, want %d", retired, len(gone))
	}

	want := []string{
		"colca/v1/_EnrolledIdentity/01NODE/_colca/identities/01NNESTED",
		"colca/v1/_EnrolledIdentity/01NODE/_colca/identities/01NOTHER",
		nested, other, own,
	}
	sort.Strings(want)
	if got := heldTopics(t, st); strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("after the retire the node holds\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}

	// Each tombstone rides the stream its contract rises on, so the uplink
	// carries it and every ancestor retires its copy.
	ents, mets := tombstonesFrom(t, st, "entities", entitiesHead), tombstonesFrom(t, st, "metrics", metricsHead)
	for _, topic := range gone {
		in := ents
		if topic == metric {
			in = mets
		}
		if !in[topic] {
			t.Errorf("no tombstone for %s on its stream", topic)
		}
	}
	if ents[metric] || mets[gone[0]] {
		t.Error("a tombstone landed on the wrong stream; the parent above would refuse the batch")
	}
	for _, topic := range gone {
		found := false
		for _, c := range cleared {
			found = found || c == topic
		}
		if !found {
			t.Errorf("retained copy of %s not cleared on the local bus", topic)
		}
	}

	for _, stream := range []string{"entities", "metrics"} {
		if got := st.HWMGet("01NCHILD", stream); got != 0 {
			t.Errorf("HWM(01NCHILD, %s) = %d, want 0", stream, got)
		}
	}
	if st.HWMGet("01NNESTED", "entities") != 1 || st.HWMGet("01NOTHER", "entities") != 1 {
		t.Error("the retire cleared another child's marks")
	}
	if reset, err := st.AdoptChildStore("01NCHILD", "next-store"); err != nil || reset {
		t.Fatalf("a store seen after the retire: reset=%v err=%v, want a first sighting", reset, err)
	}
	if _, ok := space.PathOf(elementAt("site/edge1/press3")); ok {
		t.Error("the retired element still resolves in the namespace")
	}
	if _, ok := m.Get("01NCHILD"); ok {
		t.Error("the retired identity is still enrolled")
	}
}

// Retirement is defined over what a child node replicated. Other kinds author
// here, so the request is refused and nothing changes.
func TestRetireRefusesAnIdentityThatIsNotANode(t *testing.T) {
	st := openStore(t, t.TempDir())
	m, _ := newManager(t, st, "z/a")
	if _, _, err := m.Enroll(entryJSON(t, machine("01M1", "z/a", pub("ab")))); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := m.Retire("01M1"); !errors.Is(err, ErrNotChildNode) {
		t.Fatalf("Retire of a machine: err=%v, want ErrNotChildNode", err)
	}
	if _, ok := m.Get("01M1"); !ok {
		t.Fatal("a refused retire revoked the machine")
	}
	if _, _, _, err := m.Retire("01NOPE"); !errors.Is(err, ErrNotEnrolled) {
		t.Fatalf("Retire of an unknown identity: err=%v, want ErrNotEnrolled", err)
	}
}

// Revoke stays the kill switch: a revoked child may be enrolled again and
// resume from its own uplink cursor, so its replicated state, marks and
// incarnation all stay.
func TestRevokeWithoutRetireKeepsTheChildsReplicatedState(t *testing.T) {
	st := openStore(t, t.TempDir())
	m, _ := newManager(t, st, "site/edge1")
	if _, _, err := m.Enroll(entryJSON(t, node("01NCHILD", "site/edge1", pub("ab")))); err != nil {
		t.Fatal(err)
	}
	if _, err := st.AdoptChildStore("01NCHILD", "child-store"); err != nil {
		t.Fatal(err)
	}
	signal := replicated(t, st, "01NCHILD", "entities", 3, "_Signal", "01NCHILD", "site/edge1/press3/state")

	if _, _, err := m.Revoke("01NCHILD"); err != nil {
		t.Fatal(err)
	}

	if got := heldTopics(t, st); len(got) != 1 || got[0] != signal {
		t.Fatalf("after the revoke the node holds %v, want %s kept", got, signal)
	}
	if got := st.HWMGet("01NCHILD", "entities"); got != 3 {
		t.Fatalf("HWM = %d, want 3 kept", got)
	}
	if reset, err := st.AdoptChildStore("01NCHILD", "other-store"); err != nil || !reset {
		t.Fatalf("reset=%v err=%v: the incarnation must survive a revoke so a rebuilt store is still recognised", reset, err)
	}
}
