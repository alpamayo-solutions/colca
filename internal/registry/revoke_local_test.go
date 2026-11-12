package registry

import (
	"sort"
	"testing"

	"github.com/alpamayo-solutions/colca/internal/store"
)

// seedWritten writes a retained record attributed to writtenBy, as the engine
// stores what an identity publishes.
func seedWritten(t *testing.T, st *store.Store, contract, path, writtenBy string) string {
	t.Helper()
	topic := "colca/v1/" + contract + "/01NODE/" + path
	stream := "entities"
	if contract == "_Metric" {
		stream = "metrics"
	}
	if _, _, err := st.Append(stream, []store.Record{{
		Topic: topic, Payload: []byte(`{"v":1}`), TS: 1, WrittenBy: writtenBy,
		KVPath: path, KVNode: "01NODE",
	}}); err != nil {
		t.Fatal(err)
	}
	return topic
}

func heldTopicSet(t *testing.T, st *store.Store) map[string]bool {
	t.Helper()
	out := map[string]bool{}
	for _, kv := range mustKVScan(t, st, "") {
		out[kv.Topic] = true
	}
	return out
}

// A local service registers again by name under a new identity. A revoke that
// left its catalogue, findings and command cursor behind would show a removed
// connector's tags and problems as live and hand its command position to a
// successor of the same name. Its values and other cursors stay.
func TestRevokeOfALocalServiceRetiresItsCataloguesFindingsAndCommandCursors(t *testing.T) {
	st := openStore(t, t.TempDir())
	m, _ := newManager(t, st, "line1", "line2")
	mustEnroll(t, m, `{"ulid":"01JCONN","kind":"local","name":"conn","element":"`+elementAt("line1")+`"}`)
	mustEnroll(t, m, `{"ulid":"01JOTHER","kind":"local","name":"other","element":"`+elementAt("line2")+`"}`)
	var observed []string
	m.SetObserver(func(contract, topic string, payload []byte) {
		if payload == nil {
			observed = append(observed, topic)
		}
	})

	catalogue := seedWritten(t, st, "_DataTags", "line1/conn", "01JCONN")
	oldCatalogue := seedWritten(t, st, "_DataTags", "line0/conn", "01JCONN")
	finding := seedWritten(t, st, "_Finding", "line1/conn/unreachable", "01JCONN")
	nodeFinding := seedWritten(t, st, "_Finding", "line1/conn/cursor_lag", "01NODE")
	value := seedWritten(t, st, "_Metric", "line1/motor/current", "01JCONN")
	neighbour := seedWritten(t, st, "_DataTags", "line2/other", "01JOTHER")
	for _, c := range []struct{ name, stream string }{
		{"c/conn/commands", "commands"}, {"c/conn/x/commands", "commands"},
		{"c/conn/tail", "metrics"}, {"c/other/commands", "commands"},
	} {
		if !st.CursorAck(c.name, c.stream, 2) {
			t.Fatalf("seed ack of %s did not move it", c.name)
		}
	}

	off, _, err := m.Revoke("01JCONN")
	if err != nil {
		t.Fatalf("Revoke: %v", err)
	}

	held := heldTopicSet(t, st)
	for _, gone := range []string{catalogue, oldCatalogue, finding} {
		if held[gone] {
			t.Errorf("%s survived the revoke of its author", gone)
		}
	}
	for _, kept := range []string{nodeFinding, value, neighbour} {
		if !held[kept] {
			t.Errorf("%s was retired, but the revoked service did not author it or it is a value", kept)
		}
	}
	recs, _, err := st.Read("entities", off, 20, nil)
	if err != nil {
		t.Fatal(err)
	}
	tombstones := map[string]bool{}
	for _, rec := range recs {
		if len(rec.Payload) == 0 {
			tombstones[rec.Topic] = true
		}
	}
	if !tombstones[catalogue] || !tombstones[oldCatalogue] || !tombstones[finding] {
		t.Fatalf("tombstones on the entities stream = %v; an ancestor's copies retire only by them", tombstones)
	}
	sort.Strings(observed)
	want := []string{oldCatalogue, finding, catalogue}
	sort.Strings(want)
	if len(observed) != len(want) {
		t.Fatalf("observed tombstones %v, want %v", observed, want)
	}
	for i := range want {
		if observed[i] != want[i] {
			t.Fatalf("observed tombstones %v, want %v", observed, want)
		}
	}

	left := map[string]bool{}
	for _, c := range st.Cursors() {
		left[c.Name+"@"+c.Stream] = true
	}
	for _, gone := range []string{"c/conn/commands@commands", "c/conn/x/commands@commands"} {
		if left[gone] {
			t.Errorf("command cursor %s survived the revoke", gone)
		}
	}
	for _, kept := range []string{"c/conn/tail@metrics", "c/other/commands@commands"} {
		if !left[kept] {
			t.Errorf("cursor %s was deleted; the revoke takes only the service's command cursors", kept)
		}
	}
}

// A keyed service keeps its identity across a revoke and a new enrollment, so
// its catalogue stays and keeps its tag ids.
func TestRevokeOfAKeyedServiceKeepsItsCatalogue(t *testing.T) {
	st := openStore(t, t.TempDir())
	m, _ := newManager(t, st, "line1")
	if _, _, err := m.Enroll(entryJSON(t, machine("01JKEYED", "line1", pub("ef")))); err != nil {
		t.Fatal(err)
	}
	catalogue := seedWritten(t, st, "_DataTags", "line1/01JKEYED", "01JKEYED")

	if _, _, err := m.Revoke("01JKEYED"); err != nil {
		t.Fatalf("Revoke: %v", err)
	}
	if !heldTopicSet(t, st)[catalogue] {
		t.Fatal("a keyed service's catalogue was retired by a revoke")
	}
}
