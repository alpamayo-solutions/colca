package repl

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/alpamayo-solutions/colca/internal/store"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

func TestPrepareUplinkProtectsEmptyStreamsAndPreservesProgress(t *testing.T) {
	dir := t.TempDir()
	id := mustIdentity(t, filepath.Join(dir, "child.key"))
	parent := mustIdentity(t, filepath.Join(dir, "parent.key"))
	cl := mustClient(t, "127.0.0.1:1", parent.PublicHex(), id)
	st := mustStore(t, filepath.Join(dir, "data"))
	cursor := uns.UplinkCursor(parent.PublicHex())

	if err := PrepareUplink(cl, st); err != nil {
		t.Fatal(err)
	}
	for _, stream := range uplinkStreams() {
		protected, _ := st.ProtectedCursors(stream, time.Now(), 0)
		if len(protected) != 1 || protected[0].Name != cursor || protected[0].Position != 1 {
			t.Fatalf("%s: empty stream has no persisted first-offset protection: %+v", stream, protected)
		}
		if _, _, err := st.Append(stream, []store.Record{{Topic: "test", Payload: []byte(`{}`)}}); err != nil {
			t.Fatal(err)
		}
		if removed, err := st.Prune(stream, 2, nil, nil); err != nil || removed != 0 {
			t.Fatalf("%s: pruning removed %d never-uploaded records: %v", stream, removed, err)
		}
	}

	// Initialization must not move an existing cursor or make an old stalled one
	// appear active again; the operator's explicit staleness policy still applies.
	st.CursorAck(cursor, "metrics", 2)
	before := map[string]int64{}
	for _, c := range st.Cursors() {
		before[c.Stream] = c.LastAdvanceMS
	}
	if err := PrepareUplink(cl, st); err != nil {
		t.Fatal(err)
	}
	for stream, want := range map[string]uint64{"metrics": 2, "entities": 1} {
		_, stale := st.ProtectedCursors(stream, time.Now().Add(40*24*time.Hour), 30*24*time.Hour)
		if len(stale) != 1 || stale[0].Position != want || stale[0].LastAdvanceMS != before[stream] {
			t.Fatalf("%s: reinitialization changed progress/staleness: %+v", stream, stale)
		}
	}
}

func TestPrepareUplinkAdoptsLegacyAndKeepsParentsIndependent(t *testing.T) {
	dir := t.TempDir()
	id := mustIdentity(t, filepath.Join(dir, "child.key"))
	parent := mustIdentity(t, filepath.Join(dir, "parent.key"))
	otherParent := mustIdentity(t, filepath.Join(dir, "other.key"))
	st := mustStore(t, filepath.Join(dir, "data"))
	seed(t, st, "metrics", "colca/v1/_Metric/n-child/temp%d", 5)
	st.CursorAck(legacyUplinkCursor, "metrics", 4)
	cl := mustClient(t, "127.0.0.1:1", parent.PublicHex(), id)
	if err := PrepareUplink(cl, st); err != nil {
		t.Fatal(err)
	}
	if got := st.CursorGet(uns.UplinkCursor(parent.PublicHex()), "metrics"); got != 4 {
		t.Fatalf("legacy progress lost: got %d, want 4", got)
	}
	for _, c := range st.Cursors() {
		if c.Name == legacyUplinkCursor {
			t.Fatalf("legacy cursor not retired after adoption: %+v", c)
		}
	}
	other := mustClient(t, "127.0.0.1:1", otherParent.PublicHex(), id)
	if err := PrepareUplink(other, st); err != nil {
		t.Fatal(err)
	}
	if got := st.CursorGet(uns.UplinkCursor(otherParent.PublicHex()), "metrics"); got != 1 {
		t.Fatalf("new parent inherited another parent's progress: %d", got)
	}
	if err := PrepareUplink(cl, st); err != nil {
		t.Fatal(err)
	}
	if got := st.CursorGet(uns.UplinkCursor(parent.PublicHex()), "metrics"); got != 4 {
		t.Fatalf("returning to parent rewound its cursor: %d", got)
	}
}
