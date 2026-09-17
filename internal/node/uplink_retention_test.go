package node

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/alpamayo-solutions/colca/internal/authtest"
	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/store"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// A first upload may be weeks after installation. Start must establish durable
// protection before returning, even with an unreachable parent; a real restart
// must keep it, and a later parent must receive the original samples in order.
func TestNeverConnectedBacklogSurvivesPruningRestartAndFirstUpload(t *testing.T) {
	for _, days := range []int{7, 14, 30, 40} {
		t.Run(fmt.Sprintf("%dd", days), func(t *testing.T) {
			testOfflineBacklogRecovery(t, days)
		})
	}
}

func testOfflineBacklogRecovery(t *testing.T, days int) {
	const count = 650 // More than three uplink pages; catch-up must persist progress.
	dir := t.TempDir()
	parentKey := filepath.Join(dir, "parent.key")
	childKey := filepath.Join(dir, "child.key")
	parentID := genKey(t, parentKey)
	childID := genKey(t, childKey)
	noPruning := config.Duration(0)
	pcfg := &config.Config{
		ULID: "n-parent", DataDir: filepath.Join(dir, "parent"), KeyFile: parentKey,
		Repl:      config.Endpoint{Addr: "127.0.0.1:0"},
		Retention: config.Retention{Interval: &noPruning},
	}
	parent := mustStart(t, pcfg)
	authtest.EnrollNodeAt(t, parent.Registry, parent.Engine, "n-child", childID.PublicHex(), "machine")
	offlineURL := "https://" + parent.ReplAddr
	parent.Stop()

	dataDir := filepath.Join(dir, "child")
	st, err := store.Open(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-time.Duration(days) * 24 * time.Hour).UnixMilli()
	var samples []store.Record
	for i := range count {
		samples = append(samples, store.Record{
			Topic:   "colca/v1/_Metric/n-child/filler/temperature",
			Payload: []byte(fmt.Sprintf(`{"v":%d}`, i)), TS: old + int64(i),
		})
	}
	if _, _, err := st.Append("metrics", samples); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	interval := config.Duration(5 * time.Millisecond)
	cfg := &config.Config{
		ULID: "n-child", DataDir: dataDir, KeyFile: childKey,
		Parent: &config.Parent{URL: offlineURL, Pubkey: parentID.PublicHex()},
		Retention: config.Retention{
			Interval: &interval,
			Streams:  map[string]config.StreamRetention{"metrics": {MaxBytes: 1}},
		},
	}
	cursor := uns.UplinkCursor(parentID.PublicHex())
	for boot := range 2 {
		child := mustStart(t, cfg)
		protected, _ := child.Store.ProtectedCursors("metrics", time.Now(), 0)
		if len(protected) != 1 || protected[0].Name != cursor || protected[0].Position != 1 {
			t.Fatalf("boot %d: startup left never-uploaded history unprotected: %+v", boot, protected)
		}
		// Exercise the actual deletion boundary as well as the running background
		// pruner. A cursor at 1 must clamp an otherwise legal whole-prefix prune.
		if removed, err := child.Store.Prune("metrics", count+1, nil, nil); err != nil || removed != 0 {
			t.Fatalf("boot %d: removed %d unforwarded samples: %v", boot, removed, err)
		}
		child.Stop()
	}

	parent = mustStart(t, pcfg)
	cfg.Parent.URL = "https://" + parent.ReplAddr
	child := mustStart(t, cfg)
	deadline := time.Now().Add(15 * time.Second)
	for parent.Store.NextOffset("metrics") != count+1 || child.Store.LWM("metrics") != count+1 {
		if time.Now().After(deadline) {
			t.Fatalf("catch-up/pruning did not finish: parent next=%d child cursor=%d LWM=%d",
				parent.Store.NextOffset("metrics"), child.Store.CursorGet(cursor, "metrics"), child.Store.LWM("metrics"))
		}
		time.Sleep(10 * time.Millisecond)
	}
	// Parent retention is disabled so the received aged samples remain readable.
	received, _, err := parent.Store.Read("metrics", 1, count+1, nil)
	if err != nil || len(received) != len(samples) {
		t.Fatalf("parent received %d samples, want %d: %v", len(received), len(samples), err)
	}
	for i, got := range received {
		if string(got.Payload) != string(samples[i].Payload) || got.TS != samples[i].TS ||
			got.Topic != "colca/v1/_Metric/n-child/machine/filler/temperature" {
			t.Fatalf("sample %d changed during offline recovery: %+v", i, got)
		}
	}
	child.Stop()
	child = mustStart(t, cfg)
	if got := child.Store.CursorGet(cursor, "metrics"); got != count+1 {
		t.Fatalf("acknowledged progress lost on restart: %d", got)
	}
}
