package node

import (
	"log/slog"
	"path/filepath"
	"testing"

	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/store"
)

// A store written by a node that still knew application time holds retained
// _ClockDefinition and _ClockProgress state. The node removes it at startup,
// keeps everything else, writes no stream record for it, and finds nothing to
// remove on the next start.
func TestStartupPurgesRetainedStateOfRetiredContracts(t *testing.T) {
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "db")
	st, err := store.Open(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	kv := func(contract, path, payload string) store.Record {
		return store.Record{Topic: "colca/v1/" + contract + "/n-old/" + path, Payload: []byte(payload), TS: 1, KVPath: path, KVNode: "n-old"}
	}
	if _, _, err := st.Append("definitions", []store.Record{kv("_ClockDefinition", "factory", `{"id":"factory"}`)}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := st.Append("metrics", []store.Record{kv("_ClockProgress", "site/historian", `{"run_id":"r"}`)}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := st.Append("entities", []store.Record{kv("_Signal", "site/m1/temp", `{"id":"temp"}`)}); err != nil {
		t.Fatal(err)
	}
	heads := map[string]uint64{}
	// The streams the retired records live in; the node writes its own _Node
	// entity at startup.
	for _, stream := range []string{"definitions", "metrics"} {
		heads[stream] = st.NextOffset(stream)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	cfg := &config.Config{ULID: "n-old", DataDir: dataDir, KeyFile: filepath.Join(dir, "key.pem"),
		API: config.API{Addr: "127.0.0.1:0", LocalAddr: "127.0.0.1:0", Token: tok}}
	n := mustStart(t, cfg)
	entries, err := n.Store.KVScan("")
	if err != nil {
		t.Fatal(err)
	}
	signal := false
	for _, e := range entries {
		switch e.Topic {
		case "colca/v1/_ClockDefinition/n-old/factory", "colca/v1/_ClockProgress/n-old/site/historian":
			t.Errorf("retained %s survived startup", e.Topic)
		case "colca/v1/_Signal/n-old/site/m1/temp":
			signal = true
		}
	}
	if !signal {
		t.Fatal("startup removed the _Signal too")
	}
	for stream, head := range heads {
		if got := n.Store.NextOffset(stream); got != head {
			t.Errorf("%s head moved from %d to %d: the purge must not write stream records", stream, head, got)
		}
	}
	if removed, err := purgeRetiredState(n.Store, slog.Default()); err != nil || removed != 0 {
		t.Fatalf("second purge removed %d (%v), want 0", removed, err)
	}
}
