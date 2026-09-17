package node

import (
	"encoding/json"
	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/store"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
)

func TestStandaloneRetiresParentTrustBeforeServingAndSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	cfg := &config.Config{ULID: "n-machine", DataDir: filepath.Join(dir, "db"), KeyFile: filepath.Join(dir, "key.pem"), API: config.API{Addr: "127.0.0.1:0", LocalAddr: "127.0.0.1:0", Token: tok}}
	n := mustStart(t, cfg)
	// Pending upload plus an ordinary local consumer, not an artificial empty DB.
	if _, _, err := n.Store.Append("metrics", []store.Record{{Topic: "colca/v1/_Metric/n-machine/temp", Payload: []byte(`{"value":21}`)}}); err != nil {
		t.Fatal(err)
	}
	if _, err := n.Store.CursorSetIfAbsent("up:old-parent", "metrics", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := n.Store.CursorSetIfAbsent("c/local/history", "metrics", 1); err != nil {
		t.Fatal(err)
	}
	if _, _, err := n.Store.Append("commands", []store.Record{
		{Topic: "colca/v1/_CmdParam/n-machine/old", Payload: []byte(`{"correlation_id":"old","expires_at":9999999999999}`)},
		{Topic: "colca/v1/_Ack/n-machine/old", Payload: []byte(`{"correlation_id":"old","result_code":200}`)},
	}); err != nil {
		t.Fatal(err)
	}
	n.Stop()
	// Restore a learned position as a managed child would retain it offline.
	st, err := store.Open(cfg.DataDir)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.AncestryPut([]byte(`[{"element":"fleet-machine","name":"machine"}]`)); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	cfg.Standalone = true
	n = mustStart(t, cfg)
	if cfg.API.Token != "" {
		t.Fatal("old fleet admin token survived")
	}
	if n.Store.NextOffset("metrics") != 2 {
		t.Fatal("handover removed process history")
	}
	for _, cur := range n.Store.Cursors() {
		if strings.HasPrefix(cur.Name, "up:") {
			t.Fatal("parent retention cursor survived")
		}
	}
	state, err := n.Store.StandaloneGet()
	if err != nil || state == nil || state.Ready {
		t.Fatalf("handover not fenced: %+v %v", state, err)
	}
	if len(state.FormerAncestors) != 1 || state.FormerAncestors[0] != "fleet-machine" {
		t.Fatal("lost old grant scope")
	}
	if state.CommandsBefore != 3 {
		t.Fatalf("command retirement boundary = %d, want 3", state.CommandsBefore)
	}
	if _, _, err := n.Store.Append("commands", []store.Record{
		{Topic: "colca/v1/_CmdParam/n-machine/new", Payload: []byte(`{"correlation_id":"new","expires_at":9999999999999}`)},
	}); err != nil {
		t.Fatal(err)
	}
	request := func(method, path string) map[string]any {
		t.Helper()
		req, _ := http.NewRequest(method, "http://"+n.LocalAPIAddr+path, nil)
		req.Header.Set("X-Colca-Service", "fleet-identity")
		response, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusOK {
			t.Fatalf("handover route: %d", response.StatusCode)
		}
		var body map[string]any
		if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		return body
	}
	if request("GET", "/self")["standalone_ready"] != false {
		t.Fatal("premature readiness")
	}
	completed := request("POST", "/standalone/complete")
	if completed["standalone_ready"] != true {
		t.Fatal("completion not recorded")
	}
	cutoff := completed["standalone_since"]
	checkCommands := func() {
		t.Helper()
		rows := request("GET", "/fetch?stream=commands&cursor=c/fleet-identity/handover")["records"].([]any)
		if len(rows) != 2 || rows[0].(map[string]any)["offset"] != float64(2) || rows[1].(map[string]any)["offset"] != float64(3) {
			t.Fatalf("old command escaped retirement or receipt/new command lost: %v", rows)
		}
		if n.Store.NextOffset("commands") != 4 {
			t.Fatal("handover deleted stored command history")
		}
	}
	checkCommands()
	n.Stop()
	n = mustStart(t, cfg)
	if request("POST", "/standalone/complete")["standalone_since"] != cutoff {
		t.Fatal("retry invalidated new owner's sessions")
	}
	checkCommands()
	n.Stop()
	cfg.Standalone = false
	if old, err := Start(cfg); err == nil {
		old.Stop()
		t.Fatal("config rollback restored fleet trust")
	}
	cfg.Standalone = true
	cfg.Parent = &config.Parent{URL: "https://old-parent", Pubkey: strings.Repeat("a", 64)}
	if err := cfg.Validate(); err == nil {
		t.Fatal("standalone accepted parent")
	}
}

func TestStandaloneUpgradesLegacyCommandBoundaryOnlyOnce(t *testing.T) {
	dir := t.TempDir()
	cfg := &config.Config{ULID: "n-machine", Standalone: true, DataDir: filepath.Join(dir, "db"),
		KeyFile: filepath.Join(dir, "key.pem"), API: config.API{LocalAddr: "127.0.0.1:0"}}
	st, err := store.Open(cfg.DataDir)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.StandalonePut(&store.StandaloneState{Since: 123, PATs: map[string]bool{}, Ready: true}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := st.Append("commands", []store.Record{{Topic: "colca/v1/_CmdParam/n-machine/old"}}); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	n := mustStart(t, cfg)
	state, err := n.Store.StandaloneGet()
	if err != nil || state.CommandsBefore != 2 || state.Since != 123 || !state.Ready {
		t.Fatalf("legacy transition was not upgraded safely: %+v, %v", state, err)
	}
	if _, _, err := n.Store.Append("commands", []store.Record{{Topic: "colca/v1/_CmdParam/n-machine/new"}}); err != nil {
		t.Fatal(err)
	}
	n.Stop()
	n = mustStart(t, cfg)
	defer n.Stop()
	state, err = n.Store.StandaloneGet()
	if err != nil || state.CommandsBefore != 2 {
		t.Fatalf("restart retired the new owner's commands: %+v, %v", state, err)
	}
}
