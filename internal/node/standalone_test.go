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
	n.Stop()
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
	n.Stop()
	n = mustStart(t, cfg)
	if request("POST", "/standalone/complete")["standalone_since"] != cutoff {
		t.Fatal("retry invalidated new owner's sessions")
	}
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
