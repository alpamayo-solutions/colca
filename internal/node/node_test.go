package node

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"

	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/identity"
)

const tok = "test-admin-token"

func genKey(t *testing.T, path string) *identity.Identity {
	t.Helper()
	id, err := identity.Generate(path)
	if err != nil {
		t.Fatalf("identity.Generate(%s): %v", path, err)
	}
	return id
}

// mustStart validates the config the way config.Load would and starts the node.
func mustStart(t *testing.T, cfg *config.Config) *Node {
	t.Helper()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("config %s invalid: %v", cfg.ULID, err)
	}
	n, err := Start(cfg)
	if err != nil {
		t.Fatalf("Start(%s): %v", cfg.ULID, err)
	}
	t.Cleanup(n.Stop) // Stop must tolerate being called after an explicit Stop
	return n
}

// apiCall performs an authenticated request against a node's HTTP API.
func apiCall(t *testing.T, n *Node, method, path string, body any) (int, map[string]any) {
	t.Helper()
	var rd io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal body: %v", err)
		}
		rd = strings.NewReader(string(raw))
	}
	req, err := http.NewRequest(method, "http://"+n.APIAddr+path, rd)
	if err != nil {
		t.Fatalf("new request %s %s: %v", method, path, err)
	}
	req.Header.Set("X-Colca-Token", tok)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("%s %s: decode body: %v", method, path, err)
	}
	return resp.StatusCode, out
}

func nextOffset(t *testing.T, n *Node, stream string) float64 {
	t.Helper()
	code, out := apiCall(t, n, http.MethodGet, "/debug/state", nil)
	if code != http.StatusOK {
		t.Fatalf("/debug/state: status %d: %v", code, out)
	}
	streams, ok := out["streams"].(map[string]any)
	if !ok {
		t.Fatalf("/debug/state: no streams object: %v", out)
	}
	st, ok := streams[stream].(map[string]any)
	if !ok {
		t.Fatalf("/debug/state: no %s stream: %v", stream, out)
	}
	off, ok := st["next_offset"].(float64)
	if !ok {
		t.Fatalf("/debug/state: %s.next_offset is not a number: %v", stream, st)
	}
	return off
}

// canListen reports whether addr can be bound again right now.
func canListen(t *testing.T, addr string) error {
	t.Helper()
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	return ln.Close()
}

func TestStartStopResolvesAddressesAndReleasesPorts(t *testing.T) {
	base := t.TempDir()
	keyFile := filepath.Join(base, "n1.key")
	genKey(t, keyFile)

	cfg := &config.Config{
		ULID:     "n1",
		DataDir:  filepath.Join(base, "data"),
		LogLevel: "debug",
		KeyFile:  keyFile,
		API:      config.API{Addr: "127.0.0.1:0", Token: tok},
		MQTT:     config.Endpoint{Addr: "127.0.0.1:0"},
		// A repl address without children must NOT produce a listener.
		Repl:    config.Endpoint{Addr: "127.0.0.1:0"},
		Clients: []config.Client{{ULID: "m1", Token: "m1-secret", Mount: "m1"}},
	}
	n := mustStart(t, cfg)

	if n.APIAddr == "" || strings.HasSuffix(n.APIAddr, ":0") {
		t.Errorf("APIAddr = %q, want a resolved address", n.APIAddr)
	}
	if n.MQTTAddr == "" || strings.HasSuffix(n.MQTTAddr, ":0") {
		t.Errorf("MQTTAddr = %q, want a resolved address", n.MQTTAddr)
	}
	if n.ReplAddr != "" {
		t.Errorf("ReplAddr = %q, want empty (node has no children)", n.ReplAddr)
	}
	if n.ReplSrv != nil {
		t.Error("ReplSrv is set although the node has no children")
	}

	resp, err := http.Get("http://" + n.APIAddr + "/healthz")
	if err != nil {
		t.Fatalf("GET /healthz: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("GET /healthz status = %d, want 200", resp.StatusCode)
	}
	var health map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&health); err != nil {
		t.Fatalf("GET /healthz: decode: %v", err)
	}
	if health["ulid"] != "n1" {
		t.Errorf("/healthz ulid = %v, want n1", health["ulid"])
	}

	n.Stop()

	if err := canListen(t, n.APIAddr); err != nil {
		t.Errorf("api port %s still bound after Stop: %v", n.APIAddr, err)
	}
	if err := canListen(t, n.MQTTAddr); err != nil {
		t.Errorf("mqtt port %s still bound after Stop: %v", n.MQTTAddr, err)
	}
	n.Stop() // must not panic (double Close of Pebble/mochi/http)
}

func TestRestartSameDataDirKeepsOffsets(t *testing.T) {
	base := t.TempDir()
	keyFile := filepath.Join(base, "n1.key")
	genKey(t, keyFile)

	cfg := &config.Config{
		ULID:     "n1",
		DataDir:  filepath.Join(base, "data"),
		LogLevel: "debug",
		KeyFile:  keyFile,
		API:      config.API{Addr: "127.0.0.1:0", Token: tok},
	}

	first := mustStart(t, cfg)
	code, out := apiCall(t, first, http.MethodPost, "/publish", map[string]any{
		"topic":   "colca/v1/_Metric/m1/temp",
		"payload": json.RawMessage(`{"v":1}`),
	})
	if code != http.StatusOK {
		t.Fatalf("POST /publish: status %d: %v", code, out)
	}
	if got := out["offset"]; got != float64(1) {
		t.Fatalf("published offset = %v, want 1", got)
	}
	if got := nextOffset(t, first, "metrics"); got != 2 {
		t.Fatalf("metrics next_offset before restart = %v, want 2", got)
	}
	first.Stop()

	second, err := Start(cfg)
	if err != nil {
		t.Fatalf("restart on the same data dir: %v", err)
	}
	t.Cleanup(second.Stop)

	if got := nextOffset(t, second, "metrics"); got != 2 {
		t.Errorf("metrics next_offset after restart = %v, want 2 (offsets must survive a restart)", got)
	}
}

func TestParentChildUplinkThroughNodes(t *testing.T) {
	base := t.TempDir()
	parentKey := filepath.Join(base, "parent.key")
	childKey := filepath.Join(base, "child.key")
	parentID := genKey(t, parentKey)
	childID := genKey(t, childKey)

	parentCfg := &config.Config{
		ULID:     "n-parent",
		DataDir:  filepath.Join(base, "parent-data"),
		LogLevel: "debug",
		KeyFile:  parentKey,
		API:      config.API{Addr: "127.0.0.1:0", Token: tok},
		Repl:     config.Endpoint{Addr: "127.0.0.1:0"},
		Children: []config.Child{{ULID: "n-child", Pubkey: childID.PublicHex(), Mount: "child1"}},
	}
	parent := mustStart(t, parentCfg)
	if parent.ReplAddr == "" || strings.HasSuffix(parent.ReplAddr, ":0") {
		t.Fatalf("parent ReplAddr = %q, want a resolved address", parent.ReplAddr)
	}
	if parent.MQTTAddr != "" {
		t.Errorf("parent MQTTAddr = %q, want empty (no mqtt configured)", parent.MQTTAddr)
	}

	childCfg := &config.Config{
		ULID:     "n-child",
		DataDir:  filepath.Join(base, "child-data"),
		LogLevel: "debug",
		KeyFile:  childKey,
		API:      config.API{Addr: "127.0.0.1:0", Token: tok},
		MQTT:     config.Endpoint{Addr: "127.0.0.1:0"},
		Parent:   &config.Parent{URL: "https://" + parent.ReplAddr, Pubkey: parentID.PublicHex()},
		Clients:  []config.Client{{ULID: "m1", Token: "m1-secret", Mount: "m1"}},
	}
	child := mustStart(t, childCfg)
	if child.ReplAddr != "" {
		t.Errorf("child ReplAddr = %q, want empty (no children)", child.ReplAddr)
	}

	opts := paho.NewClientOptions().
		AddBroker("tcp://" + child.MQTTAddr).
		SetClientID("m1-node-test").
		SetUsername("m1").
		SetPassword("m1-secret").
		SetConnectTimeout(5 * time.Second)
	cl := paho.NewClient(opts)
	ctok := cl.Connect()
	if !ctok.WaitTimeout(5 * time.Second) {
		t.Fatal("mqtt connect: timed out")
	}
	if err := ctok.Error(); err != nil {
		t.Fatalf("mqtt connect: %v", err)
	}
	defer cl.Disconnect(100)

	ptok := cl.Publish("colca/v1/_Metric/m1/temp", 1, false, []byte(`{"v":42}`))
	if !ptok.WaitTimeout(5 * time.Second) {
		t.Fatal("mqtt publish: timed out waiting for PUBACK")
	}
	if err := ptok.Error(); err != nil {
		t.Fatalf("mqtt publish: %v", err)
	}

	// mqtt hook → engine → child store → uplink → parent mount-insert → parent KV.
	var entries []any
	deadline := time.Now().Add(15 * time.Second)
	for {
		code, out := apiCall(t, parent, http.MethodGet, "/kv?prefix=child1/m1/temp", nil)
		if code != http.StatusOK {
			t.Fatalf("parent GET /kv: status %d: %v", code, out)
		}
		got, ok := out["entries"].([]any)
		if !ok {
			t.Fatalf("parent GET /kv: entries is not an array: %v", out)
		}
		if len(got) > 0 {
			entries = got
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("metric never reached the parent KV within 15s (child metrics next_offset=%v, parent metrics next_offset=%v)",
				nextOffset(t, child, "metrics"), nextOffset(t, parent, "metrics"))
		}
		time.Sleep(100 * time.Millisecond)
	}

	if len(entries) != 1 {
		t.Fatalf("parent KV entries = %d, want exactly 1: %v", len(entries), entries)
	}
	entry, ok := entries[0].(map[string]any)
	if !ok {
		t.Fatalf("parent KV entry is not an object: %v", entries[0])
	}
	if want := "colca/v1/_Metric/m1/child1/m1/temp"; entry["topic"] != want {
		t.Errorf("parent KV topic = %v, want %q", entry["topic"], want)
	}
	if entry["node_id"] != "m1" {
		t.Errorf("parent KV node_id = %v, want m1", entry["node_id"])
	}
	if entry["path"] != "child1/m1/temp" {
		t.Errorf("parent KV path = %v, want child1/m1/temp", entry["path"])
	}
	if got := fmt.Sprintf("%v", entry["payload"]); !strings.Contains(got, "42") {
		t.Errorf("parent KV payload = %v, want the published {\"v\":42}", entry["payload"])
	}
}
