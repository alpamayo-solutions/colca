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
	"github.com/alpamayo-solutions/colca/internal/store"
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

// connectMQTT connects a paho client to a node's broker and fails the test on error.
func connectMQTT(t *testing.T, addr, clientID, user, pass string) paho.Client {
	t.Helper()
	opts := paho.NewClientOptions().
		AddBroker("tcp://" + addr).
		SetClientID(clientID).
		SetUsername(user).
		SetPassword(pass).
		SetConnectTimeout(5 * time.Second)
	cl := paho.NewClient(opts)
	ctok := cl.Connect()
	if !ctok.WaitTimeout(5 * time.Second) {
		t.Fatalf("mqtt connect %s as %s: timed out", addr, user)
	}
	if err := ctok.Error(); err != nil {
		t.Fatalf("mqtt connect %s as %s: %v", addr, user, err)
	}
	t.Cleanup(func() { cl.Disconnect(100) })
	return cl
}

// publishMQTT publishes at QoS 1 and requires the PUBACK (i.e. the node persisted it).
func publishMQTT(t *testing.T, cl paho.Client, topic, payload string) {
	t.Helper()
	ptok := cl.Publish(topic, 1, false, []byte(payload))
	if !ptok.WaitTimeout(5 * time.Second) {
		t.Fatalf("mqtt publish %s: timed out waiting for PUBACK", topic)
	}
	if err := ptok.Error(); err != nil {
		t.Fatalf("mqtt publish %s: %v", topic, err)
	}
}

// TestRestartRepopulatesRetainedFromKV pins the restart half of the "retained
// set ≡ KV view" contract: mochi's retained store is in-memory, so a restarted
// node must re-seed it from the KV projection — a fresh subscriber connecting
// after a restart gets the current value of every state path with no new
// publish, commands are NOT replayed, and (the seed-before-Serve ordering) a
// value published right after the restart wins over the stale snapshot.
func TestRestartRepopulatesRetainedFromKV(t *testing.T) {
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
		Clients: []config.Client{
			{ULID: "m1", Token: "m1-secret", Mount: "m1"},
			{ULID: "obs", Token: "obs-secret"}, // no mount → read-only observer
		},
	}

	first := mustStart(t, cfg)
	m1 := connectMQTT(t, first.MQTTAddr, "m1-pre", "m1", "m1-secret")
	// Two state topics: "pressure" is never touched again — only the KV
	// re-seed can bring it back, so it is the assertion the mutation check
	// bites on. "temp" gets a FRESH value right after the restart — it pins
	// the seed-before-Serve ordering instead.
	publishMQTT(t, m1, "colca/v1/_Metric/m1/pressure", `{"v":7}`)
	publishMQTT(t, m1, "colca/v1/_Metric/m1/temp", `{"v":1}`)
	// A command in the commands stream: it must NOT come back retained.
	code, out := apiCall(t, first, http.MethodPost, "/publish", map[string]any{
		"topic":   "colca/v1/_CmdParam/m1/m1/set-speed",
		"payload": json.RawMessage(`{"correlation_id":"c1","expires_at":1}`),
	})
	if code != http.StatusOK {
		t.Fatalf("POST /publish command: status %d: %v", code, out)
	}
	m1.Disconnect(100)
	first.Stop()

	second, err := Start(cfg)
	if err != nil {
		t.Fatalf("restart on the same data dir: %v", err)
	}
	t.Cleanup(second.Stop)

	// Seed-ordering assertion (the race a post-Serve replay would open): a
	// FRESH value published immediately after the restart must win over the
	// pre-restart snapshot value in the retained set.
	m1b := connectMQTT(t, second.MQTTAddr, "m1-post", "m1", "m1-secret")
	publishMQTT(t, m1b, "colca/v1/_Metric/m1/temp", `{"v":2}`)

	type received struct {
		topic    string
		payload  string
		retained bool
	}
	msgs := make(chan received, 64)
	obs := connectMQTT(t, second.MQTTAddr, "obs-post", "obs", "obs-secret")
	stok := obs.Subscribe("colca/#", 1, func(_ paho.Client, m paho.Message) {
		msgs <- received{topic: m.Topic(), payload: string(m.Payload()), retained: m.Retained()}
	})
	if !stok.WaitTimeout(5 * time.Second) {
		t.Fatal("mqtt subscribe colca/#: timed out")
	}
	if err := stok.Error(); err != nil {
		t.Fatalf("mqtt subscribe colca/#: %v", err)
	}

	// Drain until BOTH canonical metric topics arrived (deadline-bounded),
	// then keep draining briefly: if a command had been wrongly retained it
	// would be replayed in the same on-subscribe burst.
	seen := map[string]received{}
	deadline := time.After(10 * time.Second)
	for len(seen) < 2 {
		select {
		case m := <-msgs:
			if strings.HasPrefix(m.topic, "colca/v1/_Cmd") {
				t.Fatalf("a command was replayed to a fresh post-restart subscriber: %s (retained=%v)", m.topic, m.retained)
			}
			if m.topic == "colca/v1/_Metric/m1/m1/temp" || m.topic == "colca/v1/_Metric/m1/m1/pressure" {
				seen[m.topic] = m
			}
		case <-deadline:
			t.Fatalf("fresh post-restart subscriber got %d of the 2 retained metric topics within 10s (saw: %v) — the retained set was not re-seeded from KV", len(seen), seen)
		}
	}
	// The untouched topic can only come from the KV re-seed.
	pressure := seen["colca/v1/_Metric/m1/m1/pressure"]
	if !pressure.retained || !strings.Contains(pressure.payload, `"v":7`) {
		t.Errorf("pressure after restart = %+v, want retained {\"v\":7} restored from KV", pressure)
	}
	// The re-published topic must show the FRESH value, not the stale snapshot.
	temp := seen["colca/v1/_Metric/m1/m1/temp"]
	if !temp.retained {
		t.Errorf("post-restart temp arrived unretained — retained flag lost across restart")
	}
	if !strings.Contains(temp.payload, `"v":2`) {
		t.Errorf("retained temp after restart = %s, want the FRESH {\"v\":2} — the stale KV snapshot overwrote a live publish", temp.payload)
	}
	grace := time.NewTimer(500 * time.Millisecond)
	for {
		select {
		case m := <-msgs:
			if strings.HasPrefix(m.topic, "colca/v1/_Cmd") {
				t.Fatalf("a command was replayed to a fresh post-restart subscriber: %s (retained=%v)", m.topic, m.retained)
			}
		case <-grace.C:
			return
		}
	}
}

// TestRetainedSeedStartupCostTenThousandPaths bounds the availability cost of
// the retained re-seed: Start replays one in-memory mochi publish per KV path
// before the API listener opens, so /healthz is gated on it. At the 10k-path
// cardinality the benchmarks anticipate this must stay far below a second;
// the generous bound only catches pathological regressions (per-entry fsyncs,
// accidental O(n²)). The measured number is logged.
func TestRetainedSeedStartupCostTenThousandPaths(t *testing.T) {
	const paths = 10_000
	base := t.TempDir()
	keyFile := filepath.Join(base, "n1.key")
	genKey(t, keyFile)
	dataDir := filepath.Join(base, "data")

	st, err := store.Open(dataDir)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	recs := make([]store.Record, 0, paths)
	for i := 0; i < paths; i++ {
		recs = append(recs, store.Record{
			Topic:   fmt.Sprintf("colca/v1/_Metric/m1/m1/temp%d", i),
			Payload: []byte(`{"v":1}`),
			TS:      1,
			KVPath:  fmt.Sprintf("m1/temp%d", i),
			KVNode:  "m1",
		})
	}
	if _, _, err := st.Append("metrics", recs); err != nil {
		t.Fatalf("seed append: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("store.Close: %v", err)
	}

	cfg := &config.Config{
		ULID:    "n1",
		DataDir: dataDir,
		KeyFile: keyFile,
		API:     config.API{Addr: "127.0.0.1:0", Token: tok},
		MQTT:    config.Endpoint{Addr: "127.0.0.1:0"},
		Clients: []config.Client{{ULID: "obs", Token: "obs-secret"}},
	}
	started := time.Now()
	n := mustStart(t, cfg)
	elapsed := time.Since(started)
	t.Logf("node.Start with %d KV paths (retained re-seed included): %v", paths, elapsed)
	if elapsed > 10*time.Second {
		t.Fatalf("node.Start with %d KV paths took %v — the retained re-seed is delaying readiness pathologically", paths, elapsed)
	}

	// The timing is only meaningful if the seed actually happened: spot-check
	// one retained path on a fresh subscriber.
	obs := connectMQTT(t, n.MQTTAddr, "obs-cost", "obs", "obs-secret")
	got := make(chan paho.Message, 1)
	stok := obs.Subscribe("colca/v1/_Metric/m1/m1/temp9999", 1, func(_ paho.Client, m paho.Message) {
		select {
		case got <- m:
		default:
		}
	})
	if !stok.WaitTimeout(5 * time.Second) {
		t.Fatal("mqtt subscribe: timed out")
	}
	if err := stok.Error(); err != nil {
		t.Fatalf("mqtt subscribe: %v", err)
	}
	select {
	case m := <-got:
		if !m.Retained() {
			t.Error("seeded path arrived unretained")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("seeded path colca/v1/_Metric/m1/m1/temp9999 never arrived retained — the 10k seed did not reach the broker")
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
