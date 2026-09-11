package node

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"

	"github.com/alpamayo-solutions/colca/door"
	"github.com/alpamayo-solutions/colca/internal/authtest"
	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/identity"
	"github.com/alpamayo-solutions/colca/internal/store"
	"github.com/alpamayo-solutions/colca/secrets"
)

const tok = "test-admin-token"

// httpsClient accepts the node's self-signed API cert (pinning model, no CA).
var httpsClient = &http.Client{Transport: &http.Transport{
	TLSClientConfig: &tls.Config{InsecureSkipVerify: true, MinVersion: tls.VersionTLS13}, // #nosec G402 -- test
}}

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
	req, err := http.NewRequest(method, "https://"+n.APIAddr+path, rd)
	if err != nil {
		t.Fatalf("new request %s %s: %v", method, path, err)
	}
	req.Header.Set("X-Colca-Token", tok)
	resp, err := httpsClient.Do(req)
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

// mustKVScan is KVScan that fails the test on error.
func mustKVScan(t *testing.T, st *store.Store, prefix string) []store.KVEntry {
	t.Helper()
	entries, err := st.KVScan(prefix)
	if err != nil {
		t.Fatal(err)
	}
	return entries
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
		API:      config.API{Addr: "127.0.0.1:0", LocalAddr: "127.0.0.1:0", Token: tok},
		MQTT:     config.Endpoint{Addr: "127.0.0.1:0"},
		// Children are enrolled at runtime, so a repl address always opens a listener.
		Repl: config.Endpoint{Addr: "127.0.0.1:0"},
	}
	n := mustStart(t, cfg)

	if n.APIAddr == "" || strings.HasSuffix(n.APIAddr, ":0") {
		t.Errorf("APIAddr = %q, want a resolved address", n.APIAddr)
	}
	if n.LocalAPIAddr == "" || strings.HasSuffix(n.LocalAPIAddr, ":0") {
		t.Errorf("LocalAPIAddr = %q, want a resolved address", n.LocalAPIAddr)
	}
	if n.MQTTAddr == "" || strings.HasSuffix(n.MQTTAddr, ":0") {
		t.Errorf("MQTTAddr = %q, want a resolved address", n.MQTTAddr)
	}
	if n.ReplAddr == "" || strings.HasSuffix(n.ReplAddr, ":0") {
		t.Errorf("ReplAddr = %q, want a resolved address (repl addr configured)", n.ReplAddr)
	}

	resp, err := httpsClient.Get("https://" + n.APIAddr + "/healthz")
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

	// The local door is plain HTTP with no credential; a plain http.Get proves it
	// end to end.
	localResp, err := http.Get("http://" + n.LocalAPIAddr + "/healthz")
	if err != nil {
		t.Fatalf("GET local /healthz: %v", err)
	}
	defer localResp.Body.Close()
	if localResp.StatusCode != http.StatusOK {
		t.Errorf("GET local /healthz status = %d, want 200", localResp.StatusCode)
	}
	// And the admin surface must not exist there at all: a 404, not a 401/403.
	enrollResp, err := http.Post("http://"+n.LocalAPIAddr+"/enroll", "application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatalf("POST local /enroll: %v", err)
	}
	defer enrollResp.Body.Close()
	if enrollResp.StatusCode != http.StatusNotFound {
		t.Errorf("POST local /enroll status = %d, want 404 (route never mounted)", enrollResp.StatusCode)
	}

	n.Stop()

	if err := canListen(t, n.APIAddr); err != nil {
		t.Errorf("api port %s still bound after Stop: %v", n.APIAddr, err)
	}
	if err := canListen(t, n.LocalAPIAddr); err != nil {
		t.Errorf("local api port %s still bound after Stop: %v", n.LocalAPIAddr, err)
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

func TestNodeLocalSecretStoreSurvivesRestartAndDecryptsOnlyInService(t *testing.T) {
	base := t.TempDir()
	keyFile := filepath.Join(base, "n1.key")
	genKey(t, keyFile)
	cfg := &config.Config{
		ULID:       "n1",
		DataDir:    filepath.Join(base, "data"),
		SecretsDir: filepath.Join(base, "secrets"),
		KeyFile:    keyFile,
		API:        config.API{LocalAddr: "127.0.0.1:0"},
	}
	keyring, err := secrets.OpenKeyring(filepath.Join(base, "assistant-keys"))
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := keyring.SealForActive([]byte("provider-api-key"))
	if err != nil {
		t.Fatal(err)
	}

	first := mustStart(t, cfg)
	client := &door.Client{BaseURL: "http://" + first.LocalAPIAddr, Service: "assistant"}
	created, err := client.PutSecret(t.Context(), "model-providers/alpha", door.SecretWrite{Envelope: envelope})
	if err != nil {
		t.Fatal(err)
	}
	if created.Revision != 1 || created.KeyID != envelope.KeyID {
		t.Fatalf("created metadata = %+v", created)
	}
	first.Stop()

	second, err := Start(cfg)
	if err != nil {
		t.Fatalf("restart with same secret directory: %v", err)
	}
	t.Cleanup(second.Stop)
	client = &door.Client{BaseURL: "http://" + second.LocalAPIAddr, Service: "assistant"}
	record, err := client.GetSecret(t.Context(), "model-providers/alpha")
	if err != nil {
		t.Fatal(err)
	}
	plaintext, err := keyring.Open(record.Envelope)
	if err != nil || string(plaintext) != "provider-api-key" {
		t.Fatalf("service decrypt after restart = %q, %v", plaintext, err)
	}
}

func TestSecretStoreNeverReplicatesToParent(t *testing.T) {
	base := t.TempDir()
	parentKey := filepath.Join(base, "parent.key")
	childKey := filepath.Join(base, "child.key")
	parentID := genKey(t, parentKey)
	childID := genKey(t, childKey)

	parent := mustStart(t, &config.Config{
		ULID:       "n-parent",
		DataDir:    filepath.Join(base, "parent-data"),
		SecretsDir: filepath.Join(base, "parent-secrets"),
		KeyFile:    parentKey,
		API:        config.API{Addr: "127.0.0.1:0", Token: tok},
		Repl:       config.Endpoint{Addr: "127.0.0.1:0"},
	})
	authtest.EnrollNodeAt(t, parent.Registry, parent.Engine, "n-child", childID.PublicHex(), "child1")

	child := mustStart(t, &config.Config{
		ULID:       "n-child",
		DataDir:    filepath.Join(base, "child-data"),
		SecretsDir: filepath.Join(base, "child-secrets"),
		KeyFile:    childKey,
		API:        config.API{Addr: "127.0.0.1:0", LocalAddr: "127.0.0.1:0", Token: tok},
		Parent:     &config.Parent{URL: "https://" + parent.ReplAddr, Pubkey: parentID.PublicHex()},
	})

	keyring, err := secrets.OpenKeyring(filepath.Join(base, "assistant-keys"))
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := keyring.SealForActive([]byte("child-only"))
	if err != nil {
		t.Fatal(err)
	}
	local := &door.Client{BaseURL: "http://" + child.LocalAPIAddr, Service: "assistant"}
	if _, err := local.PutSecret(t.Context(), "primary", door.SecretWrite{Envelope: envelope}); err != nil {
		t.Fatal(err)
	}

	code, out := apiCall(t, child, http.MethodPost, "/publish", map[string]any{
		"topic": "colca/v1/_Metric/n-child/m1/temp", "payload": map[string]any{"v": 42},
	})
	if code != http.StatusOK {
		t.Fatalf("publish replication witness = %d: %v", code, out)
	}
	deadline := time.Now().Add(15 * time.Second)
	for {
		if entries := mustKVScan(t, parent.Store, "child1/m1/temp"); len(entries) == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("ordinary stream record did not replicate to parent")
		}
		time.Sleep(20 * time.Millisecond)
	}
	items, err := parent.Secrets.List("assistant")
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 0 {
		t.Fatalf("child secret replicated into parent store: %+v", items)
	}
	if _, err := child.Secrets.Get("assistant", "primary"); err != nil {
		t.Fatalf("child lost its local secret: %v", err)
	}
}

// connectMQTT connects a paho client over TLS with the machine's pinned key.
func connectMQTT(t *testing.T, addr, clientID string, m *authtest.Machine) paho.Client {
	t.Helper()
	opts := paho.NewClientOptions().
		AddBroker("ssl://" + addr).
		SetTLSConfig(m.TLSConfig()).
		SetClientID(clientID).
		SetUsername(m.ULID).
		SetConnectTimeout(5 * time.Second)
	cl := paho.NewClient(opts)
	ctok := cl.Connect()
	if !ctok.WaitTimeout(5 * time.Second) {
		t.Fatalf("mqtt connect %s as %s: timed out", addr, m.ULID)
	}
	if err := ctok.Error(); err != nil {
		t.Fatalf("mqtt connect %s as %s: %v", addr, m.ULID, err)
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

// TestRestartRepopulatesRetainedFromKV: mochi's retained store is in memory, so
// a restarted node reseeds it from KV. A fresh subscriber after the restart gets
// every state path and no commands, and a value published right after the
// restart wins over the snapshot.
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
	}

	first := mustStart(t, cfg)
	m1machine := authtest.NewMachine(t, "m1")
	obsMachine := authtest.NewMachine(t, "obs")
	authtest.EnrollAt(t, first.Registry, first.Engine, m1machine, "m1", "write:"+authtest.ElementID("m1")+"/#")
	authtest.EnrollAt(t, first.Registry, first.Engine, obsMachine, "obs", "read:#")
	m1 := connectMQTT(t, first.MQTTAddr, "m1-pre", m1machine)
	// "pressure" is never published again, so only the reseed can restore it;
	// "temp" gets a new value after the restart to check the reseed runs before
	// Serve. A _Signal on the same path must survive too: a KV key without the
	// contract would lose it.
	if _, err := first.Engine.IngestAdmin(
		"colca/v1/_Signal/n1/m1/pressure",
		[]byte(`{"id":"01HSIGPRESSURE","name":"pressure"}`),
	); err != nil {
		t.Fatalf("publish same-path signal: %v", err)
	}
	publishMQTT(t, m1, "colca/v1/_Metric/n1/m1/pressure", `{"v":7}`)
	publishMQTT(t, m1, "colca/v1/_Metric/n1/m1/temp", `{"v":1}`)
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

	// A value published right after the restart must win over the snapshot.
	m1b := connectMQTT(t, second.MQTTAddr, "m1-post", m1machine)
	publishMQTT(t, m1b, "colca/v1/_Metric/n1/m1/temp", `{"v":2}`)

	type received struct {
		topic    string
		payload  string
		retained bool
	}
	msgs := make(chan received, 64)
	obs := connectMQTT(t, second.MQTTAddr, "obs-post", obsMachine)
	stok := obs.Subscribe("colca/#", 1, func(_ paho.Client, m paho.Message) {
		msgs <- received{topic: m.Topic(), payload: string(m.Payload()), retained: m.Retained()}
	})
	if !stok.WaitTimeout(5 * time.Second) {
		t.Fatal("mqtt subscribe colca/#: timed out")
	}
	if err := stok.Error(); err != nil {
		t.Fatalf("mqtt subscribe colca/#: %v", err)
	}

	// Drain until both metrics and the signal arrived, then briefly longer: a
	// wrongly retained command would arrive in the same burst.
	seen := map[string]received{}
	deadline := time.After(10 * time.Second)
	for len(seen) < 3 {
		select {
		case m := <-msgs:
			if strings.HasPrefix(m.topic, "colca/v1/_Cmd") {
				t.Fatalf("a command was replayed to a fresh post-restart subscriber: %s (retained=%v)", m.topic, m.retained)
			}
			if m.topic == "colca/v1/_Metric/n1/m1/temp" ||
				m.topic == "colca/v1/_Metric/n1/m1/pressure" ||
				m.topic == "colca/v1/_Signal/n1/m1/pressure" {
				seen[m.topic] = m
			}
		case <-deadline:
			t.Fatalf("fresh post-restart subscriber got %d of the 3 retained state topics within 10s (saw: %v) — the retained set was not re-seeded from contract-aware KV", len(seen), seen)
		}
	}
	// The untouched topic can only come from the KV re-seed.
	pressure := seen["colca/v1/_Metric/n1/m1/pressure"]
	if !pressure.retained || !strings.Contains(pressure.payload, `"v":7`) {
		t.Errorf("pressure after restart = %+v, want retained {\"v\":7} restored from KV", pressure)
	}
	signal := seen["colca/v1/_Signal/n1/m1/pressure"]
	if !signal.retained || !strings.Contains(signal.payload, `"id":"01HSIGPRESSURE"`) {
		t.Errorf("same-path signal after restart = %+v, want the retained _Signal restored independently of _Metric", signal)
	}
	// The re-published topic must show the FRESH value, not the stale snapshot.
	temp := seen["colca/v1/_Metric/n1/m1/temp"]
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

// TestRetainedSeedStartupCostTenThousandPaths bounds the reseed cost: Start
// replays one publish per KV path before the API opens. The bound only catches
// pathological regressions; the measured time is logged.
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
	}
	started := time.Now()
	n := mustStart(t, cfg)
	elapsed := time.Since(started)
	t.Logf("node.Start with %d KV paths (retained re-seed included): %v", paths, elapsed)
	if elapsed > 10*time.Second {
		t.Fatalf("node.Start with %d KV paths took %v — the retained re-seed is delaying readiness pathologically", paths, elapsed)
	}

	// The reseed count equals the seeded paths plus the node's own _Node record,
	// written at startup on the root.
	resp, err := httpsClient.Get("https://" + n.APIAddr + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatalf("GET /metrics: read body: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /metrics without token: want 200, got %d", resp.StatusCode)
	}
	if want := fmt.Sprintf("colca_retained_reseed_records %d", paths+1); !strings.Contains(string(body), want) {
		t.Fatalf("/metrics must report %q after the seed, got:\n%s", want, body)
	}

	// Check that the seed really reached the broker. The observer is enrolled after
	// Start, so it does not change the reseed count.
	obsMachine := authtest.NewMachine(t, "obs")
	authtest.EnrollAt(t, n.Registry, n.Engine, obsMachine, "obs", "read:#")
	obs := connectMQTT(t, n.MQTTAddr, "obs-cost", obsMachine)
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
	}
	parent := mustStart(t, parentCfg)
	if parent.ReplAddr == "" || strings.HasSuffix(parent.ReplAddr, ":0") {
		t.Fatalf("parent ReplAddr = %q, want a resolved address", parent.ReplAddr)
	}
	authtest.EnrollNodeAt(t, parent.Registry, parent.Engine, "n-child", childID.PublicHex(), "child1")
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
	}
	child := mustStart(t, childCfg)
	if child.ReplAddr != "" {
		t.Errorf("child ReplAddr = %q, want empty (no children)", child.ReplAddr)
	}

	m1machine := authtest.NewMachine(t, "m1")
	authtest.EnrollAt(t, child.Registry, child.Engine, m1machine, "m1", "write:"+authtest.ElementID("m1")+"/#")
	cl := connectMQTT(t, child.MQTTAddr, "m1-node-test", m1machine)

	ptok := cl.Publish("colca/v1/_Metric/n-child/m1/temp", 1, false, []byte(`{"v":42}`))
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
	if want := "colca/v1/_Metric/n-child/child1/m1/temp"; entry["topic"] != want {
		t.Errorf("parent KV topic = %v, want %q", entry["topic"], want)
	}
	if entry["node_id"] != "n-child" {
		t.Errorf("parent KV node_id = %v, want n-child", entry["node_id"])
	}
	if entry["path"] != "child1/m1/temp" {
		t.Errorf("parent KV path = %v, want child1/m1/temp", entry["path"])
	}
	if got := fmt.Sprintf("%v", entry["payload"]); !strings.Contains(got, "42") {
		t.Errorf("parent KV payload = %v, want the published {\"v\":42}", entry["payload"])
	}
}

// The pruner runs inside the node's lifecycle: it prunes on schedule, Stop never
// closes the store under a running cycle, and a restart picks up the persisted
// low-water mark. Stop runs while the 5ms pruner is active.
func TestRetentionPrunerRunsInNodeLifecycleAndRestartsSafely(t *testing.T) {
	base := t.TempDir()
	keyFile := filepath.Join(base, "n-ret.key")
	genKey(t, keyFile)
	dataDir := filepath.Join(base, "data")

	// Seed history old enough for a 1h max_age BEFORE the node starts.
	st, err := store.Open(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-48 * time.Hour).UnixMilli()
	var recs []store.Record
	for i := 0; i < 10; i++ {
		recs = append(recs, store.Record{Topic: fmt.Sprintf("colca/v1/_Metric/n-ret/plant/s%d", i), Payload: []byte(`{"v":1}`), TS: old + int64(i)})
	}
	if _, _, err := st.Append("metrics", recs); err != nil {
		t.Fatal(err)
	}
	st.Close()

	interval := config.Duration(5 * time.Millisecond)
	cfg := &config.Config{
		ULID:    "n-ret",
		DataDir: dataDir,
		KeyFile: keyFile,
		API:     config.API{Addr: "127.0.0.1:0", Token: tok},
		Retention: config.Retention{
			Interval: &interval,
			Streams:  map[string]config.StreamRetention{"metrics": {MaxAge: config.Duration(time.Hour)}},
		},
	}
	n := mustStart(t, cfg)
	deadline := time.Now().Add(10 * time.Second)
	for n.Store.LWM("metrics") != 11 {
		if time.Now().After(deadline) {
			t.Fatalf("pruner never pruned inside the node: LWM = %d, want 11", n.Store.LWM("metrics"))
		}
		time.Sleep(5 * time.Millisecond)
	}
	n.Stop() // while the 5ms pruner is ticking — must not panic the store

	// Restart on the same dir: the LWM is persisted state, and the pruner
	// starts again without tripping over it.
	n2 := mustStart(t, cfg)
	if got := n2.Store.LWM("metrics"); got != 11 {
		t.Fatalf("LWM after restart = %d, want 11", got)
	}
	n2.Stop()
}

// A tombstoned path has no KV key, so the reseed does not bring it back after a
// restart, while the surviving path is still retained.
func TestTombstonedPathStaysGoneAcrossRestart(t *testing.T) {
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
	}

	first := mustStart(t, cfg)
	m1machine := authtest.NewMachine(t, "m1")
	obsMachine := authtest.NewMachine(t, "obs")
	authtest.EnrollAt(t, first.Registry, first.Engine, m1machine, "m1", "write:"+authtest.ElementID("m1")+"/#")
	authtest.EnrollAt(t, first.Registry, first.Engine, obsMachine, "obs", "read:#")
	m1 := connectMQTT(t, first.MQTTAddr, "m1-tomb", m1machine)
	publishMQTT(t, m1, "colca/v1/_Metric/n1/m1/pressure", `{"v":7}`)
	publishMQTT(t, m1, "colca/v1/_Metric/n1/m1/temp", `{"v":1}`)
	// The tombstone: empty payload on the pressure path. PUBACK ⇒ persisted.
	publishMQTT(t, m1, "colca/v1/_Metric/n1/m1/pressure", "")
	if got := mustKVScan(t, first.Store, "m1/pressure"); len(got) != 0 {
		t.Fatalf("KV key survived the tombstone before restart: %+v", got)
	}
	m1.Disconnect(100)
	first.Stop()

	second, err := Start(cfg)
	if err != nil {
		t.Fatalf("restart on the same data dir: %v", err)
	}
	t.Cleanup(second.Stop)

	// The stream still carries the full history (2 sets + 1 tombstone)…
	if got := nextOffset(t, second, "metrics"); got != 4 {
		t.Errorf("metrics next_offset after restart = %v, want 4 (tombstone is history)", got)
	}
	// …but the KV view has retired the path, and so must the retained set.
	if got := mustKVScan(t, second.Store, "m1/pressure"); len(got) != 0 {
		t.Fatalf("tombstoned KV key resurrected across restart: %+v", got)
	}

	type received struct {
		topic, payload string
		retained       bool
	}
	msgs := make(chan received, 64)
	obs := connectMQTT(t, second.MQTTAddr, "obs-tomb", obsMachine)
	stok := obs.Subscribe("colca/#", 1, func(_ paho.Client, m paho.Message) {
		msgs <- received{topic: m.Topic(), payload: string(m.Payload()), retained: m.Retained()}
	})
	if !stok.WaitTimeout(5 * time.Second) {
		t.Fatal("mqtt subscribe colca/#: timed out")
	}
	if err := stok.Error(); err != nil {
		t.Fatalf("mqtt subscribe colca/#: %v", err)
	}

	// The surviving path proves the reseed ran, so the grace period after it is a
	// real check that the retired path stays away.
	deadline := time.After(10 * time.Second)
	for {
		select {
		case m := <-msgs:
			if m.topic == "colca/v1/_Metric/n1/m1/pressure" {
				t.Fatalf("tombstoned path resurrected via reseed: %+v", m)
			}
			if m.topic == "colca/v1/_Metric/n1/m1/temp" {
				if !m.retained || !strings.Contains(m.payload, `"v":1`) {
					t.Fatalf("surviving path = %+v, want retained {\"v\":1}", m)
				}
				goto graceDrain
			}
		case <-deadline:
			t.Fatal("surviving path never replayed retained — the reseed did not run")
		}
	}
graceDrain:
	grace := time.NewTimer(500 * time.Millisecond)
	for {
		select {
		case m := <-msgs:
			if m.topic == "colca/v1/_Metric/n1/m1/pressure" {
				t.Fatalf("tombstoned path resurrected via reseed: %+v", m)
			}
		case <-grace.C:
			return
		}
	}
}

// A tombstone replicates upward and retires the path at the parent too: the KV
// key is deleted and the retained message cleared.
func TestTombstoneReplicatesUpwardAndRetiresParent(t *testing.T) {
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
		MQTT:     config.Endpoint{Addr: "127.0.0.1:0"},
		Repl:     config.Endpoint{Addr: "127.0.0.1:0"},
	}
	parent := mustStart(t, parentCfg)
	// The child's key and the observer are enrolled before the child starts.
	authtest.EnrollNodeAt(t, parent.Registry, parent.Engine, "n-child", childID.PublicHex(), "child1")
	obsMachine := authtest.NewMachine(t, "obs")
	authtest.EnrollAt(t, parent.Registry, parent.Engine, obsMachine, "obs", "read:#")

	childCfg := &config.Config{
		ULID:     "n-child",
		DataDir:  filepath.Join(base, "child-data"),
		LogLevel: "debug",
		KeyFile:  childKey,
		API:      config.API{Addr: "127.0.0.1:0", Token: tok},
		MQTT:     config.Endpoint{Addr: "127.0.0.1:0"},
		Parent:   &config.Parent{URL: "https://" + parent.ReplAddr, Pubkey: parentID.PublicHex()},
	}
	child := mustStart(t, childCfg)

	m1machine := authtest.NewMachine(t, "m1")
	authtest.EnrollAt(t, child.Registry, child.Engine, m1machine, "m1", "write:"+authtest.ElementID("m1")+"/#")
	m1 := connectMQTT(t, child.MQTTAddr, "m1-up", m1machine)
	publishMQTT(t, m1, "colca/v1/_Metric/n-child/m1/temp", `{"v":42}`)
	publishMQTT(t, m1, "colca/v1/_Metric/n-child/m1/keep", `{"v":1}`)

	// waitParentKV polls the parent KV until prefix holds want entries.
	waitParentKV := func(prefix string, want int, why string) {
		t.Helper()
		deadline := time.Now().Add(15 * time.Second)
		for {
			code, out := apiCall(t, parent, http.MethodGet, "/kv?prefix="+prefix, nil)
			if code != http.StatusOK {
				t.Fatalf("parent GET /kv: status %d: %v", code, out)
			}
			entries, _ := out["entries"].([]any)
			if len(entries) == want {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s: parent KV %q has %d entries, want %d", why, prefix, len(entries), want)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	waitParentKV("child1/m1/temp", 1, "setup: metric never replicated up")
	waitParentKV("child1/m1/keep", 1, "setup: sibling never replicated up")

	// The live observer receiving the empty-payload clear is the sync point: mochi
	// updates its retained store before delivering, so the fresh subscriber check
	// below cannot race it.
	type received struct {
		topic    string
		payload  string
		retained bool
	}
	liveMsgs := make(chan received, 64)
	live := connectMQTT(t, parent.MQTTAddr, "obs-parent-live", obsMachine)
	ltok := live.Subscribe("colca/#", 1, func(_ paho.Client, m paho.Message) {
		liveMsgs <- received{topic: m.Topic(), payload: string(m.Payload()), retained: m.Retained()}
	})
	if !ltok.WaitTimeout(5 * time.Second) {
		t.Fatal("parent live subscribe: timed out")
	}
	if err := ltok.Error(); err != nil {
		t.Fatalf("parent live subscribe: %v", err)
	}

	// The tombstone at the child: empty payload through the normal publish path.
	publishMQTT(t, m1, "colca/v1/_Metric/n-child/m1/temp", "")

	// It replicates upward and retires the parent's KV copy…
	waitParentKV("child1/m1/temp", 0, "tombstone did not retire the parent KV")
	// …while the sibling path survives untouched.
	waitParentKV("child1/m1/keep", 1, "sibling was wrongly retired")

	// …and the live observer sees the empty-payload clear on the parent bus.
	clearDeadline := time.After(15 * time.Second)
	for {
		var m received
		select {
		case m = <-liveMsgs:
		case <-clearDeadline:
			t.Fatal("empty-payload clear never arrived on the parent bus")
		}
		if m.topic == "colca/v1/_Metric/n-child/child1/m1/temp" && m.payload == "" {
			break
		}
	}

	// The parent's retained set agrees: a fresh subscriber on the parent bus
	// gets the sibling's retained value but nothing for the retired path.
	msgs := make(chan received, 64)
	obs := connectMQTT(t, parent.MQTTAddr, "obs-parent", obsMachine)
	stok := obs.Subscribe("colca/#", 1, func(_ paho.Client, m paho.Message) {
		msgs <- received{topic: m.Topic(), retained: m.Retained()}
	})
	if !stok.WaitTimeout(5 * time.Second) {
		t.Fatal("parent mqtt subscribe: timed out")
	}
	if err := stok.Error(); err != nil {
		t.Fatalf("parent mqtt subscribe: %v", err)
	}
	deadline := time.After(10 * time.Second)
	for {
		select {
		case m := <-msgs:
			if m.topic == "colca/v1/_Metric/n-child/child1/m1/temp" && m.retained {
				t.Fatalf("retired path still retained on the parent bus: %+v", m)
			}
			if m.topic == "colca/v1/_Metric/n-child/child1/m1/keep" && m.retained {
				goto graceDrain
			}
		case <-deadline:
			t.Fatal("sibling's retained value never arrived on the parent bus")
		}
	}
graceDrain:
	grace := time.NewTimer(500 * time.Millisecond)
	for {
		select {
		case m := <-msgs:
			if m.topic == "colca/v1/_Metric/n-child/child1/m1/temp" && m.retained {
				t.Fatalf("retired path still retained on the parent bus: %+v", m)
			}
		case <-grace.C:
			return
		}
	}
}

// TestAddrFileNamesEveryResolvedDoor: a supervisor that configures every door
// as ":0" gets a complete file naming the resolved addresses, five doors on
// five distinct ports.
func TestAddrFileNamesEveryResolvedDoor(t *testing.T) {
	base := t.TempDir()
	keyFile := filepath.Join(base, "n-addr.key")
	genKey(t, keyFile)
	addrFile := filepath.Join(base, "run", "addresses.json")

	cfg := &config.Config{
		ULID:      "n-addr",
		DataDir:   filepath.Join(base, "data"),
		LogLevel:  "info",
		KeyFile:   keyFile,
		AddrFile:  addrFile,
		API:       config.API{Addr: "127.0.0.1:0", LocalAddr: "127.0.0.1:0", Token: tok},
		MQTT:      config.Endpoint{Addr: "127.0.0.1:0"},
		MQTTLocal: config.Endpoint{Addr: "127.0.0.1:0"},
		Repl:      config.Endpoint{Addr: "127.0.0.1:0"},
	}
	n := mustStart(t, cfg)

	raw, err := os.ReadFile(addrFile)
	if err != nil {
		t.Fatalf("addr_file after Start: %v", err)
	}
	var got map[string]string
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("addr_file is not JSON: %v\n%s", err, raw)
	}
	want := map[string]string{
		"api": n.APIAddr, "api_local": n.LocalAPIAddr,
		"mqtt": n.MQTTAddr, "mqtt_local": n.MQTTLocalAddr, "repl": n.ReplAddr,
	}
	ports := map[string]string{}
	for door, addr := range want {
		if got[door] != addr {
			t.Errorf("addr_file[%s] = %q, want the resolved %q", door, got[door], addr)
		}
		if addr == "" || strings.HasSuffix(addr, ":0") {
			t.Errorf("%s resolved to %q — `:0` must have been replaced by a real port", door, addr)
		}
		_, port, _ := net.SplitHostPort(addr)
		if other, dup := ports[port]; dup {
			t.Errorf("%s and %s share port %s — the whole point is that they cannot", door, other, port)
		}
		ports[port] = door
	}
	if _, err := os.Stat(addrFile + ".tmp"); err == nil {
		t.Errorf("temp file left behind: the write must finish with a rename")
	}
}

// Requests that arrive while Stop runs must finish before the store closes or be
// turned away; none may reach a closed store.
func TestStopWhileRequestsArrive(t *testing.T) {
	for round := 0; round < 5; round++ {
		base := t.TempDir()
		keyFile := filepath.Join(base, "n1.key")
		genKey(t, keyFile)
		n := mustStart(t, &config.Config{
			ULID:    "n1",
			DataDir: filepath.Join(base, "data"),
			KeyFile: keyFile,
			API:     config.API{Addr: "127.0.0.1:0", Token: tok},
			MQTT:    config.Endpoint{Addr: "127.0.0.1:0"},
		})

		done := make(chan struct{})
		var clients sync.WaitGroup
		for i := 0; i < 8; i++ {
			clients.Add(1)
			go func() {
				defer clients.Done()
				client := &http.Client{Timeout: 2 * time.Second, Transport: httpsClient.Transport}
				for {
					select {
					case <-done:
						return
					default:
					}
					req, _ := http.NewRequest(http.MethodGet, "https://"+n.APIAddr+"/debug/state", nil)
					req.Header.Set("X-Colca-Token", tok)
					if resp, err := client.Do(req); err == nil {
						_, _ = io.Copy(io.Discard, resp.Body)
						_ = resp.Body.Close()
					}
				}
			}()
		}
		time.Sleep(50 * time.Millisecond)
		n.Stop()
		close(done)
		clients.Wait()
	}
}
