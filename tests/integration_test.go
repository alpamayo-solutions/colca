// Package tests contains the end-to-end suite for a real 4-node / 3-level Colca
// topology started in-process:
//
//	n-global (level 1)                mounts: site1 → n-site1
//	   ▲ mTLS
//	n-site1  (level 2)                mounts: edge1 → n-edge1, edge2 → n-edge2
//	   ▲ mTLS          ▲ mTLS
//	n-edge1  (level 3) n-edge2        edge1 holds client m1, edge2 holds client m2
//
// Everything here is real: generated ed25519 keys, mTLS between the nodes,
// embedded MQTT brokers with paho clients as machines, Pebble data directories
// on disk. The topic strings asserted below are the specification of the
// NODE-to-node replication mount chain (still a rewrite: each ancestor
// inserts the child's mount into the path on the way up) — they are not
// negotiable. A client's own publish carries no rewrite any more: level 4 is
// always the node it is attached to, and the path is exactly what it sent.
//
// The suite is deliberately sequential (no t.Parallel): the nodes bind real
// ports and share a process-wide slog default.
package tests

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	pahomqtt "github.com/eclipse/paho.mqtt.golang"

	"github.com/alpamayo-solutions/colca/internal/authtest"
	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/identity"
	"github.com/alpamayo-solutions/colca/internal/node"
	"github.com/alpamayo-solutions/colca/internal/tokenauth/tokentest"
)

const tok = "test-admin-token"

// httpsClient accepts the nodes' self-signed API certs (pinning model, no CA).
var httpsClient = &http.Client{Transport: &http.Transport{
	TLSClientConfig: &tls.Config{InsecureSkipVerify: true, MinVersion: tls.VersionTLS13}, // #nosec G402 -- test
}}

type topo struct {
	global, site1, edge1, edge2 *node.Node
	cfgs                        map[string]*config.Config
	keys                        map[string]*identity.Identity
	dirs                        map[string]string
	m1, m2                      *authtest.Machine            // machines at edge1/edge2
	obs                         map[string]*authtest.Machine // per-node read-all observers, keyed by node ulid
	iss                         *tokentest.Issuer            // fake OIDC issuer every node trusts (human world)
}

// startTopo builds the whole topology top-down: a parent must be listening
// before its children can be configured with its resolved repl address.
func startTopo(t *testing.T) *topo {
	t.Helper()
	tp := &topo{cfgs: map[string]*config.Config{}, keys: map[string]*identity.Identity{},
		dirs: map[string]string{}, obs: map[string]*authtest.Machine{},
		iss: tokentest.NewIssuer(t)}
	base := t.TempDir()
	for _, n := range []string{"n-global", "n-site1", "n-edge1", "n-edge2"} {
		kp := filepath.Join(base, n+".key")
		id, err := identity.Generate(kp)
		if err != nil {
			t.Fatal(err)
		}
		tp.keys[n] = id
		tp.dirs[n] = filepath.Join(base, n+"-data")
	}
	// EVERY node runs a broker, including the two that have no machine attached:
	// the bus mirrors the store at every level, so a subscriber at the hub sees
	// the whole tree.
	mk := func(ulid string, parent *config.Parent) *config.Config {
		return &config.Config{
			ULID: ulid, DataDir: tp.dirs[ulid], LogLevel: "debug",
			KeyFile: filepath.Join(base, ulid+".key"),
			API:     config.API{Addr: "127.0.0.1:0", Token: tok},
			// The broker binds in node.Start and reports the resolved port as
			// Node.MQTTAddr — never read cfg.MQTT.Addr, it stays "127.0.0.1:0".
			MQTT:   config.Endpoint{Addr: "127.0.0.1:0"},
			Repl:   config.Endpoint{Addr: "127.0.0.1:0"},
			Parent: parent,
			// Every node trusts the same fake issuer and opens both human
			// doors (human-authz §4/§5.1) — the human contract is tested at
			// every level of the tree.
			Auth: &config.Auth{Issuer: tp.iss.Iss(), Audience: tp.iss.Aud(), JWKSURL: tp.iss.JWKSURL()},
			MQTTHuman: config.MQTTHuman{
				TCPAddr: "127.0.0.1:0",
				WSAddr:  "127.0.0.1:0",
			},
		}
	}
	// Every node gets a read-all `observer` identity (may subscribe to
	// everything via its read:# grant, can never publish; its own placement at
	// "observer" carries no extra scope beyond that grant). The two edges each
	// mount one machine besides. Enrollment happens AFTER a node starts —
	// entry-before-connect is the contract, and the registry is runtime state,
	// not config.
	enrollObserver := func(n *node.Node) {
		o := authtest.NewMachine(t, "observer")
		authtest.EnrollAt(t, n.Registry, n.Engine, o, "observer", "read:#")
		tp.obs[n.Cfg.ULID] = o
	}

	gcfg := mk("n-global", nil)
	g, err := node.Start(gcfg)
	if err != nil {
		t.Fatal(err)
	}
	tp.global, tp.cfgs["n-global"] = g, gcfg
	enrollObserver(g)
	authtest.EnrollNodeAt(t, g.Registry, g.Engine, "n-site1", tp.keys["n-site1"].PublicHex(), "site1")

	scfg := mk("n-site1",
		&config.Parent{URL: "https://" + g.ReplAddr, Pubkey: tp.keys["n-global"].PublicHex()})
	s, err := node.Start(scfg)
	if err != nil {
		t.Fatal(err)
	}
	tp.site1, tp.cfgs["n-site1"] = s, scfg
	enrollObserver(s)
	authtest.EnrollNodeAt(t, s.Registry, s.Engine, "n-edge1", tp.keys["n-edge1"].PublicHex(), "edge1")
	authtest.EnrollNodeAt(t, s.Registry, s.Engine, "n-edge2", tp.keys["n-edge2"].PublicHex(), "edge2")

	e1cfg := mk("n-edge1",
		&config.Parent{URL: "https://" + s.ReplAddr, Pubkey: tp.keys["n-site1"].PublicHex()})
	e1, err := node.Start(e1cfg)
	if err != nil {
		t.Fatal(err)
	}
	tp.edge1, tp.cfgs["n-edge1"] = e1, e1cfg
	enrollObserver(e1)
	tp.m1 = authtest.NewMachine(t, "m1")
	authtest.EnrollAt(t, e1.Registry, e1.Engine, tp.m1, "m1", "write:"+authtest.ElementID("m1")+"/#")

	e2cfg := mk("n-edge2",
		&config.Parent{URL: "https://" + s.ReplAddr, Pubkey: tp.keys["n-site1"].PublicHex()})
	e2, err := node.Start(e2cfg)
	if err != nil {
		t.Fatal(err)
	}
	tp.edge2, tp.cfgs["n-edge2"] = e2, e2cfg
	enrollObserver(e2)
	tp.m2 = authtest.NewMachine(t, "m2")
	authtest.EnrollAt(t, e2.Registry, e2.Engine, tp.m2, "m2", "write:"+authtest.ElementID("m2")+"/#")

	t.Cleanup(func() { e2.Stop(); e1.Stop(); s.Stop(); g.Stop() })

	// Every non-root node must know its root-frame prefix before tests run
	// (cmdadmin design §3): human grants are root-frame, and a node that has
	// not yet learned its prefix fails closed on scoped grants — a legitimate
	// startup state, but a race in tests. The first downlink poll teaches it.
	for ulid, n := range map[string]*node.Node{"n-site1": s, "n-edge1": e1, "n-edge2": e2} {
		waitFor(t, ulid+" learns its prefix", 15*time.Second, func() bool {
			_, ok := n.Engine.Prefix()
			return ok
		})
	}
	return tp
}

// ---- helpers ----

func api(t *testing.T, n *node.Node, method, path string, body any) map[string]any {
	t.Helper()
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal request body: %v", err)
		}
		rd = strings.NewReader(string(b))
	}
	req, err := http.NewRequest(method, "https://"+n.APIAddr+path, rd)
	if err != nil {
		t.Fatalf("build request %s %s: %v", method, path, err)
	}
	req.Header.Set("X-Colca-Token", tok)
	resp, err := httpsClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("%s %s: read body: %v", method, path, err)
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("%s %s → %d: body is not JSON (%v): %s", method, path, resp.StatusCode, err, raw)
	}
	if resp.StatusCode >= 300 {
		t.Fatalf("%s %s → %d: %v", method, path, resp.StatusCode, out)
	}
	return out
}

// awaitElement waits until n can RESOLVE the system element sitting at path —
// the fact a grant check consults, not merely the record being in its KV.
//
// A grant naming an element deep in the tree is inert at an ancestor until that
// element's record has replicated up to it (id-grants design §4): the ancestor
// places elements from its own subtree at their mount-inserted paths, and it
// cannot place one it has not received yet. Provisioning propagates; the wait
// is what makes a test observe that rather than race it.
//
// Resolvable and durable are two different moments, and the gap between them
// is real work: Engine.IngestReplicated commits the replicated batch to the
// store (KV included) and only THEN loops over the applied records calling
// ElementIndex.Observe. Polling /kv therefore returns as soon as the record is
// durable, while a grant naming that element still resolves to nothing —
// "no cmd grant covers …", PUBACK 0x87 — until Observe reaches it. Invisible
// on an idle machine, and 1 of 12 concurrent -race batches under load.
//
// That ordering is right: durable first, projection second. It is the waiting
// that was wrong. The index is strictly downstream of the durable write, so
// waiting on it implies the KV holds the record too — the reverse is what was
// never true. IDAt and PathOf read the same pair of maps, written together by
// one apply, so this is the same fact authorization asks for.
func awaitElement(t *testing.T, n *node.Node, path string) {
	t.Helper()
	waitFor(t, "the element at "+path+" to be resolvable at "+n.Cfg.ULID, 20*time.Second, func() bool {
		_, ok := n.Engine.Elements().IDAt(path)
		return ok
	})
}

func kvAt(t *testing.T, n *node.Node, prefix string) []any {
	t.Helper()
	out := api(t, n, "GET", "/kv?prefix="+prefix, nil)
	if out["entries"] == nil {
		return nil
	}
	return out["entries"].([]any)
}

// fetchRecords reads a stream through a named cursor. /fetch never advances a
// cursor, so re-reading with the same cursor name always yields the full set
// from its position and two different cursor names are independent.
func fetchRecords(t *testing.T, n *node.Node, stream, cursor, prefix string, max int) []any {
	t.Helper()
	path := fmt.Sprintf("/fetch?stream=%s&cursor=%s&max=%d", stream, cursor, max)
	if prefix != "" {
		// prefix filters the uns hierarchy path, not the raw topic.
		path += "&prefix=" + prefix
	}
	out := api(t, n, "GET", path, nil)
	return out["records"].([]any)
}

func waitFor(t *testing.T, desc string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for: %s", desc)
}

func machine(t *testing.T, addr string, m *authtest.Machine) pahomqtt.Client {
	t.Helper()
	opts := pahomqtt.NewClientOptions().AddBroker("ssl://" + addr).
		SetTLSConfig(m.TLSConfig()).
		SetClientID(m.ULID).SetUsername(m.ULID).SetConnectTimeout(5 * time.Second)
	c := pahomqtt.NewClient(opts)
	tk := c.Connect()
	if !tk.WaitTimeout(10*time.Second) || tk.Error() != nil {
		t.Fatalf("machine connect: %v", tk.Error())
	}
	t.Cleanup(func() { c.Disconnect(100) })
	return c
}

// observer connects a READ-ONLY client: it holds no write: grant, so the
// engine rejects everything it publishes ("write_denied"), but its read:#
// grant lets it subscribe to everything. This is how a backend service taps a
// node's bus.
func observer(t *testing.T, addr, clientID string, m *authtest.Machine) pahomqtt.Client {
	t.Helper()
	opts := pahomqtt.NewClientOptions().AddBroker("ssl://" + addr).
		SetTLSConfig(m.TLSConfig()).
		SetClientID(clientID).SetUsername(m.ULID).
		SetCleanSession(true).SetConnectTimeout(5 * time.Second)
	c := pahomqtt.NewClient(opts)
	tk := c.Connect()
	if !tk.WaitTimeout(10*time.Second) || tk.Error() != nil {
		t.Fatalf("observer %s connect: %v", clientID, tk.Error())
	}
	t.Cleanup(func() { c.Disconnect(100) })
	return c
}

// subscribeAll subscribes to filter and returns every message that arrives on
// it, in order, through a buffered channel.
func subscribeAll(t *testing.T, c pahomqtt.Client, filter string) <-chan pahomqtt.Message {
	t.Helper()
	msgs := make(chan pahomqtt.Message, 256)
	tk := c.Subscribe(filter, 1, func(_ pahomqtt.Client, m pahomqtt.Message) { msgs <- m })
	if !tk.WaitTimeout(10*time.Second) || tk.Error() != nil {
		t.Fatalf("subscribe %s: %v", filter, tk.Error())
	}
	return msgs
}

// awaitTopic waits for a message on the exact topic and returns it. Anything
// else on the same filter is ignored (but see collectFor for the negative
// assertions, which must look at everything).
func awaitTopic(t *testing.T, msgs <-chan pahomqtt.Message, topic string, d time.Duration) pahomqtt.Message {
	t.Helper()
	deadline := time.After(d)
	var seen []string
	for {
		select {
		case m := <-msgs:
			if m.Topic() == topic {
				return m
			}
			seen = append(seen, m.Topic())
		case <-deadline:
			t.Fatalf("timeout waiting for %q on the bus; saw %v", topic, seen)
		}
	}
}

// collectFor drains everything that arrives within d — used for "must NOT be
// delivered" assertions, which can only be made after giving it time to arrive.
func collectFor(msgs <-chan pahomqtt.Message, d time.Duration) []pahomqtt.Message {
	deadline := time.After(d)
	var out []pahomqtt.Message
	for {
		select {
		case m := <-msgs:
			out = append(out, m)
		case <-deadline:
			return out
		}
	}
}

func mustJSON(v any) string { b, _ := json.Marshal(v); return string(b) }

// ---- tests ----

// TestUplinkMountChainAndKV pins the exact stored topic at every level of the
// chain and that the originating node id survives all three hops.
func TestUplinkMountChainAndKV(t *testing.T) {
	tp := startTopo(t)
	m1 := machine(t, tp.edge1.MQTTAddr, tp.m1)
	tk := m1.Publish("colca/v1/_Metric/n-edge1/m1/temp", 1, false, `{"v": 21.5}`)
	tk.WaitTimeout(5 * time.Second)

	waitFor(t, "metric at global", 10*time.Second, func() bool {
		return len(kvAt(t, tp.global, "site1/edge1/m1/temp")) == 1
	})
	entries := kvAt(t, tp.global, "site1/edge1/m1/temp")
	e := entries[0].(map[string]any)
	if e["topic"] != "colca/v1/_Metric/n-edge1/site1/edge1/m1/temp" {
		t.Fatalf("topic at global: %v", e["topic"])
	}
	if e["node_id"] != "n-edge1" {
		t.Fatalf("provenance lost: %v", e["node_id"])
	}
	// intermediate views
	waitFor(t, "metric at edge1", 10*time.Second, func() bool {
		return len(kvAt(t, tp.edge1, "m1/temp")) == 1
	})
	edgeEntry := kvAt(t, tp.edge1, "m1/temp")[0].(map[string]any)
	if edgeEntry["topic"] != "colca/v1/_Metric/n-edge1/m1/temp" {
		t.Fatalf("edge view: %v", edgeEntry["topic"])
	}
	if edgeEntry["node_id"] != "n-edge1" {
		t.Fatalf("provenance lost at edge1: %v", edgeEntry["node_id"])
	}
	waitFor(t, "metric at site1", 10*time.Second, func() bool {
		return len(kvAt(t, tp.site1, "edge1/m1/temp")) == 1
	})
	siteEntry := kvAt(t, tp.site1, "edge1/m1/temp")[0].(map[string]any)
	if siteEntry["topic"] != "colca/v1/_Metric/n-edge1/edge1/m1/temp" {
		t.Fatalf("site view: %v", siteEntry["topic"])
	}
	if siteEntry["node_id"] != "n-edge1" {
		t.Fatalf("provenance lost at site1: %v", siteEntry["node_id"])
	}
}

// TestTwoEdgesFanIn proves two sibling edges land side by side under the same
// site without their mounts interfering.
func TestTwoEdgesFanIn(t *testing.T) {
	tp := startTopo(t)
	m1 := machine(t, tp.edge1.MQTTAddr, tp.m1)
	m2 := machine(t, tp.edge2.MQTTAddr, tp.m2)
	m1.Publish("colca/v1/_Metric/n-edge1/m1/temp", 1, false, `{"v": 1}`).WaitTimeout(5 * time.Second)
	m2.Publish("colca/v1/_Metric/n-edge2/m2/temp", 1, false, `{"v": 2}`).WaitTimeout(5 * time.Second)
	waitFor(t, "both edges at global", 10*time.Second, func() bool {
		return len(kvAt(t, tp.global, "site1/edge1/m1/temp")) == 1 &&
			len(kvAt(t, tp.global, "site1/edge2/m2/temp")) == 1
	})
}

// TestDownlinkCommandAckRoundtrip walks a command down two mount-stripping hops
// to the machine and its ack back up three mount-inserting hops.
func TestDownlinkCommandAckRoundtrip(t *testing.T) {
	tp := startTopo(t)
	m1 := machine(t, tp.edge1.MQTTAddr, tp.m1)

	received := make(chan pahomqtt.Message, 1)
	m1.Subscribe("colca/v1/_CmdParam/+/m1/#", 1, func(_ pahomqtt.Client, msg pahomqtt.Message) {
		received <- msg
	}).WaitTimeout(5 * time.Second)

	api(t, tp.global, "POST", "/publish", map[string]any{
		"topic":   "colca/v1/_CmdParam/m1/site1/edge1/m1/set-speed",
		"payload": map[string]any{"correlation_id": "corr-1", "expires_at": float64(time.Now().Add(time.Hour).UnixMilli()), "params": map[string]any{"speed": 5}},
	})

	var msg pahomqtt.Message
	select {
	case msg = <-received:
	case <-time.After(15 * time.Second):
		t.Fatal("command never reached the machine")
	}
	if msg.Topic() != "colca/v1/_CmdParam/m1/m1/set-speed" {
		t.Fatalf("machine-local topic: %s", msg.Topic())
	}

	// machine acks
	m1.Publish("colca/v1/_Ack/n-edge1/m1/set-speed", 1, false, `{"correlation_id":"corr-1","result_code":200,"message":"done"}`).WaitTimeout(5 * time.Second)

	waitFor(t, "ack visible at global", 10*time.Second, func() bool {
		for _, r := range fetchRecords(t, tp.global, "commands", "acktest", "", 100) {
			rec := r.(map[string]any)
			if rec["topic"] == "colca/v1/_Ack/n-edge1/site1/edge1/m1/set-speed" {
				var p map[string]any
				if err := json.Unmarshal([]byte(mustJSON(rec["payload"])), &p); err != nil {
					return false
				}
				return p["correlation_id"] == "corr-1" && p["result_code"].(float64) == 200
			}
		}
		return false
	})
}

// TestOfflineBufferingCatchupOrder cuts the middle of the chain, proves nothing
// leaks past the gap, and that the buffer drains exactly once and in order.
func TestOfflineBufferingCatchupOrder(t *testing.T) {
	tp := startTopo(t)
	m1 := machine(t, tp.edge1.MQTTAddr, tp.m1)

	// take the site down → edge1 is offline towards its parent
	tp.site1.Stop()
	time.Sleep(300 * time.Millisecond)

	for i := 1; i <= 50; i++ {
		m1.Publish("colca/v1/_Metric/n-edge1/m1/seq", 1, false, fmt.Sprintf(`{"v": %d}`, i)).WaitTimeout(5 * time.Second)
	}
	// edge buffered locally, global saw nothing
	if len(kvAt(t, tp.global, "site1/edge1/m1/seq")) != 0 {
		t.Fatal("global must not see data while site is down")
	}

	// restart site on the SAME data dir and SAME repl address
	scfg := tp.cfgs["n-site1"]
	scfg.Repl.Addr = tp.site1.ReplAddr // reuse resolved addr so edge reconnects
	s2, err := node.Start(scfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s2.Stop)
	tp.site1 = s2

	waitFor(t, "catch-up complete at global", 30*time.Second, func() bool {
		return len(fetchRecords(t, tp.global, "metrics", "catchup", "site1/edge1/m1/seq", 500)) >= 50
	})
	recs := fetchRecords(t, tp.global, "metrics", "catchup2", "site1/edge1/m1/seq", 500)
	if len(recs) != 50 {
		t.Fatalf("want exactly 50 (no dupes), got %d", len(recs))
	}
	for i, r := range recs { // strict order
		var p map[string]any
		if err := json.Unmarshal([]byte(mustJSON(r.(map[string]any)["payload"])), &p); err != nil {
			t.Fatalf("payload %d: %v", i, err)
		}
		if int(p["v"].(float64)) != i+1 {
			t.Fatalf("order broken at %d: %v", i, p)
		}
	}
}

// TestCommandExpiryDuringOffline proves the command queue never drops or
// reorders anything while the path is down: expiry is decided at execution
// time, by the machine, not by the queue.
func TestCommandExpiryDuringOffline(t *testing.T) {
	tp := startTopo(t)
	m1 := machine(t, tp.edge1.MQTTAddr, tp.m1)
	acked := make(chan string, 2)
	m1.Subscribe("colca/v1/_CmdParam/+/m1/#", 1, func(_ pahomqtt.Client, msg pahomqtt.Message) {
		var cmd map[string]any
		if err := json.Unmarshal(msg.Payload(), &cmd); err != nil {
			return
		}
		code := 200
		if int64(cmd["expires_at"].(float64)) < time.Now().UnixMilli() {
			code = 498
		}
		parts := strings.Split(msg.Topic(), "/")
		name := parts[len(parts)-1]
		ack, _ := json.Marshal(map[string]any{"correlation_id": cmd["correlation_id"], "result_code": code, "message": name})
		m1.Publish("colca/v1/_Ack/n-edge1/m1/"+name, 1, false, ack)
		acked <- fmt.Sprintf("%s:%d", cmd["correlation_id"], code)
	}).WaitTimeout(5 * time.Second)

	tp.site1.Stop()
	time.Sleep(300 * time.Millisecond)

	// short-TTL command (expires in 1s) and long-TTL command, issued while path is down
	api(t, tp.global, "POST", "/publish", map[string]any{
		"topic":   "colca/v1/_CmdParam/m1/site1/edge1/m1/short",
		"payload": map[string]any{"correlation_id": "c-short", "expires_at": float64(time.Now().Add(1 * time.Second).UnixMilli())}})
	api(t, tp.global, "POST", "/publish", map[string]any{
		"topic":   "colca/v1/_CmdParam/m1/site1/edge1/m1/long",
		"payload": map[string]any{"correlation_id": "c-long", "expires_at": float64(time.Now().Add(1 * time.Hour).UnixMilli())}})

	time.Sleep(2 * time.Second) // let the short one expire

	scfg := tp.cfgs["n-site1"]
	scfg.Repl.Addr = tp.site1.ReplAddr
	s2, err := node.Start(scfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s2.Stop)

	got := map[string]bool{}
	for i := 0; i < 2; i++ {
		select {
		case a := <-acked:
			got[a] = true
		case <-time.After(30 * time.Second):
			t.Fatalf("acks incomplete after replay: %v", got)
		}
	}
	if !got["c-short:498"] || !got["c-long:200"] {
		t.Fatalf("expiry semantics wrong: %v (want c-short:498 expired-skip, c-long:200 executed)", got)
	}
}

// TestAuthRejections pins the three write-side rejections. A rejected publish
// gets no PUBACK under MQTT 3.1.1 (mochi drops the packet), so every assertion
// here is on the STORE, never on the publish token.
func TestAuthRejections(t *testing.T) {
	tp := startTopo(t)
	// un-enrolled key → CONNECT refused (the key IS the credential now)
	evil := authtest.NewMachine(t, "m1") // right name, WRONG key
	opts := pahomqtt.NewClientOptions().AddBroker("ssl://" + tp.edge1.MQTTAddr).
		SetTLSConfig(evil.TLSConfig()).
		SetClientID("evil").SetUsername("m1").SetConnectTimeout(3 * time.Second)
	c := pahomqtt.NewClient(opts)
	tk := c.Connect()
	tk.WaitTimeout(5 * time.Second)
	if tk.Error() == nil {
		t.Fatal("un-enrolled key must be refused")
	}

	// write-scope violation: m1 tries to write under m2's zone → not persisted
	// anywhere. Level 4 is always this node's own ULID now, so there is no more
	// "foreign identity at level 4" to spoof — the write-scope check is what
	// keeps m1 out of m2's zone.
	m1 := machine(t, tp.edge1.MQTTAddr, tp.m1)
	m1.Publish("colca/v1/_Metric/n-edge1/m2/temp", 1, false, `{"v": 666}`).WaitTimeout(5 * time.Second)
	time.Sleep(1 * time.Second)
	if got := kvAt(t, tp.edge1, "m2/temp"); len(got) != 0 {
		t.Fatalf("out-of-scope publish persisted: %v", got)
	}

	// invalid payload → not persisted
	m1.Publish("colca/v1/_Metric/n-edge1/m1/temp", 1, false, `{"v":"not-a-number"}`).WaitTimeout(5 * time.Second)
	time.Sleep(1 * time.Second)
	if len(kvAt(t, tp.edge1, "m1/temp")) != 0 {
		t.Fatal("invalid payload persisted")
	}
}

// TestRestartDurabilityEdge restarts the leaf on its own data dir: offsets must
// survive so the parent neither loses the 11th record nor re-applies the first
// ten.
func TestRestartDurabilityEdge(t *testing.T) {
	tp := startTopo(t)
	m1 := machine(t, tp.edge1.MQTTAddr, tp.m1)
	for i := 1; i <= 10; i++ {
		m1.Publish("colca/v1/_Metric/n-edge1/m1/d", 1, false, fmt.Sprintf(`{"v": %d}`, i)).WaitTimeout(5 * time.Second)
	}
	waitFor(t, "10 at global", 15*time.Second, func() bool {
		return len(fetchRecords(t, tp.global, "metrics", "dur", "site1/edge1/m1/d", 100)) == 10
	})
	m1.Disconnect(100)
	tp.edge1.Stop()

	e1cfg := tp.cfgs["n-edge1"]
	e1b, err := node.Start(e1cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(e1b.Stop)
	m1b := machine(t, e1b.MQTTAddr, tp.m1)
	m1b.Publish("colca/v1/_Metric/n-edge1/m1/d", 1, false, `{"v": 11}`).WaitTimeout(5 * time.Second)
	waitFor(t, "11th after restart, no dupes", 15*time.Second, func() bool {
		return len(fetchRecords(t, tp.global, "metrics", "dur2", "site1/edge1/m1/d", 100)) == 11
	})
}

// TestHubMQTTMirrorsWholeTree is the point of mirroring the store onto the bus:
// a subscriber at the GLOBAL node sees the whole plant live, under the topic the
// global node stored — three mount insertions away from what m1 published.
func TestHubMQTTMirrorsWholeTree(t *testing.T) {
	tp := startTopo(t)
	obs := observer(t, tp.global.MQTTAddr, "hub-observer", tp.obs["n-global"])
	msgs := subscribeAll(t, obs, "colca/#")

	m1 := machine(t, tp.edge1.MQTTAddr, tp.m1)
	m1.Publish("colca/v1/_Metric/n-edge1/m1/temp", 1, false, `{"v": 21.5}`).WaitTimeout(5 * time.Second)

	msg := awaitTopic(t, msgs, "colca/v1/_Metric/n-edge1/site1/edge1/m1/temp", 15*time.Second)
	var p map[string]any
	if err := json.Unmarshal(msg.Payload(), &p); err != nil {
		t.Fatalf("hub bus payload is not JSON: %v (%s)", err, msg.Payload())
	}
	if p["v"].(float64) != 21.5 {
		t.Fatalf("hub bus payload: %v", p)
	}
}

// TestHTTPPublishVisibleOnMQTT: a record that enters through the HTTP API is on
// the bus like any other — "even when I publish over HTTP I want to see it in
// MQTT".
func TestHTTPPublishVisibleOnMQTT(t *testing.T) {
	tp := startTopo(t)
	obs := observer(t, tp.global.MQTTAddr, "http-observer", tp.obs["n-global"])
	msgs := subscribeAll(t, obs, "colca/#")

	topic := "colca/v1/_CmdParam/m1/site1/edge1/m1/set-speed"
	api(t, tp.global, "POST", "/publish", map[string]any{
		"topic":   topic,
		"payload": map[string]any{"correlation_id": "http-1", "expires_at": float64(time.Now().Add(time.Hour).UnixMilli())},
	})

	msg := awaitTopic(t, msgs, topic, 15*time.Second)
	var p map[string]any
	if err := json.Unmarshal(msg.Payload(), &p); err != nil {
		t.Fatalf("payload is not JSON: %v (%s)", err, msg.Payload())
	}
	if p["correlation_id"] != "http-1" {
		t.Fatalf("payload: %v", p)
	}
}

// TestRetainedSetEqualsKVView pins "the bus mirrors the store" as a single set
// equality instead of spot checks: after a mix of data/entity publishes (one
// path overwritten) and a command/ack round-trip, the retained set a fresh
// subscriber receives and the node's KV view must be equal as sets — same
// topics AND same payloads — and no command/ack topic may ever be retained.
func TestRetainedSetEqualsKVView(t *testing.T) {
	tp := startTopo(t)
	m1 := machine(t, tp.edge1.MQTTAddr, tp.m1)

	// state traffic: three metric paths (temp published twice — only the last
	// value is state) and one entity.
	cmds := subscribeAll(t, m1, "colca/v1/_CmdParam/+/m1/#")
	m1.Publish("colca/v1/_Metric/n-edge1/m1/temp", 1, false, `{"v": 1}`).WaitTimeout(5 * time.Second)
	m1.Publish("colca/v1/_Metric/n-edge1/m1/temp", 1, false, `{"v": 2}`).WaitTimeout(5 * time.Second)
	m1.Publish("colca/v1/_Metric/n-edge1/m1/rpm", 1, false, `{"v": 900}`).WaitTimeout(5 * time.Second)
	m1.Publish("colca/v1/_Signal/n-edge1/m1/cfg", 1, false, `{"id":"sig-1"}`).WaitTimeout(5 * time.Second)

	// command/ack traffic through the same node's streams
	api(t, tp.global, "POST", "/publish", map[string]any{
		"topic":   "colca/v1/_CmdParam/m1/site1/edge1/m1/set-speed",
		"payload": map[string]any{"correlation_id": "kv-eq-1", "expires_at": float64(time.Now().Add(time.Hour).UnixMilli())},
	})
	awaitTopic(t, cmds, "colca/v1/_CmdParam/m1/m1/set-speed", 20*time.Second)
	m1.Publish("colca/v1/_Ack/n-edge1/m1/set-speed", 1, false, `{"correlation_id":"kv-eq-1","result_code":200}`).WaitTimeout(5 * time.Second)

	// settle: all three state paths in KV (plus the two _EnrolledIdentity registry
	// entities enrollment wrote, the two _SystemElement records m1 and the
	// observer each bind to — a machine must be placed now, so the observer's
	// own enrollment authors one too — and the node's own _Node, which
	// it authored on learning its position: all state like any other entity),
	// the ack in the commands stream
	waitFor(t, "state and ack persisted at edge1", 10*time.Second, func() bool {
		if len(kvAt(t, tp.edge1, "")) != 8 {
			return false
		}
		for _, r := range fetchRecords(t, tp.edge1, "commands", "kv-eq-settle", "", 100) {
			if r.(map[string]any)["topic"] == "colca/v1/_Ack/n-edge1/m1/set-speed" {
				return true
			}
		}
		return false
	})

	// the KV view: topic → payload (payload normalized through JSON)
	kvView := map[string]string{}
	for _, e := range kvAt(t, tp.edge1, "") {
		entry := e.(map[string]any)
		kvView[entry["topic"].(string)] = mustJSON(entry["payload"])
	}

	// the retained set: everything a brand-new subscriber receives with no new
	// publish happening. collectFor is the honest form — the set is complete
	// only after nothing more arrives.
	fresh := observer(t, tp.edge1.MQTTAddr, "kv-eq-observer", tp.obs["n-edge1"])
	retained := map[string]string{}
	for _, m := range collectFor(subscribeAll(t, fresh, "colca/#"), 3*time.Second) {
		if strings.HasPrefix(m.Topic(), "colca/v1/_Cmd") || strings.HasPrefix(m.Topic(), "colca/v1/_Ack/") {
			t.Fatalf("command/ack topic %q was retained — commands are events, not state", m.Topic())
		}
		if !m.Retained() {
			t.Fatalf("fresh subscriber got a non-retained message on %q — nothing was published after it connected", m.Topic())
		}
		var p any
		if err := json.Unmarshal(m.Payload(), &p); err != nil {
			t.Fatalf("retained payload on %q is not JSON: %v (%s)", m.Topic(), err, m.Payload())
		}
		retained[m.Topic()] = mustJSON(p)
	}

	if len(retained) != len(kvView) {
		t.Fatalf("retained set and KV view differ in size: retained %v vs kv %v", retained, kvView)
	}
	for topic, kvPayload := range kvView {
		got, ok := retained[topic]
		if !ok {
			t.Fatalf("KV topic %q missing from the retained set %v", topic, retained)
		}
		if got != kvPayload {
			t.Fatalf("payload mismatch on %q: retained %s vs kv %s", topic, got, kvPayload)
		}
	}
	// the overwritten path must carry the LAST value in both views
	if got := retained["colca/v1/_Metric/n-edge1/m1/temp"]; got != `{"v":2}` {
		t.Fatalf("overwritten path retained stale state: %s (want {\"v\":2})", got)
	}
}

// TestRetainedDeliversCurrentStateOnConnect pins the two halves of the retain
// rule at once: state contracts are retained, so a brand-new subscriber gets the
// current value with no new publish happening; commands are NOT retained, so a
// command delivered before it connected is never replayed to it.
func TestRetainedDeliversCurrentStateOnConnect(t *testing.T) {
	tp := startTopo(t)
	m1 := machine(t, tp.edge1.MQTTAddr, tp.m1)

	// a command travels down and is delivered on edge1's bus BEFORE the fresh
	// subscriber exists
	cmds := subscribeAll(t, m1, "colca/v1/_CmdParam/+/m1/#")
	api(t, tp.global, "POST", "/publish", map[string]any{
		"topic":   "colca/v1/_CmdParam/m1/site1/edge1/m1/set-speed",
		"payload": map[string]any{"correlation_id": "retain-1", "expires_at": float64(time.Now().Add(time.Hour).UnixMilli())},
	})
	awaitTopic(t, cmds, "colca/v1/_CmdParam/m1/m1/set-speed", 20*time.Second)

	// metrics have flowed
	m1.Publish("colca/v1/_Metric/n-edge1/m1/temp", 1, false, `{"v": 33.25}`).WaitTimeout(5 * time.Second)
	waitFor(t, "metric stored at edge1", 10*time.Second, func() bool {
		return len(kvAt(t, tp.edge1, "m1/temp")) == 1
	})

	// BRAND NEW subscriber, no publish after this point
	fresh := observer(t, tp.edge1.MQTTAddr, "fresh-observer", tp.obs["n-edge1"])
	msgs := subscribeAll(t, fresh, "colca/#")

	got := collectFor(msgs, 3*time.Second)
	var metric pahomqtt.Message
	for _, m := range got {
		switch m.Topic() {
		case "colca/v1/_Metric/n-edge1/m1/temp":
			metric = m
		case "colca/v1/_CmdParam/m1/m1/set-speed":
			t.Fatal("a command was retained and replayed to a fresh subscriber — commands are events, not state")
		}
	}
	if metric == nil {
		var topics []string
		for _, m := range got {
			topics = append(topics, m.Topic())
		}
		t.Fatalf("fresh subscriber got no retained metric; saw %v", topics)
	}
	if !metric.Retained() {
		t.Fatal("the metric must arrive with the RETAINED flag — it is the current state, not a live publish")
	}
	var p map[string]any
	if err := json.Unmarshal(metric.Payload(), &p); err != nil {
		t.Fatalf("retained payload is not JSON: %v (%s)", err, metric.Payload())
	}
	if p["v"].(float64) != 33.25 {
		t.Fatalf("retained value is not the current one: %v", p)
	}
}

// A definition authored at the ROOT reaches a level-3 edge through two hops
// with no special routing, and arrives byte-identical: a node applies it to its
// own store, and its children read it from there (definition-stream design §5).
//
// This is the transitivity the design claims falls out of the transport rather
// than being built into it — nothing in the code knows how deep the tree is.
func TestDefinitionAuthoredAtTheRootReachesEveryLevel(t *testing.T) {
	tp := startTopo(t)
	const topic = "colca/v1/_Group/n-global/01HGRP-OPS"

	api(t, tp.global, "POST", "/publish", map[string]any{
		"topic": topic,
		"payload": map[string]any{
			"id": "01HGRP-OPS", "name": "Ops", "grants": []any{"read:el-edge1/#"},
		},
	})

	for _, n := range []*node.Node{tp.site1, tp.edge1, tp.edge2} {
		waitFor(t, "the definition to reach "+n.Cfg.ULID, 20*time.Second, func() bool {
			for _, e := range kvAt(t, n, "01HGRP-OPS") {
				if e.(map[string]any)["topic"] == topic {
					return true
				}
			}
			return false
		})
	}

	// Byte-identical at the bottom: no mount was inserted on the way down,
	// because a definition has no position to translate.
	recs := fetchRecords(t, tp.edge1, "definitions", unique("def-cursor"), "", 100)
	var seen []string
	for _, r := range recs {
		seen = append(seen, r.(map[string]any)["topic"].(string))
	}
	if len(seen) != 1 || seen[0] != topic {
		t.Fatalf("edge1's definitions stream = %v, want exactly %q unchanged", seen, topic)
	}
}

// TestResourceUpsertAtAChildPullsTheBlobFromItsParent is the wiring guard
// for node.go's SetFetcher call (resources design §3, §7.1): a blob staged
// only at the parent (n-site1), never at the child, must reach the child
// (n-edge1) as a side effect of resource/upsert executing there. This
// exercises the real path — node.Start's blobPort injection, ConfigExec's
// ensureBlob, BlobPort.Pull, the repl client's BlobGet — not just the
// BlobPort unit in isolation. A getter would only prove the getter works;
// this proves the wiring the getter would have been for.
func TestResourceUpsertAtAChildPullsTheBlobFromItsParent(t *testing.T) {
	tp := startTopo(t)

	content := "press 3 manual, staged only at the parent"
	sha, _, err := tp.site1.Blobs.Put(strings.NewReader(content), "")
	if err != nil {
		t.Fatal(err)
	}

	// Denominator: the child must not already hold it — otherwise a
	// successful upsert below would prove nothing about the pull.
	if _, ok := tp.edge1.Blobs.Has(sha); ok {
		t.Fatalf("precondition: edge1 must not hold %s yet", sha)
	}

	corr := cmdAdmin(t, tp.edge1, "colca/v1/_CmdConfigure/n-edge1/resource/upsert", map[string]any{
		"resources": []any{map[string]any{
			"path": "m1/manual",
			"resource": map[string]any{
				"id": "01HRESOURCE1", "system_element_id": "el-m1",
				"filename": "manual.pdf", "content_type": "application/pdf",
				"size_bytes": len(content), "sha256": sha,
			},
		}},
	})
	awaitAdminAck(t, tp.edge1, "colca/v1/_Ack/n-edge1/resource/upsert", corr, 200)

	if _, ok := tp.edge1.Blobs.Has(sha); !ok {
		t.Fatalf("edge1 must hold %s after resource/upsert pulled it from its parent", sha)
	}
}

// TestANodeDescribesItselfWhereItsParentMountedIt pins the wiring of
// node.go's SetOnPosition hook. `_Node` is "authored by the node it
// describes" (the contract's words), and the one fact about itself a node
// cannot read from config is where it sits: the element its parent bound it
// to, which it learns from the ancestry the downlink hands down. So the
// record must appear at the ROOT, mount-inserted like any other entity, naming
// exactly the element the parent's enrollment created — without any operator
// running a bootstrap against the child. The root itself is bound to nothing
// above it and says so.
func TestANodeDescribesItselfWhereItsParentMountedIt(t *testing.T) {
	tp := startTopo(t)

	record := func(n *node.Node, prefix string) map[string]any {
		for _, e := range kvAt(t, n, prefix) {
			entry := e.(map[string]any)
			if strings.Contains(entry["topic"].(string), "/_Node/") {
				return entry["payload"].(map[string]any)
			}
		}
		return nil
	}

	// edge1 was enrolled at site1 with mount "edge1"; site1 at global with
	// "site1". Each describes itself bound to the element at its own mount.
	for _, tc := range []struct{ ulid, prefix, element string }{
		{"n-edge1", "site1/edge1/_colca/nodes/n-edge1", authtest.ElementID("edge1")},
		{"n-site1", "site1/_colca/nodes/n-site1", authtest.ElementID("site1")},
	} {
		waitFor(t, tc.ulid+"'s own record to reach the root", 20*time.Second, func() bool {
			p := record(tp.global, tc.prefix)
			return p != nil && p["root_system_element_id"] == tc.element
		})
		p := record(tp.global, tc.prefix)
		if p["id"] != tc.ulid || p["name"] != tc.ulid {
			t.Fatalf("%s's record = %v, want id and name %s (no configured name: the ulid)", tc.ulid, p, tc.ulid)
		}
	}

	p := record(tp.global, "_colca/nodes/n-global")
	if p == nil {
		t.Fatal("the root never described itself")
	}
	if root, _ := p["root_system_element_id"].(string); root != "" {
		t.Fatalf("the root is bound to %q, want nothing above itself", root)
	}
}
