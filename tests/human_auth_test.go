// End-to-end human-authorization scenarios on the live 4-node tree
// (human-authz design §9 core level): scoped reads over the human doors,
// commands through the tree, admin:# enrollment over Bearer, expiry kicks,
// and the regression pin that machine/repl doors still reject tokens.
package tests

import (
	"crypto/tls"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	pahomqtt "github.com/eclipse/paho.mqtt.golang"

	"github.com/alpamayo-solutions/colca/internal/authtest"
	"github.com/alpamayo-solutions/colca/internal/node"
)

// unique returns a per-call unique suffix for cursor names and correlation ids.
func unique(prefix string) string { return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano()) }

// human connects a token-authenticated client to a node's human door.
// scheme: "ssl" (raw MQTT TCP) or "wss" (websocket).
func human(t *testing.T, n *node.Node, scheme, sub, token string) pahomqtt.Client {
	t.Helper()
	var url string
	switch scheme {
	case "ssl":
		url = "ssl://" + n.MQTTHumanTCPAddr
	case "wss":
		url = "wss://" + n.MQTTHumanWSAddr + "/"
	default:
		t.Fatalf("unknown scheme %q", scheme)
	}
	opts := pahomqtt.NewClientOptions().AddBroker(url).
		SetTLSConfig(&tls.Config{InsecureSkipVerify: true, MinVersion: tls.VersionTLS13}). // #nosec G402 -- test
		SetClientID(fmt.Sprintf("h-%s-%d", sub, time.Now().UnixNano())).
		SetUsername(sub).SetPassword(token).
		SetProtocolVersion(4).SetAutoReconnect(false).
		SetConnectTimeout(5 * time.Second)
	c := pahomqtt.NewClient(opts)
	tk := c.Connect()
	if !tk.WaitTimeout(10*time.Second) || tk.Error() != nil {
		t.Fatalf("human connect %s at %s: %v", sub, url, tk.Error())
	}
	t.Cleanup(func() { c.Disconnect(100) })
	return c
}

// bearer performs an HTTPS request with a Bearer token and returns the status.
func bearer(t *testing.T, n *node.Node, method, path, token, body string) (int, string) {
	t.Helper()
	req, err := newRequest(method, "https://"+n.APIAddr+path, "", body)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := httpsClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	buf := make([]byte, 4096)
	nread, _ := resp.Body.Read(buf)
	return resp.StatusCode, string(buf[:nread])
}

// A human at the SITE with a read grant for edge1's subtree sees edge1's
// replicated data live over BOTH human doors and cannot subscribe to edge2's.
func TestHumanScopedReadOnTree(t *testing.T) {
	tp := startTopo(t)
	tok := tp.iss.Mint("anna", []string{"read:edge1/#"}, time.Now().Add(5*time.Minute))

	for _, scheme := range []string{"ssl", "wss"} {
		t.Run(scheme, func(t *testing.T) {
			c := human(t, tp.site1, scheme, "anna", tok)
			msgs := subscribeAll(t, c, "colca/v1/_Metric/+/edge1/#")

			m1 := machine(t, tp.edge1.MQTTAddr, tp.m1)
			m1.Publish("colca/v1/_Metric/m1/temp", 1, false, `{"v": 9}`).WaitTimeout(5 * time.Second)
			awaitTopic(t, msgs, "colca/v1/_Metric/m1/edge1/m1/temp", 15*time.Second)

			// edge2's subtree is out of scope → SUBACK 0x80.
			dtok := c.Subscribe("colca/v1/+/+/edge2/#", 1, func(pahomqtt.Client, pahomqtt.Message) {})
			dtok.WaitTimeout(3 * time.Second)
			if st, ok := dtok.(*pahomqtt.SubscribeToken); ok {
				if qos, found := st.Result()["colca/v1/+/+/edge2/#"]; !found || qos != 0x80 {
					t.Fatalf("edge2 subscription must be denied, qos %v", qos)
				}
			}
		})
	}
}

// A human at the HUB with a cmd grant commands the leaf machine through two
// downlink hops; the machine's ack replicates back up.
func TestHumanCommandsThroughTree(t *testing.T) {
	tp := startTopo(t)
	m1 := machine(t, tp.edge1.MQTTAddr, tp.m1)
	cmds := subscribeAll(t, m1, "colca/v1/_CmdParam/+/m1/#")

	tok := tp.iss.Mint("operator-ole",
		[]string{"cmd:site1/edge1/m1/#:param", "read:site1/edge1/#"}, time.Now().Add(5*time.Minute))
	c := human(t, tp.global, "ssl", "operator-ole", tok)

	corr := unique("h-corr")
	payload := fmt.Sprintf(`{"correlation_id":%q,"expires_at":%d}`, corr, time.Now().Add(time.Hour).UnixMilli())
	if tk := c.Publish("colca/v1/_CmdParam/m1/site1/edge1/m1/set-speed", 1, false, payload); !tk.WaitTimeout(5 * time.Second) {
		t.Fatal("human command publish: no PUBACK")
	}

	msg := awaitTopic(t, cmds, "colca/v1/_CmdParam/m1/m1/set-speed", 20*time.Second)
	if !strings.Contains(string(msg.Payload()), corr) {
		t.Fatalf("command payload: %s", msg.Payload())
	}
	m1.Publish("colca/v1/_Ack/m1/set-speed", 1, false,
		fmt.Sprintf(`{"correlation_id":%q,"result_code":200}`, corr)).WaitTimeout(5 * time.Second)

	waitFor(t, "ack visible at global", 15*time.Second, func() bool {
		for _, r := range fetchRecords(t, tp.global, "commands", unique("h-ack"), "", 200) {
			rec := r.(map[string]any)
			if rec["topic"] == "colca/v1/_Ack/m1/site1/edge1/m1/set-speed" &&
				strings.Contains(mustJSON(rec["payload"]), corr) {
				return true
			}
		}
		return false
	})

	// The same human may NOT command outside the granted zone.
	before := tp.global.Store.NextOffset("commands")
	c.Publish("colca/v1/_CmdParam/m2/site1/edge2/m2/set-speed", 1, false, payload).WaitTimeout(time.Second)
	time.Sleep(500 * time.Millisecond)
	if got := tp.global.Store.NextOffset("commands"); got != before {
		t.Fatalf("out-of-zone human command persisted: %d → %d", before, got)
	}
}

// admin:# over Bearer enrolls a machine at an edge; the machine connects.
// Attributable admin: the action rides a token, not the shared secret.
func TestHumanAdminEnrollsOverBearer(t *testing.T) {
	tp := startTopo(t)
	adminTok := tp.iss.Mint("boss", []string{"admin:#"}, time.Now().Add(5*time.Minute))

	m3 := authtest.NewMachine(t, "m3")
	entry := string(m3.EntryJSON(t, "machine", "m3"))
	code, body := bearer(t, tp.edge1, "POST", "/enroll", adminTok, entry)
	if code != 200 {
		t.Fatalf("human admin enroll: %d %s", code, body)
	}
	c := machine(t, tp.edge1.MQTTAddr, m3)
	if !c.IsConnectionOpen() {
		t.Fatal("enrolled machine did not connect")
	}

	// Without admin:# the same call is 403.
	plainTok := tp.iss.Mint("anna", []string{"read:#"}, time.Now().Add(5*time.Minute))
	if code, _ := bearer(t, tp.edge1, "GET", "/enroll", plainTok, ""); code != http.StatusForbidden {
		t.Fatalf("non-admin human on /enroll: want 403, got %d", code)
	}
}

// Expiry kicks a live human session on a running tree.
func TestHumanExpiryKickOnTree(t *testing.T) {
	tp := startTopo(t)
	shortTok := tp.iss.Mint("anna", []string{"read:#"}, time.Now().Add(2*time.Second))
	c := human(t, tp.global, "ssl", "anna", shortTok)
	if !c.IsConnectionOpen() {
		t.Fatal("precondition: not connected")
	}

	// Drive the sweeper via wall clock: the node's own 10s ticker will fire;
	// poll with a deadline comfortably past exp + one sweep interval.
	deadline := time.Now().Add(20 * time.Second)
	for c.IsConnectionOpen() {
		if time.Now().After(deadline) {
			t.Fatal("expired human session not kicked within exp + sweep interval")
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Fresh token reconnects.
	c2 := human(t, tp.global, "ssl", "anna", tp.iss.Mint("anna", []string{"read:#"}, time.Now().Add(5*time.Minute)))
	c2.Disconnect(100)
}

// Regression pin: tokens are NOT credentials at the machine doors.
func TestTokensRejectedAtMachineDoors(t *testing.T) {
	tp := startTopo(t)
	tok := tp.iss.Mint("anna", []string{"read:#"}, time.Now().Add(5*time.Minute))

	opts := pahomqtt.NewClientOptions().AddBroker("ssl://" + tp.edge1.MQTTAddr).
		SetTLSConfig(&tls.Config{InsecureSkipVerify: true, MinVersion: tls.VersionTLS13}). // #nosec G402 -- test
		SetClientID("token-at-machine-door").SetUsername("anna").SetPassword(tok).
		SetProtocolVersion(4).SetConnectTimeout(3 * time.Second)
	c := pahomqtt.NewClient(opts)
	defer c.Disconnect(50)
	tk := c.Connect()
	tk.WaitTimeout(5 * time.Second)
	if tk.Error() == nil {
		t.Fatal("machine door accepted a token")
	}
}
