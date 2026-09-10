// Human authorization on the live four-node tree: scoped reads over the token
// doors, commands through the tree, admin enrollment with a bearer token,
// expiry kicks, and machine and repl doors that still reject tokens.
package tests

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/packets"
	pahov5 "github.com/eclipse/paho.golang/paho"
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

// humanMQTT5Conn dials a node's token MQTT door and returns a connection whose
// packet writes are serialised.
//
// paho.golang writes one packet with several Write calls and only locks across
// them when the connection is a sync.Locker; a *tls.Conn is not. Its pinger
// sends the first PINGREQ right after connecting, from its own goroutine, and
// those two bytes could land inside a PUBLISH: the broker then saw a malformed
// packet and never acked, or the payload failed JSON validation.
func humanMQTT5Conn(t *testing.T, addr string) net.Conn {
	t.Helper()
	conn, err := tls.Dial("tcp", addr,
		&tls.Config{InsecureSkipVerify: true, MinVersion: tls.VersionTLS13}) // #nosec G402 -- test
	if err != nil {
		t.Fatalf("human publisher dial: %v", err)
	}
	return packets.NewThreadSafeConn(conn)
}

// humanPublisher uses the synchronous MQTT 5 client for PUBACK assertions: the
// deadline covers the exchange itself and the reason code is exposed.
func humanPublisher(t *testing.T, n *node.Node, sub, token string) *pahov5.Client {
	t.Helper()
	c := pahov5.NewClient(pahov5.ClientConfig{Conn: humanMQTT5Conn(t, n.MQTTHumanTCPAddr)})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ack, err := c.Connect(ctx, &pahov5.Connect{
		ClientID: fmt.Sprintf("h5-%s-%d", sub, time.Now().UnixNano()),
		Username: sub, UsernameFlag: true,
		Password: []byte(token), PasswordFlag: true,
		KeepAlive: 30, CleanStart: true,
		Properties: &pahov5.ConnectProperties{},
	})
	if err != nil || ack.ReasonCode != 0 {
		t.Fatalf("human publisher connect: ack=%v err=%v", ack, err)
	}
	t.Cleanup(func() { _ = c.Disconnect(&pahov5.Disconnect{ReasonCode: 0}) })
	return c
}

func publishHuman5(t *testing.T, c *pahov5.Client, topic, payload string) byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ack, err := c.Publish(ctx, &pahov5.Publish{
		Topic: topic, QoS: 1, Payload: []byte(payload),
	})
	// A refused publish returns an ack with a reason code and an error, so the ack
	// decides. No ack at all is the failure to report.
	if ack == nil {
		t.Fatalf("human publish %s: no PUBACK within the deadline: %v", topic, err)
	}
	return ack.ReasonCode
}

// bearer performs an HTTPS request with a Bearer token and returns the status.
func bearer(t *testing.T, n *node.Node, method, path, token, body string) (int, string) {
	t.Helper()
	resp := doRequest(t, httpsClient, method+" "+path, func() (*http.Request, error) {
		req, err := newRequest(method, "https://"+n.APIAddr+path, "", body)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+token)
		return req, nil
	})
	defer resp.Body.Close()
	buf := make([]byte, 4096)
	nread, _ := resp.Body.Read(buf)
	return resp.StatusCode, string(buf[:nread])
}

// A person at site1 with a read grant for edge1's subtree sees edge1's data on
// both token doors and cannot subscribe to edge2's.
func TestHumanScopedReadOnTree(t *testing.T) {
	tp := startTopo(t)
	tok := tp.iss.Mint("anna", []string{"read:" + authtest.ElementID("edge1") + "/#"}, time.Now().Add(5*time.Minute))

	for _, scheme := range []string{"ssl", "wss"} {
		t.Run(scheme, func(t *testing.T) {
			c := human(t, tp.site1, scheme, "anna", tok)
			msgs := subscribeAll(t, c, "colca/v1/_Metric/+/edge1/#")

			m1 := machine(t, tp.edge1.MQTTAddr, tp.m1)
			m1.Publish("colca/v1/_Metric/n-edge1/m1/temp", 1, false, `{"v": 9}`).WaitTimeout(5 * time.Second)
			awaitTopic(t, msgs, "colca/v1/_Metric/n-edge1/edge1/m1/temp", 15*time.Second)

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

// paho serialises a packet's writes only when the connection is a sync.Locker,
// so check exactly that. A repetition test would only pass on the runs where
// the race happened to be lost.
func TestTheHumanMQTT5ConnectionSerialisesPacketWrites(t *testing.T) {
	tp := startTopo(t)
	conn := humanMQTT5Conn(t, tp.global.MQTTHumanTCPAddr)
	t.Cleanup(func() { _ = conn.Close() })

	if _, ok := conn.(sync.Locker); !ok {
		t.Fatalf("the human MQTT-5 connection (%T) is not a sync.Locker, so paho writes a packet's "+
			"header, topic and payload without a lock — its pinger's immediate PINGREQ then lands "+
			"inside a PUBLISH, and the command is either refused as malformed or never acked", conn)
	}
}

// A human at the HUB with a cmd grant commands the leaf machine through two
// downlink hops; the machine's ack replicates back up.
func TestHumanCommandsThroughTree(t *testing.T) {
	tp := startTopo(t)
	m1 := machine(t, tp.edge1.MQTTAddr, tp.m1)
	cmds := subscribeAll(t, m1, "colca/v1/_CmdParam/+/m1/#")

	awaitElement(t, tp.global, "site1/edge1/m1") // the grants' elements must have reached the hub
	tok := tp.iss.Mint("operator-ole",
		[]string{"cmd:" + authtest.ElementID("m1") + "/#:param", "read:" + authtest.ElementID("edge1") + "/#"}, time.Now().Add(5*time.Minute))
	c := humanPublisher(t, tp.global, "operator-ole", tok)

	corr := unique("h-corr")
	payload := fmt.Sprintf(`{"correlation_id":%q,"expires_at":%d}`, corr, time.Now().Add(time.Hour).UnixMilli())
	if reason := publishHuman5(t, c,
		"colca/v1/_CmdParam/m1/site1/edge1/m1/set-speed", payload); reason != 0 {
		t.Fatalf("human command PUBACK reason = %#x, want success", reason)
	}

	msg := awaitTopic(t, cmds, "colca/v1/_CmdParam/m1/m1/set-speed", 20*time.Second)
	if !strings.Contains(string(msg.Payload()), corr) {
		t.Fatalf("command payload: %s", msg.Payload())
	}
	m1.Publish("colca/v1/_Ack/n-edge1/m1/set-speed", 1, false,
		fmt.Sprintf(`{"correlation_id":%q,"result_code":200}`, corr)).WaitTimeout(5 * time.Second)

	waitFor(t, "ack visible at global", 15*time.Second, func() bool {
		for _, r := range fetchRecords(t, tp.global, "commands", unique("h-ack"), "", 200) {
			rec := r.(map[string]any)
			if rec["topic"] == "colca/v1/_Ack/n-edge1/site1/edge1/m1/set-speed" &&
				strings.Contains(mustJSON(rec["payload"]), corr) {
				return true
			}
		}
		return false
	})

	// The same human may NOT command outside the granted zone.
	before := tp.global.Store.NextOffset("commands")
	if reason := publishHuman5(t, c,
		"colca/v1/_CmdParam/m2/site1/edge2/m2/set-speed", payload); reason != 0x87 {
		t.Fatalf("out-of-zone PUBACK reason = %#x, want not authorized", reason)
	}
	if got := tp.global.Store.NextOffset("commands"); got != before {
		t.Fatalf("out-of-zone human command persisted: %d → %d", before, got)
	}
}

// admin:# over a bearer token enrolls a machine at an edge, and the machine
// connects.
func TestHumanAdminEnrollsOverBearer(t *testing.T) {
	tp := startTopo(t)
	adminTok := tp.iss.Mint("boss", []string{"admin:#"}, time.Now().Add(5*time.Minute))

	m3 := authtest.NewMachine(t, "m3")
	entry := string(m3.EntryJSON(t, "external", authtest.Place(t, tp.edge1.Engine, "m3")))
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

	// The node sweeps expired sessions every 10s; wait past expiry plus one sweep.
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
