// End-to-end authorization scenarios on the live 4-node tree (auth design
// §12): unknown keys at every
// door, revocation kicks, fleet inventory with local-only authority, scoped
// subscriptions on replicated data, the enrollment-door-only rule for
// _EnrolledIdentity, and cmd grants.
package tests

import (
	"crypto/tls"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	pahomqtt "github.com/eclipse/paho.mqtt.golang"

	"github.com/alpamayo-solutions/colca/internal/authtest"
)

func newRequest(method, url, token, body string) (*http.Request, error) {
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, url, rd)
	if err != nil {
		return nil, err
	}
	if token != "" {
		req.Header.Set("X-Colca-Token", token)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	return req, nil
}

// newTLSClient builds an HTTPS client presenting the machine's key.
func newTLSClient(m *authtest.Machine) *http.Client {
	return &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{
		Certificates:       []tls.Certificate{m.Cert},
		InsecureSkipVerify: true, // #nosec G402 -- test, pinning model
		MinVersion:         tls.VersionTLS13,
	}}}
}

// tryMachine attempts an MQTT connect and returns the error (nil = connected).
func tryMachine(addr string, m *authtest.Machine) error {
	opts := pahomqtt.NewClientOptions().AddBroker("ssl://" + addr).
		SetTLSConfig(m.TLSConfig()).
		SetClientID(m.ULID + "-try").SetUsername(m.ULID).
		SetProtocolVersion(4).SetConnectTimeout(3 * time.Second)
	c := pahomqtt.NewClient(opts)
	defer c.Disconnect(50)
	tk := c.Connect()
	if !tk.WaitTimeout(5 * time.Second) {
		return errTimeout
	}
	return tk.Error()
}

var errTimeout = &timeoutErr{}

type timeoutErr struct{}

func (*timeoutErr) Error() string { return "connect timed out" }

// apiStatus performs a request and returns just the status code (no failing on
// non-2xx like api() does).
func apiStatus(t *testing.T, url, method, path, token string, body string) int {
	t.Helper()
	req, err := newRequest(method, url+path, token, body)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := httpsClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	resp.Body.Close()
	return resp.StatusCode
}

// TestAuthRejectionsAtEveryDoor: an un-enrolled key is turned away at the
// MQTT door and the HTTP door of every node it tries; the repl door of the
// parent rejects it too (covered at unit level; here the black-box check is
// that a stranger cannot read ANYTHING out of a live tree).
func TestAuthRejectionsAtEveryDoor(t *testing.T) {
	tp := startTopo(t)
	stranger := authtest.NewMachine(t, "stranger")

	for name, addr := range map[string]string{
		"edge1 mqtt": tp.edge1.MQTTAddr, "global mqtt": tp.global.MQTTAddr,
	} {
		if err := tryMachine(addr, stranger); err == nil {
			t.Fatalf("%s: un-enrolled key connected", name)
		}
	}
	// HTTP: a presented unknown cert never falls through to anything.
	strangerClient := newTLSClient(stranger)
	req, err := newRequest("GET", "https://"+tp.edge1.APIAddr+"/fetch?stream=metrics&cursor=stranger/c&max=1", "", "")
	if err != nil {
		t.Fatal(err)
	}
	resp, err := strangerClient.Do(req)
	if err != nil {
		t.Fatalf("stranger fetch: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != 401 {
		t.Fatalf("stranger fetch: want 401, got %d", resp.StatusCode)
	}
}

// TestRevocationKicksAcrossTheTree: revoking a leaf machine kicks its live
// session, blocks reconnects, and the retirement record replicates upward
// into the hub's entities history.
func TestRevocationKicksAcrossTheTree(t *testing.T) {
	tp := startTopo(t)
	m1 := machine(t, tp.edge1.MQTTAddr, tp.m1)
	if !m1.IsConnectionOpen() {
		t.Fatal("precondition: m1 not connected")
	}

	if _, _, err := tp.edge1.Registry.Revoke("m1"); err != nil {
		t.Fatalf("revoke: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for m1.IsConnectionOpen() {
		if time.Now().After(deadline) {
			t.Fatal("revoked m1 still connected after 5s")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err := tryMachine(tp.edge1.MQTTAddr, tp.m1); err == nil {
		t.Fatal("revoked key reconnected")
	}

	// The retirement (empty-payload _EnrolledIdentity) replicates upward like any
	// entity record: the hub's entities history shows it under the full path.
	waitFor(t, "retirement record at global", 15*time.Second, func() bool {
		for _, r := range fetchRecords(t, tp.global, "entities", "auth-revoke", "site1/edge1/_colca/identities/m1", 100) {
			rec := r.(map[string]any)
			if rec["topic"] == "colca/v1/_EnrolledIdentity/n-edge1/site1/edge1/_colca/identities/m1" && rec["payload"] == nil {
				return true
			}
		}
		return false
	})
}

// TestFleetInventoryAndLocalAuthority: an enrollment at a leaf is VISIBLE at
// the hub (security inventory via the replicated _EnrolledIdentity entity) but does NOT
// authenticate there — authority is local, delegation like DNS (§2.2).
func TestFleetInventoryAndLocalAuthority(t *testing.T) {
	tp := startTopo(t)

	// m1's registry entity replicates up under the mount-prefixed path.
	waitFor(t, "m1 inventory entry at global", 15*time.Second, func() bool {
		return len(kvAt(t, tp.global, "site1/edge1/_colca/identities/m1")) >= 1
	})
	entries := kvAt(t, tp.global, "site1/edge1/_colca/identities/m1")
	found := false
	for _, e := range entries {
		if e.(map[string]any)["topic"] == "colca/v1/_EnrolledIdentity/n-edge1/site1/edge1/_colca/identities/m1" {
			found = true
		}
	}
	if !found {
		t.Fatalf("m1's _EnrolledIdentity entity not in the hub inventory: %v", entries)
	}

	// But the hub does NOT authenticate m1's key: enrolled-at-leaf ≠ trusted-at-hub.
	if err := tryMachine(tp.global.MQTTAddr, tp.m1); err == nil {
		t.Fatal("leaf-enrolled key authenticated at the hub — authority must be local")
	}
}

// TestScopedSubscribeOnReplicatedTree: a machine at the SITE with a read
// grant for edge1's subtree sees edge1's replicated data on the site bus and
// cannot subscribe to edge2's.
func TestScopedSubscribeOnReplicatedTree(t *testing.T) {
	tp := startTopo(t)

	analyst := authtest.NewMachine(t, "analyst")
	authtest.EnrollAt(t, tp.site1.Registry, tp.site1.Engine, analyst, "analyst", "read:"+authtest.ElementID("edge1")+"/#")
	c := machine(t, tp.site1.MQTTAddr, analyst)

	// edge2's subtree is out of scope → SUBACK failure (0x80).
	tk := c.Subscribe("colca/v1/+/+/edge2/#", 1, func(pahomqtt.Client, pahomqtt.Message) {})
	tk.WaitTimeout(3 * time.Second)
	if st, ok := tk.(*pahomqtt.SubscribeToken); ok {
		if qos, found := st.Result()["colca/v1/+/+/edge2/#"]; !found || qos != 0x80 {
			t.Fatalf("edge2 subscription must be rejected, got qos %v", qos)
		}
	}

	// edge1's subtree is granted: live replicated data arrives.
	msgs := subscribeAll(t, c, "colca/v1/_Metric/+/edge1/#")
	m1 := machine(t, tp.edge1.MQTTAddr, tp.m1)
	m1.Publish("colca/v1/_Metric/n-edge1/m1/temp", 1, false, `{"v": 5}`).WaitTimeout(5 * time.Second)
	awaitTopic(t, msgs, "colca/v1/_Metric/n-edge1/edge1/m1/temp", 15*time.Second)
}

// TestEnrolledIdentityRejectedAtEveryOrdinaryDoor: no door but the enrollment
// endpoint accepts a registry contract — not the machine's MQTT publish, not
// the machine's HTTP publish, not even the admin's /publish.
func TestEnrolledIdentityRejectedAtEveryOrdinaryDoor(t *testing.T) {
	tp := startTopo(t)
	entitiesBefore := tp.edge1.Store.NextOffset("entities")

	// MQTT door (no PUBACK for rejected packets — assert on the store).
	m1 := machine(t, tp.edge1.MQTTAddr, tp.m1)
	m1.Publish("colca/v1/_EnrolledIdentity/m1/_colca/identities/m1", 1, false, `{"ulid":"m1"}`).WaitTimeout(time.Second)
	time.Sleep(500 * time.Millisecond)
	if got := tp.edge1.Store.NextOffset("entities"); got != entitiesBefore {
		t.Fatalf("_EnrolledIdentity via MQTT persisted: entities %d → %d", entitiesBefore, got)
	}

	// Admin /publish.
	code := apiStatus(t, "https://"+tp.edge1.APIAddr, "POST", "/publish", tok,
		`{"topic":"colca/v1/_EnrolledIdentity/n1/_colca/identities/x","payload":{"ulid":"x"}}`)
	if code != 422 {
		t.Fatalf("_EnrolledIdentity via admin /publish: want 422, got %d", code)
	}
	if got := tp.edge1.Store.NextOffset("entities"); got != entitiesBefore {
		t.Fatal("_EnrolledIdentity via admin /publish persisted")
	}
}

// TestCmdGrantEndToEnd: an HMI machine with a cmd grant commands m1 through
// the local node; m1 acks. Without the grant the command never persists.
func TestCmdGrantEndToEnd(t *testing.T) {
	tp := startTopo(t)

	hmi := authtest.NewMachine(t, "hmi")
	authtest.EnrollAt(t, tp.edge1.Registry, tp.edge1.Engine, hmi, "hmi",
		"cmd:"+authtest.ElementID("m1")+"/#:param", "read:"+authtest.ElementID("m1")+"/#")
	hc := machine(t, tp.edge1.MQTTAddr, hmi)

	m1 := machine(t, tp.edge1.MQTTAddr, tp.m1)
	cmds := subscribeAll(t, m1, "colca/v1/_CmdParam/+/m1/#")

	payload := `{"correlation_id":"hmi-1","expires_at":99999999999999}`
	tk := hc.Publish("colca/v1/_CmdParam/m1/m1/set-speed", 1, false, payload)
	if !tk.WaitTimeout(5 * time.Second) {
		t.Fatal("granted cmd publish: no PUBACK")
	}
	msg := awaitTopic(t, cmds, "colca/v1/_CmdParam/m1/m1/set-speed", 10*time.Second)
	if !strings.Contains(string(msg.Payload()), "hmi-1") {
		t.Fatalf("command payload: %s", msg.Payload())
	}

	// Without a covering grant (class outside): rejected, nothing persisted.
	before := tp.edge1.Store.NextOffset("commands")
	hc.Publish("colca/v1/_CmdMaintain/m1/m1/calibrate", 1, false,
		`{"correlation_id":"hmi-2","expires_at":99999999999999}`).WaitTimeout(time.Second)
	time.Sleep(500 * time.Millisecond)
	if got := tp.edge1.Store.NextOffset("commands"); got != before {
		t.Fatalf("ungranted cmd class persisted: commands %d → %d", before, got)
	}
}
