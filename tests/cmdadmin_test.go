// CmdAdmin tree scenarios (cmdadmin design §10): remote enroll/revoke ride
// the commands stream down the real 3-level topology, execute at the target
// node, and ack back up — including through an offline window.
package tests

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/alpamayo-solutions/colca/internal/authtest"
	"github.com/alpamayo-solutions/colca/internal/node"
)

// cmdAdmin publishes an admin command at n (admin token) and returns corr.
func cmdAdmin(t *testing.T, n *node.Node, topic string, fields map[string]any) string {
	t.Helper()
	corr := unique("adm")
	fields["correlation_id"] = corr
	if _, ok := fields["expires_at"]; !ok {
		fields["expires_at"] = time.Now().Add(time.Hour).UnixMilli()
	}
	api(t, n, "POST", "/publish", map[string]any{"topic": topic, "payload": fields})
	return corr
}

// awaitAdminAck polls n's commands stream for the verb ack with corr and
// returns its payload JSON.
func awaitAdminAck(t *testing.T, n *node.Node, ackTopic, corr string, code int) {
	t.Helper()
	waitFor(t, fmt.Sprintf("ack %s (corr %s, code %d)", ackTopic, corr, code), 20*time.Second, func() bool {
		for _, r := range fetchRecords(t, n, "commands", unique("adm-ack"), "", 500) {
			rec := r.(map[string]any)
			if rec["topic"] != ackTopic {
				continue
			}
			body := mustJSON(rec["payload"])
			if strings.Contains(body, corr) && strings.Contains(body, fmt.Sprintf(`"result_code":%d`, code)) {
				return true
			}
		}
		return false
	})
}

// A machine enrolled REMOTELY from the hub connects at the edge and its data
// replicates all the way back up. Then a remote revoke kicks it.
func TestCmdAdminRemoteEnrollAndRevokeThroughTree(t *testing.T) {
	tp := startTopo(t)
	m9 := authtest.NewMachine(t, "m9")

	corr := cmdAdmin(t, tp.global, "colca/v1/_CmdAdmin/n-edge1/site1/edge1/enroll", map[string]any{
		"entry": map[string]any{"ulid": "m9", "pubkey": m9.Pubkey, "kind": "machine",
			"element": authtest.Place(t, tp.edge1.Engine, "m9")},
	})
	awaitAdminAck(t, tp.global, "colca/v1/_Ack/n-edge1/site1/edge1/enroll", corr, 200)

	c := machine(t, tp.edge1.MQTTAddr, m9)
	if tk := c.Publish("colca/v1/_Metric/m9/temp", 1, false, `{"v": 42}`); !tk.WaitTimeout(5 * time.Second) {
		t.Fatal("remote-enrolled machine publish: no PUBACK")
	}
	waitFor(t, "m9's metric in the hub KV", 15*time.Second, func() bool {
		return len(kvAt(t, tp.global, "site1/edge1/m9/temp")) == 1
	})

	// Remote revoke: ack 200, the live session dies, reconnect is refused.
	corr = cmdAdmin(t, tp.global, "colca/v1/_CmdAdmin/n-edge1/site1/edge1/revoke", map[string]any{"ulid": "m9"})
	awaitAdminAck(t, tp.global, "colca/v1/_Ack/n-edge1/site1/edge1/revoke", corr, 200)
	waitFor(t, "revoked m9 kicked from edge1's broker", 10*time.Second, func() bool {
		return !c.IsConnectionOpen()
	})

	// Idempotent re-revoke acks 200 as well ("already revoked").
	corr = cmdAdmin(t, tp.global, "colca/v1/_CmdAdmin/n-edge1/site1/edge1/revoke", map[string]any{"ulid": "m9"})
	awaitAdminAck(t, tp.global, "colca/v1/_Ack/n-edge1/site1/edge1/revoke", corr, 200)
}

// The flagship (design §10): the enroll is issued while the target is DOWN,
// waits in the durable commands stream, and executes on catch-up.
func TestCmdAdminExecutesAfterOfflineCatchup(t *testing.T) {
	tp := startTopo(t)
	m9 := authtest.NewMachine(t, "m9")
	// The element m9 will bind to is placed while edge1 is still up: the
	// enrollment names it, and it must be there when the command executes.
	element := authtest.Place(t, tp.edge1.Engine, "m9")

	tp.edge1.Stop()
	time.Sleep(300 * time.Millisecond)

	corr := cmdAdmin(t, tp.global, "colca/v1/_CmdAdmin/n-edge1/site1/edge1/enroll", map[string]any{
		"entry": map[string]any{"ulid": "m9", "pubkey": m9.Pubkey, "kind": "machine", "element": element},
	})

	// While the target is down there is no ack — the command waits durably.
	time.Sleep(1 * time.Second)
	for _, r := range fetchRecords(t, tp.global, "commands", unique("no-ack"), "", 500) {
		rec := r.(map[string]any)
		if rec["topic"] == "colca/v1/_Ack/n-edge1/site1/edge1/enroll" && strings.Contains(mustJSON(rec["payload"]), corr) {
			t.Fatal("ack arrived while the target node was down")
		}
	}

	// Restart edge1 on the same data dir; catch-up executes the command.
	e1cfg := tp.cfgs["n-edge1"]
	e1, err := node.Start(e1cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { e1.Stop() })
	awaitAdminAck(t, tp.global, "colca/v1/_Ack/n-edge1/site1/edge1/enroll", corr, 200)

	c := machine(t, e1.MQTTAddr, m9)
	if tk := c.Publish("colca/v1/_Metric/m9/temp", 1, false, `{"v": 7}`); !tk.WaitTimeout(5 * time.Second) {
		t.Fatal("post-catchup publish: no PUBACK")
	}
}

// An admin command that expires while its target is offline never executes:
// 498 ack on catch-up, no registry entry. (A live tree delivers in
// milliseconds, so expiry-in-flight needs the offline window.)
func TestCmdAdminExpiredNeverExecutes(t *testing.T) {
	tp := startTopo(t)
	m9 := authtest.NewMachine(t, "m9")
	// The element m9 will bind to is placed while edge1 is still up: the
	// enrollment names it, and it must be there when the command executes.
	element := authtest.Place(t, tp.edge1.Engine, "m9")

	tp.edge1.Stop()
	time.Sleep(300 * time.Millisecond)

	corr := cmdAdmin(t, tp.global, "colca/v1/_CmdAdmin/n-edge1/site1/edge1/enroll", map[string]any{
		"entry": map[string]any{"ulid": "m9", "pubkey": m9.Pubkey, "kind": "machine", "element": element},
		"expires_at": time.Now().Add(500 * time.Millisecond).UnixMilli(),
	})
	time.Sleep(700 * time.Millisecond) // now it is expired — and still undelivered

	e1, err := node.Start(tp.cfgs["n-edge1"])
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { e1.Stop() })
	awaitAdminAck(t, tp.global, "colca/v1/_Ack/n-edge1/site1/edge1/enroll", corr, 498)
	if _, ok := e1.Registry.Get("m9"); ok {
		t.Fatal("expired enroll must not create a registry entry")
	}
}

// Humans issue admin commands under the normal class authz (design §4): the
// grant is scoped to edge1's zone, so edge1 works and sibling edge2 is
// refused at the door.
func TestCmdAdminHumanIssuerZoneScoped(t *testing.T) {
	tp := startTopo(t)
	m9 := authtest.NewMachine(t, "m9")
	awaitElement(t, tp.global, "site1/edge1") // the grant's element must have reached the hub
	tok := tp.iss.Mint("site-admin", []string{"cmd:" + authtest.ElementID("edge1") + "/#:admin"}, time.Now().Add(5*time.Minute))
	c := human(t, tp.global, "ssl", "site-admin", tok)

	corr := unique("h-adm")
	payload := fmt.Sprintf(`{"correlation_id":%q,"expires_at":%d,"entry":{"ulid":"m9","pubkey":%q,"kind":"machine","element":%q}}`,
		corr, time.Now().Add(time.Hour).UnixMilli(), m9.Pubkey, authtest.Place(t, tp.edge1.Engine, "m9"))
	if tk := c.Publish("colca/v1/_CmdAdmin/n-edge1/site1/edge1/enroll", 1, false, payload); !tk.WaitTimeout(5 * time.Second) {
		t.Fatal("in-zone human admin command: no PUBACK")
	}
	awaitAdminAck(t, tp.global, "colca/v1/_Ack/n-edge1/site1/edge1/enroll", corr, 200)

	// Out of zone (edge2): refused at the door, nothing persisted.
	before := tp.global.Store.NextOffset("commands")
	c.Publish("colca/v1/_CmdAdmin/n-edge2/site1/edge2/enroll", 1, false, payload).WaitTimeout(time.Second)
	time.Sleep(500 * time.Millisecond)
	if got := tp.global.Store.NextOffset("commands"); got != before {
		t.Fatalf("out-of-zone admin command persisted: %d → %d", before, got)
	}
}
