package tests

import (
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	pahomqtt "github.com/eclipse/paho.mqtt.golang"

	"github.com/alpamayo-solutions/colca/internal/authtest"
	"github.com/alpamayo-solutions/colca/internal/node"
)

// acksAt returns the payloads of the _Ack records on n's commands stream at
// topic that answer correlation id, in stream order.
func acksAt(t *testing.T, n *node.Node, topic, correlationID string) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, r := range fetchRecords(t, n, "commands", "durable-acks-"+correlationID, "", 1000) {
		rec := r.(map[string]any)
		if rec["topic"] != topic {
			continue
		}
		var p map[string]any
		if err := json.Unmarshal([]byte(mustJSON(rec["payload"])), &p); err != nil {
			t.Fatal(err)
		}
		if p["correlation_id"] == correlationID {
			out = append(out, p)
		}
	}
	return out
}

// hasCommand reports whether n's commands stream holds a command with this
// correlation id under topic. It reads the store, since a standalone node's
// admin token no longer opens its HTTP door.
func hasCommand(t *testing.T, n *node.Node, topic, correlationID string) bool {
	t.Helper()
	recs, _, err := n.Engine.Store().Read("commands", 1, 10000, func(tp string) bool { return tp == topic })
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range recs {
		var p map[string]any
		if json.Unmarshal(r.Payload, &p) == nil && p["correlation_id"] == correlationID {
			return true
		}
	}
	return false
}

// restart stops n and starts it again on the same data directory and
// replication address, so its children reconnect.
func restart(t *testing.T, tp *topo, name string, n *node.Node) *node.Node {
	t.Helper()
	n.Stop()
	cfg := tp.cfgs[name]
	cfg.Repl.Addr = n.ReplAddr
	again, err := node.Start(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(again.Stop)
	return again
}

// countingMachine connects m1 at edge1, answers every command with 200 and
// counts what it received per correlation id.
type countingMachine struct {
	mu   sync.Mutex
	seen map[string]int
}

func (c *countingMachine) times(id string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.seen[id]
}

func connectCountingMachine(t *testing.T, addr string, m *authtest.Machine, counts *countingMachine) {
	t.Helper()
	cl := machine(t, addr, m)
	cl.Subscribe("colca/v1/_CmdParam/+/m1/#", 1, func(_ pahomqtt.Client, msg pahomqtt.Message) {
		var cmd map[string]any
		if json.Unmarshal(msg.Payload(), &cmd) != nil {
			return
		}
		id, _ := cmd["correlation_id"].(string)
		counts.mu.Lock()
		counts.seen[id]++
		counts.mu.Unlock()
		parts := strings.Split(msg.Topic(), "/")
		ack, _ := json.Marshal(map[string]any{"correlation_id": id, "result_code": 200, "message": "done"})
		cl.Publish("colca/v1/_Ack/n-edge1/m1/"+parts[len(parts)-1], 1, false, ack)
	}).WaitTimeout(5 * time.Second)
}

// A command without expires_at, sent at the root while the edge that executes
// it is offline, waits in the tree across restarts of every node and is
// executed once when the edge is back. The sender sees queued, forwarded and
// the executor's answer as _Ack records at the command's position.
func TestACommandWithoutExpiryWaitsForAnOfflineEdgeAcrossRestarts(t *testing.T) {
	tp := startTopo(t)
	tp.edge1.Stop()

	const id = "c-durable"
	api(t, tp.global, "POST", "/publish", map[string]any{
		"topic":   "colca/v1/_CmdParam/m1/site1/edge1/m1/set-speed",
		"payload": map[string]any{"correlation_id": id, "progress": true, "command": map[string]any{"speed": 5}},
	})
	waitFor(t, "command queued at site1", 15*time.Second, func() bool {
		return hasCommand(t, tp.site1, "colca/v1/_CmdParam/m1/edge1/m1/set-speed", id)
	})

	// The parents restart while the edge is away; the queue is on their disks.
	tp.global = restart(t, tp, "n-global", tp.global)
	tp.site1 = restart(t, tp, "n-site1", tp.site1)
	time.Sleep(2 * time.Second)

	counts := &countingMachine{seen: map[string]int{}}
	edge := restart(t, tp, "n-edge1", tp.edge1)
	tp.edge1 = edge
	connectCountingMachine(t, edge.MQTTAddr, tp.m1, counts)

	ackTopic := "colca/v1/_Ack/m1/site1/edge1/m1/set-speed"
	finalTopic := "colca/v1/_Ack/n-edge1/site1/edge1/m1/set-speed"
	waitFor(t, "the executor's answer at the root", 30*time.Second, func() bool {
		return len(acksAt(t, tp.global, finalTopic, id)) > 0
	})
	// The edge restarted once more after executing: nothing runs twice.
	edge = restart(t, tp, "n-edge1", edge)
	tp.edge1 = edge
	connectCountingMachine(t, edge.MQTTAddr, tp.m1, counts)
	time.Sleep(3 * time.Second)
	if n := counts.times(id); n != 1 {
		t.Fatalf("the machine received the command %d times, want once", n)
	}
	if final := acksAt(t, tp.global, finalTopic, id); len(final) != 1 || final[0]["result_code"].(float64) != 200 {
		t.Fatalf("final answers at the root = %v, want one 200", final)
	}

	stages := map[string]int{}
	for _, a := range acksAt(t, tp.global, ackTopic, id) {
		if a["result_code"].(float64) != 202 {
			t.Fatalf("a node answered the command itself: %v", a)
		}
		stages[a["stage"].(string)]++
	}
	// queued at the root; forwarded by the root to site1 and by site1 to edge1.
	if stages["queued"] != 1 || stages["forwarded"] < 2 {
		t.Fatalf("progress at the root = %v, want queued once and forwarded by both hops", stages)
	}
}

// Retiring a child that is offline drops what is queued for it, visibly: each
// sender gets a 410 _Ack.
func TestRetiringAnOfflineChildAnswersItsQueuedCommands(t *testing.T) {
	tp := startTopo(t)
	tp.edge1.Stop()

	const id = "c-retired"
	api(t, tp.global, "POST", "/publish", map[string]any{
		"topic":   "colca/v1/_CmdParam/m1/site1/edge1/m1/set-speed",
		"payload": map[string]any{"correlation_id": id},
	})
	waitFor(t, "command queued at site1", 15*time.Second, func() bool {
		return hasCommand(t, tp.site1, "colca/v1/_CmdParam/m1/edge1/m1/set-speed", id)
	})
	api(t, tp.site1, "DELETE", "/enroll/n-edge1?retire=true", nil)

	waitFor(t, "a 410 answer at the root", 15*time.Second, func() bool {
		acks := acksAt(t, tp.global, "colca/v1/_Ack/m1/site1/edge1/m1/set-speed", id)
		return len(acks) == 1 && acks[0]["result_code"].(float64) == 410
	})
}

// After a standalone handover, commands the former parent queued for the node
// are never delivered: the node no longer talks to it, and a retirement at the
// former parent answers them.
func TestAStandaloneNodeNeverReceivesCommandsQueuedByItsFormerParent(t *testing.T) {
	tp := startTopo(t)
	tp.edge1.Stop()

	const id = "c-handover"
	api(t, tp.global, "POST", "/publish", map[string]any{
		"topic":   "colca/v1/_CmdParam/m1/site1/edge1/m1/set-speed",
		"payload": map[string]any{"correlation_id": id},
	})
	waitFor(t, "command queued at site1", 15*time.Second, func() bool {
		return hasCommand(t, tp.site1, "colca/v1/_CmdParam/m1/edge1/m1/set-speed", id)
	})

	cfg := tp.cfgs["n-edge1"]
	cfg.Parent = nil
	cfg.Standalone = true
	standalone, err := node.Start(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(standalone.Stop)
	time.Sleep(3 * time.Second)
	if hasCommand(t, standalone, "colca/v1/_CmdParam/m1/m1/set-speed", id) {
		t.Fatal("the standalone node received a command its former parent queued")
	}

	api(t, tp.site1, "DELETE", "/enroll/n-edge1?retire=true", nil)
	waitFor(t, "a 410 answer at the root", 15*time.Second, func() bool {
		acks := acksAt(t, tp.global, "colca/v1/_Ack/m1/site1/edge1/m1/set-speed", id)
		return len(acks) == 1 && acks[0]["result_code"].(float64) == 410
	})
}

// A service's command queued for an offline child is not forwarded once the
// service is revoked: its sender gets a 403 _Ack and the machine never sees it.
func TestARevokedSendersQueuedCommandIsNotForwarded(t *testing.T) {
	tp := startTopo(t)
	svc := authtest.NewMachine(t, "svc-fleet")
	authtest.EnrollAt(t, tp.global.Registry, tp.global.Engine, svc, "fleet",
		"cmd:"+authtest.ElementID("site1")+"/#:param")
	tp.site1.Stop()

	const id = "c-revoked"
	topic := "colca/v1/_CmdParam/m1/site1/edge1/m1/set-speed"
	if _, err := tp.global.Engine.IngestClient("svc-fleet", topic, []byte(`{"correlation_id":"`+id+`"}`)); err != nil {
		t.Fatal(err)
	}
	api(t, tp.global, "DELETE", "/enroll/svc-fleet", nil)

	counts := &countingMachine{seen: map[string]int{}}
	connectCountingMachine(t, tp.edge1.MQTTAddr, tp.m1, counts)
	tp.site1 = restart(t, tp, "n-site1", tp.site1)

	waitFor(t, "a 403 answer at the root", 20*time.Second, func() bool {
		acks := acksAt(t, tp.global, "colca/v1/_Ack/m1/site1/edge1/m1/set-speed", id)
		return len(acks) == 1 && acks[0]["result_code"].(float64) == 403
	})
	time.Sleep(2 * time.Second)
	if counts.times(id) != 0 || hasCommand(t, tp.site1, "colca/v1/_CmdParam/m1/edge1/m1/set-speed", id) {
		t.Fatal("a revoked sender's command was forwarded")
	}
}
