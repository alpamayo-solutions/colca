package tests

import (
	"testing"
	"time"

	pahomqtt "github.com/eclipse/paho.mqtt.golang"

	"github.com/alpamayo-solutions/colca/internal/node"
	"github.com/alpamayo-solutions/colca/internal/tokenauth/tokentest"
)

// defineGroupAtRoot writes a _Group definition at the root, the way
// colca-grantsync does, and waits until it has descended to every edge.
func defineGroupAtRoot(t *testing.T, tp *topo, id string, grants ...string) {
	t.Helper()
	topic := "colca/v1/_Group/n-global/" + id
	api(t, tp.global, "POST", "/publish", map[string]any{
		"topic":   topic,
		"payload": map[string]any{"id": id, "name": id, "grants": grants},
	})
	for _, n := range []*node.Node{tp.edge1, tp.edge2} {
		waitFor(t, "group "+id+" to reach "+n.Cfg.ULID, 20*time.Second, func() bool {
			for _, e := range kvAt(t, n, id) {
				if e.(map[string]any)["topic"] == topic {
					return true
				}
			}
			return false
		})
	}
}

// subscribeQoS subscribes and returns the broker's granted QoS (0x80 refused).
func subscribeQoS(t *testing.T, c pahomqtt.Client, filter string) byte {
	t.Helper()
	tok := c.Subscribe(filter, 1, func(pahomqtt.Client, pahomqtt.Message) {})
	if !tok.WaitTimeout(5 * time.Second) {
		t.Fatalf("subscribe %s: no SUBACK", filter)
	}
	st, ok := tok.(*pahomqtt.SubscribeToken)
	if !ok {
		t.Fatalf("subscribe %s: unexpected token %T", filter, tok)
	}
	qos, found := st.Result()[filter]
	if !found {
		t.Fatalf("subscribe %s: no result", filter)
	}
	return qos
}

// One group, defined once at the root with a node-relative grant, gives each
// edge's operators that edge: an operator signed in at edge 1 reads edge 1's
// machine, one signed in at edge 2 reads edge 2's, with no element id per
// machine in the definition.
func TestANodeRelativeGrantGivesEachEdgesOperatorsTheirEdge(t *testing.T) {
	tp := startTopo(t)
	defineGroupAtRoot(t, tp, "operators", "read:$node/#")

	for _, tc := range []struct {
		name    string
		edge    *node.Node
		addr    string
		machine string
	}{
		{"edge1", tp.edge1, tp.edge1.MQTTAddr, "m1"},
		{"edge2", tp.edge2, tp.edge2.MQTTAddr, "m2"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sub := "operator-" + tc.name
			token := tp.iss.MintOpt(tokentest.MintOpts{Sub: sub, Groups: []string{"operators"}})
			c := human(t, tc.edge, "ssl", sub, token)
			msgs := subscribeAll(t, c, "colca/v1/_Metric/+/"+tc.machine+"/#")

			m := tp.m1
			if tc.machine == "m2" {
				m = tp.m2
			}
			mc := machine(t, tc.addr, m)
			topic := "colca/v1/_Metric/n-" + tc.name + "/" + tc.machine + "/temp"
			mc.Publish(topic, 1, false, `{"v": 7}`).WaitTimeout(5 * time.Second)
			awaitTopic(t, msgs, topic, 15*time.Second)

			if qos := subscribeQoS(t, c, "colca/#"); qos == 0x80 {
				t.Fatalf("read:$node/# must cover the whole node %s signed in at", tc.name)
			}
		})
	}
}

// A path below $node is resolved in the frame of the node the person signed
// in at: the same definition covers the machine at edge 1, and nothing at edge
// 2, which holds no element at that path.
func TestANodeRelativePathIsResolvedAtTheNodeSignedInAt(t *testing.T) {
	tp := startTopo(t)
	defineGroupAtRoot(t, tp, "m1-readers", "read:$node/m1/#")

	token1 := tp.iss.MintOpt(tokentest.MintOpts{Sub: "reader-1", Groups: []string{"m1-readers"}})
	c1 := human(t, tp.edge1, "ssl", "reader-1", token1)
	msgs := subscribeAll(t, c1, "colca/v1/_Metric/+/m1/#")
	m1 := machine(t, tp.edge1.MQTTAddr, tp.m1)
	m1.Publish("colca/v1/_Metric/n-edge1/m1/temp", 1, false, `{"v": 3}`).WaitTimeout(5 * time.Second)
	awaitTopic(t, msgs, "colca/v1/_Metric/n-edge1/m1/temp", 15*time.Second)
	if qos := subscribeQoS(t, c1, "colca/#"); qos != 0x80 {
		t.Fatalf("read:$node/m1/# must not cover the whole node, granted qos %#x", qos)
	}

	token2 := tp.iss.MintOpt(tokentest.MintOpts{Sub: "reader-2", Groups: []string{"m1-readers"}})
	c2 := human(t, tp.edge2, "ssl", "reader-2", token2)
	if qos := subscribeQoS(t, c2, "colca/v1/_Metric/+/m2/#"); qos != 0x80 {
		t.Fatalf("read:$node/m1/# must not cover edge 2's machine m2, granted qos %#x", qos)
	}
}
