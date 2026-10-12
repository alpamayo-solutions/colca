package tests

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	pahomqtt "github.com/eclipse/paho.mqtt.golang"

	"github.com/alpamayo-solutions/colca/internal/node"
)

// copyDir copies a stopped node's data directory, which is what a volume
// backup is.
func copyDir(t *testing.T, from, to string) {
	t.Helper()
	err := filepath.WalkDir(from, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(from, path)
		if err != nil {
			return err
		}
		if d.IsDir() {
			return os.MkdirAll(filepath.Join(to, rel), 0o700)
		}
		b, err := os.ReadFile(path) //nolint:gosec // a path below the test's own temp dir
		if err != nil {
			return err
		}
		return os.WriteFile(filepath.Join(to, rel), b, 0o600)
	})
	if err != nil {
		t.Fatalf("copy %s to %s: %v", from, to, err)
	}
}

// backUp stops a node, copies its data directory and starts it again.
func backUp(t *testing.T, tp *topo, ulid string, n *node.Node) (running *node.Node, backup string) {
	t.Helper()
	n.Stop()
	backup = filepath.Join(t.TempDir(), ulid+"-backup")
	copyDir(t, tp.dirs[ulid], backup)
	return restart(t, tp, ulid, n), backup
}

// restore stops a node, replaces its data directory with the backup and starts
// it again: the volume restore of a disaster recovery.
func restore(t *testing.T, tp *topo, ulid string, n *node.Node, backup string) *node.Node {
	t.Helper()
	n.Stop()
	if err := os.RemoveAll(tp.dirs[ulid]); err != nil {
		t.Fatal(err)
	}
	copyDir(t, backup, tp.dirs[ulid])
	return restart(t, tp, ulid, n)
}

// metricValue reads one unlabelled metric from a node's /metrics.
func metricValue(t *testing.T, n *node.Node, name string) float64 {
	t.Helper()
	resp := doRequest(t, httpsClient, "GET /metrics", func() (*http.Request, error) {
		return http.NewRequest(http.MethodGet, "https://"+n.APIAddr+"/metrics", nil)
	})
	defer resp.Body.Close()
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		if v, ok := strings.CutPrefix(sc.Text(), name+" "); ok {
			f, err := strconv.ParseFloat(v, 64)
			if err != nil {
				t.Fatalf("metric %s: %v", name, err)
			}
			return f
		}
	}
	t.Fatalf("metric %s is not on /metrics of %s", name, n.Cfg.ULID)
	return 0
}

// resyncOf reads uplink.resync from a node's /healthz, nil when it never
// re-synced.
func resyncOf(t *testing.T, n *node.Node) map[string]any {
	t.Helper()
	uplink, _ := api(t, n, "GET", "/healthz", nil)["uplink"].(map[string]any)
	resync, _ := uplink["resync"].(map[string]any)
	return resync
}

// commandLog counts what a machine was handed, per correlation id.
type commandLog struct {
	mu   sync.Mutex
	seen map[string]int
}

func (c *commandLog) count(id string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.seen[id]
}

func subscribeCommands(t *testing.T, cl pahomqtt.Client, filter string) *commandLog {
	t.Helper()
	log := &commandLog{seen: map[string]int{}}
	tok := cl.Subscribe(filter, 1, func(_ pahomqtt.Client, msg pahomqtt.Message) {
		var body struct {
			CorrelationID string `json:"correlation_id"`
		}
		if json.Unmarshal(msg.Payload(), &body) == nil {
			log.mu.Lock()
			log.seen[body.CorrelationID]++
			log.mu.Unlock()
		}
	})
	if !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("subscribe %s: %v", filter, tok.Error())
	}
	return log
}

func sendCommand(t *testing.T, at *node.Node, id string) {
	t.Helper()
	api(t, at, "POST", "/publish", map[string]any{
		"topic": "colca/v1/_CmdParam/m1/site1/edge1/m1/set-speed",
		"payload": map[string]any{"correlation_id": id,
			"expires_at": float64(time.Now().Add(time.Hour).UnixMilli()), "params": map[string]any{"speed": 5}},
	})
}

func publishMetrics(t *testing.T, m pahomqtt.Client, node, name string, n int) {
	t.Helper()
	for i := 1; i <= n; i++ {
		tok := m.Publish("colca/v1/_Metric/"+node+"/m1/"+name, 1, false, fmt.Sprintf(`{"v": %d}`, i))
		if !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
			t.Fatalf("publish %s #%d: %v", name, i, tok.Error())
		}
	}
}

// restoredParent names the node restoreAndFollow restores and how its child's
// records and acks are addressed there.
type restoredParent struct {
	ulid string
	node func(*topo) **node.Node
	// child is the node directly below, which has to re-sync.
	child func(*topo) *node.Node
	// mount is what the restored node inserts before edge1's topics.
	mount string
}

// restoreAndFollow restores a node from a volume backup and follows what its
// children do. Without the store-incarnation check they are stranded twice
// over: what they sent since the backup is gone at the parent and never sent
// again, because their uplink cursors stand past it; and they take no commands
// until the parent's commands stream has grown past the head they remember.
// It returns with the tree replicating again.
func restoreAndFollow(t *testing.T, tp *topo, p restoredParent) *commandLog {
	t.Helper()
	parent := p.node(tp)
	m1 := machine(t, tp.edge1.MQTTAddr, tp.m1)
	commands := subscribeCommands(t, m1, "colca/v1/_CmdParam/+/m1/#")
	prefix := p.mount + "edge1/m1/"
	ackTopic := "colca/v1/_Ack/n-edge1/" + p.mount + "edge1/m1/set-speed"
	ackAt := func(n *node.Node, cursor, topic, id string) bool {
		for _, r := range fetchRecords(t, n, "commands", cursor, "", 500) {
			rec := r.(map[string]any)
			if rec["topic"] != topic {
				continue
			}
			var payload map[string]any
			if json.Unmarshal([]byte(mustJSON(rec["payload"])), &payload) == nil &&
				payload["correlation_id"] == id && payload["result_code"] == float64(200) {
				return true
			}
		}
		return false
	}

	publishMetrics(t, m1, "n-edge1", "before", 5)
	waitFor(t, "the records from before the backup at the parent", 20*time.Second, func() bool {
		return len(fetchRecords(t, *parent, "metrics", "r0", prefix+"before", 100)) == 5
	})

	var backup string
	*parent, backup = backUp(t, tp, p.ulid, *parent)

	// Between the backup and the restore: the child sends records, and takes
	// and acknowledges a command.
	publishMetrics(t, m1, "n-edge1", "since", 20)
	waitFor(t, "the records sent since the backup at the parent", 30*time.Second, func() bool {
		return len(fetchRecords(t, *parent, "metrics", "r1", prefix+"since", 100)) == 20
	})
	sendCommand(t, tp.global, "before-restore")
	waitFor(t, "the command issued before the restore at the machine", 20*time.Second, func() bool {
		return commands.count("before-restore") == 1
	})
	m1.Publish("colca/v1/_Ack/n-edge1/m1/set-speed", 1, false,
		`{"correlation_id":"before-restore","result_code":200,"message":"done"}`).WaitTimeout(5 * time.Second)
	waitFor(t, "the ack sent before the restore at the parent", 20*time.Second, func() bool {
		return ackAt(*parent, "a1", ackTopic, "before-restore")
	})

	*parent = restore(t, tp, p.ulid, *parent, backup)
	if n := len(fetchRecords(t, *parent, "metrics", "r2", prefix+"before", 100)); n != 5 {
		t.Fatalf("the restored parent holds %d records from before the backup, want 5 — the restore itself is broken", n)
	}

	// A command issued right after the restore is executed promptly: the child
	// does not wait for the parent's stream to pass the head it remembered.
	sendCommand(t, tp.global, "after-restore")
	waitFor(t, "the command issued after the restore at the machine", 15*time.Second, func() bool {
		return commands.count("after-restore") == 1
	})

	// What the child sent since the backup arrives at the parent again.
	waitFor(t, "the records sent since the backup at the restored parent", 30*time.Second, func() bool {
		return len(fetchRecords(t, *parent, "metrics", "r3", prefix+"since", 100)) >= 20
	})
	waitFor(t, "the ack sent before the restore at the restored parent", 20*time.Second, func() bool {
		return ackAt(*parent, "a2", ackTopic, "before-restore")
	})

	// The re-sync shows on the child's /healthz and ends.
	var resync map[string]any
	waitFor(t, "the child's re-sync to complete", 30*time.Second, func() bool {
		resync = resyncOf(t, p.child(tp))
		return resync != nil && resync["state"] == "complete"
	})
	streams, _ := resync["streams"].(map[string]any)
	if streams["metrics"] == nil {
		t.Fatalf("the child's re-sync names no replayed metrics range: %v", resync)
	}
	if resync["parent_store"] == resync["previous_parent_store"] || resync["previous_parent_store"] == nil {
		t.Fatalf("the child's re-sync does not name the parent's old and new store: %v", resync)
	}
	if gaps, _ := resync["gaps"].([]any); len(gaps) != 0 {
		t.Fatalf("the child's re-sync reports gaps although it still held everything: %v", gaps)
	}
	// Once: the parent's marks drop what it held when the child sent it again.
	if n := len(fetchRecords(t, *parent, "metrics", "r4", prefix+"since", 100)); n != 20 {
		t.Fatalf("the restored parent holds %d records sent since the backup, want exactly 20", n)
	}
	// The restored node says what happened to its store.
	store, _ := api(t, *parent, "GET", "/healthz", nil)["store"].(map[string]any)
	if changed, _ := store["changed"].(map[string]any); changed == nil || changed["reason"] == "" {
		t.Fatalf("the restored node's /healthz does not say its store changed incarnation: %v", store)
	}

	// Replication goes on through the restored node, in both directions. The
	// command issued after the restore has been executed, so the older one is
	// not merely still on its way: the machine got each once.
	m1.Publish("colca/v1/_Ack/n-edge1/m1/set-speed", 1, false,
		`{"correlation_id":"after-restore","result_code":200,"message":"done"}`).WaitTimeout(5 * time.Second)
	waitFor(t, "the ack sent after the restore at the root", 30*time.Second, func() bool {
		return ackAt(tp.global, "a3", "colca/v1/_Ack/n-edge1/site1/edge1/m1/set-speed", "after-restore")
	})
	for _, id := range []string{"before-restore", "after-restore"} {
		if n := commands.count(id); n != 1 {
			t.Fatalf("the machine got the command %s %d times, want 1", id, n)
		}
	}
	return commands
}

// The root is where commands are issued: one issued after the restore reuses an
// offset its child has read past.
func TestARootRestoredFromABackupResyncsItsChildren(t *testing.T) {
	tp := startTopo(t)
	restoreAndFollow(t, tp, restoredParent{
		ulid:  "n-global",
		node:  func(tp *topo) **node.Node { return &tp.global },
		child: func(tp *topo) *node.Node { return tp.site1 },
		mount: "site1/",
	})
}

// A restored node in the middle of the tree is behind its children and behind
// its own parent at once. The command from before the restore comes down to it
// a second time, from the root, and it hands it on to the child that executed
// it already.
func TestAMiddleNodeRestoredFromABackupResyncsItsChildren(t *testing.T) {
	tp := startTopo(t)
	commands := restoreAndFollow(t, tp, restoredParent{
		ulid:  "n-site1",
		node:  func(tp *topo) **node.Node { return &tp.site1 },
		child: func(tp *topo) *node.Node { return tp.edge1 },
	})
	// The sibling sent nothing since the backup; it learns of the new store all
	// the same and has nothing to send again.
	waitFor(t, "the sibling's re-sync to complete", 30*time.Second, func() bool {
		r := resyncOf(t, tp.edge2)
		return r != nil && r["state"] == "complete"
	})
	waitFor(t, "the child to be handed the old command again and recognise it", 30*time.Second, func() bool {
		return metricValue(t, tp.edge1, "colca_downlink_commands_already_held_total") >= 1
	})
	if n := commands.count("before-restore"); n != 1 {
		t.Fatalf("the machine got the command issued before the restore %d times, want 1", n)
	}
}

// A node restored from a backup hands out again the offsets it lost. Its
// parent holds records at those offsets and dropped the new ones as already
// stored, reporting them delivered, until the node's streams had grown past
// what the parent held.
func TestAChildRestoredFromABackupKeepsReplicating(t *testing.T) {
	tp := startTopo(t)
	m1 := machine(t, tp.edge1.MQTTAddr, tp.m1)
	publishMetrics(t, m1, "n-edge1", "before", 5)
	waitFor(t, "the records from before the backup at the parent", 20*time.Second, func() bool {
		return len(fetchRecords(t, tp.site1, "metrics", "c0", "edge1/m1/before", 100)) == 5
	})
	m1.Disconnect(100)

	var backup string
	tp.edge1, backup = backUp(t, tp, "n-edge1", tp.edge1)
	m1 = machine(t, tp.edge1.MQTTAddr, tp.m1)
	publishMetrics(t, m1, "n-edge1", "since", 20)
	waitFor(t, "the records sent since the backup at the parent", 30*time.Second, func() bool {
		return len(fetchRecords(t, tp.site1, "metrics", "c1", "edge1/m1/since", 100)) == 20
	})
	m1.Disconnect(100)

	tp.edge1 = restore(t, tp, "n-edge1", tp.edge1, backup)
	m1 = machine(t, tp.edge1.MQTTAddr, tp.m1)
	// Fewer records than were lost: every one reuses an offset the parent holds.
	publishMetrics(t, m1, "n-edge1", "restored", 3)
	waitFor(t, "the records written after the restore at the parent", 30*time.Second, func() bool {
		return len(fetchRecords(t, tp.site1, "metrics", "c2", "edge1/m1/restored", 100)) == 3
	})
	store, _ := api(t, tp.edge1, "GET", "/healthz", nil)["store"].(map[string]any)
	if changed, _ := store["changed"].(map[string]any); changed == nil {
		t.Fatalf("the restored node's /healthz does not say its store changed incarnation: %v", store)
	}
	// What the parent held from before the restore stays, once.
	if n := len(fetchRecords(t, tp.site1, "metrics", "c3", "edge1/m1/since", 100)); n != 20 {
		t.Fatalf("the parent holds %d records the child sent before its restore, want 20", n)
	}
}
