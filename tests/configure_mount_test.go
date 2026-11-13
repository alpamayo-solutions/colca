// A parent refuses a configure command that would write below the element a
// child node is attached at, through a real tree: the command arrives at the
// parent's admin door, the ack says why, and the child's own records still
// replicate up to the same positions.
package tests

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/identity"
	"github.com/alpamayo-solutions/colca/internal/node"
)

// ackOf waits for the ack of the command corr on ackTopic at n and returns
// its result code and message.
func ackOf(t *testing.T, n *node.Node, ackTopic, corr string) (int, string) {
	t.Helper()
	var ack struct {
		CorrelationID string `json:"correlation_id"`
		ResultCode    int    `json:"result_code"`
		Message       string `json:"message"`
	}
	waitFor(t, "ack "+ackTopic+" (corr "+corr+")", 20*time.Second, func() bool {
		for _, r := range fetchRecords(t, n, "commands", unique("mount-ack"), "", 500) {
			rec := r.(map[string]any)
			if rec["topic"] != ackTopic {
				continue
			}
			if json.Unmarshal([]byte(mustJSON(rec["payload"])), &ack) == nil &&
				ack.CorrelationID == corr && ack.ResultCode != 202 {
				return true
			}
		}
		return false
	})
	return ack.ResultCode, ack.Message
}

func TestConfigureAtTheParentRefusesWritesBelowAChildNodesMount(t *testing.T) {
	base := t.TempDir()
	keys := map[string]*identity.Identity{}
	for _, n := range []string{"n-parent", "n-child"} {
		id, err := identity.Generate(filepath.Join(base, n+".key"))
		if err != nil {
			t.Fatal(err)
		}
		keys[n] = id
	}
	bundle := bindingBundle(t)
	mk := func(ulid string, parent *config.Parent) *config.Config {
		return &config.Config{
			ULID: ulid, DataDir: filepath.Join(base, ulid+"-data"), LogLevel: "debug",
			Identity:  config.Identity{KeyFile: filepath.Join(base, ulid+".key")},
			API:       config.API{Addr: "127.0.0.1:0", Token: tok},
			MQTT:      config.Endpoint{Addr: "127.0.0.1:0"},
			Repl:      config.Endpoint{Addr: "127.0.0.1:0"},
			Parent:    parent,
			Contracts: config.Contracts{Bundle: bundle},
		}
	}
	parent, err := node.Start(mk("n-parent", nil))
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Stop()
	authtestEnrollNode(t, parent, "n-child", keys["n-child"].PublicHex(), "beans")
	child, err := node.Start(mk("n-child", &config.Parent{
		URL: "https://" + parent.ReplAddr, Pubkey: keys["n-parent"].PublicHex(),
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer child.Stop()
	waitForPrefix(t, "n-child", child)

	atParent := func(verb string, body map[string]any) (int, string) {
		t.Helper()
		corr := cmdAdmin(t, parent, "colca/v1/_CmdConfigure/n-parent/"+verb, body)
		return ackOf(t, parent, "colca/v1/_Ack/n-parent/"+verb, corr)
	}
	element := func(path, id string) map[string]any {
		return map[string]any{"elements": []any{map[string]any{
			"path": path, "element": map[string]any{"id": id, "name": path[strings.LastIndex(path, "/")+1:]},
		}}}
	}
	signal := func(path, id string) map[string]any {
		return map[string]any{"signals": []any{map[string]any{
			"path": path, "signal": map[string]any{"id": id, "name": path[strings.LastIndex(path, "/")+1:]},
		}}}
	}
	constant := func(path, id string) map[string]any {
		return map[string]any{"constants": []any{map[string]any{
			"path": path, "constant": map[string]any{"id": id, "name": "limit", "data_type": "int64", "value": 7},
		}}}
	}

	// The model applied at the parent reaches into the child's subtree.
	for _, tc := range []struct {
		verb string
		body map[string]any
	}{
		{"element/upsert", element("beans/roaster", "el-parent-roaster")},
		{"signal/upsert", signal("beans/temp", "sig-parent-temp")},
		{"constant/upsert", constant("beans/roaster/limit", "c-parent-limit")},
	} {
		code, msg := atParent(tc.verb, tc.body)
		if code != 422 || !strings.HasPrefix(msg, "foreign_position: ") {
			t.Fatalf("%s below the child's mount = %d %q, want 422 foreign_position", tc.verb, code, msg)
		}
		if !strings.Contains(msg, "below beans,") || !strings.Contains(msg, "node n-child") {
			t.Fatalf("%s: the refusal must name the mount and the child node: %q", tc.verb, msg)
		}
	}

	// The same model beside the mount is applied.
	for _, tc := range []struct {
		verb string
		body map[string]any
	}{
		{"element/upsert", element("packaging", "el-packaging")},
		{"element/upsert", element("packaging/roaster", "el-parent-roaster")},
		{"signal/upsert", signal("packaging/temp", "sig-parent-temp")},
		{"constant/upsert", constant("packaging/roaster/limit", "c-parent-limit")},
	} {
		if code, msg := atParent(tc.verb, tc.body); code != 200 {
			t.Fatalf("%s beside the mount = %d %q, want 200", tc.verb, code, msg)
		}
	}

	// The child authors those positions: the same element, addressed to the
	// child, is written there and replicates up to below the mount.
	corr := cmdAdmin(t, parent, "colca/v1/_CmdConfigure/n-child/beans/element/upsert", element("roaster", "el-child-roaster"))
	if code, msg := ackOf(t, parent, "colca/v1/_Ack/n-child/beans/element/upsert", corr); code != 200 {
		t.Fatalf("element/upsert addressed to the child = %d %q, want 200", code, msg)
	}
	waitFor(t, "the child's element replicated to the parent", 20*time.Second, func() bool {
		return retainedAt(t, parent, "colca/v1/_SystemElement/n-child/beans/roaster") != nil
	})

	// What the parent holds: its own records beside the mount, none below it.
	for topic, want := range map[string]bool{
		"colca/v1/_SystemElement/n-parent/packaging/roaster":  true,
		"colca/v1/_Signal/n-parent/packaging/temp":            true,
		"colca/v1/_Constant/n-parent/packaging/roaster/limit": true,
		"colca/v1/_SystemElement/n-parent/beans/roaster":      false,
		"colca/v1/_Signal/n-parent/beans/temp":                false,
		"colca/v1/_Constant/n-parent/beans/roaster/limit":     false,
	} {
		if held := retainedAt(t, parent, topic) != nil; held != want {
			t.Fatalf("parent holds %s = %v, want %v", topic, held, want)
		}
	}
}
