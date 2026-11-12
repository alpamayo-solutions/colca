// A local service whose registry entry holds cmd:#:admin administers its own
// node through the local door: _CmdAdmin enroll and revoke run at once and
// the answer comes back from /publish and as an _Ack. Without the grant the
// same command is refused.
package tests

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/alpamayo-solutions/colca/door"
	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/node"
)

func enrollLocal(t *testing.T, n *node.Node, name string, grants ...string) string {
	t.Helper()
	entry := map[string]any{"ulid": "01L" + name, "kind": "local", "name": name}
	if len(grants) > 0 {
		entry["grants"] = grants
	}
	b, err := json.Marshal(entry)
	if err != nil {
		t.Fatal(err)
	}
	id, _, err := n.Registry.Enroll(b)
	if err != nil {
		t.Fatalf("enroll %s: %v", name, err)
	}
	return id
}

func adminCommand(verb string, fields map[string]any) (string, map[string]any) {
	corr := unique("cv4-" + verb)
	fields["correlation_id"] = corr
	fields["expires_at"] = time.Now().Add(time.Minute).UnixMilli()
	return corr, fields
}

func TestALocalServiceWithCmdAdminAdministersItsOwnNode(t *testing.T) {
	base := t.TempDir()
	n, err := node.Start(&config.Config{
		ULID: carryNode, DataDir: filepath.Join(base, "data"), LogLevel: "debug",
		KeyFile:   filepath.Join(base, "node.key"),
		API:       config.API{Addr: "127.0.0.1:0", LocalAddr: "127.0.0.1:0", Token: tok},
		MQTT:      config.Endpoint{Addr: "127.0.0.1:0"},
		MQTTLocal: config.Endpoint{Addr: "127.0.0.1:0"},
		Repl:      config.Endpoint{Addr: "127.0.0.1:0"},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(n.Stop)
	enrollLocal(t, n, "orchestrator", "cmd:#:admin")
	victim := enrollLocal(t, n, "conn-old")
	orch := &door.Client{BaseURL: "http://" + n.LocalAPIAddr, Service: "orchestrator"}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// enroll: the node runs it and answers in the /publish response.
	corr, payload := adminCommand("enroll", map[string]any{
		"entry": map[string]any{"ulid": "01Lconn-new", "kind": "local", "name": "conn-new"}})
	offset, out, err := orch.PublishCommand(ctx, door.CommandTopic("_CmdAdmin", carryNode, "", "enroll"), payload)
	if err != nil {
		t.Fatalf("enroll through the local door: %v", err)
	}
	if out == nil || out.ResultCode != 200 || out.CorrelationID != corr {
		t.Fatalf("enroll outcome = %+v", out)
	}
	if _, ok := n.Registry.ByName("conn-new"); !ok {
		t.Fatal("conn-new is not in the registry")
	}
	ack, err := orch.AwaitAck(ctx, carryNode, "enroll", corr, offset)
	if err != nil || ack.ResultCode != 200 {
		t.Fatalf("enroll ack = %+v, %v", ack, err)
	}

	// revoke: same, and the entry is gone.
	corr, payload = adminCommand("revoke", map[string]any{"ulid": victim})
	offset, out, err = orch.PublishCommand(ctx, door.CommandTopic("_CmdAdmin", carryNode, "", "revoke"), payload)
	if err != nil {
		t.Fatalf("revoke through the local door: %v", err)
	}
	if out == nil || out.ResultCode != 200 {
		t.Fatalf("revoke outcome = %+v", out)
	}
	ack, err = orch.AwaitAck(ctx, carryNode, "revoke", corr, offset)
	if err != nil || ack.ResultCode != 200 || ack.Topic != "colca/v1/_Ack/"+carryNode+"/revoke" {
		t.Fatalf("revoke ack = %+v, %v", ack, err)
	}
	if _, ok := n.Registry.ByName("conn-old"); ok {
		t.Fatal("conn-old is still in the registry")
	}

	// Without the grant the node refuses the same command.
	enrollLocal(t, n, "plain")
	plain := &door.Client{BaseURL: "http://" + n.LocalAPIAddr, Service: "plain"}
	_, payload = adminCommand("revoke", map[string]any{"ulid": "01Lconn-new"})
	_, out, err = plain.PublishCommand(ctx, door.CommandTopic("_CmdAdmin", carryNode, "", "revoke"), payload)
	var refused *door.HTTPError
	if !errors.As(err, &refused) || refused.StatusCode != 403 {
		t.Fatalf("revoke without cmd:#:admin: outcome=%+v err=%v, want 403", out, err)
	}
	if _, ok := n.Registry.ByName("conn-new"); !ok {
		t.Fatal("a service without the grant revoked conn-new")
	}
}
