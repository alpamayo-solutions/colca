// A local service runs door.Executor on a real node: it reads the commands sent
// to its paths through its own cursor, answers each once at the command's
// path, and answers a command that expired 498 without running it.
package tests

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/alpamayo-solutions/colca/door"
	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/node"
)

// planner answers _CmdMaintain at orch/<verb>, echoing the command's params.
type planner struct{ ran chan door.Command }

func (p *planner) Handles(contract, path string) bool {
	return contract == "_CmdMaintain" && len(path) > 5 && path[:5] == "orch/"
}

func (p *planner) Execute(_ context.Context, cmd door.Command) (door.Answer, error) {
	p.ran <- cmd
	return door.Answer{Message: "planned", Result: map[string]any{"params": cmd.Params}}, nil
}

func TestADoorExecutorAnswersALocalServicesCommands(t *testing.T) {
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
	h := &planner{ran: make(chan door.Command, 10)}
	x := &door.Executor{
		Client:    &door.Client{BaseURL: "http://" + n.LocalAPIAddr, Service: "orchestrator"},
		Handler:   h,
		Contracts: []string{"_CmdMaintain"},
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- x.Run(ctx) }()
	defer func() {
		cancel()
		<-done
	}()

	topic := "colca/v1/_CmdMaintain/" + carryNode + "/orch/plan"
	ackTopic := "colca/v1/_Ack/" + carryNode + "/orch/plan"
	corr := cmdAdmin(t, n, topic, map[string]any{"command": map[string]any{"connector": "m3"}})
	var answer map[string]any
	waitFor(t, "the executor's answer", 10*time.Second, func() bool {
		acks := acksAt(t, n, ackTopic, corr)
		if len(acks) == 1 {
			answer = acks[0]
		}
		return answer != nil
	})
	if answer["result_code"] != 200.0 || answer["message"] != "planned" {
		t.Fatalf("answer = %v", answer)
	}
	if params := answer["result"].(map[string]any)["params"].(map[string]any); params["connector"] != "m3" {
		t.Fatalf("result = %v", answer["result"])
	}
	if cmd := <-h.ran; cmd.Sender.ID == "" || cmd.CorrelationID != corr {
		t.Fatalf("command = %+v, want the node's attested sender", cmd)
	}

	expired := cmdAdmin(t, n, topic, map[string]any{"expires_at": time.Now().Add(-time.Minute).UnixMilli()})
	waitFor(t, "the answer to the expired command", 10*time.Second, func() bool {
		acks := acksAt(t, n, ackTopic, expired)
		return len(acks) == 1 && acks[0]["result_code"] == 498.0
	})
	select {
	case cmd := <-h.ran:
		t.Fatalf("an expired command ran: %+v", cmd)
	default:
	}
}
