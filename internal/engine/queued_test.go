package engine

import (
	"encoding/json"
	"testing"

	"github.com/alpamayo-solutions/colca/internal/store"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// storedCommand returns the command stored at topic.
func storedCommand(t *testing.T, e *Engine, topic string) store.StoredRecord {
	t.Helper()
	recs, _, err := e.Store().Read("commands", 1, 100, func(tp string) bool { return tp == topic })
	if err != nil || len(recs) != 1 {
		t.Fatalf("stored commands at %s: %v, %v", topic, recs, err)
	}
	return recs[0]
}

// The grant check of the door that admitted a command is repeated when the
// command is forwarded: a service that lost its grant meanwhile is refused.
// A command that came down from the parent is not checked again here.
func TestForwardingChecksTheSendersGrantAgain(t *testing.T) {
	ids := testIDs()
	ids.routes = []string{"child1"}
	ids.entries["hmi"].Grants = []string{"cmd:#:param"}
	e := newEngineWithIDs(t, ids)

	topic := "colca/v1/_CmdParam/n-child/child1/m1/go"
	if _, err := e.IngestClient("hmi", topic, []byte(`{"correlation_id":"c-grant"}`)); err != nil {
		t.Fatal(err)
	}
	rec := storedCommand(t, e, topic)
	if refusal := e.ForwardRefusal(rec); refusal != "" {
		t.Fatalf("an authorized sender's command was refused: %s", refusal)
	}
	ids.entries["hmi"].Grants = nil
	if refusal := e.ForwardRefusal(rec); refusal == "" {
		t.Fatal("a command whose sender lost its grant was forwarded")
	}

	relayed := "colca/v1/_CmdParam/n-child/child1/m1/stop"
	if _, err := e.IngestDownlink(relayed, []byte(`{"correlation_id":"c-relayed"}`), 1); err != nil {
		t.Fatal(err)
	}
	if refusal := e.ForwardRefusal(storedCommand(t, e, relayed)); refusal != "" {
		t.Fatalf("a command from the parent was checked again: %s", refusal)
	}
}

// A sender that asked for progress gets a 202 "queued" ack when its command
// waits for a child; the ack is not the answer a repeat of the command gets.
func TestAQueuedCommandIsAnsweredQueuedWhenTheSenderAsks(t *testing.T) {
	ids := testIDs()
	ids.routes = []string{"child1"}
	e := newEngineWithIDs(t, ids)

	topic := "colca/v1/_CmdParam/n-child/child1/m1/go"
	if _, err := e.IngestAdmin(topic, []byte(`{"correlation_id":"c-progress","progress":true}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := e.IngestAdmin("colca/v1/_CmdParam/n-child/child1/m1/quiet", []byte(`{"correlation_id":"c-quiet"}`)); err != nil {
		t.Fatal(err)
	}
	recs, _, err := e.Store().Read("commands", 1, 100, func(tp string) bool {
		p, err := uns.Parse(tp)
		return err == nil && p.Contract == "_Ack"
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 1 || recs[0].Topic != "colca/v1/_Ack/n-child/child1/m1/go" {
		t.Fatalf("acks = %+v, want one at the command's position", recs)
	}
	var ack map[string]any
	if err := json.Unmarshal(recs[0].Payload, &ack); err != nil {
		t.Fatal(err)
	}
	if ack["result_code"].(float64) != 202 || ack["stage"] != "queued" || ack["correlation_id"] != "c-progress" {
		t.Fatalf("ack = %v, want 202 queued", ack)
	}
	if res := e.repeated([]byte(`{"correlation_id":"c-progress"}`)); res.Command != nil {
		t.Fatalf("a repeat was answered with the progress ack: %+v", res.Command)
	}
}
