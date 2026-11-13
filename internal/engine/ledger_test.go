package engine

import (
	"strings"
	"testing"
	"time"

	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/store"
)

func ledgerEngine(t *testing.T) (*Engine, *[]delivery) {
	t.Helper()
	s, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	var delivered []delivery
	e := New(s, &config.Config{ULID: "n-edge1"}, testIDs(),
		func(topic string, payload []byte, retain bool) {
			delivered = append(delivered, delivery{Topic: topic, Payload: string(payload), Retain: retain})
		}, nil, nil)
	return e, &delivered
}

// A command resent with its correlation id is not stored or run again; the
// sender gets the first one's ack instead.
func TestARepeatedCorrelationIDDoesNotRunTheCommandAgain(t *testing.T) {
	e, delivered := ledgerEngine(t)
	anna := humanEntry(t, "cmd:#:param")
	topic := "colca/v1/_CmdParam/n-edge1/line1/operator/setDensity"
	if _, err := e.IngestHuman(anna, topic, cmdPayload("anna-1")); err != nil {
		t.Fatal(err)
	}
	res, err := e.IngestHuman(anna, topic, cmdPayload("anna-1"))
	if err != nil || !res.Duplicate || res.Persisted {
		t.Fatalf("repeat before the ack = %+v, %v", res, err)
	}

	ack := `{"correlation_id":"anna-1","result_code":200,"message":"density set"}`
	if _, err := e.IngestAdmin("colca/v1/_Ack/n-edge1/line1/operator/setDensity", []byte(ack)); err != nil {
		t.Fatal(err)
	}
	*delivered = nil
	stored := e.Store().NextOffset("commands") // the command and its ack
	res, err = e.IngestHuman(anna, topic, cmdPayload("anna-1"))
	if err != nil || !res.Duplicate || res.Command == nil || res.Command.Message != "density set" {
		t.Fatalf("repeat after the ack = %+v, %v", res, err)
	}
	if len(*delivered) != 1 || (*delivered)[0].Payload != ack {
		t.Fatalf("the stored ack must go out again, delivered %v", *delivered)
	}
	if got := e.Store().NextOffset("commands"); got != stored {
		t.Fatalf("a repeat was stored: next offset %d, want %d", got, stored)
	}
}

// Another sender cannot take over a correlation id, and with it the ack; it
// gets a 422 ack of its own instead.
func TestAnotherSendersCorrelationIDIsRefused(t *testing.T) {
	e, delivered := ledgerEngine(t)
	topic := "colca/v1/_CmdParam/n-edge1/line1/operator/setDensity"
	if _, err := e.IngestHuman(humanEntry(t, "cmd:#:param"), topic, cmdPayload("taken")); err != nil {
		t.Fatal(err)
	}
	bert := humanEntry(t, "cmd:#:param")
	bert.ULID = "bert"
	*delivered = nil
	if _, err := e.IngestHuman(bert, topic, cmdPayload("taken")); err == nil || !strings.Contains(err.Error(), "another sender") {
		t.Fatalf("reuse by another sender = %v", err)
	}
	if len(*delivered) != 1 || (*delivered)[0].Topic != "colca/v1/_Ack/n-edge1/line1/operator/setDensity" ||
		!strings.Contains((*delivered)[0].Payload, `"result_code":422`) {
		t.Fatalf("the refused sender needs an ack, delivered %v", *delivered)
	}
	refusal := (*delivered)[0]
	if recipient, _ := e.AckRecipient(refusal.Topic, []byte(refusal.Payload)); recipient != "bert" {
		t.Fatalf("the refusal goes to %q, want bert", recipient)
	}
	if recipient, isAck := e.AckRecipient("colca/v1/_Ack/n-edge1/line1/operator/setDensity",
		[]byte(`{"correlation_id":"taken"}`)); !isAck || recipient == "bert" {
		t.Fatalf("the ack belongs to %q", recipient)
	}
}

// The ledger forgets after its window, and holds no more than its limit.
func TestTheLedgerIsBounded(t *testing.T) {
	l := newCommandLedger()
	now := time.Unix(1000, 0)
	l.now = func() time.Time { return now }
	if v, _, _ := l.admit("old", "anna"); v != ledgerNew {
		t.Fatal("first admit")
	}
	now = now.Add(ledgerWindow)
	if v, _, _ := l.admit("old", "anna"); v != ledgerNew {
		t.Fatal("an id past the window must be new again")
	}
	for i := range ledgerLimit + 10 {
		l.admit(time.Duration(i).String(), "anna")
	}
	if len(l.entries) > ledgerLimit {
		t.Fatalf("ledger holds %d entries, limit %d", len(l.entries), ledgerLimit)
	}
}

// The sender of a command reads the _Ack answering it whoever wrote that ack,
// and only that ack: not another sender's at the same place, not one under its
// own correlation id elsewhere, not the answer to a command the node refused,
// and never on the strength of an attribution a writer other than the node chose.
func TestAckSentByNamesOnlyTheSenderOfTheAnsweredCommand(t *testing.T) {
	e, _ := ledgerEngine(t)
	const cmd = "colca/v1/_CmdParam/n-edge1/line1/operator/setDensity"
	const at = "colca/v1/_Ack/n-edge1/line1/operator/setDensity"
	anna := humanEntry(t, "cmd:#:param")
	bert := humanEntry(t, "cmd:#:param")
	bert.ULID = "bert"
	if _, err := e.IngestHuman(anna, cmd, cmdPayload("anna-1")); err != nil {
		t.Fatal(err)
	}
	if _, err := e.IngestHuman(bert, cmd, cmdPayload("bert-1")); err != nil {
		t.Fatal(err)
	}
	// carl holds no grant: his command is refused, and its id names no command.
	carl := humanEntry(t)
	carl.ULID = "carl"
	if _, err := e.IngestHuman(carl, cmd, cmdPayload("carl-1")); err == nil {
		t.Fatal("a command without a grant was accepted")
	}

	ack := func(topic, id, writtenBy, actor string) store.StoredRecord {
		return store.StoredRecord{Topic: topic, WrittenBy: writtenBy, ActorID: actor,
			Payload: []byte(`{"correlation_id":"` + id + `","result_code":200}`)}
	}
	for _, tc := range []struct {
		name   string
		rec    store.StoredRecord
		reader string
		want   bool
	}{
		{"own ack, written by the executing service", ack(at, "anna-1", "orchestrator", "orchestrator"), anna.ULID, true},
		{"another sender's ack at the same place", ack(at, "bert-1", "orchestrator", "orchestrator"), anna.ULID, false},
		{"the other sender's own ack", ack(at, "bert-1", "orchestrator", "orchestrator"), "bert", true},
		{"own correlation id at another place", ack("colca/v1/_Ack/n-edge1/line2/operator/setDensity", "anna-1", "orchestrator", "orchestrator"), anna.ULID, false},
		{"the answer to a refused command", ack(at, "carl-1", "orchestrator", "orchestrator"), "carl", false},
		{"an id the node never saw, attributed by its writer", ack(at, "x-1", "orchestrator", anna.ULID), anna.ULID, false},
		{"an ack the node wrote, attributed to the sender", ack(at, "x-2", "n-edge1", anna.ULID), anna.ULID, true},
		{"an ack the node wrote for someone else", ack(at, "x-3", "n-edge1", "bert"), anna.ULID, false},
		{"not an ack", store.StoredRecord{Topic: cmd, WrittenBy: "n-edge1", ActorID: anna.ULID, Payload: cmdPayload("anna-1")}, anna.ULID, false},
		{"no reader", ack(at, "anna-1", "orchestrator", "orchestrator"), "", false},
	} {
		if got := e.AckSentBy(tc.rec, tc.reader); got != tc.want {
			t.Errorf("%s: AckSentBy = %v, want %v", tc.name, got, tc.want)
		}
	}
}
