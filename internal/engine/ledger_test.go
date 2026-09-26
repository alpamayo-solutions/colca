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
