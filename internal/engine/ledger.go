package engine

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/alpamayo-solutions/colca/internal/metrics"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// A correlation id names one command for as long as the ledger remembers it:
// longer than any command may live, so a resent command cannot run twice.
const (
	ledgerWindow = 10 * time.Minute
	ledgerLimit  = 16384
)

// commandLedger remembers, per correlation id, who sent a command and the ack
// it got. The broker asks it whom an ack belongs to; the doors ask it whether a
// command was already accepted, and answer a repeat with the stored ack instead
// of running it again. It lives in memory: a restart forgets it, as it forgets
// the sessions the acks are delivered to.
type commandLedger struct {
	mu      sync.Mutex
	entries map[string]*ledgerEntry
	order   []ledgerKey // insertion order, for expiry
	now     func() time.Time
}

type ledgerEntry struct {
	actor    string
	at       time.Time
	ackTopic string
	ack      []byte
}

type ledgerKey struct {
	id string
	at time.Time
}

func newCommandLedger() *commandLedger {
	return &commandLedger{entries: map[string]*ledgerEntry{}, now: time.Now}
}

// correlationID reads a command's or an ack's correlation id; "" when it has none.
func correlationID(payload []byte) string {
	var body struct {
		CorrelationID string `json:"correlation_id"`
	}
	if json.Unmarshal(payload, &body) != nil {
		return ""
	}
	return body.CorrelationID
}

// ledgerVerdict is what the ledger says about a command about to be accepted.
type ledgerVerdict int

const (
	ledgerNew        ledgerVerdict = iota // first time: accept it
	ledgerRepeat                          // the same sender again: do not run it
	ledgerForeignUse                      // another sender's id: refuse it
)

// admit records a command's correlation id for actor, or says why it is not new.
// A repeat carries the stored ack, nil while the first one is still running.
func (l *commandLedger) admit(id, actor string) (ledgerVerdict, string, []byte) {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	l.expire(now)
	if held, ok := l.entries[id]; ok {
		if held.actor != actor {
			return ledgerForeignUse, "", nil
		}
		return ledgerRepeat, held.ackTopic, held.ack
	}
	l.entries[id] = &ledgerEntry{actor: actor, at: now}
	l.order = append(l.order, ledgerKey{id: id, at: now})
	return ledgerNew, "", nil
}

// forget drops an id whose command was not accepted after all, so resending it
// is not taken for a repeat.
func (l *commandLedger) forget(id string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.entries, id)
}

// acked keeps the ack of a remembered command, for a repeat to be answered with.
func (l *commandLedger) acked(id, topic string, payload []byte) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if held, ok := l.entries[id]; ok && held.ack == nil {
		held.ackTopic, held.ack = topic, append([]byte(nil), payload...)
	}
}

// lookup is what the ledger holds for id.
func (l *commandLedger) lookup(id string) (string, string, []byte) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if held, ok := l.entries[id]; ok {
		return held.actor, held.ackTopic, held.ack
	}
	return "", "", nil
}

// expire drops what is older than the window, and the oldest entries past the
// limit. The caller holds mu.
func (l *commandLedger) expire(now time.Time) {
	drop := 0
	for drop < len(l.order) {
		key := l.order[drop]
		if now.Sub(key.at) < ledgerWindow && len(l.entries) < ledgerLimit {
			break
		}
		if held, ok := l.entries[key.id]; ok && held.at.Equal(key.at) {
			delete(l.entries, key.id)
		}
		drop++
	}
	l.order = l.order[drop:]
}

// admitCommand applies the ledger to a command a door is about to accept, as
// sent by actor. A repeat is answered here: its stored ack goes out again and
// nothing runs. The returned id is the one to forget if the command is not
// accepted after all.
func (e *Engine) admitCommand(payload []byte, actor string) (string, bool, error) {
	id := correlationID(payload)
	if id == "" {
		return "", false, nil
	}
	verdict, ackTopic, ack := e.ledger.admit(id, actor)
	switch verdict {
	case ledgerForeignUse:
		_, err := e.reject(metrics.ReasonValidation, "correlation id %q already names another sender's command", id)
		return "", false, err
	case ledgerRepeat:
		e.log.Info("command repeated: not run again", "correlation_id", id, "ack_resent", ack != nil)
		if ack != nil && e.deliver != nil {
			e.deliver(ackTopic, ack, false)
		}
		return "", true, nil
	}
	return id, false, nil
}

// repeated is the Result a door answers a repeated command with: nothing stored,
// and the outcome of the first one when the node has its ack.
func (e *Engine) repeated(payload []byte) Result {
	res := Result{Duplicate: true}
	if _, _, ack := e.ledger.lookup(correlationID(payload)); ack != nil {
		var outcome CommandOutcome
		if json.Unmarshal(ack, &outcome) == nil {
			res.Command = &outcome
		}
	}
	return res
}

// AckRecipient says whether topic is an ack, and if so who sent the command it
// answers: "" when the node does not know, because the command came before a
// restart or carried no correlation id.
func (e *Engine) AckRecipient(topic string, payload []byte) (string, bool) {
	p, err := uns.Parse(topic)
	if err != nil || !uns.IsAck(e.ClassOf(p.Contract)) {
		return "", false
	}
	id := correlationID(payload)
	if id == "" {
		return "", true
	}
	sender, _, _ := e.ledger.lookup(id)
	return sender, true
}
