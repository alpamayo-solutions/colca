// Command execution at the target node.
//
// A command record addressed to THIS node (level-4 ULID match) is executed
// right after it was durably persisted, and the outcome goes back up as an
// _Ack in the node's own frame. The hook fires on the trusted-down paths only
// — downlink from the parent and the local doors — never on IngestReplicated:
// commands flow down, and a child pushing commands upward must not administer
// its ancestors.
//
// The engine owns the mechanism (is this addressed to me, has it expired, how
// does the outcome get acked, what is counted) and nothing about the meaning:
// what a verb does lives behind the CommandExecutor port. That is what keeps
// domain knowledge — the Colca data model in particular — out of the broker
// core and inside the plugin that owns it.
package engine

import (
	"encoding/json"
	"time"

	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// CommandExecutor executes commands addressed to this node.
//
// Handles decides ownership by contract alone, before anything is parsed or
// executed: a contract nobody claims is a machine's command riding through to
// its target, and the engine leaves it completely alone — no ack, no metric. An
// executor that claims a contract answers every verb of it, returning 422 for
// ones it does not know, because a command aimed at the node deserves an answer
// even when the node cannot carry it out.
type CommandExecutor interface {
	Handles(contract string) bool
	Execute(contract, verb string, payload []byte) (code int, message, result string)
}

// SetExecutor wires the executor in (node startup). An engine without one
// leaves every command it receives unexecuted and unacked.
func (e *Engine) SetExecutor(x CommandExecutor) { e.exec = x }

// RecordObserver is told about records this node persisted through its own
// doors, so a domain plugin can react to state without the core knowing which
// contracts are interesting. Observers run inline: they must be cheap, and
// anything they publish goes through the ordinary doors like everyone else.
type RecordObserver interface {
	Observe(contract, topic string, payload []byte)
}

// SetObserver wires the record observer in (node startup).
func (e *Engine) SetObserver(o RecordObserver) { e.observer = o }

func (e *Engine) observe(p uns.Parsed, topic string, payload []byte) {
	if e.observer != nil {
		e.observer.Observe(p.Contract, topic, payload)
	}
}

// Executors runs several executors as one, routing by the contracts each
// claims. Wiring composes the node's own registry executor with the domain
// plugin's; neither knows the other exists.
func Executors(all ...CommandExecutor) CommandExecutor { return multiExec(all) }

type multiExec []CommandExecutor

func (m multiExec) Handles(contract string) bool {
	for _, x := range m {
		if x.Handles(contract) {
			return true
		}
	}
	return false
}

func (m multiExec) Execute(contract, verb string, payload []byte) (int, string, string) {
	for _, x := range m {
		if x.Handles(contract) {
			return x.Execute(contract, verb, payload)
		}
	}
	return 500, "no executor claims " + contract, "error"
}

// cmdEnvelope is the part of a command payload every class shares (cmdadmin
// design §2): correlation_id routes the ack, expires_at bounds execution.
// Everything else is the verb's own business and stays in the raw payload.
type cmdEnvelope struct {
	CorrelationID string `json:"correlation_id"`
	ExpiresAt     int64  `json:"expires_at"`
}

// maybeExec runs after a _Cmd* record was persisted by a trusted-down path.
func (e *Engine) maybeExec(p uns.Parsed, payload []byte) {
	if p.NodeID != e.cfg.ULID || e.exec == nil || !e.exec.Handles(p.Contract) {
		return
	}
	verb := p.Path // at the target the mount-stripped path IS the verb

	var env cmdEnvelope
	if err := json.Unmarshal(payload, &env); err != nil || env.CorrelationID == "" {
		// An ack without a correlation_id is unroutable: log and count, there
		// is nothing else to be done with it.
		e.log.Error("command: unexecutable payload — no ack possible",
			"contract", p.Contract, "verb", verb, "err", err)
		e.metrics.NodeCmd(p.Contract, verb, "invalid")
		return
	}

	// Expiry is checked before the executor sees it: a command whose window
	// closed must not take effect, whatever it would have done.
	if env.ExpiresAt <= time.Now().UnixMilli() {
		e.ack(p.Contract, verb, env.CorrelationID, 498, "expired", "expired")
		return
	}

	code, msg, result := e.exec.Execute(p.Contract, verb, payload)
	e.ack(p.Contract, verb, env.CorrelationID, code, msg, result)
}

// ack publishes the execution outcome into the node's own commands stream (own
// frame — uplink mount-insert rebuilds the path hop by hop, cmdadmin design §6)
// and counts the metric.
func (e *Engine) ack(contract, verb, corr string, code int, msg, result string) {
	e.metrics.NodeCmd(contract, verb, result)
	topic := "colca/v1/_Ack/" + e.cfg.ULID + "/" + verb
	payload, err := json.Marshal(map[string]any{
		"correlation_id": corr, "result_code": code, "message": msg,
	})
	if err != nil {
		e.log.Error("command: ack encode failed", "verb", verb, "correlation_id", corr, "err", err)
		return
	}
	p, err := uns.Parse(topic)
	if err != nil {
		e.log.Error("command: ack topic invalid", "topic", topic, "err", err)
		return
	}
	if _, err := e.persist(uns.ClassAck, p, topic, payload); err != nil {
		e.log.Error("command: ack persist failed", "topic", topic, "correlation_id", corr, "err", err)
		return
	}
	e.log.Info("command executed", "contract", contract, "verb", verb,
		"correlation_id", corr, "result_code", code, "message", msg)
}
