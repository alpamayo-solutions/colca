// Command execution at the target node.
//
// A command addressed to this node is executed right after it is persisted, and
// the outcome goes back up as an _Ack. Only paths coming down execute (the
// parent's downlink and the local doors), never IngestReplicated: a child must
// not command its ancestors. The engine owns the mechanism; what a verb means
// lives behind CommandExecutor.

package engine

import (
	"encoding/json"
	"time"

	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// CommandExecutor executes commands addressed to this node.
//
// Handles decides by contract alone. A contract nobody claims is a machine's
// command passing through, and the engine leaves it alone: no ack, no metric. An
// executor that claims a contract answers every verb of it, with 422 for verbs it
// does not know.
type CommandExecutor interface {
	Handles(contract string) bool
	Execute(ctx uns.CommandContext, contract, verb string, payload []byte) (code int, message, result string)
}

type stateWritingCommandExecutor interface {
	ExecuteWithWrites(
		ctx uns.CommandContext,
		contract, verb string,
		payload []byte,
	) (code int, message, result string, writes []uns.StateWrite)
}

// CommandOutcome is returned synchronously to local HTTP command callers and
// is also persisted in the normal _Ack event. StateWrites are the exact
// records the command produced, so callers wait on state rather than merely
// waiting for the command event itself.
type CommandOutcome struct {
	CorrelationID string           `json:"correlation_id"`
	ResultCode    int              `json:"result_code"`
	Message       string           `json:"message"`
	StateWrites   []uns.StateWrite `json:"state_writes,omitempty"`
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

// ReplayRetained hands every retained record to the observer once, in store
// order, so state persisted before this process started reaches it too,
// including bindings a re-declaration wiped. Observers are idempotent, so
// replaying on every boot is harmless. Topics that do not parse are skipped.
func (e *Engine) ReplayRetained() {
	if e.observer == nil {
		return
	}
	entries, err := e.store.KVScan("")
	if err != nil {
		e.log.Warn("retained replay skipped: KV scan failed", "err", err)
		return
	}
	for _, kv := range entries {
		parsed, err := uns.Parse(kv.Topic)
		if err != nil {
			continue
		}
		e.observer.Observe(parsed.Contract, kv.Topic, kv.Payload)
	}
}

// SetSubscriberCheck wires the local-bus subscriber lookup once the broker
// exists. See HasSubscriberFor and deliverCommand.
func (e *Engine) SetSubscriberCheck(fn HasSubscriberFor) { e.hasSubscriber = fn }

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

func (m multiExec) Execute(ctx uns.CommandContext, contract, verb string, payload []byte) (int, string, string) {
	for _, x := range m {
		if x.Handles(contract) {
			return x.Execute(ctx, contract, verb, payload)
		}
	}
	return 500, "no executor claims " + contract, "error"
}

func (m multiExec) ExecuteWithWrites(
	ctx uns.CommandContext,
	contract, verb string,
	payload []byte,
) (int, string, string, []uns.StateWrite) {
	for _, x := range m {
		if !x.Handles(contract) {
			continue
		}
		if writer, ok := x.(stateWritingCommandExecutor); ok {
			return writer.ExecuteWithWrites(ctx, contract, verb, payload)
		}
		code, message, result := x.Execute(ctx, contract, verb, payload)
		return code, message, result, nil
	}
	return 500, "no executor claims " + contract, "error", nil
}

// cmdEnvelope is the part of a command payload every class shares:
// correlation_id routes the ack, expires_at bounds execution. The rest belongs to
// the verb.
type cmdEnvelope struct {
	CorrelationID string `json:"correlation_id"`
	ExpiresAt     int64  `json:"expires_at"`
}

// maybeExec runs after a _Cmd* record was persisted by a path coming down. actor
// is the identity the door authorized (nil at the admin door); for a downlinked
// record the person is reconstituted from the attested groups in its
// attribution.
func (e *Engine) maybeExec(
	p uns.Parsed,
	payload []byte,
	attribution Attribution,
	actor *uns.Entry,
) *CommandOutcome {
	if p.NodeID != e.cfg.ULID || e.exec == nil || !e.exec.Handles(p.Contract) {
		return nil
	}
	verb := p.Path // at the target the mount-stripped path IS the verb

	var env cmdEnvelope
	if err := json.Unmarshal(payload, &env); err != nil || env.CorrelationID == "" {
		// An ack without a correlation_id is unroutable: log and count, there
		// is nothing else to be done with it.
		e.log.Error("command: unexecutable payload — no ack possible",
			"contract", p.Contract, "verb", verb, "err", err)
		e.metrics.NodeCmd(p.Contract, verb, "invalid")
		return nil
	}

	// Expiry is checked before the executor sees it: a command whose window
	// closed must not take effect, whatever it would have done.
	if env.ExpiresAt <= time.Now().UnixMilli() {
		outcome := &CommandOutcome{
			CorrelationID: env.CorrelationID,
			ResultCode:    498,
			Message:       "expired",
		}
		e.ack(p.Contract, verb, outcome, "expired", attribution)
		return outcome
	}

	var code int
	var msg, result string
	var writes []uns.StateWrite
	// attribution is the command record's own door-verified attribution — the
	// same fact ack() below stamps onto this command's outcome. Carrying it
	// into ctx lets EntityStore.PublishBatch/PublishEvent attribute an
	// executor's entity writes to the commanding actor, not just to the node.
	ctx := uns.CommandContext{
		Actor:      actor,
		ActorID:    attribution.ActorID,
		ActorLabel: attribution.ActorLabel,
		ActorKind:  attribution.ActorKind,
	}
	if writer, ok := e.exec.(stateWritingCommandExecutor); ok {
		code, msg, result, writes = writer.ExecuteWithWrites(ctx, p.Contract, verb, payload)
	} else {
		code, msg, result = e.exec.Execute(ctx, p.Contract, verb, payload)
	}
	outcome := &CommandOutcome{
		CorrelationID: env.CorrelationID,
		ResultCode:    code,
		Message:       msg,
		StateWrites:   writes,
	}
	e.ack(p.Contract, verb, outcome, result, attribution)
	return outcome
}

// ack publishes the execution outcome into this node's commands stream in its
// own frame and counts it. The uplink inserts mounts hop by hop on the way up.
func (e *Engine) ack(
	contract, verb string,
	outcome *CommandOutcome,
	result string,
	attribution Attribution,
) {
	e.metrics.NodeCmd(contract, verb, result)
	topic := uns.Prefix() + "_Ack/" + e.cfg.ULID + "/" + verb
	payload, err := json.Marshal(outcome)
	if err != nil {
		e.log.Error("command: ack encode failed", "verb", verb, "correlation_id", outcome.CorrelationID, "err", err)
		return
	}
	p, err := uns.Parse(topic)
	if err != nil {
		e.log.Error("command: ack topic invalid", "topic", topic, "err", err)
		return
	}
	ackAttribution := Attribution{
		WrittenBy: e.cfg.ULID, ActorID: attribution.ActorID,
		ActorLabel: attribution.ActorLabel, ActorKind: attribution.ActorKind,
	}
	if _, err := e.persistAttributed(uns.ClassAck, p, topic, payload, ackAttribution); err != nil {
		e.log.Error("command: ack persist failed", "topic", topic, "correlation_id", outcome.CorrelationID, "err", err)
		return
	}
	e.log.Info("command executed", "contract", contract, "verb", verb,
		"correlation_id", outcome.CorrelationID, "result_code", outcome.ResultCode,
		"message", outcome.Message)
}
