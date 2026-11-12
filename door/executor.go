package door

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"
)

// Answer codes an Executor writes itself. A handler may answer any code.
const (
	CodeOK        = 200
	CodeForbidden = 403
	CodeConflict  = 409
	CodeInvalid   = 422
	CodeExpired   = 498
	CodeFailed    = 500
	CodeUnknown   = 504
)

// commandsStream is the stream commands and their answers are on.
const commandsStream = "commands"

// operationIDMax is the longest operation_id a command may carry.
const operationIDMax = 128

// actorFieldMax is the longest on_behalf_of id or label.
const actorFieldMax = 255

// idleDrain is how long the executor waits for a hint before it reads the
// stream anyway.
const idleDrain = 5 * time.Minute

// answerBackoffMax bounds the pause between two attempts to publish an answer.
const answerBackoffMax = 5 * time.Second

// Actor names a sender or the person a command acts for.
type Actor struct {
	ID    string `json:"id"`
	Label string `json:"label,omitempty"`
	Kind  string `json:"kind"`
}

// Command is one command as a handler sees it. Sender is the node's
// attestation from the stored record, never the payload's. OnBehalfOf is the
// sender's claim.
type Command struct {
	Contract      string
	Path          string // node-local, verb last
	Verb          string
	Params        json.RawMessage // the payload's "command" object, or nil
	CorrelationID string
	OperationID   string
	OnBehalfOf    *Actor
	ExpiresAt     time.Time // zero: never expires
	Sender        Actor
	TS            int64
	Offset        int64
}

// Answer is a handler's outcome. Code 0 means 200. Result is carried in the
// _Ack as "result".
type Answer struct {
	Code    int
	Message string
	Result  any
}

// Handler runs the commands an Executor routes to it.
type Handler interface {
	// Handles reports whether this handler answers contract at path.
	Handles(contract, path string) bool
	// Execute runs one command. Its context ends at the command's expires_at.
	// An error is answered 500.
	Execute(ctx context.Context, cmd Command) (Answer, error)
}

// GapError is returned by Drain and Run when commands were pruned before the
// executor read them. Recovering is the caller's decision.
type GapError struct{ Gap Gap }

func (e *GapError) Error() string {
	return fmt.Sprintf("commands: offsets %d..%d were pruned before they were read", e.Gap.FromOffset, e.Gap.ToOffset)
}

// Executor reads a local service's commands from the commands stream through
// a cursor of its own, runs each once and answers it with an _Ack at the
// command's path. Its order makes it safe to stop at any point: the ledger
// records a command before it runs and its answer before it is published,
// and the cursor moves only once every answer on a page is published. A
// command seen again is answered from the ledger, or 504 when it was started
// and never answered; it is never run twice.
//
// A command with an operation_id is also recorded under the attested sender
// and that id. A repeat is answered with the recorded outcome and
// "replayed": true, or 409 when it asks for something else.
//
// expires_at is judged by this host's clock, which is the node's: a local
// service runs beside its node. An expired command is answered 498.
type Executor struct {
	Client  *Client
	Handler Handler
	// Contracts are the command contracts the handler executes.
	Contracts []string
	// Node is the node's ULID. Empty: asked from the node.
	Node string
	// Cursor defaults to c/<Client.Service>/commands.
	Cursor string
	// Ledger defaults to a MemoryLedger.
	Ledger Ledger
	// PageSize defaults to 100.
	PageSize int
	Log      *slog.Logger
	// Now defaults to time.Now.
	Now func() time.Time

	answered  map[string]bool
	order     []string
	scannedTo int64
}

func (x *Executor) init(ctx context.Context) error {
	if x.Client == nil || x.Handler == nil || len(x.Contracts) == 0 {
		return errors.New("executor: Client, Handler and Contracts are required")
	}
	if x.Cursor == "" {
		if x.Client.Service == "" {
			return errors.New("executor: Cursor is required without Client.Service")
		}
		x.Cursor = "c/" + x.Client.Service + "/commands"
	}
	if x.Ledger == nil {
		x.Ledger = NewMemoryLedger()
	}
	if x.PageSize <= 0 {
		x.PageSize = 100
	}
	if x.Log == nil {
		x.Log = slog.Default()
	}
	if x.Now == nil {
		x.Now = time.Now
	}
	if x.answered == nil {
		x.answered = map[string]bool{}
	}
	if x.Node == "" {
		node, err := x.Client.ULID(ctx)
		if err != nil {
			return err
		}
		x.Node = node
	}
	return nil
}

func (x *Executor) contracts() []string { return append(append([]string(nil), x.Contracts...), "_Ack") }

// Run drains the stream at start, on every hint the node sends and at least
// every five minutes, until ctx ends. A failed drain is retried with backoff.
// It returns ctx's error, or a *GapError.
func (x *Executor) Run(ctx context.Context) error {
	if err := x.init(ctx); err != nil {
		return err
	}
	var signal Signal
	watchCtx, stop := context.WithCancel(ctx)
	defer stop()
	go x.Client.WatchForeverFor(watchCtx, WatchRequest{Streams: []string{commandsStream}, Contracts: x.contracts()},
		time.Second, func(Hint) { signal.Notify() },
		func(err error) { x.Log.Warn("commands: watch dropped", "err", err) })
	delay := 500 * time.Millisecond
	for {
		changed := signal.Changes()
		err := x.Drain(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		var gap *GapError
		if errors.As(err, &gap) {
			return err
		}
		if err != nil {
			wait := RetryDelay(err, delay)
			x.Log.Error("commands: drain failed", "err", err, "retry_in", wait)
			delay = min(30*time.Second, delay*2)
			if !WaitChange(ctx, nil, wait, time.Time{}) {
				return ctx.Err()
			}
			continue
		}
		delay = 500 * time.Millisecond
		if !WaitChange(ctx, changed, idleDrain, time.Time{}) {
			return ctx.Err()
		}
	}
}

// Drain handles every command up to the stream's head as it stood when the
// drain began, and moves the cursor page by page.
func (x *Executor) Drain(ctx context.Context) error {
	if err := x.init(ctx); err != nil {
		return err
	}
	tail, err := x.Client.FetchWithOptions(ctx, FetchOptions{Stream: commandsStream, Cursor: x.Cursor, Max: 1, Tail: true})
	if err != nil {
		return err
	}
	head := tail.Next - 1
	previous := int64(-1)
	for head > 0 {
		last, err := x.drainPage(ctx, head)
		if err != nil {
			return err
		}
		if last >= head {
			return nil
		}
		if last <= previous {
			return fmt.Errorf("commands: drain stalled at %d before head %d", last, head)
		}
		previous = last
	}
	return nil
}

// drainPage handles one page, acks it and returns the last offset it covered.
func (x *Executor) drainPage(ctx context.Context, head int64) (int64, error) {
	page, err := x.Client.FetchWithOptions(ctx, FetchOptions{Stream: commandsStream, Cursor: x.Cursor,
		Max: x.PageSize, Contracts: x.contracts()})
	if err != nil {
		return 0, err
	}
	if page.Gap != nil {
		return 0, &GapError{Gap: *page.Gap}
	}
	x.noteAnswers(page.Records)
	if page.Next <= head {
		// The answer to a command on this page may be further on.
		if err := x.scanAhead(ctx, page.Next, head); err != nil {
			return 0, err
		}
	}
	for _, rec := range page.Records {
		if err := x.handle(ctx, rec); err != nil {
			return 0, err
		}
	}
	last := page.Next - 1
	if last < 1 {
		return last, nil
	}
	if _, err := x.Client.AckStore(ctx, commandsStream, x.Cursor, last, page.Store); err != nil {
		return 0, err
	}
	return last, nil
}

// noteAnswers remembers the correlation ids answered on this node, so a
// command answered before a restart is neither run nor answered again.
func (x *Executor) noteAnswers(records []Record) {
	for _, rec := range records {
		contract, node, _, ok := splitTopic(rec.Topic)
		if !ok || contract != "_Ack" || node != x.Node {
			continue
		}
		var ack struct {
			CorrelationID string `json:"correlation_id"`
			ResultCode    int    `json:"result_code"`
		}
		// A 202 says a node queued or forwarded it, not that it was answered.
		if json.Unmarshal(rec.Payload, &ack) != nil || ack.CorrelationID == "" || ack.ResultCode == 202 {
			continue
		}
		x.remember(ack.CorrelationID)
	}
}

func (x *Executor) remember(correlationID string) {
	if x.answered[correlationID] {
		return
	}
	x.answered[correlationID] = true
	x.order = append(x.order, correlationID)
	for len(x.order) > ledgerLimit {
		delete(x.answered, x.order[0])
		x.order = x.order[1:]
	}
}

// scanAhead notes the answers between from and head without moving the
// cursor. What an earlier scan read is not read again.
func (x *Executor) scanAhead(ctx context.Context, from, head int64) error {
	offset := max(from, x.scannedTo)
	for offset <= head {
		page, err := x.Client.FetchWithOptions(ctx, FetchOptions{Stream: commandsStream, Cursor: x.Cursor,
			Max: 1000, Contracts: []string{"_Ack"}, From: uint64(offset)}) //nolint:gosec // offset is at least 1
		if err != nil {
			return err
		}
		x.noteAnswers(page.Records)
		if page.Next <= offset {
			return nil
		}
		offset = page.Next
		x.scannedTo = max(x.scannedTo, offset)
	}
	return nil
}

// envelope is the part of a command's payload the executor reads.
type envelope struct {
	Command       json.RawMessage `json:"command"`
	CorrelationID string          `json:"correlation_id"`
	ExpiresAt     *float64        `json:"expires_at"`
	OperationID   json.RawMessage `json:"operation_id"`
	OnBehalfOf    json.RawMessage `json:"on_behalf_of"`
}

// handle runs one record if it is a command for the handler, and answers it.
// An error means an answer is not published yet; the cursor must not move.
func (x *Executor) handle(ctx context.Context, rec Record) error {
	contract, node, path, ok := splitTopic(rec.Topic)
	if !ok || node != x.Node || !strings.HasPrefix(contract, "_Cmd") || !x.Handler.Handles(contract, path) {
		return nil
	}
	var env envelope
	_ = json.Unmarshal(rec.Payload, &env)
	cmd := Command{
		Contract: contract, Path: path, Verb: path[strings.LastIndex(path, "/")+1:],
		CorrelationID: env.CorrelationID,
		Sender:        Actor{ID: rec.ActorID, Label: rec.ActorLabel, Kind: rec.ActorKind},
		TS:            rec.TS, Offset: rec.Offset,
	}
	if len(env.Command) > 0 && env.Command[0] == '{' {
		cmd.Params = env.Command
	}
	if env.ExpiresAt != nil {
		cmd.ExpiresAt = time.UnixMilli(int64(*env.ExpiresAt))
	}
	refusal := x.checkEnvelope(&cmd, env)
	log := x.Log.With("contract", contract, "path", path, "correlation_id", cmd.CorrelationID,
		"sender", firstNonEmpty(cmd.Sender.Label, cmd.Sender.ID))

	if x.answered[cmd.CorrelationID] {
		log.Info("command was answered already; not executed again")
		return nil
	}
	ackTopic := ackTopicFor(rec.Topic)
	if cmd.CorrelationID != "" {
		started, recorded, err := x.Ledger.Entry(cmd.CorrelationID)
		if err != nil {
			return err
		}
		if started {
			if recorded == nil {
				recorded = x.answerBody(cmd, Answer{Code: CodeUnknown,
					Message: "outcome unknown: the service stopped while executing it; it may have taken effect"}, false)
				if err := x.Ledger.Answer(cmd.CorrelationID, recorded); err != nil {
					return err
				}
			}
			log.Info("command was executed before; answering it again, not re-executing")
			return x.publish(ctx, ackTopic, cmd.CorrelationID, recorded)
		}
	}

	var answer Answer
	replayed := false
	opKey := ""
	if refusal == nil {
		opKey = operationKey(cmd)
	}
	digest := operationDigest(cmd)
	opStarted, opRecorded := false, []byte(nil)
	if opKey != "" {
		var err error
		if opStarted, opRecorded, err = x.Ledger.Entry(opKey); err != nil {
			return err
		}
	}
	switch {
	case refusal != nil:
		answer = *refusal
	case opStarted:
		var err error
		if answer, replayed, err = x.replay(opKey, digest, opRecorded); err != nil {
			return err
		}
	case !cmd.ExpiresAt.IsZero() && !cmd.ExpiresAt.After(x.Now()):
		answer = Answer{Code: CodeExpired, Message: "expired before it was executed"}
	default:
		if cmd.CorrelationID != "" {
			if err := x.Ledger.Start(cmd.CorrelationID); err != nil {
				return err
			}
		}
		if opKey != "" {
			if err := x.Ledger.Start(opKey); err != nil {
				return err
			}
		}
		answer = x.run(ctx, cmd)
		if opKey != "" {
			if err := x.Ledger.Answer(opKey, operationRecord(digest, answer)); err != nil {
				return err
			}
		}
	}
	log.Info("command answered", "code", answer.Code, "message", answer.Message, "replayed", replayed)
	if cmd.CorrelationID == "" {
		log.Warn("command has no correlation_id; not answered")
		return nil
	}
	body := x.answerBody(cmd, answer, replayed)
	if err := x.Ledger.Answer(cmd.CorrelationID, body); err != nil {
		return err
	}
	return x.publish(ctx, ackTopic, cmd.CorrelationID, body)
}

// checkEnvelope reads operation_id and on_behalf_of into cmd, and returns the
// answer to a command that must not run for them.
func (x *Executor) checkEnvelope(cmd *Command, env envelope) *Answer {
	refuse := func(code int, message string) *Answer {
		return &Answer{Code: code, Message: "refused: " + message, Result: map[string]any{"outcome": "refused"}}
	}
	if len(env.OperationID) > 0 && string(env.OperationID) != "null" {
		var id string
		if json.Unmarshal(env.OperationID, &id) != nil || id == "" {
			return refuse(CodeInvalid, "operation_id must be a non-empty string")
		}
		if len([]rune(id)) > operationIDMax {
			return refuse(CodeInvalid, fmt.Sprintf("operation_id is longer than %d characters", operationIDMax))
		}
		if strings.Contains(id, "/") || hasControl(id) {
			return refuse(CodeInvalid, "operation_id must not contain '/' or control characters")
		}
		cmd.OperationID = id
	}
	if len(env.OnBehalfOf) > 0 && string(env.OnBehalfOf) != "null" {
		actor, err := parseActor(env.OnBehalfOf)
		if err != nil {
			return refuse(CodeInvalid, err.Error())
		}
		cmd.OnBehalfOf = &actor
		if cmd.Sender.Kind == "human" && actor.ID != cmd.Sender.ID {
			return refuse(CodeForbidden, "a person cannot send a command on behalf of someone else")
		}
	}
	return nil
}

func parseActor(raw json.RawMessage) (Actor, error) {
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil {
		return Actor{}, errors.New("on_behalf_of must be an object with an id")
	}
	actor := Actor{Kind: "human"}
	for key, value := range fields {
		if string(value) == "null" {
			continue
		}
		var s string
		if json.Unmarshal(value, &s) != nil {
			return Actor{}, errors.New("on_behalf_of: id, label and kind are strings")
		}
		switch key {
		case "id":
			actor.ID = s
		case "label":
			actor.Label = s
		case "kind":
			actor.Kind = s
		default:
			return Actor{}, fmt.Errorf("on_behalf_of: unknown field %q", key)
		}
	}
	switch {
	case strings.TrimSpace(actor.ID) == "":
		return Actor{}, errors.New("on_behalf_of needs an id")
	case len([]rune(actor.ID)) > actorFieldMax || len([]rune(actor.Label)) > actorFieldMax:
		return Actor{}, fmt.Errorf("on_behalf_of: id and label are at most %d characters", actorFieldMax)
	case actor.Kind != "human" && actor.Kind != "service" && actor.Kind != "system":
		return Actor{}, errors.New("on_behalf_of: kind must be one of human, service, system")
	case hasControl(actor.ID) || hasControl(actor.Label):
		return Actor{}, errors.New("on_behalf_of: control characters are not allowed")
	}
	return actor, nil
}

func hasControl(s string) bool {
	for _, r := range s {
		if r < 0x20 || r == 0x7f {
			return true
		}
	}
	return false
}

// run calls the handler with a context that ends at the command's deadline.
func (x *Executor) run(ctx context.Context, cmd Command) (answer Answer) {
	if !cmd.ExpiresAt.IsZero() {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, cmd.ExpiresAt)
		defer cancel()
	}
	defer func() {
		if r := recover(); r != nil {
			x.Log.Error("command handler panicked", "contract", cmd.Contract, "path", cmd.Path, "panic", r)
			answer = Answer{Code: CodeFailed, Message: truncateText(fmt.Sprintf("the command failed: %v", r), 200)}
		}
	}()
	answer, err := x.Handler.Execute(ctx, cmd)
	if err != nil {
		return Answer{Code: CodeFailed, Message: truncateText("the command failed: "+err.Error(), 200)}
	}
	if answer.Code == 0 {
		answer.Code = CodeOK
	}
	return answer
}

// operationKey is the ledger key of a command's operation, scoped to the
// attested sender so one sender cannot replay or block another's.
func operationKey(cmd Command) string {
	if cmd.OperationID == "" {
		return ""
	}
	raw, _ := json.Marshal([]string{cmd.Sender.ID, cmd.OperationID})
	return "op\x1f" + string(raw)
}

// operationDigest is what a repeat of an operation must match: what it asks
// for, not when it expires or how it is correlated.
func operationDigest(cmd Command) string {
	var params any
	if len(cmd.Params) > 0 {
		_ = json.Unmarshal(cmd.Params, &params)
	}
	raw, _ := json.Marshal(map[string]any{
		"contract": cmd.Contract, "path": cmd.Path, "command": params, "on_behalf_of": cmd.OnBehalfOf,
	})
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

type operationEntry struct {
	Digest     string          `json:"digest"`
	ResultCode int             `json:"result_code"`
	Message    string          `json:"message"`
	Result     json.RawMessage `json:"result,omitempty"`
}

func operationRecord(digest string, a Answer) []byte {
	entry := operationEntry{Digest: digest, ResultCode: a.Code, Message: a.Message}
	if a.Result != nil {
		entry.Result, _ = json.Marshal(a.Result)
	}
	raw, _ := json.Marshal(entry)
	return raw
}

// replay answers a repeated operation from the ledger.
func (x *Executor) replay(key, digest string, recorded []byte) (a Answer, replayed bool, err error) {
	if recorded == nil {
		// Started and never answered: recorded, so every repeat gets the same.
		a = Answer{Code: CodeUnknown,
			Message: "outcome unknown: the service stopped while executing this operation; it may have taken effect"}
		return a, true, x.Ledger.Answer(key, operationRecord(digest, a))
	}
	var entry operationEntry
	if err := json.Unmarshal(recorded, &entry); err != nil {
		return Answer{}, false, fmt.Errorf("ledger entry %q: %w", key, err)
	}
	if entry.Digest != digest {
		return Answer{Code: CodeConflict,
			Message: "refused: this operation_id was already used for a different command; not executed",
			Result:  map[string]any{"outcome": "refused"}}, false, nil
	}
	a = Answer{Code: entry.ResultCode, Message: entry.Message}
	if len(entry.Result) > 0 {
		a.Result = entry.Result
	}
	return a, true, nil
}

func (x *Executor) answerBody(cmd Command, a Answer, replayed bool) []byte {
	body := map[string]any{
		"correlation_id": cmd.CorrelationID,
		"result_code":    a.Code,
		"message":        a.Message,
		"performed_at":   float64(x.Now().UnixMilli()) / 1000,
	}
	if a.Result != nil {
		body["result"] = a.Result
	}
	if cmd.OperationID != "" {
		body["operation_id"] = cmd.OperationID
	}
	if cmd.OnBehalfOf != nil {
		body["on_behalf_of"] = cmd.OnBehalfOf
	}
	if replayed {
		body["replayed"] = true
	}
	raw, _ := json.Marshal(body)
	return raw
}

// publish sends an answer until the node takes it, or ctx ends.
func (x *Executor) publish(ctx context.Context, topic, correlationID string, answer []byte) error {
	delay := 500 * time.Millisecond
	for {
		err := x.Client.Publish(ctx, topic, json.RawMessage(answer))
		if err == nil {
			x.remember(correlationID)
			return nil
		}
		wait := RetryDelay(err, delay)
		x.Log.Error("commands: answer not published; sending it again", "topic", topic,
			"correlation_id", correlationID, "err", err, "retry_in", wait)
		delay = min(answerBackoffMax, delay*2)
		if !WaitChange(ctx, nil, wait, time.Time{}) {
			return ctx.Err()
		}
	}
}

// splitTopic returns a topic's contract, node and node-local path.
func splitTopic(topic string) (contract, node, path string, ok bool) {
	seg := strings.SplitN(topic, "/", 5)
	if len(seg) < 5 || seg[4] == "" {
		return "", "", "", false
	}
	return seg[2], seg[3], seg[4], true
}

// ackTopicFor is the _Ack topic beside a command topic.
func ackTopicFor(topic string) string {
	seg := strings.SplitN(topic, "/", 4)
	return seg[0] + "/" + seg[1] + "/_Ack/" + seg[3]
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

func truncateText(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return strings.ToValidUTF8(s[:n], "")
}
