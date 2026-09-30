// Commands queued for a child node.
//
// A command addressed below a child's mount waits in this node's commands
// stream until the child fetches it, for as long as retention keeps it. The
// child's downlink cursor on this node is the queue position. This file holds
// what the node says about such a command while it waits:
//
//   - progress: a 202 _Ack when it is queued and each time a node forwards it,
//     for a sender that asked with "progress": true;
//   - refusal at forwarding: a 403 _Ack instead of forwarding a command whose
//     sender lost the grant it was admitted on, or was revoked here;
//   - drops: a 410 _Ack for every command that will never be handed on, because
//     the child was retired or retention pruned it past a stale cursor.
//
// Every answer is an _Ack record on this node's commands stream at the
// command's own position, where the executor's answer will also arrive, so the
// sender reads all of them with one cursor. Answers rise to the ancestors like
// any ack.

package engine

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/alpamayo-solutions/colca/internal/metrics"
	"github.com/alpamayo-solutions/colca/internal/store"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// Progress stages of a queued command. A progress ack carries result_code 202;
// every other code is an outcome.
const (
	AckProgress = 202

	StageQueued    = "queued"
	StageForwarded = "forwarded"
)

// Codes for queued commands the node answers itself.
const (
	ackRevoked = 403
	ackDropped = 410
)

// progressAck is a 202 _Ack. It is never the command's outcome.
type progressAck struct {
	CorrelationID string `json:"correlation_id"`
	ResultCode    int    `json:"result_code"`
	Stage         string `json:"stage"`
	Message       string `json:"message"`
	PerformedAt   int64  `json:"performed_at"`
}

// isProgressAck reports whether an ack payload is a 202 progress ack.
func isProgressAck(payload []byte) bool {
	var body struct {
		ResultCode int `json:"result_code"`
	}
	return json.Unmarshal(payload, &body) == nil && body.ResultCode == AckProgress
}

// ackTopicFor is the _Ack topic at a command's own position on this node: the
// owner and path of the command, the contract replaced.
func ackTopicFor(p uns.Parsed) string {
	return uns.Prefix() + "_Ack/" + p.NodeID + "/" + p.Path
}

// queuedForChild reports whether a command stored here waits for a child node:
// it is not for this node, not for a machine on this node's bus, and some
// enrolled child's mount covers it.
func (e *Engine) queuedForChild(p uns.Parsed) bool {
	if p.NodeID == e.cfg.ULID {
		return false
	}
	if target, ok := e.ids.Get(p.NodeID); ok && target.MayUseDoor(uns.DoorMQTT) {
		return false
	}
	router, ok := e.ids.(routableMounts)
	return ok && router.RoutesUnder(p.Path)
}

// noteQueued writes the "queued" progress ack for a command a door just
// accepted, when its sender asked for progress and it waits for a child.
func (e *Engine) noteQueued(p uns.Parsed, payload []byte, attribution Attribution) {
	if !uns.CommandWantsProgress(payload) || !e.queuedForChild(p) {
		return
	}
	e.writeProgress(p, payload, attribution, StageQueued,
		"accepted and queued for the child node that holds "+p.Path)
}

// NoteForwarded writes the "forwarded" progress ack for a command a child node
// fetched from this node, when its sender asked for progress. child names the
// child node.
func (e *Engine) NoteForwarded(rec store.StoredRecord, child string) {
	if !uns.CommandWantsProgress(rec.Payload) {
		return
	}
	p, err := uns.Parse(rec.Topic)
	if err != nil {
		return
	}
	e.writeProgress(p, rec.Payload, attributionOf(rec), StageForwarded, "handed to child node "+child)
}

func (e *Engine) writeProgress(p uns.Parsed, payload []byte, attribution Attribution, stage, message string) {
	id := correlationID(payload)
	if id == "" {
		return
	}
	body, err := json.Marshal(progressAck{
		CorrelationID: id, ResultCode: AckProgress, Stage: stage, Message: message,
		PerformedAt: e.AuthoritativeNow().UnixMilli(),
	})
	if err != nil {
		return
	}
	e.persistAck(p, body, attribution)
}

// persistAck stores an ack this node writes about a command it holds, at the
// command's own position, attributed to the node and to the command's actor so
// it reaches the sender.
func (e *Engine) persistAck(p uns.Parsed, body []byte, attribution Attribution) {
	topic := ackTopicFor(p)
	ap, err := uns.Parse(topic)
	if err != nil {
		e.log.Error("command: ack topic invalid", "topic", topic, "err", err)
		return
	}
	if _, err := e.persistAttributed(uns.ClassAck, ap, topic, body, ackAttribution(e.cfg.ULID, attribution)); err != nil {
		e.log.Error("command: ack persist failed", "topic", topic, "correlation_id", correlationID(body), "err", err)
	}
}

func ackAttribution(node string, of Attribution) Attribution {
	return Attribution{WrittenBy: node, ActorID: of.ActorID, ActorLabel: of.ActorLabel, ActorKind: of.ActorKind}
}

func attributionOf(rec store.StoredRecord) Attribution {
	return Attribution{
		WrittenBy: rec.WrittenBy, ActorID: rec.ActorID, ActorLabel: rec.ActorLabel,
		ActorKind: rec.ActorKind, ActorGroups: rec.ActorGroups,
	}
}

// ForwardRefusal says why a command queued here must not be handed to a child
// any more, or "" when it may go. The grant check of the door that admitted the
// command on this node is repeated at the moment of forwarding, against what
// this node knows now:
//
//   - a person admitted at the human door, from the groups the door attested,
//     against this node's current _Group definitions;
//   - a service or machine admitted at the client door (or a person a local
//     service attested), against its current grants; a sender revoked here since
//     is refused outright.
//
// A command that came down from the parent was checked where it was admitted
// and is not checked again here, and neither is one the admin door took on its
// token. _CmdEdit is authorized by its executor against the plan it composes.
func (e *Engine) ForwardRefusal(rec store.StoredRecord) string {
	if rec.Door != doorClient && rec.Door != doorHuman {
		return ""
	}
	p, err := uns.Parse(rec.Topic)
	if err != nil || uns.AuthorizedAtExecutor(p.Contract) {
		return ""
	}
	actor := e.actorForAttested(attributionOf(rec))
	if actor == nil && rec.Door == doorClient {
		entry, ok := e.ids.Get(rec.WrittenBy)
		if !ok {
			if e.store.Revoked(rec.WrittenBy) {
				return fmt.Sprintf("not forwarded: its sender %s was revoked after sending it", rec.WrittenBy)
			}
			return ""
		}
		actor = entry
	}
	if actor != nil && uns.Authorize(e.Scope(), actor, uns.ActCmd, rec.Topic) {
		return ""
	}
	return fmt.Sprintf("not forwarded: its sender no longer holds a grant for %s %s", p.Contract, p.Path)
}

// answeredHere remembers the correlation ids this node answered about queued
// commands, so a refusal repeated on a re-poll is not written twice.
type answeredHere struct {
	mu    sync.Mutex
	ids   map[string]struct{}
	order []string
}

const answeredHereLimit = 4096

func (a *answeredHere) first(id string) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.ids == nil {
		a.ids = map[string]struct{}{}
	}
	if _, seen := a.ids[id]; seen {
		return false
	}
	a.ids[id] = struct{}{}
	a.order = append(a.order, id)
	if len(a.order) > answeredHereLimit {
		delete(a.ids, a.order[0])
		a.order = a.order[1:]
	}
	return true
}

// RefuseForwarding answers a queued command that is not forwarded with a 403
// _Ack. A command answered already is not answered again.
func (e *Engine) RefuseForwarding(rec store.StoredRecord, message string) {
	e.answerQueued(rec, ackRevoked, message, metrics.CommandDropRevoked)
}

func (e *Engine) answerQueued(rec store.StoredRecord, code int, message, reason string) {
	ack, ok := e.QueuedAnswer(rec, code, message)
	if !ok || !e.answered.first(correlationID(rec.Payload)) {
		return
	}
	p, err := uns.Parse(ack.Topic)
	if err != nil {
		return
	}
	attribution := Attribution{WrittenBy: ack.WrittenBy, ActorID: ack.ActorID, ActorLabel: ack.ActorLabel, ActorKind: ack.ActorKind}
	if _, err := e.persistAttributed(uns.ClassAck, p, ack.Topic, ack.Payload, attribution); err != nil {
		e.log.Error("command: answer to a queued command not stored", "topic", ack.Topic, "err", err)
		return
	}
	e.metrics.CommandDropped(reason)
	e.log.Warn("queued command answered by the node", "topic", rec.Topic,
		"correlation_id", correlationID(rec.Payload), "result_code", code, "message", message)
}

// QueuedAnswer builds the _Ack record answering a queued command with code and
// message, without storing it: the pruner appends it in its own batch. ok is
// false for a command without a correlation id, which nobody waits on.
func (e *Engine) QueuedAnswer(rec store.StoredRecord, code int, message string) (store.Record, bool) {
	p, err := uns.Parse(rec.Topic)
	id := correlationID(rec.Payload)
	if err != nil || id == "" {
		return store.Record{}, false
	}
	body, err := json.Marshal(CommandOutcome{CorrelationID: id, ResultCode: code, Message: message})
	if err != nil {
		return store.Record{}, false
	}
	a := ackAttribution(e.cfg.ULID, attributionOf(rec))
	return store.Record{
		Topic: ackTopicFor(p), Payload: body, TS: time.Now().UnixMilli(),
		WrittenBy: a.WrittenBy, ActorID: a.ActorID, ActorLabel: a.ActorLabel, ActorKind: a.ActorKind,
	}, true
}

// QueuedFor returns which commands on this node's commands stream the cursor
// named cursor holds for delivery, or ok false for any other cursor: a child
// node's downlink cursor holds the commands under its mount, a machine's
// delivery cursor the commands addressed to it.
func (e *Engine) QueuedFor(cursor string) (func(store.StoredRecord) bool, string, bool) {
	now := e.AuthoritativeNow().UnixMilli()
	if child, ok := strings.CutPrefix(cursor, uns.DownlinkCursorPrefix); ok {
		entry, known := e.ids.Get(child)
		if !known || !entry.ReplicatesUp() {
			return nil, "", false
		}
		mount, placed := e.elements.PathOf(entry.Element)
		if !placed {
			return nil, "", false
		}
		return func(r store.StoredRecord) bool {
			p, err := uns.Parse(r.Topic)
			return err == nil && uns.IsCommand(e.ClassOf(p.Contract)) && uns.UnderMount(p.Path, mount) &&
				uns.CommandStillLive(r.Payload, now)
		}, "child node " + child, true
	}
	lister, ok := e.ids.(interface{ List() []*uns.Entry })
	if !ok {
		return nil, "", false
	}
	for _, entry := range lister.List() {
		if entry.MayUseDoor(uns.DoorMQTT) && entry.CommandCursor() == cursor {
			ulid := entry.ULID
			return func(r store.StoredRecord) bool {
				return !e.CommandRetired(r) && uns.OwedCommand(r.Topic, r.Payload, ulid, now)
			}, "machine " + ulid, true
		}
	}
	return nil, "", false
}

// DropQueuedFor prepares the answer to every live command still queued for
// child, a child node about to be retired: it will never fetch them. Call it
// before the retirement, which deletes the child's downlink cursor and mount,
// and call the returned function once the retirement succeeded; answering
// after it means the child can no longer fetch a command that was already
// answered. The function answers each with a 410 _Ack and returns how many.
// It returns nil when child is not an enrolled child node.
func (e *Engine) DropQueuedFor(child string) func() int {
	entry, known := e.ids.Get(child)
	if !known || !entry.ReplicatesUp() {
		return nil
	}
	cursor := uns.DownlinkCursorPrefix + child
	queued, _, ok := e.QueuedFor(cursor)
	if !ok {
		return nil
	}
	from := e.store.CursorGet(cursor, "commands")
	return func() int {
		if lwm := e.store.LWM("commands"); lwm > from {
			from = lwm
		}
		next := e.store.NextOffset("commands")
		message := "dropped: child node " + child + " was retired before it received the command"
		dropped := 0
		for from < next {
			recs, nxt, err := e.store.ReadRecords("commands", from, 500, queued)
			if err != nil {
				e.log.Error("retire: queued commands not answered — their senders will not learn they were dropped",
					"child", child, "err", err)
				return dropped
			}
			for _, r := range recs {
				e.answerQueued(r, ackDropped, message, metrics.CommandDropRetired)
				dropped++
			}
			if nxt <= from {
				break
			}
			from = nxt
		}
		return dropped
	}
}
