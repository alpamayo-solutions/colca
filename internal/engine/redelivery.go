// Command delivery tracking and redelivery.
//
// A command is delivered once, onto a bus, to a machine that may not be
// listening. Machines connect with a clean session, so every redelivery comes from
// the record on the commands stream.
//
// The rule: a machine's delivery cursor advances only past commands that reached
// a live subscription of that machine, and everything above it is replayed when
// the machine next subscribes. The subscription must belong to the machine
// itself, or an observer could mark its commands delivered. A command with an
// older one still owed is held for the replay instead of published ahead of it,
// since a watermark cannot express "A is owed but B was delivered".

package engine

import (
	"encoding/json"
	"sync"

	"github.com/alpamayo-solutions/colca/internal/store"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// replayBatch is how many records one replay pass reads, and how often the cursor
// is committed, which bounds what a crash mid-replay can repeat.
const replayBatch = 256

// replayCap bounds how many records one subscribe may publish, so a large backlog
// cannot tie up OnSubscribed. Hitting it is logged, and the cursor stays where
// publishing stopped, so the rest goes out on the next subscribe.
const replayCap = 4096

// deliverCommand publishes a command onto the local bus and, when it is addressed
// to a machine enrolled at this node, records whether it reached a live
// subscription of that machine. The answer feeds the undelivered metric and the
// machine's delivery cursor, where ReplayOwedCommands starts. It reports
// subscriptions, not bytes on the wire, so the metric can undercount failures but
// not overcount them.
//
// Nothing is counted without a subscriber lookup, for a target this node's bus
// cannot reach (a descendant, a child node, this node itself), or for a command
// that arrived already expired.
//
// The cursor moves for a delivered or expired command, by compare-and-swap from
// this record's offset, never by a forward ack: an ack would skip commands still
// owed below this one. For the same reason the command is published live only
// when nothing older is owed; otherwise the replay delivers it in order, and
// publishing it now would hand the machine a second copy. The whole decision runs
// under the target's replay lock.
func (e *Engine) deliverCommand(class uns.Class, p uns.Parsed, topic string, payload []byte, offset uint64) {
	target, ok := e.ids.Get(p.NodeID)
	if !ok || !target.MayUseDoor(uns.DoorMQTT) || e.hasSubscriber == nil {
		// Not addressed to a machine this bus can reach, or no subscriber lookup wired: no
		// delivery floor applies, so publish as usual.
		e.deliver(topic, payload, retainFor(class))
		return
	}
	unlock := e.lockReplay(target.ULID)
	defer unlock()

	stream, cursor := uns.StreamFor(class), target.CommandCursor()
	floor := e.store.CursorGet(cursor, stream)
	if e.owedBelow(stream, floor, offset, target.ULID) {
		e.log.Info("command held for in-order replay: this machine has older commands still owed",
			"topic", topic, "offset", offset, "floor", floor,
			"correlation_id", correlationIDOf(payload))
		return
	}
	e.deliver(topic, payload, retainFor(class))

	// Asked after the publish, deliberately: the question is whether the
	// publish that just happened reached a subscription of this machine.
	delivered := e.hasSubscriber(topic, target.ULID)
	if delivered || !uns.CommandStillLive(payload, e.AuthoritativeNow().UnixMilli()) {
		// A forward ack is correct here: owedBelow just established that nothing between
		// the floor and this record is owed. It also lets the floor cross unrelated
		// records in between.
		e.store.CursorAck(cursor, stream, offset+1)
		return
	}
	e.metrics.CommandUndelivered()
	e.log.Warn("command published to the local bus had no live subscription — "+
		"holding it for replay on the target's next subscribe",
		"topic", topic, "correlation_id", correlationIDOf(payload))
}

// owedBelow reports whether a command still owed to ulid sits below offset. The
// floor only advances over this machine's records while the stream also carries
// acks and other machines' commands, so a floor far below the head is normal, and
// comparing floor with offset would hold every command. The scan ends at offset at
// the latest. A read error answers yes: a held command is only delayed, while a
// duplicate is a second physical action.
func (e *Engine) owedBelow(stream string, floor, offset uint64, ulid string) bool {
	if floor >= offset {
		return false
	}
	now := e.AuthoritativeNow().UnixMilli()
	recs, _, err := e.store.ReadRecords(stream, floor, 1, func(r store.StoredRecord) bool {
		return !e.CommandRetired(r) && uns.OwedCommand(r.Topic, r.Payload, ulid, now)
	})
	if err != nil {
		e.log.Warn("command held: could not read the commands stream to check for older owed commands",
			"ulid", ulid, "floor", floor, "offset", offset, "err", err)
		return true
	}
	return len(recs) > 0 && recs[0].Offset < offset
}

// ReplayOwedCommands republishes every command addressed to entry above its
// delivery cursor and returns how many it published. The MQTT door calls it when
// that identity subscribes.
//
// It publishes onto the bus rather than writing to one client, which would mean
// reimplementing mochi's packet ids, inflight and quotas. Other subscribers to the
// topic, such as an observer with read:#, see the copy too; which commands are
// selected still depends only on the target.
//
// A record goes out only if the machine has a live subscription for its topic
// right now, so a replay triggered before the command subscription exists
// publishes nothing and the next subscribe runs it again. Replays are serialized
// per identity because two can overlap (a session takeover, or two clients with
// one certificate), and only the lock prevents a double publish. The cursor is
// committed per batch.
func (e *Engine) ReplayOwedCommands(entry *uns.Entry) int {
	if entry == nil || !entry.MayUseDoor(uns.DoorMQTT) || e.deliver == nil || e.hasSubscriber == nil {
		return 0
	}
	unlock := e.lockReplay(entry.ULID)
	defer unlock()

	stream := uns.StreamFor(uns.ClassCmd)
	cursor := entry.CommandCursor()
	from := e.store.CursorGet(cursor, stream)
	head := e.store.NextOffset(stream)
	now := e.AuthoritativeNow().UnixMilli()

	published := 0
	for from < head && published < replayCap {
		recs, next, err := e.store.ReadRecords(stream, from, replayBatch, func(r store.StoredRecord) bool {
			return !e.CommandRetired(r) && uns.OwedCommand(r.Topic, r.Payload, entry.ULID, now)
		})
		if err != nil {
			e.log.Warn("command replay stopped: stream read failed",
				"ulid", entry.ULID, "from", from, "err", err)
			break
		}
		stalled := false
		for _, r := range recs {
			// Asked per record: a record whose topic the machine is not listening on must stay
			// owed rather than be marked delivered.
			if !e.hasSubscriber(r.Topic, entry.ULID) {
				next, stalled = r.Offset, true
				break
			}
			e.deliver(r.Topic, r.Payload, retainFor(uns.ClassCmd))
			e.metrics.CommandRedelivered()
			e.log.Info("command replayed from the durable stream",
				"topic", r.Topic, "offset", r.Offset,
				"correlation_id", correlationIDOf(r.Payload))
			published++
		}
		// next also counts records the filter skipped, so the cursor advances over other
		// machines' commands and expired ones. On a stall it was reset above to the
		// record that blocked.
		from = next
		e.store.CursorAck(cursor, stream, from)
		if stalled {
			break
		}
	}
	if published >= replayCap {
		e.log.Warn("command replay hit its per-subscribe cap; the remainder stays owed "+
			"and follows on this machine's next subscribe",
			"ulid", entry.ULID, "published", published, "cap", replayCap)
	}
	return published
}

// lockReplay serializes replays per identity and returns the unlock. The map
// grows by one entry per machine ever seen, which the registry bounds.
func (e *Engine) lockReplay(ulid string) func() {
	e.replayMu.Lock()
	if e.replayLocks == nil {
		e.replayLocks = map[string]*sync.Mutex{}
	}
	mu, ok := e.replayLocks[ulid]
	if !ok {
		mu = &sync.Mutex{}
		e.replayLocks[ulid] = mu
	}
	e.replayMu.Unlock()
	mu.Lock()
	return mu.Unlock
}

// correlationIDOf extracts correlation_id for log lines, best effort; validation
// already requires it on every _Cmd* payload.
func correlationIDOf(payload []byte) string {
	var env cmdEnvelope
	_ = json.Unmarshal(payload, &env)
	return env.CorrelationID
}

// countIfUnroutable counts and logs a downward command that no enrolled child's
// mount covers. No downlink poll will ever select it, and since expiry is checked
// at the target, nothing else ever signals it.
//
// It never refuses: a target can be missing briefly during enrollment or a
// reparent. Commands for this node, for a machine enrolled here (deliverCommand
// counts those) and registries that cannot answer are excluded. The warning is
// logged per command on purpose, since the log is the only place the wrong address
// shows up.
func (e *Engine) countIfUnroutable(p uns.Parsed, topic string, payload []byte) {
	if p.NodeID == e.cfg.ULID {
		return
	}
	if target, ok := e.ids.Get(p.NodeID); ok && target.MayUseDoor(uns.DoorMQTT) {
		return // a machine on this node's own bus, not a routing question
	}
	router, ok := e.ids.(routableMounts)
	if !ok || router.RoutesUnder(p.Path) {
		return
	}
	e.metrics.CommandUnroutable()
	e.log.Warn("command addressed downward falls under no enrolled child's mount — "+
		"no downlink will hand it over, so it can never execute or ack",
		"topic", topic, "target", p.NodeID, "path", p.Path,
		"correlation_id", correlationIDOf(payload))
}
