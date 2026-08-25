// Command delivery tracking and redelivery (command-redelivery design §3).
//
// A command is the one class that is neither state a subscriber can re-read
// nor a sample it can afford to miss: it is delivered once, onto a bus, to a
// machine that may not be listening.
//
// Redelivery is the ONLY mechanism, since cmd/colca-machine connects with
// CleanSession=true. mochi used to cover the ordinary reconnect from its
// in-memory session, and that session is exactly what could not survive its
// own process — no storage hook is registered (mqttsrv.New), so a broker
// restart lost everything queued in it, and it never existed at all for a
// command issued before a machine's first connect. Rather than keep a
// volatile mechanism beside a durable one (two paths, different guarantees,
// the same command delivered twice), the machine asks for a clean session and
// every redelivery — ordinary reconnect included — now comes from the durable
// copy that was always there: the record on the commands stream.
//
// The rule in one line: a machine's delivery cursor advances only past
// commands that reached a live subscription OF THAT MACHINE, and everything
// above it is replayed when it next subscribes.
//
// Both halves of that sentence are load-bearing:
//
//   - "of that machine". HasSubscriberFor resolves subscriptions back to
//     identities. An identity-blind check would let an observer subscribed to
//     colca/v1/_CmdParam/# mark another machine's commands delivered while it is
//     offline, silently reopening the whole gap.
//   - "only past". Before a command is delivered live, the floor is checked
//     for anything owed BELOW it (owedBelow), and a command with an older one
//     still outstanding is held for the replay rather than published ahead of
//     it. A watermark cursor cannot express "A is owed but B was delivered",
//     so the two decisions — publish, and record it delivered — are one.
package engine

import (
	"encoding/json"
	"sync"

	"github.com/alpamayo-solutions/colca/internal/store"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// replayBatch is how many stream records one read pass examines, and also how
// often the cursor is committed. Commands are low-volume and TTL-bounded, so
// this bounds a single Pebble scan rather than throughput — and it bounds how
// much a crash mid-replay can re-run, since everything published in a batch is
// committed before the next batch begins.
const replayBatch = 256

// replayCap bounds how many records one subscribe may publish, so a machine
// returning to a node holding an improbable backlog cannot occupy its
// OnSubscribed callback indefinitely. The remainder is NOT silently deferred:
// hitting the cap is logged, and because the cursor stops where the publishing
// stopped, the very next command addressed to this machine finds the floor
// still standing below the remainder and leaves it there for the next
// subscribe.
const replayCap = 4096

// deliverCommand publishes a command onto the local bus and, when it is
// addressed to a machine enrolled at THIS node, records what became of it. It
// is called from persistTSAttributed in place of the plain delivery every
// other class gets, because for that one case "publish it" and "record that it
// was delivered" are a single decision — see the gating note further down.
//
// It answers one question: did this publish reach a live subscription of the
// machine it is addressed to? The answer goes to the two places that need it —
// the metric, and the machine's delivery cursor, which is the floor
// ReplayOwedCommands works up from.
//
// Read the question literally, not as "the bytes arrived": e.hasSubscriber
// (mqttsrv.HasSubscriberFor) answers subscription existence, not byte-level
// delivery confirmation — see its doc comment for exactly what it can and
// cannot see (a per-client write mochi attempts and fails is invisible to it,
// swallowed inside mochi at Debug). The undelivered counter can therefore
// undercount real delivery failures; it cannot overcount them.
//
// Three things deliberately do NOT trip the counter, each guarded below:
//
//   - No broker, or the subscriber lookup was never wired (e.hasSubscriber
//     == nil): nothing can be claimed either way, so nothing is counted.
//     This also covers every existing unit test that builds an *Engine
//     without a broker — they see zero calls to this metric, not false
//     positives.
//   - A command this node has no MQTT door for. Level 4 of a _Cmd* topic is
//     the TARGET identity, and it is the one part of a command topic that is
//     never rewritten on the way down (only the path is: the mount prefix is
//     stripped hop by hop), so the same lookup asks the right question at
//     every node the command passes through: "is the target something I can
//     hand to over my own bus?". Three answers say no, and all three are
//     healthy. (a) The target is not in this node's registry at all: the
//     command is transiting toward a descendant, and it is persisted and
//     mirrored at EVERY ancestor on the way (IngestDownlinkAttributed →
//     persistTSAttributed), where zero subscribers is simply what a relay
//     looks like — the target is reached over the replication door, not this
//     bus. (b) The target is a kind=node child: same thing at the last hop, a
//     child node is fed over replication (9443) and never subscribes here.
//     (c) The target is this node itself: _CmdConfigure, _CmdEdit and
//     _CmdAdmin execute in-process via maybeExec, which runs right after this
//     same persist call returns, and no machine is meant to listen on its own
//     node's command topic. That case needs no branch of its own — a node
//     holds no kind=machine entry for itself, so it falls out of the same
//     question. A kind=local service is a no too, and for a reason worth
//     writing down since it DOES share this bus: a local service is found by
//     the name it presents and never learns the ULID the node mints for it
//     (uns.LocalCursorPrefix), so nothing can address a command to one at
//     level 4. MayUseDoor(DoorMQTT) — "is this a machine" — is therefore the
//     whole question, and adding DoorLocal beside it would be a branch no
//     traffic can reach.
//   - An already-expired command: arriving expired is not a delivery
//     failure, it is an issuer that waited too long before the command was
//     even persisted. uns.CommandStillLive is the one place this "is it
//     still live" question is answered — the same predicate move-drain
//     completion (repl/drain.go) and uns.OwedCommand use, so they can never
//     disagree about the same expires_at field.
//
// The floor moves for a delivered command (there is nothing left to redeliver)
// and for an expired one (it can never be owed again, and leaving it would
// make every future replay rescan it forever). It stays put only when a live
// command reached nobody — which is exactly the record ReplayOwedCommands must
// find.
//
// It moves by COMPARE-AND-SWAP from this record's own offset, never by a
// forward ack, and that distinction is a dropped command rather than a style
// choice. CursorAck means "everything below is done", which this caller has no
// standing to claim: it knows the fate of ONE record. If the floor still stood
// below this record, an ack forward would leapfrog whatever is owed in between
// and those commands would never be replayed. Monotonicity is no protection,
// because the harmful move is forwards.
//
// Publishing is gated on the SAME condition, and that is what makes the two
// decisions one. A watermark cursor can express "everything below is handled";
// it cannot express "A is still owed but B was delivered". So when the floor
// stands below this record, this record is not published live at all — the
// replay will deliver it in stream order, after the older records it is queued
// behind. Publishing it anyway would both jump the queue and hand the machine
// a second copy when the replay later reached it, and a command run twice is a
// physical-world action, not a duplicate log line.
//
// Held under the target's replay lock for the whole decision, so a replay
// running concurrently cannot publish the same record from the other side.
func (e *Engine) deliverCommand(class uns.Class, p uns.Parsed, topic string, payload []byte, offset uint64) {
	target, ok := e.ids.Get(p.NodeID)
	if !ok || !target.MayUseDoor(uns.DoorMQTT) || e.hasSubscriber == nil {
		// Not addressed to a machine this node's own bus can reach: a command
		// relaying toward a descendant, one addressed to a child node or to
		// this node itself, or a node with no subscriber lookup wired. None of
		// them has a delivery floor, so none of them is gated by one — they go
		// to the bus exactly as before.
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
		// A plain forward ack is correct HERE and only here: owedBelow has
		// just established that nothing between the floor and this record is
		// owed, which is exactly the claim CursorAck makes. It is also what
		// lets the floor cross the records in between — acks, and commands
		// addressed to other machines — instead of stalling one offset behind
		// the first of them forever.
		e.store.CursorAck(cursor, stream, offset+1)
		return
	}
	e.metrics.CommandUndelivered()
	e.log.Warn("command published to the local bus had no live subscription — "+
		"holding it for replay on the target's next subscribe",
		"topic", topic, "correlation_id", correlationIDOf(payload))
}

// owedBelow reports whether any command still owed to ulid sits strictly below
// offset. It is the question the live-delivery gate actually needs, and it is
// NOT "is the floor exactly at this record".
//
// The floor advances only over records concerning this machine, while the
// commands stream also carries acks and other machines' commands. So a floor
// far below head is the ordinary, healthy state — a machine that was handed a
// command at offset 10 has floor 11 while the next command addressed to it
// lands at offset 30. Comparing floor to offset would call that "something is
// owed" and hold every command forever, delivering nothing.
//
// The scan is bounded by construction: the caller's own record is live, owed to
// this machine, and at `offset`, so the first match is at `offset` at the very
// latest. The distance is the number of stream records since this machine's
// last command, which on a commands stream is small.
//
// A read error answers "yes, hold". Publishing anyway would risk handing the
// machine a second copy when the replay later reaches the same record, and a
// command run twice is a physical-world action; a held command is only
// delayed, and the log line says so.
func (e *Engine) owedBelow(stream string, floor, offset uint64, ulid string) bool {
	if floor >= offset {
		return false
	}
	now := e.AuthoritativeNow().UnixMilli()
	recs, _, err := e.store.ReadRecords(stream, floor, 1, func(r store.StoredRecord) bool {
		return uns.OwedCommand(r.Topic, r.Payload, ulid, now)
	})
	if err != nil {
		e.log.Warn("command held: could not read the commands stream to check for older owed commands",
			"ulid", ulid, "floor", floor, "offset", offset, "err", err)
		return true
	}
	return len(recs) > 0 && recs[0].Offset < offset
}

// ReplayOwedCommands republishes onto the local bus every command addressed to
// entry that its delivery cursor still stands before, and returns how many it
// published. It is called from the MQTT door when that identity subscribes.
//
// Publishing to the bus, rather than writing to the one subscribing client, is
// deliberate — and it is the only option that does not reimplement mochi. A
// per-client write would have to rebuild packet-id allocation, the inflight
// map, send quotas and QoS downgrade from outside mochi's package, against
// unexported state; publishing goes through the same path every other local
// delivery uses and inherits all of it. The visible consequence is that a
// third party subscribed to this command's topic — an observer holding
// read:# — sees the replayed copy too. That is what a bus is: a replay is a
// publish, and every matching subscriber sees a publish. It is not a
// widening of who may see WHAT, which is the property that actually matters
// and which uns.OwedCommand pins: nothing is ever selected for a subscriber's
// own sake except the commands addressed to it.
//
// Trigger and selection are deliberately separate questions. Any subscribe by
// a machine runs this; what it publishes is decided per record by whether THAT
// MACHINE has a live subscription matching the record's topic right now. That
// is what makes the concurrent subscribes in cmd/colca-machine (command filter
// and time-sync beacon, issued back to back) safe in either order: a replay
// attempt that arrives before the command subscription is registered finds no
// subscriber of its own, publishes nothing, leaves the cursor where it was,
// and the subscribe that registers the command filter runs it again.
//
// Serialized per identity. Two replays for one machine may genuinely overlap —
// mochi processes one connection's packets serially, but a session TAKEOVER
// does not wait for the displaced connection's OnSubscribed to return
// (inheritClientSession → DisconnectClient), and two clients presenting the
// same certificate under different client IDs coexist without takeover at all
// (OnConnectAuthenticate pins the username to the ULID, never the client ID).
// Both replays would read the same cursor and publish the same records, and a
// command executed twice is a physical-world action, not a duplicate log line.
// Neither the per-record subscriber check nor the cursor's compare-and-swap
// dedupes a publish — only the lock does.
//
// The cursor is committed once per batch rather than once at the end, which
// bounds what a crash mid-replay can re-run to one batch instead of the whole
// cap.
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
			return uns.OwedCommand(r.Topic, r.Payload, entry.ULID, now)
		})
		if err != nil {
			e.log.Warn("command replay stopped: stream read failed",
				"ulid", entry.ULID, "from", from, "err", err)
			break
		}
		stalled := false
		for _, r := range recs {
			// Re-asked per record rather than once per pass: the machine's
			// subscription is registered by its own goroutine, and a record
			// whose topic it is not listening on must stay owed rather than
			// be published into the void and marked delivered.
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
		// next counts records the filter rejected too, so a cursor crossing
		// other machines' commands and expired ones advances over them
		// instead of rescanning them on every future subscribe. On a stall it
		// was reset above to the offset of the record that blocked, so that
		// record stays owed.
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
// only ever grows by one entry per machine ever seen at this node, which is
// bounded by the registry — small, and not worth reference-counting to reclaim.
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

// correlationIDOf extracts correlation_id for the log lines above,
// best-effort: uns.Validate already requires every _Cmd* payload to carry one,
// so a decode failure here cannot happen for real data. An empty string is
// still a useful log line — it says "look at the topic" rather than nothing at
// all.
func correlationIDOf(payload []byte) string {
	var env cmdEnvelope
	_ = json.Unmarshal(payload, &env)
	return env.CorrelationID
}

// countIfUnroutable observes, at admission, a command addressed downward that
// no enrolled child's mount covers — the one command outcome that produces no
// signal of any kind on its own.
//
// A command for a descendant is persisted here and handed to a child by its
// downlink poll, which selects by mount prefix (repl.Server.handleDownlink).
// When no child's mount covers the path, no poll ever selects it: it is never
// delivered, never executed, never acked. It does not even expire visibly,
// because expiry is evaluated at the TARGET inside maybeExec — so from the
// issuer's side "the route was wrong" and "the target is slow" look identical,
// forever.
//
// It counts and warns; it never refuses. Refusing at admission would reject a
// legitimate command issued during the window in which its target is briefly
// absent from the registry — publish-before-enroll, re-enrollment, reparent —
// and whether that window should be closed is a design decision, not something a
// counter decides.
//
// Three cases are excluded, each because a different mechanism owns it:
//
//   - addressed to THIS node: maybeExec runs it in-process; nothing routes.
//   - addressed to a machine enrolled HERE: the local bus delivers it, and
//     deliverCommand's colca_command_undelivered_total owns that outcome.
//   - a registry that cannot answer the routing question (routableMounts not
//     implemented — every unit fake in this package): no claim is made either
//     way, exactly as deliverCommand makes none without a subscriber lookup.
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
