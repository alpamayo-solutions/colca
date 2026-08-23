// Command redelivery selection (command-redelivery design §3).
//
// A command is the one class that is neither state a subscriber can re-read
// nor a sample it can afford to miss: it is delivered once, onto a bus, to a
// machine that may not be listening. mochi holds an offline machine's QoS-1
// messages in its session, so an ordinary reconnect needs nothing from this
// file — but that session is in-memory, so it dies with the broker process,
// and it never existed at all for a command issued before the machine's first
// connect. Those two cases are what this selection rule serves: the durable
// copy on the commands stream is replayed to the machine when it subscribes.
package uns

import "strings"

// OwedCommand reports whether one stored commands-stream record must be
// (re)delivered to the identity ulid.
//
// Selection is by TARGET IDENTITY, deliberately, and not by what the
// subscriber is allowed to read. The two come apart for exactly the identity
// that matters: an observer holding read:# passes the ACL check on every
// machine's command topic, so a read-zone-shaped rule would hand it every
// machine's commands the moment it subscribed. What a subscriber may READ and
// what it is OWED are different questions, and only the second one belongs
// here.
//
// The identity is topic level 4 (segment index 3) — the one segment no hop
// rewrites on the way down, while the route after it is re-prefixed at every
// hop. Reading the identity there means the same record is selected correctly
// whether it is being replayed at the root or at the leaf.
//
// Three refusals, all fail-closed:
//
//   - A record that is not a command at all. The commands stream holds only
//     commands, so this can only fire if a caller aims the predicate at
//     another stream — it must not trust that aim.
//   - An empty ulid. The same rule Ancestry.Covers applies to an absent
//     element id: "no identity" must never read as "matches everything". A
//     caller that lost its identity would otherwise drain another machine's
//     commands onto the bus.
//   - An expired command. Arriving after expires_at is not a delivery, and
//     CommandStillLive is the single place that question is answered — the
//     same predicate move-drain completion and the undelivered counter use,
//     so the three can never disagree about one expires_at field.
func OwedCommand(topic string, payload []byte, ulid string, authoritativeNowMS int64) bool {
	if ulid == "" {
		return false
	}
	p, err := Parse(topic)
	if err != nil || !IsCommand(ClassOf(p.Contract)) {
		return false
	}
	if p.NodeID != ulid {
		return false
	}
	return CommandStillLive(payload, authoritativeNowMS)
}

// CommandCursor names the cursor recording how far this identity's command
// delivery has got on the commands stream.
//
// It sits inside the entry's own cursor prefix, which is the boundary /fetch
// and /ack already use to refuse one identity moving another's cursor: putting
// the delivery floor anywhere else would let a machine ack away another's.
// Retention protection then comes for free — ProtectedCursors already keeps a
// stream from pruning underneath any cursor, so an absent machine's owed
// commands are not swept away while it is gone.
//
// Deliberately NOT nil-safe, for the reason CursorPrefix documents at length:
// the only string a nil entry could return is "", and "" is a prefix of every
// cursor in the store. Callers resolve the entry from the registry first.
func (e *Entry) CommandCursor() string { return e.CursorPrefix() + commandCursorSuffix }

const commandCursorSuffix = "cmd"

// OwnsCursor reports whether this identity may move `cursor` itself — the
// question /fetch and /ack ask before letting a caller advance one.
//
// Carrying the identity's own prefix is necessary but NOT sufficient, and the
// exception is this design's own cursor. The delivery floor lives inside the
// identity's namespace so that nobody ELSE can move it, but the node is its
// only legitimate writer: a machine that acked its own floor to head would
// silently discard every command it is owed, and would do it with no error and
// no metric. Reserving the name also stops a plain collision — a machine using
// /fetch and /ack with a cursor it happens to call "cmd" would otherwise share
// the broker's floor, which is two writers for one fact.
//
// Nil-safe, unlike CursorPrefix: "no identity owns no cursor" is a truthful
// answer, and it is the safe one.
func (e *Entry) OwnsCursor(cursor string) bool {
	if e == nil || !strings.HasPrefix(cursor, e.CursorPrefix()) {
		return false
	}
	return cursor != e.CommandCursor()
}
