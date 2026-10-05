// Command redelivery. mochi keeps an offline machine's QoS-1 messages in its
// session, but that session is lost when the broker restarts and never exists
// for a command issued before the machine's first connect. For those cases the
// durable copy on the commands stream is replayed when the machine subscribes.

package uns

import "strings"

// OwedCommand reports whether a stored commands-stream record must be
// (re)delivered to the identity ulid.
//
// It selects by target identity, not by what the subscriber may read: an
// observer with read:# passes the ACL on every command topic but is owed none.
// The identity is topic level 4, the one segment no hop rewrites, so the same
// record is selected correctly at the root and at the leaf. It refuses
// non-commands, an empty ulid, and commands past expires_at (CommandStillLive).
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

// CommandCursor names the cursor tracking this identity's command delivery on
// the commands stream. It lives inside the entry's cursor prefix, so no other
// identity can move it, and ProtectedCursors keeps retention from pruning an
// absent machine's commands. Not nil-safe, for the same reason as CursorPrefix.
func (e *Entry) CommandCursor() string { return e.CursorPrefix() + commandCursorSuffix }

const commandCursorSuffix = "cmd"

// OwnsCursor reports whether this identity may move cursor itself through
// /fetch and /ack. Its own prefix is required but not enough: the delivery
// floor (CommandCursor) is written only by the node, or a machine could ack it
// to head and silently drop its commands. A nil entry owns no cursor.
func (e *Entry) OwnsCursor(cursor string) bool {
	if e == nil || !strings.HasPrefix(cursor, e.CursorPrefix()) {
		return false
	}
	return cursor != e.CommandCursor()
}

// MayRetireCursor reports whether e may retire cursor although it is not its
// own: e holds the admin command class over the whole node (cmd:<node>/#:admin,
// or a zone covering this node), and the cursor sits in an identity's namespace
// (c/<name>/... or <ulid>/...). A command delivery floor (<prefix>cmd) is not
// retirable this way: it belongs to the node, which drops it with the identity.
// Node-owned cursors (downlink:, up:, down-def:) have no namespace and are not
// retirable either. A nil entry may retire nothing.
func MayRetireCursor(sc Scope, e *Entry, cursor string) bool {
	if e == nil || !identityCursor(cursor) {
		return false
	}
	for _, g := range e.Grants {
		pg, err := ParseGrant(g)
		if err != nil || pg.Verb != "cmd" {
			continue
		}
		if zone, ok := grantZone(sc, e, pg.Element); !ok || zone != "#" {
			continue
		}
		for _, c := range pg.Classes {
			if c == "admin" {
				return true
			}
		}
	}
	return false
}

func identityCursor(cursor string) bool {
	prefix, rest, ok := strings.Cut(strings.TrimPrefix(cursor, LocalCursorPrefix), "/")
	return ok && prefix != "" && rest != "" && rest != commandCursorSuffix && !strings.Contains(prefix, ":")
}
