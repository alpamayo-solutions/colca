// Package uns is the Colca-specific plugin layer: topic grammar, contract
// classes, mount insert/strip and payload validation for the `colca/#` namespace.
//
// It has no dependencies beyond the Go standard library and must never import
// the core — the core stays generic, the domain knowledge lives here. The core
// calls into this package (engine, httpapi, repl); never the other way around.
// Enforced by arch_test.go.
package uns

import (
	"encoding/json"
	"fmt"
	"strings"
)

// Class is the routing class of a contract; it decides the stream a record
// lands in and the direction it flows between nodes.
type Class int

const (
	ClassNone     Class = iota
	ClassData           // _Metric …    write: owner (level4 == identity)
	ClassEntity         // _EdgeNode, _SystemElement, _Signal
	ClassDefinition     // _Group, _MetadataType …  write: any node, flows DOWN, applied as state
	ClassCmd            // _Cmd*        write: ancestors/admin, flows down
	ClassAck            // _Ack         write: owner, flows up
	ClassGap            // _StreamGap   write: pruner only. Event, no KV, not retained (design §6.4).
	ClassTimeSync       // _TimeSync    write: node-local-publish-only. Ephemeral: no stream, never persisted, never retained (time-sync design §2.2).
)

// Parsed is a decomposed UNS topic: colca/v1/_Contract/{node-id}/{path…}
type Parsed struct {
	Prefix, Version, Contract, NodeID, Path string
}

// IsUns reports whether the topic belongs to the uns namespace.
func IsUns(topic string) bool { return strings.HasPrefix(topic, "colca/") }

// Parse decomposes an UNS topic. It requires at least 5 segments (so there is
// always a non-empty hierarchy path) and a _Contract at segment index 2 — with
// one exception: _TimeSync (time-sync design §2.2) is the only contract whose
// wire topic has no hierarchy path at all (colca/v1/_TimeSync/{node-ulid},
// exactly 4 segments). That shape is accepted here with an empty Path so the
// engine's reject-path can classify and count a client's attempted _TimeSync
// publish with its own reject reason instead of falling through to the
// generic "grammar" rejection. No other contract gets this relaxation: doing
// it length-only (instead of contract-gated) would let MountInsert/MountStrip
// silently no-op on a 4-segment topic for contracts whose mount rewrite is
// load-bearing (data/entity ownership).
func Parse(topic string) (Parsed, error) {
	seg := strings.Split(topic, "/")
	if len(seg) == 4 && seg[2] == "_TimeSync" {
		return Parsed{Prefix: seg[0], Version: seg[1], Contract: seg[2], NodeID: seg[3]}, nil
	}
	if len(seg) < 5 {
		return Parsed{}, fmt.Errorf("uns grammar: need >=5 segments, got %d in %q", len(seg), topic)
	}
	if !strings.HasPrefix(seg[2], "_") {
		return Parsed{}, fmt.Errorf("uns grammar: level 3 must be _Contract, got %q", seg[2])
	}
	return Parsed{
		Prefix:   seg[0],
		Version:  seg[1],
		Contract: seg[2],
		NodeID:   seg[3],
		Path:     strings.Join(seg[4:], "/"),
	}, nil
}

// ClassOf maps a contract name to its routing class. Concrete names are matched
// before the _Cmd prefix rule, so an exact contract can never be swallowed by
// the prefix; every _Cmd* contract is a command and never falls through to
// ClassNone.
func ClassOf(contract string) Class {
	switch {
	case contract == "_Metric":
		return ClassData
	case contract == "_EdgeNode" || contract == "_SystemElement" || contract == "_Signal":
		return ClassEntity
	case contract == "_Group":
		return ClassDefinition
	case contract == "_Ack":
		return ClassAck
	case contract == "_StreamGap":
		return ClassGap
	case contract == "_TimeSync":
		return ClassTimeSync
	case strings.HasPrefix(contract, "_Cmd"):
		return ClassCmd
	}
	return ClassNone
}

// IsState reports whether a class is STATE rather than an event: latest value
// per path, KV-projected, retained on the bus, and retractable by an empty
// payload (the tombstone). Data, entities and definitions are state; commands,
// acks and gap markers are events, which is why retaining them would re-deliver
// stale instructions to every new subscriber.
//
// One definition of "state" so the three places that care — the KV projection,
// the retained flag and the tombstone rule — can never drift apart.
func IsState(c Class) bool {
	return c == ClassData || c == ClassEntity || c == ClassDefinition
}

// StreamFor maps a class to the persistent stream that stores it.
//
// ClassGap is deliberately NOT mapped to a fixed stream here: a _StreamGap
// marker is appended into whichever stream it describes (design §6.4), which
// varies per record and is carried in the topic itself — Parsed.Path is the
// stream name for a _StreamGap topic (colca/v1/_StreamGap/{node-ulid}/{stream},
// ordinary uns grammar, so Parse needs no special case). Callers writing or
// routing a _StreamGap record must use Parsed.Path, not StreamFor.
func StreamFor(c Class) string {
	switch c {
	case ClassData:
		return "metrics"
	case ClassEntity:
		return "entities"
	case ClassDefinition:
		return "definitions"
	case ClassCmd, ClassAck:
		return "commands"
	case ClassGap:
		return ""
	case ClassTimeSync:
		return "" // ephemeral: no stream, never persisted (time-sync design §2.2)
	}
	return ""
}

// TimeSyncTopic builds the wire topic for the periodic time beacon (time-sync
// design §2.2): colca/v1/_TimeSync/{node-ulid} — the only UNS topic with no
// hierarchy path at all (Parse's 4-segment exception below mirrors this
// shape). nodeULID is the publishing node's own identity, never a machine's.
func TimeSyncTopic(nodeULID string) string {
	return "colca/v1/_TimeSync/" + nodeULID
}

// DownlinkCursorPrefix names the PARENT-side cursor a repl server persists
// per child on its own commands stream (move-drain design §3.2/§3.4,
// carried over from spec §5.1 [delta]): DownlinkCursorPrefix+{child-ulid} on
// stream "commands" is the delivery floor — the next offset that child has
// not yet fetched via GET /downlink. Exported here (rather than living only
// in internal/repl) so the move-drain completion predicate, which reads it
// from internal/repl but is conceptually about registry lifecycle, and any
// future reader agree on one name instead of two hand-kept copies.
const DownlinkCursorPrefix = "downlink:"

// DownlinkDefCursorPrefix is the same idea for the definitions stream
// (definition-stream design §5): DownlinkDefCursorPrefix+{child-ulid} on stream
// "definitions" is how far that child has read. It is separate from the command
// cursor because the two streams advance independently — and because
// compaction's floor is this cursor, so a definition may only be superseded
// once every child has read past it.
const DownlinkDefCursorPrefix = "downlink-def:"

// MountInsert inserts the mount name directly after segment 4 (node-id), i.e.
// at the head of the hierarchy path — the uplink rewrite done on every hop.
func MountInsert(topic, mount string) string {
	seg := strings.SplitN(topic, "/", 5)
	if len(seg) < 5 {
		return topic
	}
	return strings.Join([]string{seg[0], seg[1], seg[2], seg[3], mount + "/" + seg[4]}, "/")
}

// MountStrip removes the mount prefix from the hierarchy part — the exact
// inverse of MountInsert, done on every downlink hop. ok=false when the path
// does not start with the mount, in which case the record belongs to a foreign
// mount and must not be delivered.
func MountStrip(topic, mount string) (string, bool) {
	seg := strings.SplitN(topic, "/", 5)
	if len(seg) < 5 {
		return topic, false
	}
	rest, found := strings.CutPrefix(seg[4], mount+"/")
	if !found {
		return topic, false
	}
	return strings.Join([]string{seg[0], seg[1], seg[2], seg[3], rest}, "/"), true
}

// Validate applies minimal per-contract schema checks (hand-rolled stand-in for
// the generated schema bundle — same enforcement point, swappable later).
// Unknown contracts are rejected: that is the point of a validated namespace.
func Validate(contract string, payload []byte) error {
	// Empty payload is the tombstone (retention design §7.1): valid exactly for
	// the KV-projecting state classes (data/entity), where it retires the path —
	// KV key deleted, retained message cleared. For every other contract an
	// empty payload was never a valid value and deletion is not meaningful
	// (§7.3): commands/acks/gaps are events, there is nothing to retire.
	if len(payload) == 0 {
		if IsState(ClassOf(contract)) {
			return nil
		}
		return fmt.Errorf("%s: empty payload (tombstone) is only valid for KV-projecting state contracts", contract)
	}
	var m map[string]any
	if err := json.Unmarshal(payload, &m); err != nil {
		return fmt.Errorf("%s: payload is not valid JSON: %w", contract, err)
	}
	reqNum := func(k string) error {
		if _, ok := m[k].(float64); !ok {
			return fmt.Errorf("%s: field %q must be a number", contract, k)
		}
		return nil
	}
	reqStr := func(k string) error {
		if v, ok := m[k].(string); !ok || v == "" {
			return fmt.Errorf("%s: field %q must be a non-empty string", contract, k)
		}
		return nil
	}
	switch {
	case contract == "_Metric":
		return reqNum("v")
	case contract == "_Ack":
		if err := reqStr("correlation_id"); err != nil {
			return err
		}
		return reqNum("result_code")
	case contract == "_EdgeNode":
		// A registry entry names itself by the enrolled identity.
		return reqStr("ulid")
	case contract == "_SystemElement" || contract == "_Signal" || contract == "_Group":
		// Data-model records name themselves by "id" — the field grants and
		// bindings reference them through. They shared _EdgeNode's "ulid" rule
		// until the binding cutover renamed it; a floor that still asked for
		// "ulid" rejected every real element and signal.
		return reqStr("id")
	case contract == "_TimeSync":
		// Reachable only from direct Validate callers (tests, defense in
		// depth): the engine rejects _TimeSync by class before Validate is
		// ever called on a client/admin/replicated publish (time-sync design
		// §2.2/§4) — only the node's own beacon loop publishes this shape,
		// straight to the local bus, bypassing Validate entirely.
		return reqNum("now_ms")
	case contract == "_StreamGap":
		if err := reqStr("stream"); err != nil {
			return err
		}
		for _, k := range []string{"from_offset", "to_offset", "first_ts", "last_ts"} {
			if err := reqNum(k); err != nil {
				return err
			}
		}
		cursors, ok := m["overridden_cursors"].([]any)
		if !ok || len(cursors) == 0 {
			return fmt.Errorf("%s: field %q must be a non-empty array", contract, "overridden_cursors")
		}
		for _, oc := range cursors {
			if s, ok := oc.(string); !ok || s == "" {
				return fmt.Errorf("%s: field %q must contain only non-empty strings", contract, "overridden_cursors")
			}
		}
		return nil
	case strings.HasPrefix(contract, "_Cmd"):
		if err := reqStr("correlation_id"); err != nil {
			return err
		}
		return reqNum("expires_at")
	}
	return fmt.Errorf("unknown contract %q — validated namespace rejects unknown contracts", contract)
}
