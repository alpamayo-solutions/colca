// Package uns is where ALL Colca domain knowledge lives: topic grammar,
// contract classes, mount insert/strip, payload validation, the identity and
// grant model, and the element namespace for the `colca/#` namespace.
//
// It depends on the Go standard library only, so it can never reach back into
// colca's infrastructure — that ceiling is what keeps domain knowledge from
// scattering. The core imports this package (engine, httpapi, repl, retention,
// registry …) and that direction is by design; the reverse is forbidden and
// enforced by TestPluginDependsOnStdlibOnly. Where the domain needs something
// from infrastructure, it declares the port here and the core implements it
// (EntityStore, Bindings, Placements, Namespace, Scope).
//
// The core asks this package questions; it never switches on its vocabulary.
// A decision spelled `class == uns.ClassCmd` inside internal/ is a domain rule
// living in two packages at once, so decisions go through predicates —
// IsState, IsCommand, IsOwnedState, Entry.IsDraining, Entry.MayUseDoor — and
// TestCoreAsksQuestionsRatherThanSwitchingOnVocabulary keeps it that way.
package uns

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Class is the routing class of a contract; it decides the stream a record
// lands in and the direction it flows between nodes.
type Class int

const (
	ClassNone       Class = iota
	ClassData             // _Metric …    node-owned state, authorized by write scope
	ClassEntity           // _Node, _EnrolledIdentity, _SystemElement, _Signal, _Constant
	ClassDefinition       // _Group, _MetadataType …  write: any node, flows DOWN, applied as state
	ClassCmd              // _Cmd*        write: ancestors/admin, flows down
	ClassAck              // _Ack         write: owner, flows up
	ClassGap              // _StreamGap   write: pruner only. Event, no KV, not retained (design §6.4).
	ClassTimeSync         // _TimeSync    write: node-local-publish-only. Ephemeral: no stream, never persisted, never retained (time-sync design §2.2).
	ClassAudit            // _AuditEvent  append-only security event, local/internal write, flows up
	ClassAlarm            // _AlarmStateChange, _NotificationDispatched — append-only alarm event. Event: no KV, not retained. Its own stream so it never queues behind a metrics backlog.
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
	// An alarm is an event with a lifecycle; a metric is a sample. That is the
	// whole difference, and it is why these two ride their own stream rather
	// than a place in the sample lane's queue.
	case contract == "_AlarmStateChange" || contract == "_NotificationDispatched":
		return ClassAlarm
	case contract == "_EnrolledIdentity" || contract == "_Node" ||
		contract == "_ServiceDetails" || contract == "_SystemElement" ||
		contract == "_Signal" || contract == "_Constant" || contract == "_ExternalReference" ||
		contract == "_EditOperation" || contract == "_AlarmNotificationConfig" ||
		contract == "_NotificationConfigStatus":
		return ClassEntity
	case contract == "_Group" || contract == "_MetadataType" ||
		contract == "_AnnotationType" || contract == "_Interface" ||
		contract == "_ExternalSystem" || contract == "_SemanticTag":
		return ClassDefinition
	case contract == "_Ack":
		return ClassAck
	case contract == "_StreamGap":
		return ClassGap
	case contract == "_TimeSync":
		return ClassTimeSync
	case contract == "_AuditEvent":
		return ClassAudit
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

// The predicates below exist so the core can ask what a class DOES without
// learning which class it is. Every one of them is a domain fact that used to
// be spelled out as a `class == Class…` comparison inside internal/ — i.e. a
// rule living in two packages at once. Adding a class here is the whole change;
// no door, no replicator and no pruner has to be edited to agree.

// IsKnown reports whether the contract resolved to a class this node handles at
// all. ClassNone is the "no such contract" answer from ClassOf and from the
// bundle authority alike: the validated namespace rejects it rather than
// storing something nothing can interpret.
func IsKnown(c Class) bool { return c != ClassNone }

// IsCommand reports whether a record belongs to the command flow: it targets an
// ABSOLUTE node-local path (no mount rewrite, no level-4 identity rule, because
// the author is not the target's owner), needs a covering cmd grant, is refused
// while its destination drains, and travels DOWN the tree.
func IsCommand(c Class) bool { return c == ClassCmd }

// CommandStillLive reports whether a ClassCmd record's expires_at has not yet
// passed authoritativeNowMS. Validate already guarantees every persisted
// _Cmd* payload carries a numeric expires_at (move-drain design §3.2:
// "Validate already requires a numeric expires_at on every _Cmd*, so the
// drain deadline is bounded"), so a decode failure here cannot happen for
// real data — treated as still-live defensively rather than silently
// completing a drain, or silently swallowing an undelivered-command signal,
// on malformed input.
//
// One definition of "still live" so the two places that ask it — move-drain
// completion and the undelivered-command observability signal — can never
// disagree about the same expires_at field.
func CommandStillLive(payload []byte, authoritativeNowMS int64) bool {
	var body struct {
		ExpiresAt float64 `json:"expires_at"`
	}
	if err := json.Unmarshal(payload, &body); err != nil {
		return true
	}
	return int64(body.ExpiresAt) >= authoritativeNowMS
}

// IsNodeLocal reports whether a class may only ever be produced by the node
// itself. No door accepts one from a client, a human or an admin — the beacon
// loop publishes it straight to the local bus, and it is ephemeral: no stream,
// never persisted, never retained (time-sync design §2.2).
func IsNodeLocal(c Class) bool { return c == ClassTimeSync }

// IsOwnedState reports whether a class is state that an identity authors under
// its OWN mount — the set that KV-projects at mount-rewritten coordinates and
// replicates UP the tree. Definitions are state too (IsState covers them), but
// they descend instead: their path is their own identity and no hop rewrites
// them, so they are deliberately not in this set.
func IsOwnedState(c Class) bool { return c == ClassData || c == ClassEntity }

// IsCommandAuthoredState reports whether a class is state a command executor
// may author. Every domain command commits its complete result as one atomic
// batch, and this is the admission rule for what may sit in one: the entity
// graph an Edit intent or a `_CmdConfigure` verb edits, and the definitions
// that same door files under their own ids.
//
// Metrics are state too and are deliberately excluded: a sample is a machine's
// to publish at its own door, and letting one ride an entity mutation would put
// the metric lane behind a command's commit. Commands, acks, gap markers, audit
// events and the ephemeral beacon are not state at all.
//
// A batch still has to land on ONE stream — entities and definitions have their
// own — so the door that admits records also refuses a batch that mixes them.
func IsCommandAuthoredState(c Class) bool { return c == ClassEntity || c == ClassDefinition }

// IsDefinition reports whether a class travels DOWN the tree and is applied
// unconditionally as state wherever it lands. A definition's path is its own
// identity, so no hop rewrites it — which is what lets the same definition mean
// the same thing at every node (definition-stream design §2).
func IsDefinition(c Class) bool { return c == ClassDefinition }

// IsAudit reports whether a record is a security event. Audit events are
// append-only, never KV-projected, and rise without being filtered.
func IsAudit(c Class) bool { return c == ClassAudit }

// ValidActorKind is the stable attribution vocabulary shared by every record
// envelope and `_AuditEvent` payload.
func ValidActorKind(kind string) bool {
	switch kind {
	case "human", "service", "node", "system", "anonymous":
		return true
	default:
		return false
	}
}

// FlowsUp reports whether a child may offer the class to its parent. Commands
// and definitions travel down; time sync never leaves the local bus.
func FlowsUp(c Class) bool {
	return c == ClassData || c == ClassEntity || c == ClassAck || c == ClassGap ||
		c == ClassAudit || c == ClassAlarm
}

// MatchesUplinkStream binds an upward record's domain class to the physical
// stream named by the replication request. Gap markers live in the stream
// named by their topic path; every other upward class has one fixed stream.
func MatchesUplinkStream(c Class, p Parsed, stream string) bool {
	if !FlowsUp(c) {
		return false
	}
	if c == ClassGap {
		return p.Path == stream
	}
	return StreamFor(c) == stream
}

// ValidateAuditTopic pins the append-only event identity to the canonical
// `_colca/audit/{event-id}` path at its authoring node.
func ValidateAuditTopic(p Parsed, payload []byte) error {
	if p.Contract != "_AuditEvent" {
		return fmt.Errorf("audit topic validator received %s", p.Contract)
	}
	var value struct {
		EventID string `json:"event_id"`
	}
	if err := json.Unmarshal(payload, &value); err != nil {
		return fmt.Errorf("_AuditEvent: payload is not valid JSON: %w", err)
	}
	if value.EventID == "" {
		return fmt.Errorf("_AuditEvent: field %q must be a non-empty string", "event_id")
	}
	want := "_colca/audit/" + value.EventID
	if p.Path != want {
		return fmt.Errorf("_AuditEvent path %q does not name event_id %q", p.Path, value.EventID)
	}
	return nil
}

// NeedsStateRefresh reports whether the pruner must re-append a class's KV
// entries to keep them alive across a retention boundary (retention §6.5).
//
// It is exactly the state nothing else re-supplies. Entities are authored here
// and would simply be lost when their original records age out. Definitions
// arrive on the downlink and the parent re-sends them, so refreshing locally
// would duplicate work. Data are samples — ageing out is the point.
func NeedsStateRefresh(c Class) bool { return c == ClassEntity }

// ClassFromManifest maps a bundle manifest's class name to its Class. The
// manifest's vocabulary is domain vocabulary, so it is spelled here and not in
// the loader — the loader's job is to reject what it cannot map, not to know
// what the names mean.
//
// ClassGap and ClassTimeSync have no manifest name on purpose: both are
// node-authored and builtin, so a bundle can never declare one (schema-bundle
// design §10.2).
func ClassFromManifest(name string) (Class, bool) {
	switch name {
	case "data":
		return ClassData, true
	case "entity":
		return ClassEntity, true
	case "definition":
		return ClassDefinition, true
	case "cmd":
		return ClassCmd, true
	case "ack":
		return ClassAck, true
	case "audit":
		return ClassAudit, true
	case "alarm":
		return ClassAlarm, true
	}
	return ClassNone, false
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
	case ClassAudit:
		return "audit"
	case ClassAlarm:
		return "alarms"
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

// The CHILD-side replication cursors. Each names a position in one specific
// parent's stream, so the parent's pinned pubkey is part of the name: a node
// that changes parents must not resume against the new one at the old one's
// offsets (parent-scoped-cursors design §3.1). Scoping by pubkey rather than
// by the parent's ULID is deliberate — the pubkey is the config pin, known
// before first contact and verified on every connection, while the ULID is
// only learned after connecting.
//
// These use their own "up:"/"down:"/"down-def:" prefixes, deliberately
// distinct from DownlinkCursorPrefix/DownlinkDefCursorPrefix above, which are
// the PARENT-side cursors keyed by CHILD ulid. A child pubkey and a parent
// ulid are different-length strings today, but the name must not depend on
// that arithmetic to stay collision-free — different node, different fact,
// so the prefix itself carries the distinction.
func UplinkCursor(parentPubkey string) string { return "up:" + parentPubkey }

func DownlinkCursor(parentPubkey string) string { return "down:" + parentPubkey }

func DownlinkDefCursor(parentPubkey string) string { return "down-def:" + parentPubkey }

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
	reqOneOf := func(k string, allowed ...string) error {
		v, ok := m[k].(string)
		if !ok || v == "" {
			return fmt.Errorf("%s: field %q must be a non-empty string", contract, k)
		}
		for _, candidate := range allowed {
			if v == candidate {
				return nil
			}
		}
		return fmt.Errorf("%s: field %q has unsupported value %q", contract, k, v)
	}
	switch {
	case contract == "_Metric":
		return reqNum("v")
	case contract == "_Ack":
		if err := reqStr("correlation_id"); err != nil {
			return err
		}
		return reqNum("result_code")
	case contract == "_AuditEvent":
		if err := reqStr("event_id"); err != nil {
			return err
		}
		if err := reqOneOf("source", "colca", "api", "keycloak", "projector", "node_manager"); err != nil {
			return err
		}
		if err := reqOneOf("action", "sign_in", "sign_out", "authorize", "identity_admin", "credential_admin", "execute", "rebuild", "restore", "security_config"); err != nil {
			return err
		}
		if err := reqOneOf("outcome", "success", "failure", "denied"); err != nil {
			return err
		}
		if err := reqOneOf("actor_kind", "human", "service", "node", "system", "anonymous"); err != nil {
			return err
		}
		if err := reqNum("occurred_at"); err != nil {
			return err
		}
		if raw, exists := m["metadata"]; exists {
			metadata, ok := raw.(map[string]any)
			if !ok {
				return fmt.Errorf("%s: field %q must be an object", contract, "metadata")
			}
			for key, value := range metadata {
				switch value.(type) {
				case nil, string, float64, bool:
					// Safe scalar; source-specific allow-lists are producer-side.
				case []any:
					for _, item := range value.([]any) {
						switch item.(type) {
						case nil, string, float64, bool:
						default:
							return fmt.Errorf("%s: metadata %q contains a nested value", contract, key)
						}
					}
				default:
					return fmt.Errorf("%s: metadata %q must be a scalar or scalar array", contract, key)
				}
			}
		}
		return nil
	case contract == "_EnrolledIdentity":
		// A registry entry names itself by the enrolled identity.
		return reqStr("ulid")
	case contract == "_Constant":
		_, err := validateConstantPayload(payload)
		return err
	case contract == "_EditOperation":
		if err := reqStr("id"); err != nil {
			return err
		}
		if err := reqStr("digest"); err != nil {
			return err
		}
		if err := reqStr("message"); err != nil {
			return err
		}
		if err := reqStr("result"); err != nil {
			return err
		}
		topics, ok := m["topics"].([]any)
		if !ok || len(topics) == 0 {
			return fmt.Errorf("%s: field %q must be a non-empty array", contract, "topics")
		}
		for _, topic := range topics {
			if value, ok := topic.(string); !ok || value == "" {
				return fmt.Errorf("%s: field %q must contain only non-empty strings", contract, "topics")
			}
		}
		return nil
	case contract == "_Node" || contract == "_ServiceDetails" ||
		contract == "_SystemElement" || contract == "_Signal" ||
		contract == "_ExternalReference" || contract == "_Group" ||
		contract == "_MetadataType" || contract == "_AnnotationType" ||
		contract == "_Interface" || contract == "_ExternalSystem" ||
		contract == "_SemanticTag":
		// Data-model records name themselves by "id" — the field grants and
		// bindings reference them through. They shared the registry's "ulid" rule
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

// placedConstant is the authoritative authored value stored at one namespace
// path. Value stays raw so int64 validation never passes through float64 and
// silently loses precision before the record reaches storage.
type placedConstant struct {
	ID       string          `json:"id"`
	Name     string          `json:"name"`
	DataType string          `json:"data_type"`
	Value    json.RawMessage `json:"value"`
}

func validateConstantPayload(payload []byte) (placedConstant, error) {
	var constant placedConstant
	if err := json.Unmarshal(payload, &constant); err != nil {
		return placedConstant{}, fmt.Errorf("_Constant: payload is not valid JSON: %w", err)
	}
	if constant.ID == "" {
		return placedConstant{}, fmt.Errorf("_Constant: field %q must be a non-empty string", "id")
	}
	if constant.Name == "" {
		return placedConstant{}, fmt.Errorf("_Constant: field %q must be a non-empty string", "name")
	}
	if len(constant.Value) == 0 {
		return placedConstant{}, fmt.Errorf("_Constant: field %q is required", "value")
	}

	decoder := json.NewDecoder(bytes.NewReader(constant.Value))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return placedConstant{}, fmt.Errorf("_Constant: field %q is not valid JSON", "value")
	}
	wrongType := func() (placedConstant, error) {
		return placedConstant{}, fmt.Errorf("_Constant: value does not match data_type %q", constant.DataType)
	}

	switch constant.DataType {
	case "float64":
		number, ok := value.(json.Number)
		if !ok {
			return wrongType()
		}
		if _, err := strconv.ParseFloat(number.String(), 64); err != nil {
			return wrongType()
		}
	case "int64":
		number, ok := value.(json.Number)
		if !ok {
			return wrongType()
		}
		if _, err := strconv.ParseInt(number.String(), 10, 64); err != nil {
			return wrongType()
		}
	case "boolean":
		if _, ok := value.(bool); !ok {
			return wrongType()
		}
	case "string":
		if _, ok := value.(string); !ok {
			return wrongType()
		}
	case "datetime":
		text, ok := value.(string)
		if !ok {
			return wrongType()
		}
		if _, err := time.Parse(time.RFC3339, text); err != nil {
			return placedConstant{}, fmt.Errorf("_Constant: datetime value must use RFC 3339: %w", err)
		}
	case "json":
		// The outer unmarshal already proved that value is valid JSON. Unlike
		// the scalar types, JSON deliberately accepts objects, arrays and null.
	default:
		return placedConstant{}, fmt.Errorf("_Constant: unsupported data_type %q", constant.DataType)
	}
	return constant, nil
}
