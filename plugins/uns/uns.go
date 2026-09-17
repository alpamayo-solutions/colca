// Package uns holds Colca's domain knowledge: topic grammar, contract classes,
// mount rewriting, payload validation, the identity and grant model, and the
// element namespace under the topic root.
//
// It uses only the standard library, so it cannot reach into colca's
// infrastructure; TestPluginDependsOnStdlibOnly enforces that. Where the domain
// needs infrastructure it declares a port (EntityStore, Bindings, Placements,
// Namespace, Scope) and the core implements it.
//
// The core asks this package questions instead of switching on its vocabulary:
// decisions go through predicates such as IsState, IsCommand and
// Entry.MayUseDoor, and TestCoreAsksQuestionsRatherThanSwitchingOnVocabulary
// keeps it that way.
package uns

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Class is the routing class of a contract; it decides the stream a record
// lands in and the direction it flows between nodes.
type Class int

// Routing classes; ClassNone is the zero value.
const (
	ClassNone       Class = iota
	ClassData             // _Metric …    node-owned state, authorized by write scope
	ClassEntity           // _Node, _EnrolledIdentity, _SystemElement, _Signal, _Constant, _Resource
	ClassDefinition       // _Group, _MetadataType, ...: written by any node, flows down, applied as state
	ClassCmd              // _Cmd*        write: ancestors/admin, flows down
	ClassAck              // _Ack         write: owner, flows up
	ClassGap              // _StreamGap: written by the pruner only; event, no KV, not retained
	ClassTimeSync         // _TimeSync: published by the node itself; no stream, never persisted or retained
	ClassAudit            // _AuditEvent  append-only security event, local/internal write, flows up
	ClassAlarm            // _AlarmStateChange, _NotificationDispatched: events on their own stream, never behind metrics
	ClassLog              // _Log: events on their own stream; logs are the loudest class and would crowd out samples
	ClassAnnotation       // _Annotation: events on their own stream; too many per machine to keep as KV state
)

// Parsed is a decomposed UNS topic: colca/v1/_Contract/{node-id}/{path…}
type Parsed struct {
	Prefix, Version, Contract, NodeID, Path string
}

// IsUns reports whether the topic is under the topic root.
func IsUns(topic string) bool { return strings.HasPrefix(topic, Root()+"/") }

// Parse decomposes a UNS topic. It requires at least 5 segments and a _Contract
// at index 2. The one exception is _TimeSync, whose topic has no hierarchy path
// (4 segments); it parses with an empty Path so the engine can reject it with
// its own reason. The exception is tied to the contract, so MountInsert and
// MountStrip never silently skip a short topic of another contract.
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

// ClassOf maps a contract name to its class. Exact names are matched before the
// _Cmd prefix, and every _Cmd* contract is a command.
func ClassOf(contract string) Class {
	switch {
	case contract == "_Metric":
		return ClassData
	// Alarms are events with a lifecycle, not samples, so they get their own
	// stream.
	case contract == "_AlarmStateChange" || contract == "_NotificationDispatched":
		return ClassAlarm
	// Annotations are appended like alarms; see ClassAnnotation.
	case contract == "_Annotation":
		return ClassAnnotation
	// A log line is an event: a later line does not replace an earlier one.
	case contract == "_Log":
		return ClassLog
	case contract == "_EnrolledIdentity" || contract == "_Node" ||
		contract == "_ServiceDetails" || contract == "_SystemElement" ||
		contract == "_Signal" || contract == "_Constant" || contract == "_ExternalReference" ||
		contract == "_Resource" ||
		contract == "_EditOperation" || contract == "_AlarmNotificationConfig" ||
		contract == "_NotificationConfigStatus":
		return ClassEntity
	case contract == "_Group" || contract == "_MetadataType" ||
		contract == "_AnnotationType" || contract == "_DataModel" ||
		contract == "_ExternalSystem" || contract == "_SemanticTag" ||
		contract == PersonalAccessTokenContract:
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

// IsState reports whether a class is state rather than an event: latest value
// per path, KV-projected, retained, and retired by an empty payload. Data,
// entities and definitions are state; commands, acks and gap markers are
// events, and retaining them would replay stale instructions. The KV
// projection, the retained flag and the tombstone rule all ask this.
func IsState(c Class) bool {
	return c == ClassData || c == ClassEntity || c == ClassDefinition
}

// The predicates below let the core ask what a class does without knowing
// which class it is. Adding a class here needs no change in the doors, the
// replicator or the pruner.

// IsKnown reports whether the contract resolved to a class this node handles.
// ClassNone means "no such contract", and the validated namespace rejects it.
func IsKnown(c Class) bool { return c != ClassNone }

// IsCommand reports whether a record belongs to the command flow: it targets an
// absolute node-local path (no mount rewrite, since the author does not own the
// target), needs a cmd grant, is refused while its destination drains, and
// travels down the tree.
func IsCommand(c Class) bool { return c == ClassCmd }

// CommandStillLive reports whether a ClassCmd record's expires_at is still
// after authoritativeNowMS. Validate requires a numeric expires_at on every
// _Cmd*, so a decode failure should not happen; it counts as live rather than
// ending a drain or hiding an undelivered command. Move-drain completion and
// the undelivered-command signal both ask this.
func CommandStillLive(payload []byte, authoritativeNowMS int64) bool {
	var body struct {
		ExpiresAt float64 `json:"expires_at"`
	}
	if err := json.Unmarshal(payload, &body); err != nil {
		return true
	}
	return int64(body.ExpiresAt) >= authoritativeNowMS
}

// IsNodeLocal reports whether only the node itself may produce a class. No
// door accepts one; the beacon loop publishes it to the local bus, and it is
// never stored or retained.
func IsNodeLocal(c Class) bool { return c == ClassTimeSync }

// IsOwnedState reports whether a class is state an identity authors under its
// own mount: KV-projected at mount-rewritten paths and replicated up the tree.
// Definitions are state too but travel down, so they are not included.
func IsOwnedState(c Class) bool { return c == ClassData || c == ClassEntity }

// IsCommandAuthoredState reports whether a command executor may author a class
// in its atomic batch: entities and definitions. Metrics are excluded, since a
// sample belongs to its machine's door and should not wait for a command's
// commit. A batch must still land on one stream, so the door refuses a batch
// that mixes entities and definitions.
func IsCommandAuthoredState(c Class) bool { return c == ClassEntity || c == ClassDefinition }

// IsCommandAuthoredEvent reports whether a command executor may append a class
// as a single event through EntityStore.PublishEvent: annotations. They are not
// state and live on a different stream than the _EditOperation receipt, so they
// cannot join PublishBatch; the receipt is written with its own batch after.
func IsCommandAuthoredEvent(c Class) bool { return c == ClassAnnotation }

// IsDefinition reports whether a class travels down the tree and is applied as
// state wherever it lands. Its path is its own id, so no hop rewrites it.
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
		c == ClassAudit || c == ClassAlarm || c == ClassAnnotation || c == ClassLog
}

// GapStream names the stream a _StreamGap marker describes. Each hop up
// prepends the child's mount to the marker's path, so only the last segment
// still names the stream everywhere.
func GapStream(p Parsed) string {
	if i := strings.LastIndex(p.Path, "/"); i >= 0 {
		return p.Path[i+1:]
	}
	return p.Path
}

// MatchesUplinkStream binds an upward record's domain class to the physical
// stream named by the replication request. Gap markers live in the stream
// their topic names; every other upward class has one fixed stream.
func MatchesUplinkStream(c Class, p Parsed, stream string) bool {
	if !FlowsUp(c) {
		return false
	}
	if c == ClassGap {
		return GapStream(p) == stream
	}
	return StreamFor(c) == stream
}

// uplinkStreamSet is every stream a child may replicate onto: the streams of
// the classes that flow up. It is derived from the classes, so a new class
// cannot be forgotten and definitions, which flow down, never appear; a child
// writing them would author policy for the whole tree. _StreamGap markers ride
// the stream they describe.
var uplinkStreamSet = func() map[string]bool {
	set := map[string]bool{}
	for _, c := range manifestClasses {
		if !FlowsUp(c) {
			continue
		}
		if stream := StreamFor(c); stream != "" {
			set[stream] = true
		}
	}
	return set
}()

// IsUplinkStream reports whether a child may replicate onto stream at all. It
// is checked before any record in the request is looked at.
func IsUplinkStream(stream string) bool { return uplinkStreamSet[stream] }

// UplinkStreams lists those streams, sorted: the enumeration a pusher can
// walk to seed a cursor per stream, and to check its own lane list against.
func UplinkStreams() []string {
	out := make([]string, 0, len(uplinkStreamSet))
	for stream := range uplinkStreamSet {
		out = append(out, stream)
	}
	sort.Strings(out)
	return out
}

// nodePrivateContracts are state contracts only the authoring node reads. Their
// class flows up, so they are projected and retained locally, but the records
// stay home. _EditOperation, the replay receipt, is read only by the same
// node's durableReplay; copies at an ancestor had no reader and filled its KV.
var nodePrivateContracts = map[string]bool{
	"_EditOperation": true,
}

// IsNodePrivate reports whether a contract's records, tombstones included,
// stay on the node that wrote them. The uplink still advances its cursor past
// them, so a private record never blocks a lane.
func IsNodePrivate(contract string) bool { return nodePrivateContracts[contract] }

// NodePrivateStreams lists, sorted, the streams node-private records can sit
// on: the streams a node sweeps for private copies other nodes sent before the
// uplink kept them home. It is derived from nodePrivateContracts.
func NodePrivateStreams() []string {
	set := map[string]bool{}
	for contract := range nodePrivateContracts {
		if stream := StreamFor(ClassOf(contract)); stream != "" {
			set[stream] = true
		}
	}
	out := make([]string, 0, len(set))
	for stream := range set {
		out = append(out, stream)
	}
	sort.Strings(out)
	return out
}

// partialUplinkStreams are streams whose uplink carries only a subset of the
// child's records, so gaps in child offsets are expected there: commands (only
// acks and gap markers rise) and streams holding a node-private contract
// (entities). Derived like uplinkStreamSet.
var partialUplinkStreams = func() map[string]bool {
	set := map[string]bool{}
	for _, c := range manifestClasses {
		if !FlowsUp(c) {
			if stream := StreamFor(c); stream != "" {
				set[stream] = true
			}
		}
	}
	for contract := range nodePrivateContracts {
		if stream := StreamFor(ClassOf(contract)); stream != "" {
			set[stream] = true
		}
	}
	return set
}()

// UplinkCarriesEveryRecord reports whether a child's uplink of stream carries
// every record, which a parent's offset gap detection relies on. Where it does
// not, only _StreamGap markers report real loss.
func UplinkCarriesEveryRecord(stream string) bool { return !partialUplinkStreams[stream] }

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
// entries to keep them across a retention boundary. Only entities: definitions
// are re-sent by the parent, and samples are meant to age out.
func NeedsStateRefresh(c Class) bool { return c == ClassEntity }

// manifestClasses maps a bundle manifest's class names to classes. It is data,
// not a switch, so tests can enumerate it. ClassGap and ClassTimeSync have no
// manifest name: the node authors them itself.
var manifestClasses = map[string]Class{
	"data":       ClassData,
	"entity":     ClassEntity,
	"definition": ClassDefinition,
	"cmd":        ClassCmd,
	"ack":        ClassAck,
	"audit":      ClassAudit,
	"alarm":      ClassAlarm,
	"annotation": ClassAnnotation,
	"log":        ClassLog,
}

// ClassFromManifest returns the class a bundle manifest names, and whether the name is known.
func ClassFromManifest(name string) (Class, bool) {
	c, ok := manifestClasses[name]
	return c, ok
}

// ManifestClassNames lists every class name a bundle may declare, sorted, so
// anything that must stay in step can iterate it.
func ManifestClassNames() []string {
	names := make([]string, 0, len(manifestClasses))
	for name := range manifestClasses {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// StreamFor maps a class to the stream that stores it. ClassGap has no fixed
// stream: a _StreamGap marker goes into the stream named by its topic path, so
// callers use Parsed.Path for those.
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
	case ClassAnnotation:
		return "annotations"
	case ClassLog:
		return "logs"
	case ClassGap:
		return ""
	case ClassTimeSync:
		return "" // ephemeral: no stream, never persisted
	}
	return ""
}

// TimeSyncTopic builds the beacon topic {root}/v1/_TimeSync/{node-ulid}, the
// only topic without a hierarchy path. nodeULID is the publishing node, never a
// machine.
func TimeSyncTopic(nodeULID string) string {
	return Prefix() + "_TimeSync/" + nodeULID
}

// IsMetric reports whether contract is _Metric, so the core never compares the
// literal itself.
func IsMetric(contract string) bool {
	return contract == "_Metric"
}

// SignalTopicForMetric returns the _Signal topic with p's node and path: the
// signal a _Metric at p needs. A signal and its metrics always share node and
// path, so p's contract is not checked.
func SignalTopicForMetric(p Parsed) string {
	return Prefix() + "_Signal/" + p.NodeID + "/" + p.Path
}

// DownlinkCursorPrefix names the parent-side cursor a repl server keeps per
// child on its commands stream: DownlinkCursorPrefix+{child-ulid} is the next
// offset that child has not fetched. It lives here so repl and the move-drain
// completion check use one name.
const DownlinkCursorPrefix = "downlink:"

// DownlinkDefCursorPrefix is the same for the definitions stream. It is a
// separate cursor because the streams advance independently, and compaction
// only supersedes a definition once every child has read past it.
const DownlinkDefCursorPrefix = "downlink-def:"

// UplinkCursor names this node's uplink cursor against one parent. The name
// includes the parent's pinned pubkey, which is known before first contact, so
// a node that changes parents never resumes at old offsets. The "up:", "down:"
// and "down-def:" prefixes keep these apart from the parent-side cursors.
func UplinkCursor(parentPubkey string) string { return "up:" + parentPubkey }

// DownlinkCursor is the commands-side counterpart of UplinkCursor.
func DownlinkCursor(parentPubkey string) string { return "down:" + parentPubkey }

// DownlinkDefCursor is the definitions-side counterpart of UplinkCursor.
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

// UnderMount reports whether a node-local path lies strictly below mount. The
// downlink filter, the move-drain completion scan, the draining-mount gate and
// the routability check all use it, so they cannot disagree. The boundary is
// the path separator ("werk10/x" is not under "werk1"), and an empty mount
// covers nothing.
func UnderMount(path, mount string) bool {
	return mount != "" && strings.HasPrefix(path, mount+"/")
}

// MountStrip removes the mount prefix from the hierarchy path, the inverse of
// MountInsert on every downlink hop. ok is false when the path is not under the
// mount; such a record belongs elsewhere and must not be delivered.
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

// Validate applies minimal per-contract checks, a hand-written fallback for the
// generated schema bundle. Unknown contracts are rejected.
func Validate(contract string, payload []byte) error {
	// An empty payload is a tombstone, valid only for data and entity
	// classes, where it deletes the KV key and clears the retained message.
	// Events have nothing to retire.
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
				switch v := value.(type) {
				case nil, string, float64, bool:
					// Safe scalar; source-specific allow-lists are producer-side.
				case []any:
					for _, item := range v {
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
	case contract == "_Resource":
		_, err := validateResourcePayload(payload)
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
	case contract == "_Signal":
		if err := reqStr("id"); err != nil {
			return err
		}
		if value, present := m["replication_policy"]; present {
			if value != "replicate_to_parents" && value != "source_local_only" {
				return fmt.Errorf("_Signal: replication_policy must be replicate_to_parents or source_local_only")
			}
		}
		return nil
	case contract == "_Node" || contract == "_ServiceDetails" ||
		contract == "_SystemElement" ||
		contract == "_ExternalReference" || contract == "_Group" ||
		contract == "_MetadataType" || contract == "_AnnotationType" ||
		contract == "_DataModel" || contract == "_ExternalSystem" ||
		contract == "_SemanticTag" || contract == PersonalAccessTokenContract:
		// Data-model records name themselves by "id", which grants and
		// bindings reference.
		return reqStr("id")
	case contract == "_TimeSync":
		// Only direct callers reach this: the engine rejects _TimeSync by
		// class first, and the node's own beacon bypasses Validate.
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
	case contract == "_Annotation":
		// Deployed nodes validate against the schema bundle; this checks only
		// the required fields, for a node without one.
		if err := reqStr("annotation_id"); err != nil {
			return err
		}
		if err := reqStr("annotation_type_id"); err != nil {
			return err
		}
		return reqNum("time_start")
	}
	return fmt.Errorf("unknown contract %q — validated namespace rejects unknown contracts", contract)
}

// placedConstant is the authored value stored at one namespace path. Value
// stays raw so int64 values never lose precision through float64.
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

// placedResource is a file-backed entity attached to one system element. The
// sha256 and size_bytes identify the blob, so metadata and bytes can travel
// separately and the bytes can be verified.
type placedResource struct {
	ID              string `json:"id"`
	SystemElementID string `json:"system_element_id"`
	Filename        string `json:"filename"`
	ContentType     string `json:"content_type"`
	SizeBytes       int64  `json:"size_bytes"`
	SHA256          string `json:"sha256"`
}

// isSHA256Hex matches what the blob store accepts: 64 lowercase hex
// characters.
func isSHA256Hex(s string) bool {
	if len(s) != 64 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

func validateResourcePayload(payload []byte) (placedResource, error) {
	var resource placedResource
	if err := json.Unmarshal(payload, &resource); err != nil {
		return placedResource{}, fmt.Errorf("_Resource: payload is not valid JSON: %w", err)
	}
	for field, value := range map[string]string{
		"id":                resource.ID,
		"system_element_id": resource.SystemElementID,
		"filename":          resource.Filename,
		"content_type":      resource.ContentType,
	} {
		if value == "" {
			return placedResource{}, fmt.Errorf("_Resource: field %q must be a non-empty string", field)
		}
	}
	if !isSHA256Hex(resource.SHA256) {
		return placedResource{}, fmt.Errorf(
			"_Resource: field %q must be 64 lowercase hex characters", "sha256")
	}
	if resource.SizeBytes < 0 {
		return placedResource{}, fmt.Errorf("_Resource: field %q must not be negative", "size_bytes")
	}
	return resource, nil
}

// ResourceContract is the contract name of a resource record, so the core can
// filter a KV scan without the literal.
const ResourceContract = "_Resource"

// ResourceBlob returns the digest a _Resource record references, so the core
// never parses the payload itself.
func ResourceBlob(payload []byte) (string, bool) {
	resource, err := validateResourcePayload(payload)
	if err != nil {
		return "", false
	}
	return resource.SHA256, true
}

// LiveBlobDigests returns the digests referenced by these _Resource records.
// The caller scopes records to _Resource (EntityStore.KVScanAll). An unreadable
// record keeps no blob alive.
func LiveBlobDigests(records []KVRecord) map[string]struct{} {
	live := make(map[string]struct{})
	for _, rec := range records {
		if sha, ok := ResourceBlob(rec.Payload); ok {
			live[sha] = struct{}{}
		}
	}
	return live
}

// ResourceID reports the id a _Resource record carries.
func ResourceID(payload []byte) (string, bool) {
	resource, err := validateResourcePayload(payload)
	if err != nil {
		return "", false
	}
	return resource.ID, true
}
