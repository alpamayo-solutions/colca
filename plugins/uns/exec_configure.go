package uns

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
)

// ConfigExec answers `_CmdConfigure`: editing the node's data model.
//
// The node executes these because a human may command but may not author state
// (human-authz §5.2) — so every path that creates a binding, whether a person in
// a UI, a preprovisioned model file, or autobind, arrives here as the same
// command and produces the same records. That is what makes "define it once"
// true by construction rather than by discipline.
type ConfigExec struct {
	store EntityStore
	// mu serializes commands. Every verb reads the node's current state, decides
	// on a complete set of records, and commits them in one transition; the lock
	// is what keeps that read-decide-commit sequence whole against another
	// command — and against the lifecycle trigger, which runs the same binding
	// logic from a record's arrival rather than from a verb.
	mu sync.Mutex
	// bound answers which identities bind to an element, so retiring a position
	// cannot strand the things standing on it, and who an identity is — the
	// name and element autobind needs to COMPUTE a connector's catalogue
	// topic (local-service-trust design §6).
	bound Bindings
	// elements resolves an element to this node's local path for it. Autobind
	// needs it for the same computation: an entry names an element, not a
	// path, and the path is what the catalogue topic is built from.
	elements Namespace
	// newID mints a fresh identity for a newly autobound signal — a ULID, per
	// this system's convention (node ids, registry entries, elements, and
	// Signal.id in the data model). plugins/uns is stdlib-only (arch_test.go),
	// so it cannot encode one itself; the domain declares this port and the
	// core supplies it with github.com/oklog/ulid/v2, the same library
	// registry/local.go already uses to mint an entry's ULID. A test may
	// inject a counter here for deterministic ids instead of special-casing
	// production code.
	newID func() string
	// autobindNew binds a connector's catalogue the first time the node sees
	// one, without waiting for anyone to ask (settings key
	// "autobind" = "on_new_connector").
	autobindNew bool
}

// NewConfigExec builds the executor. settings is the node's opaque plugin bag;
// unknown keys are ignored, so an operator's typo disables a feature rather
// than stopping a node.
func NewConfigExec(s EntityStore, bound Bindings, elements Namespace, newID func() string, settings map[string]string) *ConfigExec {
	return &ConfigExec{
		store: s, bound: bound, elements: elements, newID: newID,
		autobindNew: settings["autobind"] == "on_new_connector",
	}
}

func (c *ConfigExec) Handles(contract string) bool { return contract == "_CmdConfigure" }

// Observe reacts to a record the node just persisted.
//
// The only reaction is the lifecycle trigger: a connector's catalogue arriving
// with nothing bound to it yet gets bound, running the exact binding logic the
// `signal/autobind` verb runs. The record being SHAPED like a catalogue is not
// proof it IS one — anything with read scope can see another connector's tag
// ids, so a record naming real tag ids is not enough either — only a path some
// enrolled entry's own identity computes to is (local-service-trust design
// §6), which is why entryOwns runs before anything in the record is trusted.
// Once past that gate, autobind is idempotent by invariant, so this path needs
// no coordination with the people and commands that may also invoke it — the
// second caller simply finds nothing left to do.
func (c *ConfigExec) Observe(contract, topic string, payload []byte) {
	if !c.autobindNew || contract != "_DataTags" || len(payload) == 0 {
		return // not the trigger's contract, or the catalogue was retired
	}
	p, err := Parse(topic)
	if err != nil {
		return
	}
	// The trigger authors state exactly as the verb does, so it takes the same
	// lock: its read of what is already bound and its commit of what is not must
	// not interleave with a command doing the same work.
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.entryOwns(topic) {
		return // no enrolled entry's computed catalogue topic matches: ignore it
	}
	var cat catalogue
	if err := json.Unmarshal(payload, &cat); err != nil {
		return
	}
	bindings := c.bindings()
	for _, tag := range cat.DataTags {
		if bindings.propose(tag.ID, "") != bindFree {
			return // already bound: this is a republish, not a new connector
		}
	}
	c.bindCatalogue(p.Path, payload)
}

// entryOwns reports whether some identity this node has enrolled computes
// topic as its own catalogue topic — the read-side mirror of what autobind
// computes forward (node + mount + name), run over the local registry rather
// than over records. The comparison is the FULL topic, node id included, not
// just the path: two records that agree on path but not on which node
// published them are not the same catalogue, and comparing paths alone would
// let one stand in for the other. The registry is a handful of identities, so
// this scan costs nothing; it is a scan over IDENTITIES, which is what the
// reverted design's scan over RECORDS was not.
func (c *ConfigExec) entryOwns(topic string) bool {
	for _, e := range c.bound.Entries() {
		mount, ok := c.mountFor(e.Element)
		if !ok {
			continue // cannot place this entry here: it owns nothing
		}
		if "colca/v1/_DataTags/"+c.store.NodeID()+"/"+joinPath(mount, e.Name) == topic {
			return true
		}
	}
	return false
}

// mountFor resolves an identity's element to this node's local path,
// distinguishing "legitimately unplaced" (element == "", bound to the node
// itself) from "cannot be resolved here" (element names something this node
// does not hold). PathOf already fails closed on the latter (elements.go
// §PathOf); this wrapper is what stops a caller from collapsing that failure
// into the empty mount an unplaced identity gets, which would silently widen
// where that identity is treated as bound.
func (c *ConfigExec) mountFor(element string) (mount string, ok bool) {
	if element == "" {
		return "", true
	}
	return c.elements.PathOf(element)
}

// signalRef is one record to write: where it goes, and what goes there.
type signalRef struct {
	Path   string          `json:"path"`
	Signal json.RawMessage `json:"signal"`
}

type upsertBody struct {
	Signals []signalRef `json:"signals"`
}

// constantRef is one typed authored value and its position. Constants are not
// signals: they have no acquisition binding or metric topic.
type constantRef struct {
	Path     string          `json:"path"`
	Constant json.RawMessage `json:"constant"`
}

type constantUpsertBody struct {
	Constants []constantRef `json:"constants"`
}

type deleteBody struct {
	Paths []string `json:"paths"`
}

// elementRef is one element to write: where it sits, and what sits there.
type elementRef struct {
	Path    string          `json:"path"`
	Element json.RawMessage `json:"element"`
}

type elementUpsertBody struct {
	Elements []elementRef `json:"elements"`
}

// placedElement is the part of a _SystemElement record this needs: the identity
// that grants and bindings name it by.
type placedElement struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// definitionRef is one definition to write: which contract it is, and the
// record itself. There is deliberately NO path — a definition has no position,
// and its own id is where it goes (definition-stream design §2/§3). Letting a
// caller name the path is exactly how a definition would end up filed at a
// place, which is the thing that must not happen.
type definitionRef struct {
	Contract   string          `json:"contract"`
	Definition json.RawMessage `json:"definition"`
}

type definitionUpsertBody struct {
	Definitions []definitionRef `json:"definitions"`
}

// definitionDeleteRef names one definition to retract.
type definitionDeleteRef struct {
	Contract string `json:"contract"`
	ID       string `json:"id"`
}

type definitionDeleteBody struct {
	Definitions []definitionDeleteRef `json:"definitions"`
}

// entityRef is one positionless platform-inventory entity whose path is
// derived by the node. Plant-positioned elements and signals keep their
// dedicated verbs because their placement is part of the command.
type entityRef struct {
	Contract string          `json:"contract"`
	Entity   json.RawMessage `json:"entity"`
}

type entityUpsertBody struct {
	Entities []entityRef `json:"entities"`
}

type entityDeleteRef struct {
	Contract string `json:"contract"`
	ID       string `json:"id"`
}

type entityDeleteBody struct {
	Entities []entityDeleteRef `json:"entities"`
}

// identified is the part of any definition record this needs: the id that IS
// its address.
type identified struct {
	ID string `json:"id"`
}

type autobindBody struct {
	Connector string `json:"connector"`
	Under     string `json:"under"`
}

// catalogue is the part of a connector's _DataTags record this needs.
type catalogue struct {
	DataTags []struct {
		// ID is the tag's own identity — a ULID minted by the connector at
		// discovery, stable across rediscovery (design §6). Signal.data_tag
		// points at this, never at Name or a source address.
		ID       string `json:"id"`
		Name     string `json:"name"`
		DataType string `json:"data_type"`
	} `json:"data_tags"`
}

// boundSignal is the part of a _Signal record that identifies its binding.
// There is no connector field: the connector is reached THROUGH the tag,
// never stored beside it (design §6).
type boundSignal struct {
	ID      string `json:"id"`
	Name    string `json:"name"`
	DataTag string `json:"data_tag"`
}

func (c *ConfigExec) Execute(contract, verb string, payload []byte) (int, string, string) {
	code, message, result, _ := c.ExecuteWithWrites(contract, verb, payload)
	return code, message, result
}

// ExecuteWithWrites is the atomic-batch extension consumed by the engine.
// Every verb commits its whole record set through a single PublishBatch call
// before returning, so the writes here are commit coordinates (stream,
// offset, topic) already durable in the store — not records the executor
// reads back to learn what it just did. The engine folds them into the
// command's ack (CommandOutcome.StateWrites) so a caller learns exactly what
// landed without re-reading the store itself. The ordinary Execute method
// remains the stable executor interface for command handlers that do not
// produce state.
func (c *ConfigExec) ExecuteWithWrites(
	contract, verb string,
	payload []byte,
) (int, string, string, []StateWrite) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.execute(contract, verb, payload)
}

func (c *ConfigExec) execute(contract, verb string, payload []byte) (int, string, string, []StateWrite) {
	switch verb {
	case "signal/upsert":
		return c.upsert(payload)
	case "signal/delete":
		return c.delete(payload)
	case "signal/autobind":
		return c.autobind(payload)
	case "constant/upsert":
		return c.constantUpsert(payload)
	case "constant/delete":
		return c.constantDelete(payload)
	case "element/upsert":
		return c.elementUpsert(payload)
	case "element/delete":
		return c.elementDelete(payload)
	case "entity/upsert":
		return c.entityUpsert(payload)
	case "entity/delete":
		return c.entityDelete(payload)
	case "definition/upsert":
		return c.definitionUpsert(payload)
	case "definition/delete":
		return c.definitionDelete(payload)
	default:
		return 422, fmt.Sprintf("unknown configure verb %q", verb), "invalid", nil
	}
}

// commit writes a verb's complete record set as ONE state transition.
//
// This is the only way this executor writes. Every verb reads the node's state,
// decides on the whole set, and arrives here once: either every record takes a
// durable stream position and becomes current KV state, or none of them do. The
// alternative — a write per record — is what left a node half-configured when
// the fortieth signal of an autobind was refused, with the first thirty-nine
// already committed and an error returned to a caller who had no way to know
// which half took.
//
// A set that turned out empty is a successful no-op: an autobind whose tags are
// all bound already, or a delete of nothing. That is not a failed write and must
// not reach the store as one.
func (c *ConfigExec) commit(records []StateRecord) ([]StateWrite, error) {
	if len(records) == 0 {
		return nil, nil
	}
	return c.store.PublishBatch(records)
}

func (c *ConfigExec) constantUpsert(payload []byte) (int, string, string, []StateWrite) {
	var body constantUpsertBody
	if err := json.Unmarshal(payload, &body); err != nil {
		return 422, "constant/upsert: unreadable payload: " + err.Error(), "invalid", nil
	}
	if len(body.Constants) == 0 {
		return 422, "constant/upsert: no constants given", "invalid", nil
	}

	records := make([]StateRecord, 0, len(body.Constants))
	seen := make(map[string]bool, len(body.Constants))
	for i, ref := range body.Constants {
		if err := validatePositionPath(ref.Path); err != nil {
			return 422, fmt.Sprintf("constant/upsert: entry %d: %v", i, err), "invalid", nil
		}
		if len(ref.Constant) == 0 {
			return 422, fmt.Sprintf("constant/upsert: entry %d has no constant", i), "invalid", nil
		}
		incoming, err := validateConstantPayload(ref.Constant)
		if err != nil {
			return 422, fmt.Sprintf("constant/upsert: entry %d: %v", i, err), "invalid", nil
		}
		topic := c.constantTopic(ref.Path)
		if seen[topic] {
			return 422, fmt.Sprintf("constant/upsert: entry %d repeats path %s", i, ref.Path), "invalid", nil
		}
		seen[topic] = true
		if existing, ok := c.store.KVGet(topic); ok {
			held, err := validateConstantPayload(existing)
			if err != nil || held.ID != incoming.ID {
				heldID := held.ID
				if heldID == "" {
					heldID = "an unreadable retained record"
				}
				return 409, fmt.Sprintf("constant/upsert: %s is already constant %s — two constants "+
					"cannot share one position", ref.Path, heldID), "conflict", nil
			}
		}
		records = append(records, StateRecord{Topic: topic, Payload: ref.Constant})
	}

	writes, err := c.commit(records)
	if err != nil {
		return 422, "constant/upsert: rejected: " + err.Error(), "invalid", nil
	}
	return 200, fmt.Sprintf("upserted %d", len(records)), "ok", writes
}

func (c *ConfigExec) constantDelete(payload []byte) (int, string, string, []StateWrite) {
	var body deleteBody
	if err := json.Unmarshal(payload, &body); err != nil {
		return 422, "constant/delete: unreadable payload: " + err.Error(), "invalid", nil
	}
	if len(body.Paths) == 0 {
		return 422, "constant/delete: no paths given", "invalid", nil
	}
	records := make([]StateRecord, 0, len(body.Paths))
	missing := make([]string, 0)
	seen := make(map[string]bool, len(body.Paths))
	for i, path := range body.Paths {
		if err := validatePositionPath(path); err != nil {
			return 422, fmt.Sprintf("constant/delete: entry %d: %v", i, err), "invalid", nil
		}
		topic := c.constantTopic(path)
		if seen[topic] {
			return 422, fmt.Sprintf("constant/delete: entry %d repeats path %s", i, path), "invalid", nil
		}
		seen[topic] = true
		if _, ok := c.store.KVGet(topic); !ok {
			missing = append(missing, path)
		}
		records = append(records, StateRecord{Topic: topic})
	}
	if len(missing) > 0 {
		return 404, "constant/delete: no constant at " + strings.Join(missing, ", "), "invalid", nil
	}
	writes, err := c.commit(records)
	if err != nil {
		return 500, "constant/delete: failed: " + err.Error(), "error", nil
	}
	return 200, fmt.Sprintf("deleted %d", len(records)), "ok", writes
}

func validatePositionPath(path string) error {
	if path == "" {
		return fmt.Errorf("path is required")
	}
	for _, segment := range strings.Split(path, "/") {
		if segment == "" {
			return fmt.Errorf("path %q is not canonical", path)
		}
		if strings.ContainsAny(segment, "+#") {
			return fmt.Errorf("path %q contains an MQTT wildcard", path)
		}
		for _, char := range segment {
			if char < 0x20 || char == 0x7f {
				return fmt.Errorf("path %q contains a control character", path)
			}
		}
	}
	return nil
}

func (c *ConfigExec) entityUpsert(payload []byte) (int, string, string, []StateWrite) {
	var body entityUpsertBody
	if err := json.Unmarshal(payload, &body); err != nil {
		return 422, "entity/upsert: unreadable payload: " + err.Error(), "invalid", nil
	}
	if len(body.Entities) == 0 {
		return 422, "entity/upsert: no entities given", "invalid", nil
	}
	records := make([]StateRecord, 0, len(body.Entities))
	for i, ref := range body.Entities {
		if err := c.checkCommandEntity(ref.Contract); err != nil {
			return 422, fmt.Sprintf("entity/upsert: entry %d: %v", i, err), "invalid", nil
		}
		var incoming identified
		if err := json.Unmarshal(ref.Entity, &incoming); err != nil || incoming.ID == "" {
			return 422, fmt.Sprintf("entity/upsert: entry %d has no id", i), "invalid", nil
		}
		if err := c.checkCommandEntityIdentity(ref.Contract, incoming.ID, ref.Entity); err != nil {
			return 422, fmt.Sprintf("entity/upsert: entry %d: %v", i, err), "invalid", nil
		}
		records = append(records, StateRecord{
			Topic:   c.commandEntityTopic(ref.Contract, incoming.ID),
			Payload: ref.Entity,
		})
	}
	writes, err := c.commit(records)
	if err != nil {
		return 422, "entity/upsert: rejected: " + err.Error(), "invalid", nil
	}
	return 200, fmt.Sprintf("upserted %d", len(records)), "ok", writes
}

func (c *ConfigExec) entityDelete(payload []byte) (int, string, string, []StateWrite) {
	var body entityDeleteBody
	if err := json.Unmarshal(payload, &body); err != nil {
		return 422, "entity/delete: unreadable payload: " + err.Error(), "invalid", nil
	}
	if len(body.Entities) == 0 {
		return 422, "entity/delete: no entities given", "invalid", nil
	}
	var missing []string
	records := make([]StateRecord, 0, len(body.Entities))
	seen := make(map[string]bool, len(body.Entities))
	for i, ref := range body.Entities {
		if err := c.checkCommandEntity(ref.Contract); err != nil {
			return 422, fmt.Sprintf("entity/delete: entry %d: %v", i, err), "invalid", nil
		}
		if ref.ID == "" {
			return 422, fmt.Sprintf("entity/delete: entry %d has no id", i), "invalid", nil
		}
		if err := c.checkCommandEntityIdentity(ref.Contract, ref.ID, nil); err != nil {
			return 422, fmt.Sprintf("entity/delete: entry %d: %v", i, err), "invalid", nil
		}
		topic := c.commandEntityTopic(ref.Contract, ref.ID)
		// A repeat is refused rather than tombstoned twice: the set is decided
		// before anything is written, so the second mention cannot discover that
		// the first already retired it (constant/delete refuses repeats for the
		// same reason).
		if seen[topic] {
			return 422, fmt.Sprintf("entity/delete: entry %d repeats %s %s",
				i, ref.Contract, ref.ID), "invalid", nil
		}
		seen[topic] = true
		if _, ok := c.store.KVGet(topic); !ok {
			missing = append(missing, ref.Contract+" "+ref.ID)
			continue
		}
		records = append(records, StateRecord{Topic: topic})
	}
	if len(missing) > 0 {
		return 404, "entity/delete: no entity at " + strings.Join(missing, ", "), "invalid", nil
	}
	writes, err := c.commit(records)
	if err != nil {
		return 500, "entity/delete: failed: " + err.Error(), "error", nil
	}
	return 200, fmt.Sprintf("deleted %d", len(records)), "ok", writes
}

func (c *ConfigExec) checkCommandEntity(contract string) error {
	switch contract {
	case "_Node", "_ExternalReference", "_AlarmNotificationConfig":
		return nil
	case "_ServiceDetails", "_EnrolledIdentity", "_NotificationConfigStatus":
		return fmt.Errorf("%s is observed state and has its own writer", contract)
	default:
		return fmt.Errorf("%s is not a platform-inventory entity authored by this command", contract)
	}
}

func (c *ConfigExec) checkCommandEntityIdentity(contract, id string, raw []byte) error {
	if contract != "_AlarmNotificationConfig" {
		return nil
	}
	if id != "alarm-notification-config" {
		return fmt.Errorf("_AlarmNotificationConfig id must be alarm-notification-config")
	}
	if raw == nil { // delete is already addressed to this executing node
		return nil
	}
	var config struct {
		TargetNodeID string `json:"target_node_id"`
	}
	if err := json.Unmarshal(raw, &config); err != nil {
		return fmt.Errorf("_AlarmNotificationConfig is unreadable: %v", err)
	}
	if config.TargetNodeID == "" {
		return fmt.Errorf("_AlarmNotificationConfig target_node_id is required")
	}
	if config.TargetNodeID != c.store.NodeID() {
		return fmt.Errorf("_AlarmNotificationConfig target_node_id %q must equal local node %q",
			config.TargetNodeID, c.store.NodeID())
	}
	return nil
}

func (c *ConfigExec) commandEntityTopic(contract, id string) string {
	leaf := map[string]string{
		"_Node":              "nodes",
		"_ExternalReference":       "external-references",
		"_AlarmNotificationConfig": "alarm-notification-config",
	}[contract]
	return "colca/v1/" + contract + "/" + c.store.NodeID() + "/_colca/" + leaf + "/" + id
}

func (c *ConfigExec) upsert(payload []byte) (int, string, string, []StateWrite) {
	var body upsertBody
	if err := json.Unmarshal(payload, &body); err != nil {
		return 422, "signal/upsert: unreadable payload: " + err.Error(), "invalid", nil
	}
	if len(body.Signals) == 0 {
		return 422, "signal/upsert: no signals given", "invalid", nil
	}
	records := make([]StateRecord, 0, len(body.Signals))
	for i, ref := range body.Signals {
		if ref.Path == "" {
			return 422, fmt.Sprintf("signal/upsert: entry %d has no path", i), "invalid", nil
		}
		if len(ref.Signal) == 0 {
			return 422, fmt.Sprintf("signal/upsert: entry %d has no signal", i), "invalid", nil
		}
		records = append(records, StateRecord{Topic: c.signalTopic(ref.Path), Payload: ref.Signal})
	}
	writes, err := c.commit(records)
	if err != nil {
		// The bundle rejected a record, or the store did, and NOTHING was
		// written. The commit names the record it refused, so the caller still
		// learns which entry and why rather than a bare failure.
		return 422, "signal/upsert: rejected: " + err.Error(), "invalid", nil
	}
	return 200, fmt.Sprintf("upserted %d", len(records)), "ok", writes
}

func (c *ConfigExec) delete(payload []byte) (int, string, string, []StateWrite) {
	var body deleteBody
	if err := json.Unmarshal(payload, &body); err != nil {
		return 422, "signal/delete: unreadable payload: " + err.Error(), "invalid", nil
	}
	if len(body.Paths) == 0 {
		return 422, "signal/delete: no paths given", "invalid", nil
	}
	var missing []string
	records := make([]StateRecord, 0, len(body.Paths))
	seen := make(map[string]bool, len(body.Paths))
	for i, path := range body.Paths {
		topic := c.signalTopic(path)
		if seen[topic] {
			return 422, fmt.Sprintf("signal/delete: entry %d repeats path %s", i, path), "invalid", nil
		}
		seen[topic] = true
		if _, ok := c.store.KVGet(topic); !ok {
			missing = append(missing, path)
			continue
		}
		// An empty payload is the tombstone: the path is retired, not blanked.
		records = append(records, StateRecord{Topic: topic})
	}
	if len(missing) > 0 {
		return 404, "signal/delete: no signal at " + strings.Join(missing, ", "), "invalid", nil
	}
	writes, err := c.commit(records)
	if err != nil {
		return 500, "signal/delete: failed: " + err.Error(), "error", nil
	}
	return 200, fmt.Sprintf("deleted %d", len(records)), "ok", writes
}

// autobind creates one signal per unbound tag of a connector's catalogue.
//
// Idempotent by invariant: a tag that already has a binding is skipped and an
// existing binding is never overwritten, so re-running changes nothing. That is
// what lets the same verb be issued by a person, replayed from the commands
// stream after an offline period, or fired by a node lifecycle trigger, without
// any of those paths needing to know about the others.
func (c *ConfigExec) autobind(payload []byte) (int, string, string, []StateWrite) {
	var body autobindBody
	if err := json.Unmarshal(payload, &body); err != nil {
		return 422, "signal/autobind: unreadable payload: " + err.Error(), "invalid", nil
	}
	if body.Connector == "" {
		return 422, "signal/autobind: no connector given", "invalid", nil
	}

	name, element, ok := c.bound.EntryOf(body.Connector)
	if !ok {
		// Not enrolled here. A parent asked to bind a connector only its child
		// holds must refuse, not guess — the command travels down and executes
		// at the node that owns the identity.
		return 404, "signal/autobind: " + body.Connector + " is not enrolled at this node", "invalid", nil
	}
	mount, ok := c.mountFor(element)
	if !ok {
		// The entry is enrolled and placed, but this node cannot resolve
		// where — fail closed rather than treat it as unplaced, or a
		// connector this node genuinely cannot locate would read as bound to
		// the node itself and its catalogue topic would be computed wrong.
		return 409, "signal/autobind: " + name + " is bound to an element this node cannot resolve", "conflict", nil
	}
	catTopic := "colca/v1/_DataTags/" + c.store.NodeID() + "/" + joinPath(mount, name)
	raw, found := c.store.KVGet(catTopic)
	if !found {
		// Nothing to bind against yet — the connector has not published its
		// catalogue. A retry after it does will succeed, so this is a conflict
		// with the current state, not a bad request.
		return 409, "signal/autobind: " + name + " has published no catalogue", "conflict", nil
	}

	under := body.Under
	if under == "" {
		under = joinPath(mount, name)
	}
	return c.bindCatalogue(under, raw)
}

// bindCatalogue creates one signal per unbound tag in a catalogue, placed
// under one path. Shared by the explicit `signal/autobind` verb (which
// computes the catalogue and the default placement from the registry) and the
// lifecycle trigger (which already has both, straight from the record it just
// observed).
//
// Idempotent by invariant: a tag that already has a signal is skipped and an
// existing binding is never overwritten, so re-running changes nothing. That is
// what lets the same verb be issued by a person, replayed from the commands
// stream after an offline period, or fired by a node lifecycle trigger, without
// any of those paths needing to know about the others.
//
// The two things this reads about itself as it goes — which tags are already
// bound, and which paths are already taken — it tracks locally rather than by
// re-reading the store, so composing the whole set before committing it reads
// exactly as writing one at a time did. A catalogue whose tags sanitize to the
// same segment still gets one path each.
func (c *ConfigExec) bindCatalogue(under string, raw []byte) (int, string, string, []StateWrite) {
	var cat catalogue
	if err := json.Unmarshal(raw, &cat); err != nil {
		return 422, "signal/autobind: unreadable catalogue: " + err.Error(), "invalid", nil
	}

	bindings := c.bindings()
	taken := c.takenPaths()

	records := make([]StateRecord, 0, len(cat.DataTags))
	skipped := 0
	for _, tag := range cat.DataTags {
		// The invariant is asked of its one owner; skipping is this operation's
		// own answer to it. The edit asks the same question and refuses
		// instead — see signalBindings. An empty signal id says the signal does
		// not exist yet, which is exactly what provisioning proposes.
		if bindings.propose(tag.ID, "") != bindFree {
			skipped++
			continue
		}
		leaf := uniquePath(sanitize(tag.Name), under, taken)
		path := under + "/" + leaf
		// The signal's own identity: never composed from what it is bound to.
		// Every Metric carries signal_id, so rebinding this signal to a
		// different tag later must leave it — and the whole metric history
		// under it — untouched (design §6).
		id := c.newID()
		signal := map[string]any{
			"id":           id,
			"name":         leaf,
			"data_tag":     tag.ID,
			"is_published": true,
			"metadata":     map[string]any{"tag_name": tag.Name},
		}
		if tag.DataType != "" {
			signal["data_type"] = tag.DataType
		}
		encoded, err := json.Marshal(signal)
		if err != nil {
			return 500, "signal/autobind: encode failed: " + err.Error(), "error", nil
		}
		records = append(records, StateRecord{Topic: c.signalTopic(path), Payload: encoded})
		bindings.bind(tag.ID, id)
		taken[path] = true
	}
	writes, err := c.commit(records)
	if err != nil {
		return 422, "signal/autobind: rejected: " + err.Error(), "invalid", nil
	}
	return 200, fmt.Sprintf(`{"created":%d,"skipped":%d}`, len(records), skipped), "ok", writes
}

// joinPath composes a mount and a leaf into one path. An unplaced identity's
// mount is "" (bound to the node itself), and the result must still be one
// clean path — no leading or doubled slash.
func joinPath(mount, leaf string) string {
	if mount == "" {
		return leaf
	}
	return mount + "/" + leaf
}

func (c *ConfigExec) signalTopic(path string) string {
	return "colca/v1/_Signal/" + c.store.NodeID() + "/" + path
}

func (c *ConfigExec) constantTopic(path string) string {
	return "colca/v1/_Constant/" + c.store.NodeID() + "/" + path
}

// bindings is the tag↔signal state this node already holds — what autobind
// consults to learn which of a catalogue's tags need no work. A tag's id is its
// own ULID, minted by the connector that discovered it, so this is exact
// without scoping it to a connector: nothing about a connector appears on a
// signal any more (design §6) — the tag id alone is what a signal points at.
//
// A retained signal carrying a binding but no id of its own still holds its
// tag; its path stands in as the identity, so a curated record missing a field
// can never read as unbound and be overwritten.
func (c *ConfigExec) bindings() *signalBindings {
	bindings := newSignalBindings()
	for _, rec := range c.store.KVScan("_Signal", c.store.NodeID()) {
		var s boundSignal
		if json.Unmarshal(rec.Payload, &s) != nil {
			continue
		}
		identity := s.ID
		if identity == "" {
			identity = rec.Path
		}
		bindings.bind(s.DataTag, identity)
	}
	return bindings
}

func (c *ConfigExec) takenPaths() map[string]bool {
	taken := map[string]bool{}
	for _, rec := range c.store.KVScan("_Signal", c.store.NodeID()) {
		taken[rec.Path] = true
	}
	return taken
}

// sanitize turns a tag name into one topic segment. MQTT separators and
// wildcards cannot survive in a path, and a name that sanitizes to nothing
// still needs an addressable place to live.
func sanitize(name string) string {
	var b strings.Builder
	for _, r := range name {
		switch {
		case r == '/' || r == '+' || r == '#' || r == ' ' || r < 0x20:
			b.WriteByte('_')
		default:
			b.WriteRune(r)
		}
	}
	out := strings.Trim(b.String(), "_")
	if out == "" {
		return "tag"
	}
	return out
}

// uniquePath suffixes until the path is free. Two tags can sanitize to the same
// segment (or repeat across branches), and silently dropping one would lose data
// no one asked to lose.
func uniquePath(leaf, under string, taken map[string]bool) string {
	if !taken[under+"/"+leaf] {
		return leaf
	}
	for n := 2; ; n++ {
		candidate := fmt.Sprintf("%s-%d", leaf, n)
		if !taken[under+"/"+candidate] {
			return candidate
		}
	}
}

// elementUpsert writes elements at their positions in this node's namespace.
//
// The path IS the position — an element's own topic is where it sits — so two
// different elements cannot share one path: the second would be unaddressable,
// and every grant naming it would resolve to the first. That check lives here,
// at the owning node's door, because siblings share a parent and a parent has
// exactly one owning node (id-grants design §15.3).
func (c *ConfigExec) elementUpsert(payload []byte) (int, string, string, []StateWrite) {
	var body elementUpsertBody
	if err := json.Unmarshal(payload, &body); err != nil {
		return 422, "element/upsert: unreadable payload: " + err.Error(), "invalid", nil
	}
	if len(body.Elements) == 0 {
		return 422, "element/upsert: no elements given", "invalid", nil
	}
	records := make([]StateRecord, 0, len(body.Elements))
	// claimed is the same guard as the retained one below, applied to the
	// positions THIS command is taking. Nothing is written until the whole set
	// is decided, so a later entry cannot discover an earlier one in the store;
	// without this, two elements naming one path in a single command would both
	// commit and the second would silently unaddress the first.
	claimed := make(map[string]string, len(body.Elements))
	for i, ref := range body.Elements {
		if ref.Path == "" {
			return 422, fmt.Sprintf("element/upsert: entry %d has no path", i), "invalid", nil
		}
		if len(ref.Element) == 0 {
			return 422, fmt.Sprintf("element/upsert: entry %d has no element", i), "invalid", nil
		}
		var incoming placedElement
		if err := json.Unmarshal(ref.Element, &incoming); err != nil || incoming.ID == "" {
			return 422, fmt.Sprintf("element/upsert: entry %d has no element id — a position "+
				"nothing can name is not addressable", i), "invalid", nil
		}
		topic := c.elementTopic(ref.Path)
		if held, ok := claimed[topic]; ok && held != incoming.ID {
			return 409, fmt.Sprintf("element/upsert: %s is already element %s — two elements "+
				"cannot share one position", ref.Path, held), "conflict", nil
		}
		if existing, ok := c.store.KVGet(topic); ok {
			var held placedElement
			if json.Unmarshal(existing, &held) == nil && held.ID != incoming.ID {
				return 409, fmt.Sprintf("element/upsert: %s is already element %s — two elements "+
					"cannot share one position", ref.Path, held.ID), "conflict", nil
			}
		}
		claimed[topic] = incoming.ID
		records = append(records, StateRecord{Topic: topic, Payload: ref.Element})
	}
	writes, err := c.commit(records)
	if err != nil {
		return 422, "element/upsert: rejected: " + err.Error(), "invalid", nil
	}
	return 200, fmt.Sprintf("upserted %d", len(records)), "ok", writes
}

// elementDelete retires positions, refusing while anything still stands on one.
//
// Two things can stand on a position. Child elements: removing the position
// above them would strand them, their paths still working while the position
// they hang from is gone. And bound identities: an entry names an element to get
// its place, so retiring it would leave an identity that authenticates and can
// write nowhere. Both are conflicts with the current state, answerable by
// removing what is in the way first — and both are named in the refusal, because
// "no" without the reason costs a round of guessing.
func (c *ConfigExec) elementDelete(payload []byte) (int, string, string, []StateWrite) {
	var body deleteBody
	if err := json.Unmarshal(payload, &body); err != nil {
		return 422, "element/delete: unreadable payload: " + err.Error(), "invalid", nil
	}
	if len(body.Paths) == 0 {
		return 422, "element/delete: no paths given", "invalid", nil
	}

	// Two passes, because the command retires its positions together. A caller
	// retiring a subtree names the parent and its children in one command; the
	// occupancy check below must therefore know the whole set before it judges
	// any of it, or naming the parent first would read its own children as
	// stranded bystanders. Writing one at a time hid this behind list order.
	type pending struct {
		path, topic string
		element     []byte
	}
	var missing []string
	pendings := make([]pending, 0, len(body.Paths))
	retiring := make(map[string]bool, len(body.Paths))
	for i, path := range body.Paths {
		if retiring[path] {
			return 422, fmt.Sprintf("element/delete: entry %d repeats path %s", i, path), "invalid", nil
		}
		retiring[path] = true
		topic := c.elementTopic(path)
		raw, ok := c.store.KVGet(topic)
		if !ok {
			missing = append(missing, path)
			continue
		}
		pendings = append(pendings, pending{path: path, topic: topic, element: raw})
	}
	// Precedence, deliberate: a path that does not exist answers 404 for the
	// WHOLE command, ahead of any occupancy conflict. So ["gone", "occupied"]
	// is 404, not 409. Two reasons. The set is the unit — this command retires
	// its positions together or not at all — and a request naming something
	// that is not there is wrong about the state it is describing, before any
	// question of what stands on the rest of it arises. And the occupancy pass
	// below judges against `retiring`, which is only trustworthy once every
	// named path resolved: judging a subtree while one of its members turned
	// out not to exist reads that member's children as stranded bystanders.
	// The answer is also the same for either ordering of the list, which the
	// per-path refusal this replaced was not — it returned whichever conflict
	// the caller happened to list first.
	if len(missing) > 0 {
		return 404, "element/delete: no element at " + strings.Join(missing, ", "), "invalid", nil
	}

	records := make([]StateRecord, 0, len(pendings))
	for _, p := range pendings {
		if held := c.occupantsBelow(p.path, retiring); len(held) > 0 {
			return 409, fmt.Sprintf("element/delete: %s still holds %s", p.path,
				strings.Join(held, ", ")), "conflict", nil
		}
		if bound := c.boundIdentities(p.element); len(bound) > 0 {
			return 409, fmt.Sprintf("element/delete: %s is still bound by %s", p.path,
				strings.Join(bound, ", ")), "conflict", nil
		}
		records = append(records, StateRecord{Topic: p.topic})
	}
	writes, err := c.commit(records)
	if err != nil {
		return 500, "element/delete: failed: " + err.Error(), "error", nil
	}
	return 200, fmt.Sprintf("deleted %d", len(records)), "ok", writes
}

// boundIdentities lists the identities bound to the element held in raw.
func (c *ConfigExec) boundIdentities(raw []byte) []string {
	if c.bound == nil {
		return nil
	}
	var held placedElement
	if json.Unmarshal(raw, &held) != nil || held.ID == "" {
		return nil
	}
	return c.bound.BoundTo(held.ID)
}

// definitionUpsert writes definitions under this node's identity.
//
// A definition descends from here to every node below (definition-stream design
// §5), so this door is where policy and type enter the tree. The record's own id
// is its address: nothing about a definition says where it is, because it is
// not anywhere — it is the same thing at the root and at every edge.
func (c *ConfigExec) definitionUpsert(payload []byte) (int, string, string, []StateWrite) {
	var body definitionUpsertBody
	if err := json.Unmarshal(payload, &body); err != nil {
		return 422, "definition/upsert: unreadable payload: " + err.Error(), "invalid", nil
	}
	if len(body.Definitions) == 0 {
		return 422, "definition/upsert: no definitions given", "invalid", nil
	}
	records := make([]StateRecord, 0, len(body.Definitions))
	for i, ref := range body.Definitions {
		if code, msg, result := c.checkDefinitionContract(i, ref.Contract); code != 0 {
			return code, msg, result, nil
		}
		if len(ref.Definition) == 0 {
			return 422, fmt.Sprintf("definition/upsert: entry %d has no definition", i), "invalid", nil
		}
		var incoming identified
		if err := json.Unmarshal(ref.Definition, &incoming); err != nil || incoming.ID == "" {
			return 422, fmt.Sprintf("definition/upsert: entry %d has no id — a definition's id "+
				"is its address, and one without an id cannot be reached", i), "invalid", nil
		}
		if err := validDefinitionID(incoming.ID); err != nil {
			return 422, fmt.Sprintf("definition/upsert: entry %d: %v", i, err), "invalid", nil
		}
		if err := checkDefinitionContents(ref.Contract, ref.Definition); err != nil {
			return 422, fmt.Sprintf("definition/upsert: %s %s: %v",
				ref.Contract, incoming.ID, err), "invalid", nil
		}
		records = append(records, StateRecord{
			Topic:   c.definitionTopic(ref.Contract, incoming.ID),
			Payload: ref.Definition,
		})
	}
	writes, err := c.commit(records)
	if err != nil {
		return 422, "definition/upsert: rejected: " + err.Error(), "invalid", nil
	}
	return 200, fmt.Sprintf("upserted %d", len(records)), "ok", writes
}

// definitionDelete retracts definitions with a tombstone, which propagates down
// the same way the definition itself did.
func (c *ConfigExec) definitionDelete(payload []byte) (int, string, string, []StateWrite) {
	var body definitionDeleteBody
	if err := json.Unmarshal(payload, &body); err != nil {
		return 422, "definition/delete: unreadable payload: " + err.Error(), "invalid", nil
	}
	if len(body.Definitions) == 0 {
		return 422, "definition/delete: no definitions given", "invalid", nil
	}
	var missing []string
	records := make([]StateRecord, 0, len(body.Definitions))
	seen := make(map[string]bool, len(body.Definitions))
	for i, ref := range body.Definitions {
		if code, msg, result := c.checkDefinitionContract(i, ref.Contract); code != 0 {
			return code, msg, result, nil
		}
		if ref.ID == "" {
			return 422, fmt.Sprintf("definition/delete: entry %d has no id", i), "invalid", nil
		}
		topic := c.definitionTopic(ref.Contract, ref.ID)
		if seen[topic] {
			return 422, fmt.Sprintf("definition/delete: entry %d repeats %s %s",
				i, ref.Contract, ref.ID), "invalid", nil
		}
		seen[topic] = true
		if _, ok := c.store.KVGet(topic); !ok {
			missing = append(missing, ref.Contract+" "+ref.ID)
			continue
		}
		records = append(records, StateRecord{Topic: topic})
	}
	if len(missing) > 0 {
		return 404, "definition/delete: no definition at " + strings.Join(missing, ", "), "invalid", nil
	}
	writes, err := c.commit(records)
	if err != nil {
		return 500, "definition/delete: failed: " + err.Error(), "error", nil
	}
	return 200, fmt.Sprintf("deleted %d", len(records)), "ok", writes
}

// checkDefinitionContract refuses anything this door does not author. code 0
// means the contract is fine.
//
// The gate is the CLASS, not a list of names: this door authors definitions,
// and a caller reaching it with an element or a metric has misunderstood which
// door they are at — say so rather than filing the record somewhere odd.
func (c *ConfigExec) checkDefinitionContract(i int, contract string) (int, string, string) {
	if contract == "" {
		return 422, fmt.Sprintf("definition/upsert: entry %d has no contract", i), "invalid"
	}
	if ClassOf(contract) != ClassDefinition {
		return 422, fmt.Sprintf("definition: entry %d names %s, which is not a definition — "+
			"this door authors definitions, and they are the records that descend the tree",
			i, contract), "invalid"
	}
	return 0, "", ""
}

// checkDefinitionContents validates what a definition CARRIES, beyond the shape
// the bundle already checks.
//
// A group carries grant strings, and a malformed one is an authoring mistake
// that must die here rather than downstream: this definition is about to
// descend to every node below, and each of them would have to drop the bad
// grant and log it, over and over, for as long as the definition exists. One
// refusal at the door beats an error at every node forever.
func checkDefinitionContents(contract string, raw []byte) error {
	if contract != "_Group" {
		return nil
	}
	var g group
	if err := json.Unmarshal(raw, &g); err != nil {
		return fmt.Errorf("unreadable group: %w", err)
	}
	for _, grant := range g.Grants {
		if _, err := ParseGrant(grant); err != nil {
			return err
		}
	}
	return nil
}

// validDefinitionID rejects an id that would file the definition at a position.
func validDefinitionID(id string) error {
	if strings.ContainsAny(id, "/+#") {
		return fmt.Errorf("definition id %q: a definition has no position, so its id is one "+
			"segment and never a path", id)
	}
	return nil
}

func (c *ConfigExec) definitionTopic(contract, id string) string {
	return "colca/v1/" + contract + "/" + c.store.NodeID() + "/" + id
}

func (c *ConfigExec) elementTopic(path string) string {
	return "colca/v1/_SystemElement/" + c.store.NodeID() + "/" + path
}

// occupantsBelow lists the element paths sitting under one, so a refusal can
// name what is in the way instead of just saying no. A path the same command is
// retiring is not in the way: it leaves in the same transition, so nothing is
// ever stranded by it.
func (c *ConfigExec) occupantsBelow(path string, retiring map[string]bool) []string {
	var out []string
	for _, rec := range c.store.KVScan("_SystemElement", c.store.NodeID()) {
		if retiring[rec.Path] {
			continue
		}
		if strings.HasPrefix(rec.Path, path+"/") {
			out = append(out, rec.Path)
		}
	}
	sort.Strings(out)
	return out
}
