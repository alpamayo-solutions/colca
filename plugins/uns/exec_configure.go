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
	// blobs is this node's view of its file store. resource/upsert needs it
	// for the one invariant it holds: never author a record pointing at bytes
	// this node does not have (resources design §3, §8).
	blobs Blobs
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
	// observed is, per catalogue topic, the tag ids the last publish this
	// process saw carried — what lets a republish tell a NEW tag (a catalogue
	// that grew) from one whose signal an operator deleted on purpose.
	observed map[string]map[string]bool
}

// NewConfigExec builds the executor. settings is the node's opaque plugin bag;
// unknown keys are ignored, so an operator's typo disables a feature rather
// than stopping a node.
func NewConfigExec(s EntityStore, bound Bindings, elements Namespace, blobs Blobs, newID func() string, settings map[string]string) *ConfigExec {
	return &ConfigExec{
		store: s, bound: bound, elements: elements, blobs: blobs, newID: newID,
		autobindNew: settings["autobind"] == "on_new_connector",
		observed:    map[string]map[string]bool{},
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
	if _, err := Parse(topic); err != nil {
		return
	}
	// The trigger authors state exactly as the verb does, so it takes the same
	// lock: its read of what is already bound and its commit of what is not must
	// not interleave with a command doing the same work.
	c.mu.Lock()
	defer c.mu.Unlock()
	element, mount, ok := c.owningEntry(topic)
	if !ok {
		return // no enrolled entry's computed catalogue topic matches: ignore it
	}
	var cat catalogue
	if err := json.Unmarshal(payload, &cat); err != nil {
		return
	}
	ids := make(map[string]bool, len(cat.DataTags))
	for _, tag := range cat.DataTags {
		ids[tag.ID] = true
	}
	previous, seen := c.observed[topic]
	c.observed[topic] = ids

	// A catalogue can GROW after its first publish: an OPC UA connector
	// announces its synthetic heartbeat and connectivity tags before its
	// browse of the server has finished (or while the PLC is unreachable),
	// and the full catalogue follows. The tags that publish adds are bound
	// like a new connector's; the ones it carried before are left alone, so a
	// republish never revives a binding an operator deleted on purpose.
	//
	// The first publish this process sees for a topic has no "before" to
	// compare against, so it keeps the older, narrower rule: bind only when
	// nothing in it is bound yet — never revive, at the price of not binding
	// a catalogue that grew across a restart of this node (an explicit
	// `signal/autobind` still does).
	bindings := c.bindings()
	if !seen {
		for _, tag := range cat.DataTags {
			if bindings.propose(tag.ID, "") != bindFree {
				return // already bound: a republish, not a new connector
			}
		}
		c.bindCatalogue(mount, element, payload)
		return
	}
	var grown catalogue
	for _, tag := range cat.DataTags {
		if !previous[tag.ID] {
			grown.DataTags = append(grown.DataTags, tag)
		}
	}
	if len(grown.DataTags) == 0 {
		return // the same catalogue again
	}
	encoded, err := json.Marshal(grown)
	if err != nil {
		return
	}
	c.bindCatalogue(mount, element, encoded)
}

// owningEntry finds the identity this node has enrolled whose computed
// catalogue topic is topic — the read-side mirror of what autobind computes
// forward (node + mount + name), run over the local registry rather than over
// records — and answers with where that identity is bound, which is where its
// signals go. The comparison is the FULL topic, node id included, not just the
// path: two records that agree on path but not on which node published them
// are not the same catalogue, and comparing paths alone would let one stand
// in for the other. The registry is a handful of identities, so this scan
// costs nothing; it is a scan over IDENTITIES, which is what the reverted
// design's scan over RECORDS was not.
func (c *ConfigExec) owningEntry(topic string) (element, mount string, ok bool) {
	for _, e := range c.bound.Entries() {
		m, ok := c.mountFor(e.Element)
		if !ok {
			continue // cannot place this entry here: it owns nothing
		}
		if "colca/v1/_DataTags/"+c.store.NodeID()+"/"+joinPath(m, e.Name) == topic {
			return e.Element, m, true
		}
	}
	return "", "", false
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
		// Meta.Element, when set, is the node-local path of the element this
		// tag's signal belongs under, instead of the connector's own mount.
		// One unplaced participant computing for several machines (a dataops
		// service) says per output which machine it is about; without this
		// every output would land at the node root under one name.
		Meta struct {
			Element string `json:"element"`
		} `json:"meta"`
	} `json:"data_tags"`
}

// boundSignal is the part of a _Signal record that identifies its binding.
// There is no connector field: the connector is reached THROUGH the tag,
// never stored beside it (design §6).
type boundSignal struct {
	ID      string `json:"id"`
	Name    string `json:"name"`
	DataTag string `json:"data_tag"`
	// Element is the system element the signal is bound to — the position
	// it stands on. Empty for a signal bound to the node itself.
	Element string `json:"system_element_id"`
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
	case "resource/upsert":
		return c.resourceUpsert(payload)
	case "resource/delete":
		return c.resourceDelete(payload)
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

type resourceRef struct {
	Path     string          `json:"path"`
	Resource json.RawMessage `json:"resource"`
}

type resourceUpsertBody struct {
	Resources []resourceRef `json:"resources"`
}

// resourceTopic places a resource exactly as every other positioned entity:
// the node's own ULID at level 4, the element path and the resource id below.
func (c *ConfigExec) resourceTopic(path string) string {
	return "colca/v1/_Resource/" + c.store.NodeID() + "/" + path
}

func (c *ConfigExec) resourceUpsert(payload []byte) (int, string, string, []StateWrite) {
	var body resourceUpsertBody
	if err := json.Unmarshal(payload, &body); err != nil {
		return 422, "resource/upsert: unreadable payload: " + err.Error(), "invalid", nil
	}
	if len(body.Resources) == 0 {
		return 422, "resource/upsert: no resources given", "invalid", nil
	}

	records := make([]StateRecord, 0, len(body.Resources))
	seen := make(map[string]bool, len(body.Resources))
	for i, ref := range body.Resources {
		if err := validatePositionPath(ref.Path); err != nil {
			return 422, fmt.Sprintf("resource/upsert: entry %d: %v", i, err), "invalid", nil
		}
		if len(ref.Resource) == 0 {
			return 422, fmt.Sprintf("resource/upsert: entry %d has no resource", i), "invalid", nil
		}
		incoming, err := validateResourcePayload(ref.Resource)
		if err != nil {
			return 422, fmt.Sprintf("resource/upsert: entry %d: %v", i, err), "invalid", nil
		}
		topic := c.resourceTopic(ref.Path)
		if seen[topic] {
			return 422, fmt.Sprintf("resource/upsert: entry %d repeats path %s", i, ref.Path), "invalid", nil
		}
		seen[topic] = true
		if existing, ok := c.store.KVGet(topic); ok {
			held, err := validateResourcePayload(existing)
			if err != nil || held.ID != incoming.ID {
				heldID := held.ID
				if heldID == "" {
					heldID = "an unreadable retained record"
				}
				return 409, fmt.Sprintf("resource/upsert: %s is already resource %s — two resources "+
					"cannot share one position", ref.Path, heldID), "conflict", nil
			}
		}
		// The invariant: never author a record pointing at bytes we do not
		// hold. A provisioning command from above names a digest staged at an
		// ancestor, so one pull is attempted before giving up.
		//
		// A failed pull is its OWN outcome, not a malformed command
		// (resources design §9.1: "the ack reports success or the pull
		// failure"). The command was well-formed, the operator did nothing
		// wrong, and the remedy — stage the bytes where this node can reach
		// them, then reissue — is different from every other refusal here. So
		// it carries the machine-readable code first, the way every other
		// coded refusal in this executor family does
		// (entity_already_exists:, stale_version:, duplicate_name:), and it
		// classifies as blob_unreachable rather than folding into `invalid`,
		// which is what lets an operator count pull failures apart from bad
		// commands on colca_node_cmds_total{result=…}.
		if err := c.ensureBlob(incoming.SHA256); err != nil {
			return 422, fmt.Sprintf("blob_unreachable: resource/upsert entry %d: blob %s is not held "+
				"by this node and could not be fetched: %v", i, incoming.SHA256, err), "blob_unreachable", nil
		}
		records = append(records, StateRecord{Topic: topic, Payload: ref.Resource})
	}

	writes, err := c.commit(records)
	if err != nil {
		return 422, "resource/upsert: rejected: " + err.Error(), "invalid", nil
	}
	return 200, fmt.Sprintf("upserted %d", len(records)), "ok", writes
}

// ensureBlob is the one place the upsert invariant lives.
func (c *ConfigExec) ensureBlob(sha string) error {
	if c.blobs == nil {
		return fmt.Errorf("this node has no blob store")
	}
	if c.blobs.Has(sha) {
		return nil
	}
	if err := c.blobs.Pull(sha); err != nil {
		return err
	}
	if !c.blobs.Has(sha) {
		return fmt.Errorf("the fetch reported success but the blob is still absent")
	}
	return nil
}

func (c *ConfigExec) resourceDelete(payload []byte) (int, string, string, []StateWrite) {
	var body deleteBody
	if err := json.Unmarshal(payload, &body); err != nil {
		return 422, "resource/delete: unreadable payload: " + err.Error(), "invalid", nil
	}
	if len(body.Paths) == 0 {
		return 422, "resource/delete: no paths given", "invalid", nil
	}
	records := make([]StateRecord, 0, len(body.Paths))
	missing := make([]string, 0)
	seen := make(map[string]bool, len(body.Paths))
	for i, path := range body.Paths {
		if err := validatePositionPath(path); err != nil {
			return 422, fmt.Sprintf("resource/delete: entry %d: %v", i, err), "invalid", nil
		}
		topic := c.resourceTopic(path)
		if seen[topic] {
			return 422, fmt.Sprintf("resource/delete: entry %d repeats path %s", i, path), "invalid", nil
		}
		seen[topic] = true
		if _, ok := c.store.KVGet(topic); !ok {
			missing = append(missing, path)
		}
		records = append(records, StateRecord{Topic: topic})
	}
	if len(missing) > 0 {
		return 404, "resource/delete: no resource at " + strings.Join(missing, ", "), "invalid", nil
	}
	// The blob is deliberately NOT touched here. The sweep removes it once
	// nothing references it (§8), which is also what makes a shared blob safe:
	// two resources with identical content are one file.
	writes, err := c.commit(records)
	if err != nil {
		return 500, "resource/delete: failed: " + err.Error(), "error", nil
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
		entity := ref.Entity
		if ref.Contract == "_Node" && incoming.ID == c.store.NodeID() {
			entity = c.keepOwnPosition(entity)
		}
		records = append(records, StateRecord{
			Topic:   c.commandEntityTopic(ref.Contract, incoming.ID),
			Payload: entity,
		})
	}
	writes, err := c.commit(records)
	if err != nil {
		return 422, "entity/upsert: rejected: " + err.Error(), "invalid", nil
	}
	return 200, fmt.Sprintf("upserted %d", len(records)), "ok", writes
}

// keepOwnPosition carries this node's learned position across an upsert of
// its OWN `_Node` record that does not carry one.
//
// A node is the author of where it sits: it learns that from the ancestry its
// parent teaches on the downlink and writes it into its own record (node.go's
// position hook). Everything else about the record — a display name, a
// description — is an operator's to set, and an upsert replaces the record
// wholesale. So a generated bootstrap re-applied after enrollment, which
// states `root_system_element_id: null` because a deployment file cannot know
// a position, silently unplaced the node: the hook does not fire again
// (the ancestry did not change), and from then on the node projects as bound
// to nothing — its signals resolve to the root node as their owner, and
// anything addressed to the node that owns them goes to the wrong node.
//
// Only an ABSENT or empty incoming position is filled in. A writer that
// states one still wins, which is what lets the position hook itself set it,
// and what lets a re-taught position replace an older one.
func (c *ConfigExec) keepOwnPosition(entity []byte) []byte {
	var incoming map[string]json.RawMessage
	if json.Unmarshal(entity, &incoming) != nil {
		return entity
	}
	if raw, ok := incoming["root_system_element_id"]; ok {
		var held string
		if json.Unmarshal(raw, &held) == nil && held != "" {
			return entity
		}
	}
	stored, ok := c.store.KVGet(c.commandEntityTopic("_Node", c.store.NodeID()))
	if !ok {
		return entity
	}
	var current map[string]json.RawMessage
	if json.Unmarshal(stored, &current) != nil {
		return entity
	}
	position, ok := current["root_system_element_id"]
	if !ok {
		return entity
	}
	var learned string
	if json.Unmarshal(position, &learned) != nil || learned == "" {
		return entity
	}
	incoming["root_system_element_id"] = position
	merged, err := json.Marshal(incoming)
	if err != nil {
		return entity
	}
	return merged
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

	// A signal binds to a system element and sits directly under it: the
	// element tree IS the namespace, so every segment of a signal's path is
	// an element. By default that element is the one the connector itself is
	// bound to — its mount. The connector's NAME is not a segment: it is a
	// participant, not a position, and its own records (the catalogue) carry
	// it as a human-readable final segment precisely because they are
	// service-owned, which a signal is not.
	under, at := mount, element
	if body.Under != "" {
		id, ok := c.elementAt(body.Under)
		if !ok {
			return 409, "signal/autobind: no element at " + body.Under +
					" — a signal binds to the element at its path, so one must be authored there first",
				"conflict", nil
		}
		under, at = body.Under, id
	}
	return c.bindCatalogue(under, at, raw)
}

// elementAt answers which element this node holds at a local path, if any —
// read off the record at that position, the same place elementUpsert refuses
// a colliding sibling from.
func (c *ConfigExec) elementAt(path string) (string, bool) {
	raw, ok := c.store.KVGet(c.elementTopic(path))
	if !ok {
		return "", false
	}
	var e identified
	if json.Unmarshal(raw, &e) != nil || e.ID == "" {
		return "", false
	}
	return e.ID, true
}

// bindCatalogue creates one signal per unbound tag in a catalogue, placed
// under one path and bound to the element there. Shared by the explicit
// `signal/autobind` verb (which computes the catalogue and the default
// placement from the registry) and the lifecycle trigger (which already has
// both, straight from the record it just observed).
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
//
// A declared signal is bound, not shadowed. A bootstrap manifest authors the
// signals a node's tree is supposed to hold before any connector has
// published — with `data_tag: null`, because the tag's ULID is minted at
// discovery and cannot be known in advance. When the catalogue then arrives,
// a tag whose path already holds an unbound signal binds THAT record instead
// of minting `<name>-2` beside it: the declaration said what the signal is
// (unit, precision, description, semantic tag), the catalogue says where its
// value comes from, and the two meet at the path. A signal that already
// holds another tag is a different case and stays untouched — the collision
// gets a sibling exactly as before.
func (c *ConfigExec) bindCatalogue(under, element string, raw []byte) (int, string, string, []StateWrite) {
	var cat catalogue
	if err := json.Unmarshal(raw, &cat); err != nil {
		return 422, "signal/autobind: unreadable catalogue: " + err.Error(), "invalid", nil
	}

	bindings := c.bindings()
	taken := c.takenPaths()

	records := make([]StateRecord, 0, len(cat.DataTags))
	skipped, unplaced := 0, 0
	for _, tag := range cat.DataTags {
		// The invariant is asked of its one owner; skipping is this operation's
		// own answer to it. The edit asks the same question and refuses
		// instead — see signalBindings. An empty signal id says the signal does
		// not exist yet, which is exactly what provisioning proposes.
		if bindings.propose(tag.ID, "") != bindFree {
			skipped++
			continue
		}
		under, element := under, element
		if tag.Meta.Element != "" {
			// The tag names its own element. One that this node does not hold
			// (yet) is left unbound rather than misplaced at the mount: the
			// next autobind, after the element is authored, binds it.
			id, ok := c.elementAt(tag.Meta.Element)
			if !ok {
				unplaced++
				continue
			}
			under, element = tag.Meta.Element, id
		}
		leaf := sanitize(tag.Name)
		path := joinPath(under, leaf)
		if taken[path] {
			if existing, existingID, ok := c.unboundSignalAt(path); ok {
				existing["data_tag"] = tag.ID
				if _, typed := existing["data_type"]; !typed && tag.DataType != "" {
					existing["data_type"] = tag.DataType
				}
				if _, published := existing["is_published"]; !published {
					existing["is_published"] = true
				}
				encoded, err := json.Marshal(existing)
				if err != nil {
					return 500, "signal/autobind: encode failed: " + err.Error(), "error", nil
				}
				records = append(records, StateRecord{Topic: c.signalTopic(path), Payload: encoded})
				bindings.bind(tag.ID, existingID)
				continue
			}
			leaf = uniquePath(leaf, under, taken)
			path = joinPath(under, leaf)
		}
		// The signal's own identity: never composed from what it is bound to.
		// Every Metric carries signal_id, so rebinding this signal to a
		// different tag later must leave it — and the whole metric history
		// under it — untouched (design §6).
		id := c.newID()
		// The raw name is NOT copied onto the signal. The binding already
		// reaches it: `data_tag` names the catalogue entry, and that entry
		// carries `name`. A second copy here would be a second owner of the
		// same fact, drifting the moment a connector renames a tag — and it
		// travelled as a `metadata` key, which is keyed by metadata-type
		// identity, so it also asked every consumer to resolve a definition
		// nothing ships.
		signal := map[string]any{
			"id":           id,
			"name":         leaf,
			"data_tag":     tag.ID,
			"is_published": true,
		}
		// The binding to the tree. Absent only for an unplaced connector,
		// whose signals are bound to the node itself the way it is.
		if element != "" {
			signal["system_element_id"] = element
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
	if unplaced > 0 {
		return 200, fmt.Sprintf(`{"created":%d,"skipped":%d,"unplaced":%d}`, len(records), skipped, unplaced), "ok", writes
	}
	return 200, fmt.Sprintf(`{"created":%d,"skipped":%d}`, len(records), skipped), "ok", writes
}

// unboundSignalAt reads the signal record at a local path, if one is there
// and holds no tag yet. It hands the record back as the map it was written
// as, so binding it rewrites exactly the fields the declaration authored plus
// the binding — nothing this executor knows about a signal is re-stated.
func (c *ConfigExec) unboundSignalAt(path string) (map[string]any, string, bool) {
	raw, ok := c.store.KVGet(c.signalTopic(path))
	if !ok {
		return nil, "", false
	}
	var record map[string]any
	if json.Unmarshal(raw, &record) != nil {
		return nil, "", false
	}
	if tag, _ := record["data_tag"].(string); tag != "" {
		return nil, "", false
	}
	id, _ := record["id"].(string)
	if id == "" {
		return nil, "", false
	}
	return record, id, true
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
	if !taken[joinPath(under, leaf)] {
		return leaf
	}
	for n := 2; ; n++ {
		candidate := fmt.Sprintf("%s-%d", leaf, n)
		if !taken[joinPath(under, candidate)] {
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
		if held := c.resourcesBelow(p.path); len(held) > 0 {
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

// resourcesBelow lists the resources standing on this position or under it.
// A resource pins its blob alive (§8 marks from live _Resource records), so an
// element deleted out from under one would leave a file retained forever with
// no element in any tree to reach it from.
//
// Deliberately narrower than a general occupancy rule: signals and constants
// do NOT block a delete today, and making them do so is a change to existing
// behaviour that belongs in its own decision, not in this one.
func (c *ConfigExec) resourcesBelow(path string) []string {
	var out []string
	for _, rec := range c.store.KVScan("_Resource", c.store.NodeID()) {
		if rec.Path == path || strings.HasPrefix(rec.Path, path+"/") {
			out = append(out, rec.Path)
		}
	}
	sort.Strings(out)
	return out
}
