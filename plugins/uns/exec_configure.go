package uns

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// ConfigExec executes _CmdConfigure: edits to the node's data model. People,
// model files and autobind all create bindings through the same command, so
// they produce the same records.
type ConfigExec struct {
	store EntityStore
	// mu serializes commands and the lifecycle trigger, so each read-decide-commit
	// sequence runs whole.
	mu sync.Mutex
	// bound says which identities bind to an element and who an identity is,
	// which autobind needs to compute a connector's catalogue topic.
	bound Bindings
	// elements resolves an element to this node's local path; catalogue topics
	// are built from paths.
	elements Namespace
	// blobs is this node's file store. resource/upsert never authors a record
	// that points at bytes the node does not have.
	blobs Blobs
	// newID mints ULIDs for new signals. plugins/uns is stdlib-only, so the core
	// supplies it; tests can inject deterministic ids.
	newID func() string
	// autobindNew binds a connector's catalogue as soon as the node first sees it
	// (setting "autobind" = "on_new_connector").
	autobindNew bool
	// observed holds, per catalogue topic, the tag ids of the last publish this
	// process saw, so a republish can tell a new tag from one an operator deleted.
	observed map[string]map[string]bool
}

// NewConfigExec builds the executor. Unknown settings keys are ignored, so a
// typo disables a feature instead of stopping the node.
func NewConfigExec(s EntityStore, bound Bindings, elements Namespace, blobs Blobs, newID func() string, settings map[string]string) *ConfigExec {
	return &ConfigExec{
		store: s, bound: bound, elements: elements, blobs: blobs, newID: newID,
		autobindNew: settings["autobind"] == "on_new_connector",
		observed:    map[string]map[string]bool{},
	}
}

// Handles reports whether this executor runs the given contract.
func (c *ConfigExec) Handles(contract string) bool { return contract == "_CmdConfigure" }

// Observe runs the lifecycle trigger for a record the node just persisted: a
// connector catalogue with nothing bound yet gets bound, exactly as
// signal/autobind would. A record only counts as a catalogue if its topic is
// the one an enrolled entry computes to, since anyone with read scope can copy
// tag ids. Autobind is idempotent, so the trigger needs no coordination.
func (c *ConfigExec) Observe(contract, topic string, payload []byte) {
	if !c.autobindNew || contract != "_DataTags" || len(payload) == 0 {
		return // not the trigger's contract, or the catalogue was retired
	}
	if _, err := Parse(topic); err != nil {
		return
	}
	// The trigger writes state like the verb does, so it takes the same lock.
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

	// A catalogue can grow after its first publish (an OPC UA connector announces
	// its heartbeat tags before browsing the server). New tags are bound, old ones
	// left alone, so a republish never revives a binding an operator deleted. With
	// no earlier publish in this process, bind only if nothing is bound yet; an
	// explicit signal/autobind covers a catalogue that grew across a restart.
	bindings := c.bindings()
	if !seen {
		for _, tag := range cat.DataTags {
			if bindings.propose(tag.ID, "") != bindFree {
				return // already bound: a republish, not a new connector
			}
		}
		c.bindCatalogue(CommandContext{}, mount, element, payload)
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
	c.bindCatalogue(CommandContext{}, mount, element, encoded)
}

// owningEntry finds the enrolled identity whose computed catalogue topic is
// topic, and returns the element and mount it is bound to. It compares the
// full topic, node id included, so records from another node never match.
func (c *ConfigExec) owningEntry(topic string) (element, mount string, ok bool) {
	for _, e := range c.bound.Entries() {
		m, ok := c.mountFor(e.Element)
		if !ok {
			continue // cannot place this entry here: it owns nothing
		}
		if Prefix()+"_DataTags/"+c.store.NodeID()+"/"+joinPath(m, e.Name) == topic {
			return e.Element, m, true
		}
	}
	return "", "", false
}

// mountFor resolves an identity's element to this node's local path. It keeps
// "unplaced" (element == "") apart from "not resolvable here", so a failed
// lookup never widens where the identity counts as bound.
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
// that grants and bindings name it by, and the parent it declares.
//
// The parent is not the same question as the position. A record's path says
// where the element sits, and for most elements the path of its parent is a
// prefix of its own — but not for a ROOT's direct children: the publisher
// leaves the root out of the path it writes, so an element whose parent is a
// root is stored at its own segment alone. `parent_id` is then the only place
// that relationship survives, and a delete that looked only at paths could
// not see it.
type placedElement struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	ParentID string `json:"parent_id"`
}

// definitionRef is one definition to write. It has no path on purpose: a
// definition has no position, its id is its address.
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

// entityRef is a positionless inventory entity; the node derives its path.
// Elements and signals have their own verbs because placement is part of the
// command.
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
		// ID is the tag's ULID, minted by the connector at discovery and stable
		// across rediscovery. Signal.data_tag points here.
		ID       string `json:"id"`
		Name     string `json:"name"`
		DataType string `json:"data_type"`
		// Meta.Element, when set, is the node-local path of the element this tag's
		// signal belongs under instead of the connector's mount, so one service can
		// publish for several machines. Missing elements on that path are created.
		Meta struct {
			Element string `json:"element"`
			// Unit becomes the new signal's unit, or fills in a declared signal that
			// has none. It never overwrites a declared unit.
			Unit string `json:"unit"`
		} `json:"meta"`
	} `json:"data_tags"`
}

// boundSignal is the part of a _Signal record that identifies its binding.
// The connector is reached through the tag, never stored.
type boundSignal struct {
	ID      string `json:"id"`
	Name    string `json:"name"`
	DataTag string `json:"data_tag"`
	// Element is the system element the signal is bound to; empty when bound
	// to the node itself.
	Element string `json:"system_element_id"`
}

// Execute runs one _CmdConfigure command and returns its status, message and result.
func (c *ConfigExec) Execute(ctx CommandContext, contract, verb string, payload []byte) (int, string, string) {
	code, message, result, _ := c.ExecuteWithWrites(ctx, contract, verb, payload)
	return code, message, result
}

// ExecuteWithWrites is the batch variant the engine uses. Each verb commits
// its records in one PublishBatch, and the returned writes (stream, offset,
// topic) go into the command's ack.
func (c *ConfigExec) ExecuteWithWrites(
	ctx CommandContext,
	contract, verb string,
	payload []byte,
) (int, string, string, []StateWrite) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.execute(ctx, contract, verb, payload)
}

func (c *ConfigExec) execute(ctx CommandContext, contract, verb string, payload []byte) (int, string, string, []StateWrite) {
	switch verb {
	case "signal/upsert":
		return c.upsert(ctx, payload)
	case "signal/delete":
		return c.delete(ctx, payload)
	case "signal/autobind":
		return c.autobind(ctx, payload)
	case "constant/upsert":
		return c.constantUpsert(ctx, payload)
	case "constant/delete":
		return c.constantDelete(ctx, payload)
	case "element/upsert":
		return c.elementUpsert(ctx, payload)
	case "element/author":
		return c.elementAuthor(ctx, payload)
	case "element/delete":
		return c.elementDelete(ctx, payload)
	case "entity/upsert":
		return c.entityUpsert(ctx, payload)
	case "entity/delete":
		return c.entityDelete(ctx, payload)
	case "definition/upsert":
		return c.definitionUpsert(ctx, payload)
	case "definition/delete":
		return c.definitionDelete(ctx, payload)
	case "resource/upsert":
		return c.resourceUpsert(ctx, payload)
	case "resource/delete":
		return c.resourceDelete(ctx, payload)
	default:
		return 422, fmt.Sprintf("unknown configure verb %q", verb), "invalid", nil
	}
}

// commit writes a verb's record set as one transition: every record is stored
// or none is, so a refused record never leaves the node half configured. An
// empty set is a successful no-op.
func (c *ConfigExec) commit(ctx CommandContext, records []StateRecord) ([]StateWrite, error) {
	if len(records) == 0 {
		return nil, nil
	}
	return c.store.PublishBatch(ctx, records)
}

// positionsByID maps each id of a contract to the path holding it at this
// node, and each path back to the id standing on it. The upsert verbs keep one
// entity per path; this keeps one path per id. An id at two paths breaks
// snapshot() and every _CmdEdit at the node, and a later tombstone would drop
// the grants and bindings of the survivor. Only this node's own records count;
// a child's records are not ours to claim. noun names the thing in a refusal.
func (c *ConfigExec) positionsByID(contract, noun string) *idClaims {
	claims := &idClaims{
		noun: noun, at: map[string]string{}, by: map[string]string{}, placed: map[string]bool{},
	}
	for _, rec := range c.store.KVScan(contract, c.store.NodeID()) {
		var held identified
		if json.Unmarshal(rec.Payload, &held) == nil && held.ID != "" {
			claims.at[held.ID] = rec.Path
			claims.by[rec.Path] = held.ID
		}
	}
	return claims
}

// idClaims tracks where each id sits and who sits at each path, growing as a
// command claims positions. placed remembers the ids this command has already
// put somewhere, which the store cannot yet show.
type idClaims struct {
	noun   string
	at     map[string]string // id → path
	by     map[string]string // path → id
	placed map[string]bool
}

// claim takes path for id. An identity the node already holds elsewhere MOVES:
// claim answers with the path it leaves, which the caller must retire in the
// same batch. Refusing the move instead — which this did until a live
// deployment ran into it — leaves a declarative apply no way to rename or
// reparent anything, because either changes the path and _CmdConfigure has no
// move verb. Relocating upholds the same invariant the refusal did: one
// identity, one position.
//
// Two answers are still refusals, returned as the sentence to report:
// a second entry of the SAME command claiming an id an earlier entry already
// placed (no order of writes satisfies it), and a move onto a path another
// identity holds (the move would retire the mover's only record and overwrite
// the sitting one, losing an identity outright).
//
// An empty id claims nothing.
func (claims *idClaims) claim(id, path string) (vacated, refusal string) {
	if id == "" {
		return "", ""
	}
	at, known := claims.at[id]
	if !known || at == path {
		claims.take(id, path)
		return "", ""
	}
	if claims.placed[id] {
		return "", fmt.Sprintf("one command puts %s %s at %s and at %s — one identity "+
			"cannot sit at two positions", claims.noun, id, at, path)
	}
	if held, taken := claims.by[path]; taken && held != id {
		return "", fmt.Sprintf("%s is already %s %s — two %ss cannot share one position",
			path, claims.noun, held, claims.noun)
	}
	delete(claims.by, at)
	claims.take(id, path)
	return at, ""
}

func (claims *idClaims) take(id, path string) {
	claims.at[id] = path
	claims.by[path] = id
	claims.placed[id] = true
}

// retire composes the tombstones for the positions relocated identities left
// behind, skipping any position another entry of the same command filled: the
// mover is gone from there either way, and a tombstone would erase that
// entry's write depending on where the batch put it.
func retire(vacated []string, written map[string]bool) []StateRecord {
	records := make([]StateRecord, 0, len(vacated))
	for _, topic := range vacated {
		if written[topic] {
			continue
		}
		records = append(records, StateRecord{Topic: topic})
	}
	return records
}

func (c *ConfigExec) constantUpsert(ctx CommandContext, payload []byte) (int, string, string, []StateWrite) {
	var body constantUpsertBody
	if err := json.Unmarshal(payload, &body); err != nil {
		return 422, "constant/upsert: unreadable payload: " + err.Error(), "invalid", nil
	}
	if len(body.Constants) == 0 {
		return 422, "constant/upsert: no constants given", "invalid", nil
	}

	records := make([]StateRecord, 0, len(body.Constants))
	seen := make(map[string]bool, len(body.Constants))
	var vacated []string
	claims := c.positionsByID("_Constant", "constant")
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
		left, refusal := claims.claim(incoming.ID, ref.Path)
		if refusal != "" {
			return 409, "constant/upsert: " + refusal, "conflict", nil
		}
		if left != "" {
			vacated = append(vacated, c.constantTopic(left))
		}
		records = append(records, StateRecord{Topic: topic, Payload: ref.Constant})
	}

	upserted := len(records)
	records = append(records, retire(vacated, seen)...)
	writes, err := c.commit(ctx, records)
	if err != nil {
		return 422, "constant/upsert: rejected: " + err.Error(), "invalid", nil
	}
	return 200, fmt.Sprintf("upserted %d", upserted), "ok", writes
}

func (c *ConfigExec) constantDelete(ctx CommandContext, payload []byte) (int, string, string, []StateWrite) {
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
	writes, err := c.commit(ctx, records)
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
	return Prefix() + "_Resource/" + c.store.NodeID() + "/" + path
}

func (c *ConfigExec) resourceUpsert(ctx CommandContext, payload []byte) (int, string, string, []StateWrite) {
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
		// Never author a record pointing at bytes we do not hold; try one pull
		// from the parent first. A failed pull is reported as blob_unreachable,
		// not as an invalid command: the command was fine, the bytes just need
		// staging where this node can reach them.
		if err := c.ensureBlob(incoming.SHA256); err != nil {
			return 422, fmt.Sprintf("blob_unreachable: resource/upsert entry %d: blob %s is not held "+
				"by this node and could not be fetched: %v", i, incoming.SHA256, err), "blob_unreachable", nil
		}
		records = append(records, StateRecord{Topic: topic, Payload: ref.Resource})
	}

	writes, err := c.commit(ctx, records)
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

func (c *ConfigExec) resourceDelete(ctx CommandContext, payload []byte) (int, string, string, []StateWrite) {
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
	// The blob stays; the sweep removes it once nothing references it, which
	// keeps content shared by two resources safe.
	writes, err := c.commit(ctx, records)
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

func (c *ConfigExec) entityUpsert(ctx CommandContext, payload []byte) (int, string, string, []StateWrite) {
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
	writes, err := c.commit(ctx, records)
	if err != nil {
		return 422, "entity/upsert: rejected: " + err.Error(), "invalid", nil
	}
	return 200, fmt.Sprintf("upserted %d", len(records)), "ok", writes
}

// keepOwnPosition keeps the position this node learned from its parent when
// an upsert of its own _Node record leaves root_system_element_id empty, as a
// re-applied bootstrap does. A position the writer states still wins.
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

func (c *ConfigExec) entityDelete(ctx CommandContext, payload []byte) (int, string, string, []StateWrite) {
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
		// A repeated path is refused: the set is decided before anything is
		// written.
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
	writes, err := c.commit(ctx, records)
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
		return fmt.Errorf("_AlarmNotificationConfig is unreadable: %w", err)
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
		"_Node":                    "nodes",
		"_ExternalReference":       "external-references",
		"_AlarmNotificationConfig": "alarm-notification-config",
	}[contract]
	return Prefix() + contract + "/" + c.store.NodeID() + "/_colca/" + leaf + "/" + id
}

func (c *ConfigExec) upsert(ctx CommandContext, payload []byte) (int, string, string, []StateWrite) {
	var body upsertBody
	if err := json.Unmarshal(payload, &body); err != nil {
		return 422, "signal/upsert: unreadable payload: " + err.Error(), "invalid", nil
	}
	if len(body.Signals) == 0 {
		return 422, "signal/upsert: no signals given", "invalid", nil
	}
	records := make([]StateRecord, 0, len(body.Signals))
	written := make(map[string]bool, len(body.Signals))
	var vacated []string
	claims := c.positionsByID("_Signal", "signal")
	for i, ref := range body.Signals {
		if ref.Path == "" {
			return 422, fmt.Sprintf("signal/upsert: entry %d has no path", i), "invalid", nil
		}
		if len(ref.Signal) == 0 {
			return 422, fmt.Sprintf("signal/upsert: entry %d has no signal", i), "invalid", nil
		}
		var incoming identified
		if err := json.Unmarshal(ref.Signal, &incoming); err != nil {
			return 422, fmt.Sprintf("signal/upsert: entry %d unreadable: %v", i, err), "invalid", nil
		}
		left, refusal := claims.claim(incoming.ID, ref.Path)
		if refusal != "" {
			return 409, "signal/upsert: " + refusal, "conflict", nil
		}
		// A moving signal carries its binding with it: the record to preserve
		// from is the one it is leaving, not the empty position it arrives at.
		// Reading the destination would unbind every declared signal a rename
		// moves, since a declaration carries data_tag: null.
		from := ref.Path
		if left != "" {
			from = left
			vacated = append(vacated, c.signalTopic(left))
		}
		payload, err := c.preserveBinding(from, ref.Signal)
		if err != nil {
			return 422, fmt.Sprintf("signal/upsert: entry %d unreadable: %v", i, err), "invalid", nil
		}
		topic := c.signalTopic(ref.Path)
		written[topic] = true
		records = append(records, StateRecord{Topic: topic, Payload: payload})
	}
	upserted := len(records)
	records = append(records, retire(vacated, written)...)
	writes, err := c.commit(ctx, records)
	if err != nil {
		// Nothing was written; the error names the refused record.
		return 422, "signal/upsert: rejected: " + err.Error(), "invalid", nil
	}
	return 200, fmt.Sprintf("upserted %d", upserted), "ok", writes
}

// preserveBinding keeps the stored binding, learned data type and replication
// policy when an upsert does not set them. Declarations
// carry data_tag: null, and replacing the record whole would unbind every
// declared signal on each reconcile. A non-empty data_tag or an explicit
// is_published or data_type still wins.
func (c *ConfigExec) preserveBinding(path string, incoming json.RawMessage) (json.RawMessage, error) {
	existing, ok := c.store.KVGet(c.signalTopic(path))
	if !ok {
		return incoming, nil
	}
	var stored, next map[string]any
	if err := json.Unmarshal(existing, &stored); err != nil {
		return incoming, nil //nolint:nilerr // an unreadable stored record cannot constrain the new one
	}
	if err := json.Unmarshal(incoming, &next); err != nil {
		return nil, err
	}
	if tag, _ := next["data_tag"].(string); tag == "" {
		if storedTag, _ := stored["data_tag"].(string); storedTag != "" {
			next["data_tag"] = storedTag
		} else {
			delete(next, "data_tag") // never persist an explicit null
		}
	}
	for _, field := range []string{"is_published", "data_type", "replication_policy"} {
		if _, spoken := next[field]; !spoken {
			if value, has := stored[field]; has {
				next[field] = value
			}
		}
	}
	return json.Marshal(next)
}

func (c *ConfigExec) delete(ctx CommandContext, payload []byte) (int, string, string, []StateWrite) {
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
	writes, err := c.commit(ctx, records)
	if err != nil {
		return 500, "signal/delete: failed: " + err.Error(), "error", nil
	}
	return 200, fmt.Sprintf("deleted %d", len(records)), "ok", writes
}

// autobind creates one signal per unbound tag of a connector's catalogue. It
// is idempotent: existing bindings are skipped, never overwritten, so a person,
// a replay or the lifecycle trigger can all run it.
func (c *ConfigExec) autobind(ctx CommandContext, payload []byte) (int, string, string, []StateWrite) {
	var body autobindBody
	if err := json.Unmarshal(payload, &body); err != nil {
		return 422, "signal/autobind: unreadable payload: " + err.Error(), "invalid", nil
	}
	if body.Connector == "" {
		return 422, "signal/autobind: no connector given", "invalid", nil
	}

	name, element, ok := c.bound.EntryOf(body.Connector)
	if !ok {
		// Only the node that holds the enrollment binds; the command travels
		// down to it.
		return 404, "signal/autobind: " + body.Connector + " is not enrolled at this node", "invalid", nil
	}
	mount, ok := c.mountFor(element)
	if !ok {
		// Fail closed: treating the connector as unplaced would compute the
		// wrong catalogue topic.
		return 409, "signal/autobind: " + name + " is bound to an element this node cannot resolve", "conflict", nil
	}
	catTopic := Prefix() + "_DataTags/" + c.store.NodeID() + "/" + joinPath(mount, name)
	raw, found := c.store.KVGet(catTopic)
	if !found {
		// No catalogue yet. A retry after the connector publishes works, so this
		// is a conflict, not a bad request.
		return 409, "signal/autobind: " + name + " has published no catalogue", "conflict", nil
	}

	// A signal sits directly under the element it binds to; by default that is
	// the connector's mount. The connector name is not a path segment: a
	// connector is a participant, not a position.
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
	return c.bindCatalogue(ctx, under, at, raw)
}

// elementAt returns the element this node holds at a local path, if any.
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

// authorElementAt resolves a local path to its element, creating any missing
// elements along it; it is the only code that walks the tree. bindCatalogue
// calls it directly and registry.Manager.Register reaches it via
// "element/author". It calls elementUpsert directly because c.mu is already
// held and not reentrant. Each segment commits so the next one can see it.
func (c *ConfigExec) authorElementAt(ctx CommandContext, path string) (string, error) {
	var local, leaf string
	for _, seg := range strings.Split(path, "/") {
		if seg == "" {
			continue
		}
		if local == "" {
			local = seg
		} else {
			local += "/" + seg
		}
		id, ok := c.elementAt(local)
		if !ok {
			// Minted, not derived from the path, so a rename keeps the id.
			id = c.newID()
			elem, err := json.Marshal(placedElement{ID: id, Name: seg})
			if err != nil {
				return "", fmt.Errorf("author element at %s: %w", local, err)
			}
			payload, err := json.Marshal(elementUpsertBody{Elements: []elementRef{{Path: local, Element: elem}}})
			if err != nil {
				return "", fmt.Errorf("author element at %s: %w", local, err)
			}
			code, msg, _, _ := c.elementUpsert(ctx, payload)
			if code != 200 {
				return "", fmt.Errorf("author element at %s: %s", local, msg)
			}
		}
		leaf = id
	}
	return leaf, nil
}

// elementAuthorBody names the one path element/author resolves or authors.
type elementAuthorBody struct {
	Path string `json:"path"`
}

// elementAuthor exposes authorElementAt as a _CmdConfigure verb for
// registry.Manager.Register. The message carries the leaf element's id.
func (c *ConfigExec) elementAuthor(ctx CommandContext, payload []byte) (int, string, string, []StateWrite) {
	var body elementAuthorBody
	if err := json.Unmarshal(payload, &body); err != nil {
		return 422, "element/author: unreadable payload: " + err.Error(), "invalid", nil
	}
	if body.Path == "" {
		return 422, "element/author: no path given", "invalid", nil
	}
	id, err := c.authorElementAt(ctx, body.Path)
	if err != nil {
		return 500, "element/author: " + err.Error(), "error", nil
	}
	return 200, id, "ok", nil
}

// bindCatalogue creates one signal per unbound tag in a catalogue, under one
// path and bound to the element there. signal/autobind and the lifecycle
// trigger both use it, and it is idempotent. Bound tags and taken paths are
// tracked while the set is composed. A path that holds an unbound declared
// signal gets that signal bound instead of a new "<name>-2" beside it.
func (c *ConfigExec) bindCatalogue(ctx CommandContext, under, element string, raw []byte) (int, string, string, []StateWrite) {
	var cat catalogue
	if err := json.Unmarshal(raw, &cat); err != nil {
		return 422, "signal/autobind: unreadable catalogue: " + err.Error(), "invalid", nil
	}

	bindings := c.bindings()
	taken := c.takenPaths()

	records := make([]StateRecord, 0, len(cat.DataTags))
	skipped := 0
	for _, tag := range cat.DataTags {
		// Skip tags that are already bound; the edit verbs refuse instead (see
		// signalBindings). The empty signal id stands for a signal not created yet.
		if bindings.propose(tag.ID, "") != bindFree {
			skipped++
			continue
		}
		under, element := under, element
		if tag.Meta.Element != "" {
			// The tag names its own element: reuse it, or create it along the path.
			// The lock is already held, so call authorElementAt directly.
			id, ok := c.elementAt(tag.Meta.Element)
			if !ok {
				authored, err := c.authorElementAt(ctx, tag.Meta.Element)
				if err != nil {
					return 500, "signal/autobind: " + err.Error(), "error", nil
				}
				id = authored
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
				if _, hasUnit := existing["unit"]; !hasUnit && tag.Meta.Unit != "" {
					existing["unit"] = tag.Meta.Unit
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
		// The signal id is never derived from the binding: metrics carry
		// signal_id, so rebinding must not touch the signal or its history.
		id := c.newID()
		// The raw tag name is not copied onto the signal; data_tag already reaches
		// it, and a copy would drift when a connector renames the tag.
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
		if tag.Meta.Unit != "" {
			signal["unit"] = tag.Meta.Unit
		}
		encoded, err := json.Marshal(signal)
		if err != nil {
			return 500, "signal/autobind: encode failed: " + err.Error(), "error", nil
		}
		records = append(records, StateRecord{Topic: c.signalTopic(path), Payload: encoded})
		bindings.bind(tag.ID, id)
		taken[path] = true
	}
	writes, err := c.commit(ctx, records)
	if err != nil {
		return 422, "signal/autobind: rejected: " + err.Error(), "invalid", nil
	}
	return 200, fmt.Sprintf(`{"created":%d,"skipped":%d}`, len(records), skipped), "ok", writes
}

// unboundSignalAt returns the signal record at a local path if it exists and
// has no tag yet, as the map it was written as, so binding it keeps the
// declared fields.
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

// joinPath joins a mount and a leaf. An unplaced identity's mount is "", and
// the result never has a leading or doubled slash.
func joinPath(mount, leaf string) string {
	if mount == "" {
		return leaf
	}
	return mount + "/" + leaf
}

func (c *ConfigExec) signalTopic(path string) string {
	return Prefix() + "_Signal/" + c.store.NodeID() + "/" + path
}

func (c *ConfigExec) constantTopic(path string) string {
	return Prefix() + "_Constant/" + c.store.NodeID() + "/" + path
}

// bindings returns the tag-to-signal bindings this node holds. Tag ids are
// ULIDs, so they need no connector scope. A bound signal without an id of its
// own is keyed by its path, so it never reads as unbound.
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

// sanitize turns a tag name into one topic segment: no MQTT separators or
// wildcards, and never empty.
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

// uniquePath adds a suffix until the path is free, so two tags that sanitize
// alike both keep a place.
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

// elementUpsert writes elements at their positions. The path is the position,
// so two elements cannot share one; the owning node checks this because only
// it authors the parent. An element named at a position other than the one it
// holds moves there — a rename or a reparent is an upsert, not a delete and a
// recreate, which would lose everything standing on the element.
func (c *ConfigExec) elementUpsert(ctx CommandContext, payload []byte) (int, string, string, []StateWrite) {
	var body elementUpsertBody
	if err := json.Unmarshal(payload, &body); err != nil {
		return 422, "element/upsert: unreadable payload: " + err.Error(), "invalid", nil
	}
	if len(body.Elements) == 0 {
		return 422, "element/upsert: no elements given", "invalid", nil
	}
	records := make([]StateRecord, 0, len(body.Elements))
	// claimed guards positions taken within this command; nothing is written
	// until the whole set is decided, so the store cannot show them yet.
	claimed := make(map[string]string, len(body.Elements))
	// The reverse check: claims says where an id already sits, and moves it
	// here when that is somewhere else.
	var vacated []string
	claims := c.positionsByID("_SystemElement", "element")
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
		// The id becomes a grant zone once grantsync sees it; "#" would turn a
		// grant on this element into "read:#", the whole tree.
		if err := ValidElementID(incoming.ID); err != nil {
			return 422, fmt.Sprintf("element/upsert: entry %d: %v", i, err), "invalid", nil
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
		left, refusal := claims.claim(incoming.ID, ref.Path)
		if refusal != "" {
			return 409, "element/upsert: " + refusal, "conflict", nil
		}
		if left != "" {
			vacated = append(vacated, c.elementTopic(left))
		}
		claimed[topic] = incoming.ID
		records = append(records, StateRecord{Topic: topic, Payload: ref.Element})
	}
	upserted := len(records)
	written := make(map[string]bool, len(claimed))
	for topic := range claimed {
		written[topic] = true
	}
	records = append(records, retire(vacated, written)...)
	writes, err := c.commit(ctx, records)
	if err != nil {
		return 422, "element/upsert: rejected: " + err.Error(), "invalid", nil
	}
	return 200, fmt.Sprintf("upserted %d", upserted), "ok", writes
}

// elementDelete retires positions, but not while child elements or bound
// identities still stand on them. The refusal names what is in the way.
func (c *ConfigExec) elementDelete(ctx CommandContext, payload []byte) (int, string, string, []StateWrite) {
	var body deleteBody
	if err := json.Unmarshal(payload, &body); err != nil {
		return 422, "element/delete: unreadable payload: " + err.Error(), "invalid", nil
	}
	if len(body.Paths) == 0 {
		return 422, "element/delete: no paths given", "invalid", nil
	}

	// Two passes: a command can retire a parent and its children together, so
	// the occupancy check has to see the whole set first.
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
	// A path that does not exist makes the whole command 404, before any
	// occupancy conflict: the set retires together, and the occupancy pass
	// only means something once every path resolved. The order of the list
	// does not change the answer.
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
	writes, err := c.commit(ctx, records)
	if err != nil {
		return 500, "element/delete: failed: " + err.Error(), "error", nil
	}
	return 200, fmt.Sprintf("deleted %d", len(records)), "ok", writes
}

// boundIdentities lists the identities bound to the element in raw; the rule
// itself is occupantsOf.
func (c *ConfigExec) boundIdentities(raw []byte) []string {
	var held placedElement
	if json.Unmarshal(raw, &held) != nil {
		return nil
	}
	return occupantsOf(c.bound, held.ID)
}

// definitionUpsert writes definitions under this node's identity. Definitions
// descend to every node below; their id is their address, they have no
// position.
func (c *ConfigExec) definitionUpsert(ctx CommandContext, payload []byte) (int, string, string, []StateWrite) {
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
	writes, err := c.commit(ctx, records)
	if err != nil {
		return 422, "definition/upsert: rejected: " + err.Error(), "invalid", nil
	}
	return 200, fmt.Sprintf("upserted %d", len(records)), "ok", writes
}

// definitionDelete retracts definitions with a tombstone, which propagates down
// the same way the definition itself did.
func (c *ConfigExec) definitionDelete(ctx CommandContext, payload []byte) (int, string, string, []StateWrite) {
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
	writes, err := c.commit(ctx, records)
	if err != nil {
		return 500, "definition/delete: failed: " + err.Error(), "error", nil
	}
	return 200, fmt.Sprintf("deleted %d", len(records)), "ok", writes
}

// checkDefinitionContract refuses contracts that are not definitions; code 0
// means the contract is fine. It checks the class, not a list of names.
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

// checkDefinitionContents validates what a definition carries beyond the
// bundle's shape check. A malformed grant in a group is refused here, once,
// instead of being dropped and logged at every node below.
func checkDefinitionContents(contract string, raw []byte) error {
	if contract == PersonalAccessTokenContract {
		var token PersonalAccessToken
		if err := json.Unmarshal(raw, &token); err != nil {
			return fmt.Errorf("unreadable personal access token: %w", err)
		}
		if len(token.HashedSecret) != 64 {
			return fmt.Errorf("hashed_secret must be a SHA-256 hex digest")
		}
		if _, err := hex.DecodeString(token.HashedSecret); err != nil {
			return fmt.Errorf("hashed_secret must be a SHA-256 hex digest")
		}
		if token.OwnerSub == "" {
			return fmt.Errorf("owner_sub is required")
		}
		allowedScopes := map[string]bool{ScopeAPI: true, ScopeI3X: true, ScopeMCP: true, ScopeBrokerHTTP: true, ScopeBrokerMQTT: true}
		if len(token.Scopes) == 0 {
			return fmt.Errorf("scopes must not be empty")
		}
		for _, scope := range token.Scopes {
			if !allowedScopes[scope] {
				return fmt.Errorf("unknown scope %q", scope)
			}
		}
		if token.ExpiresAt != "" {
			if _, err := time.Parse(time.RFC3339, token.ExpiresAt); err != nil {
				return fmt.Errorf("expires_at must be RFC3339")
			}
		}
		for _, grant := range token.Grants {
			if _, err := ParseGrant(grant); err != nil {
				return err
			}
		}
		return nil
	}
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
	return Prefix() + contract + "/" + c.store.NodeID() + "/" + id
}

func (c *ConfigExec) elementTopic(path string) string {
	return Prefix() + "_SystemElement/" + c.store.NodeID() + "/" + path
}

// occupantsBelow lists the element paths under path, skipping those the same
// command retires.
// occupantsBelow lists the elements that would be orphaned by retiring the
// element at path: the ones stored under it, AND the ones that name it as
// their parent wherever they are stored.
//
// The second half is not belt-and-braces. A root's direct children are stored
// at their own segment alone — the publisher leaves the root out of the path —
// so by path a root has no children at all, and a delete judged on paths
// retired it happily. Seen on a live deployment: `prekit dm deploy --prune`
// removed a root, this check found nothing in the way, and the one tombstone
// went out. The projection, which knows the parent relationship because the
// payload carries it, then cascaded: four elements the node still held
// vanished from the read model, and the two disagreed permanently. The node
// is the authority, so the node is where this has to be seen.
func (c *ConfigExec) occupantsBelow(path string, retiring map[string]bool) []string {
	var target placedElement
	if raw, ok := c.store.KVGet(c.elementTopic(path)); ok {
		_ = json.Unmarshal(raw, &target)
	}
	seen := make(map[string]bool)
	var out []string
	for _, rec := range c.store.KVScan("_SystemElement", c.store.NodeID()) {
		if retiring[rec.Path] || seen[rec.Path] {
			continue
		}
		child := strings.HasPrefix(rec.Path, path+"/")
		if !child && target.ID != "" {
			var held placedElement
			if json.Unmarshal(rec.Payload, &held) == nil && held.ParentID == target.ID {
				child = true
			}
		}
		if child {
			seen[rec.Path] = true
			out = append(out, rec.Path)
		}
	}
	sort.Strings(out)
	return out
}

// resourcesBelow lists the resources on or under a position. A resource keeps
// its blob alive, so deleting its element would orphan the file. Signals and
// constants do not block a delete.
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
