package uns

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode"
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
	// observed holds, per catalogue topic, the tags of the last publish this
	// process saw and what each said, so a republish can tell a new tag from one
	// an operator deleted, and a changed unit from an unchanged one.
	observed map[string]map[string]tagMeta
}

// NewConfigExec builds the executor. Unknown settings keys are ignored, so a
// typo disables a feature instead of stopping the node.
func NewConfigExec(s EntityStore, bound Bindings, elements Namespace, blobs Blobs, newID func() string, settings map[string]string) *ConfigExec {
	return &ConfigExec{
		store: s, bound: bound, elements: elements, blobs: blobs, newID: newID,
		autobindNew: settings["autobind"] == "on_new_connector",
		observed:    map[string]map[string]tagMeta{},
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
	entry, mount, ok := c.owningEntry(topic)
	if !ok {
		return // no enrolled entry's computed catalogue topic matches: ignore it
	}
	element := entry.Element
	var cat catalogue
	if err := json.Unmarshal(payload, &cat); err != nil {
		return
	}
	metas := make(map[string]tagMeta, len(cat.DataTags))
	for _, tag := range cat.DataTags {
		metas[tag.ID] = tag.Meta.tagMeta
	}
	previous, seen := c.observed[topic]
	c.observed[topic] = metas

	// What the tags say reaches the signals already bound to them: missing
	// fields are filled on every publish, and a field this process saw change
	// is updated. Runs after any binding below, whose own signals already
	// carry the tags' fields. A refused write is left for the next publish,
	// as a refused binding is.
	changed := changedMeta(previous, metas)
	defer func() { _, _, _ = c.syncCatalogueMeta(CommandContext{}, cat, changed) }()

	// A catalogue can grow after its first publish (an OPC UA connector announces
	// its heartbeat tags before browsing the server). New tags are bound, old ones
	// left alone, so a republish never revives a binding an operator deleted. With
	// no earlier publish in this process, bind only if nothing is bound yet; an
	// explicit signal/autobind covers a catalogue that grew across a restart.
	//
	// A declared signal's bind_intent is honoured on every publish, for every
	// free tag: it is an explicit request, and unbinding a signal drops it, so
	// it never revives a binding either.
	bindings := c.bindings()
	mint := map[string]bool{}
	if !seen {
		fresh := true
		for _, tag := range cat.DataTags {
			if bindings.propose(tag.ID, "") != bindFree {
				fresh = false // already bound: a republish, not a new connector
				break
			}
		}
		if fresh {
			mint = nil // every tag
		}
	} else {
		for _, tag := range cat.DataTags {
			if _, known := previous[tag.ID]; !known {
				mint[tag.ID] = true
			}
		}
	}
	c.bindCatalogue(CommandContext{}, mount, element, entry, cat, mint)
}

// changedMeta returns the tags whose stated fields differ between two
// publishes of one catalogue. A tag new in the later publish is not a change.
func changedMeta(previous, current map[string]tagMeta) map[string]bool {
	changed := map[string]bool{}
	for id, now := range current {
		if before, known := previous[id]; known && before != now {
			changed[id] = true
		}
	}
	return changed
}

// owningEntry finds the enrolled identity whose computed catalogue topic is
// topic, and returns it with the mount it is bound to. It compares the full
// topic, node id included, so records from another node never match.
func (c *ConfigExec) owningEntry(topic string) (entry EntryRef, mount string, ok bool) {
	for _, e := range c.bound.Entries() {
		m, ok := c.mountFor(e.Element)
		if !ok {
			continue // cannot place this entry here: it owns nothing
		}
		if CatalogueTopic(c.store.NodeID(), m, e.Name) == topic {
			return e, m, true
		}
	}
	return EntryRef{}, "", false
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
	// TakeOver moves the signal's data_tag to it when another signal holds
	// the tag, and retires that signal. Only a signal autobind minted, with
	// nothing positioned below it, can be retired this way.
	TakeOver bool `json:"take_over"`
}

type upsertBody struct {
	Signals []signalRef `json:"signals"`
}

// constantRef is one typed authored value and its position. Constants are not
// signals: they have no acquisition binding or metric topic. Expected, when
// given, is the value the writer read: the write goes through only while the
// node still holds that value, so two writers that read the same value cannot
// both succeed.
type constantRef struct {
	Path     string          `json:"path"`
	Constant json.RawMessage `json:"constant"`
	Expected json.RawMessage `json:"expected,omitempty"`
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
	ParentID string `json:"parent_id,omitempty"`
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
	DataTags []catalogueTag `json:"data_tags"`
}

// catalogueTag is one tag of a catalogue.
type catalogueTag struct {
	// ID is the tag's ULID, minted by the connector at discovery and stable
	// across rediscovery. Signal.data_tag points here.
	ID       string `json:"id"`
	Name     string `json:"name"`
	DataType string `json:"data_type"`
	// Source is the tag's address at the connector (an OPC UA node id, a
	// register); a bind_intent may name the tag by it instead of by name.
	Source string `json:"source"`
	// Meta.Element, when set, is the node-local path of the element this tag's
	// signal belongs under instead of the connector's mount, so one service can
	// publish for several machines. Missing elements on that path are created.
	Meta struct {
		Element string `json:"element"`
		tagMeta
	} `json:"meta"`
}

// bindIntent is a declared signal's bind_intent: the tag it waits for, named
// by its connector and its variable (the tag's name or source).
type bindIntent struct {
	Connector string `json:"connector"`
	Variable  string `json:"variable"`
}

// names reports whether the intent names this tag of this connector. The
// connector is named by its enrolled name or its ULID.
func (i bindIntent) names(conn EntryRef, tag catalogueTag) bool {
	if i.Connector == "" || i.Variable == "" {
		return false
	}
	if i.Connector != conn.Name && i.Connector != conn.ULID {
		return false
	}
	return i.Variable == tag.Name || (tag.Source != "" && i.Variable == tag.Source)
}

// intentOf reads a signal record's bind_intent; ok is false when it has none.
func intentOf(record map[string]any) (bindIntent, bool) {
	raw, ok := record["bind_intent"].(map[string]any)
	if !ok {
		return bindIntent{}, false
	}
	connector, _ := raw["connector"].(string)
	variable, _ := raw["variable"].(string)
	if connector == "" || variable == "" {
		return bindIntent{}, false
	}
	return bindIntent{Connector: connector, Variable: variable}, true
}

// tagMeta is what a catalogue tag says about its signal beyond name and type.
// Each field becomes the new signal's, or fills in a bound signal that has
// none. A declared value is never overwritten when the tag is first bound; a
// later publish that changes a field updates the bound signal, because the
// publisher owns what its tags say. A field the tag leaves empty changes
// nothing, so catalogues without these fields behave as before.
type tagMeta struct {
	Unit string `json:"unit"`
	// SemanticType names a _SemanticTag (by name or id); it becomes the
	// signal's semantic_type_id. A name this node does not know is ignored.
	SemanticType string `json:"semantic_type"`
	Description  string `json:"description"`
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
		existing, held := c.store.KVGet(topic)
		if held {
			record, err := validateConstantPayload(existing)
			if err != nil || record.ID != incoming.ID {
				heldID := record.ID
				if heldID == "" {
					heldID = "an unreadable retained record"
				}
				return 409, fmt.Sprintf("constant/upsert: %s is already constant %s — two constants "+
					"cannot share one position", ref.Path, heldID), "conflict", nil
			}
			if len(ref.Expected) > 0 && !sameJSON(record.Value, ref.Expected) {
				return 409, fmt.Sprintf("constant/upsert: %s holds %s, not the expected %s",
					ref.Path, record.Value, ref.Expected), "conflict", nil
			}
		} else if len(ref.Expected) > 0 {
			return 409, fmt.Sprintf("constant/upsert: %s holds no constant, expected %s",
				ref.Path, ref.Expected), "conflict", nil
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

// sameJSON reports whether two JSON values are equal, numbers by value, so
// 0.5 and 5e-1 match and int64 values compare without float rounding.
func sameJSON(a, b json.RawMessage) bool {
	decode := func(raw json.RawMessage) (any, bool) {
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.UseNumber()
		var v any
		return v, decoder.Decode(&v) == nil
	}
	x, okX := decode(a)
	y, okY := decode(b)
	return okX && okY && equalJSON(x, y)
}

func equalJSON(x, y any) bool {
	switch x := x.(type) {
	case json.Number:
		n, ok := y.(json.Number)
		if !ok {
			return false
		}
		i, errI := x.Int64()
		j, errJ := n.Int64()
		if errI == nil && errJ == nil {
			return i == j
		}
		f, errX := x.Float64()
		g, errY := n.Float64()
		return errX == nil && errY == nil && f == g
	case map[string]any:
		m, ok := y.(map[string]any)
		if !ok || len(m) != len(x) {
			return false
		}
		for k, v := range x {
			w, ok := m[k]
			if !ok || !equalJSON(v, w) {
				return false
			}
		}
		return true
	case []any:
		l, ok := y.([]any)
		if !ok || len(l) != len(x) {
			return false
		}
		for i := range x {
			if !equalJSON(x[i], l[i]) {
				return false
			}
		}
		return true
	default:
		return x == y
	}
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
	bindings := c.bindings()
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
		identity := incoming.ID
		if identity == "" {
			identity = ref.Path
		}
		payload, retired, refusal := c.settleBinding(identity, ref.Path, payload, ref.TakeOver, bindings)
		if refusal != "" {
			return 409, "signal/upsert: " + refusal, "conflict", nil
		}
		for _, path := range retired {
			vacated = append(vacated, c.signalTopic(path))
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
		code, msg, status := commitRefusal("signal/upsert", err)
		return code, msg, status, nil
	}
	return 200, fmt.Sprintf("upserted %d", upserted), "ok", writes
}

// settleBinding applies the one-signal-per-tag rule to a signal record an
// upsert is about to write, and returns the record to write.
//
// A record with a data_tag drops its bind_intent, which only means something
// while the signal is unbound. When another signal holds that tag, elsewhere
// than the position this record replaces, the upsert is refused naming the
// holder, unless takeOver asks to move the tag: then every other holder is
// retired (their paths are returned) if autobind minted it and nothing is
// positioned below it. Their metric history stays where it is, under their
// ids. take_over also resolves a tag two signals already share, which an
// upsert without it leaves alone.
//
// A record without a data_tag but with a bind_intent is bound at once when the
// named connector's catalogue already holds a free tag the intent names, so a
// catalogue that arrived just before the declaration does not wait for the
// next publish.
func (c *ConfigExec) settleBinding(
	id, path string, payload json.RawMessage, takeOver bool, bindings *signalBindings,
) (json.RawMessage, []string, string) {
	var record map[string]any
	if json.Unmarshal(payload, &record) != nil {
		return payload, nil, "" // not a record: the schema check refuses it
	}
	tagID, _ := record["data_tag"].(string)
	if tagID == "" {
		tag, ok := c.resolveIntent(record, id, bindings)
		if !ok {
			return payload, nil, ""
		}
		bindings.bind(tag.ID, id)
		encoded, err := json.Marshal(adoptTag(record, tag, c.semanticTags()))
		if err != nil {
			return payload, nil, ""
		}
		return encoded, nil, ""
	}
	delete(record, "bind_intent")
	encoded, err := json.Marshal(record)
	if err != nil {
		return payload, nil, ""
	}
	standing := bindings.tagHeldBy(id) == tagID
	if standing && !takeOver {
		// This binding already stands. A second signal on the same tag, from
		// before the rule, is left alone unless take_over asks to resolve it.
		return encoded, nil, ""
	}
	holders := c.otherHolders(tagID, id, path, bindings)
	if len(holders) == 0 {
		bindings.bind(tagID, id)
		return encoded, nil, ""
	}
	first := holders[0]
	refusal := (&TagHeldError{Tag: tagID, Signal: id, Holder: first.id, HolderPath: first.path}).Error()
	if !takeOver {
		if why := c.notRetirable(first); why != "" {
			return nil, nil, refusal + ". " + why + ", so take_over cannot retire it: unbind or delete it first"
		}
		return nil, nil, refusal + ". " + first.id + " was created by autobind: send take_over: true with " +
			id + " to move the tag to it and retire " + first.id +
			" (its metric history stays in the historian under " + first.id + ")"
	}
	retired := make([]string, 0, len(holders))
	for _, held := range holders {
		if why := c.notRetirable(held); why != "" {
			refusal = (&TagHeldError{Tag: tagID, Signal: id, Holder: held.id, HolderPath: held.path}).Error()
			return nil, nil, refusal + ". take_over refused: " + why + ", so it is not retired: unbind or delete it first"
		}
		retired = append(retired, held.path)
	}
	bindings.unbind(tagID)
	bindings.bind(tagID, id)
	return encoded, retired, ""
}

// otherHolders lists the signals other than id that hold tagID: those this
// node stores, except one standing at path (the write replaces it), and one an
// earlier entry of the same command bound.
func (c *ConfigExec) otherHolders(tagID, id, path string, bindings *signalBindings) []heldSignal {
	var out []heldSignal
	seen := map[string]bool{}
	for _, rec := range c.store.KVScan("_Signal", c.store.NodeID()) {
		var record map[string]any
		if json.Unmarshal(rec.Payload, &record) != nil {
			continue
		}
		if tag, _ := record["data_tag"].(string); tag != tagID || rec.Path == path {
			continue
		}
		held, _ := record["id"].(string)
		if held == "" {
			held = rec.Path
		}
		if held == id {
			continue
		}
		seen[held] = true
		out = append(out, heldSignal{id: held, path: rec.Path, record: record})
	}
	if holder := bindings.signalHolding(tagID); holder != "" && holder != id && !seen[holder] {
		if held := c.signalWithID(holder); held.path != path {
			// Bound by an earlier entry of this command: not stored yet, so
			// nothing autobind minted, and never retirable.
			held.id = holder
			out = append(out, held)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].path < out[j].path })
	return out
}

// heldSignal is one of this node's signal records, found by id.
type heldSignal struct {
	id     string
	path   string
	record map[string]any
}

// signalWithID finds this node's signal with the given id, or the one at the
// path an id-less record is keyed by.
func (c *ConfigExec) signalWithID(id string) heldSignal {
	for _, rec := range c.store.KVScan("_Signal", c.store.NodeID()) {
		var record map[string]any
		if json.Unmarshal(rec.Payload, &record) != nil {
			continue
		}
		if held, _ := record["id"].(string); held == id || (held == "" && rec.Path == id) {
			return heldSignal{id: id, path: rec.Path, record: record}
		}
	}
	return heldSignal{}
}

// autoboundFields is every field autobind writes onto a signal it mints. A
// signal minted before is_autobound existed carries no others.
var autoboundFields = map[string]bool{
	"id": true, "name": true, "data_tag": true, "is_published": true, "system_element_id": true,
	"data_type": true, "unit": true, "description": true, "semantic_type_id": true, "is_autobound": true,
}

// notRetirable says why a take_over may not retire this signal; "" means it
// may. Only a signal autobind minted can go: it says is_autobound, or, minted
// before that field existed, it carries nothing autobind does not write. And
// nothing may be positioned below it, which retiring it would orphan.
func (c *ConfigExec) notRetirable(held heldSignal) string {
	if held.record == nil {
		return "the holder is not a signal this node holds"
	}
	id, _ := held.record["id"].(string)
	if marked, _ := held.record["is_autobound"].(bool); !marked {
		for field := range held.record {
			if !autoboundFields[field] {
				return id + " was not created by autobind"
			}
		}
	}
	var below []string
	for _, contract := range []string{"_Signal", "_SystemElement", "_Constant", "_Resource"} {
		for _, rec := range c.store.KVScan(contract, c.store.NodeID()) {
			if strings.HasPrefix(rec.Path, held.path+"/") {
				below = append(below, rec.Path)
			}
		}
	}
	if len(below) > 0 {
		sort.Strings(below)
		return id + " still holds " + strings.Join(below, ", ")
	}
	return ""
}

// resolveIntent finds the tag a record's bind_intent names in a catalogue this
// node already holds. It answers only when exactly one free tag matches.
func (c *ConfigExec) resolveIntent(record map[string]any, id string, bindings *signalBindings) (catalogueTag, bool) {
	intent, ok := intentOf(record)
	if !ok || c.bound == nil {
		return catalogueTag{}, false
	}
	var found []catalogueTag
	for _, entry := range c.bound.Entries() {
		if intent.Connector != entry.Name && intent.Connector != entry.ULID {
			continue
		}
		mount, ok := c.mountFor(entry.Element)
		if !ok {
			continue
		}
		raw, ok := c.store.KVGet(CatalogueTopic(c.store.NodeID(), mount, entry.Name))
		if !ok {
			continue
		}
		var cat catalogue
		if json.Unmarshal(raw, &cat) != nil {
			continue
		}
		for _, tag := range cat.DataTags {
			if intent.names(entry, tag) {
				found = append(found, tag)
			}
		}
	}
	if len(found) != 1 || bindings.propose(found[0].ID, id) != bindFree {
		return catalogueTag{}, false
	}
	return found[0], true
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
	// bind_intent like the binding: kept when not spoken, cleared by an
	// explicit null. A record that is bound has no use for one.
	for _, field := range []string{"is_published", "data_type", "replication_policy", "bind_intent"} {
		if _, spoken := next[field]; !spoken {
			if value, has := stored[field]; has {
				next[field] = value
			}
		}
	}
	if intent, spoken := next["bind_intent"]; spoken && intent == nil {
		delete(next, "bind_intent")
	}
	// What autobind minted stays marked as minted while the same signal is
	// rewritten; a different signal written at its position is not.
	if storedID, _ := stored["id"].(string); storedID != "" && storedID == next["id"] {
		if marked, _ := stored["is_autobound"].(bool); marked {
			if _, spoken := next["is_autobound"]; !spoken {
				next["is_autobound"] = true
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
	catTopic := CatalogueTopic(c.store.NodeID(), mount, name)
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
	var cat catalogue
	if err := json.Unmarshal(raw, &cat); err != nil {
		return 422, "signal/autobind: unreadable catalogue: " + err.Error(), "invalid", nil
	}
	conn := EntryRef{ULID: body.Connector, Name: name, Element: element}
	code, msg, status, writes := c.bindCatalogue(ctx, under, at, conn, cat, nil)
	if code != 200 {
		return code, msg, status, writes
	}
	updated, synced, err := c.syncCatalogueMeta(ctx, cat, nil)
	if err != nil {
		return 422, "signal/autobind: rejected: " + err.Error(), "invalid", writes
	}
	return 200, strings.TrimSuffix(msg, "}") + fmt.Sprintf(`,"updated":%d}`, updated), "ok", append(writes, synced...)
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
	var local, leaf, parent string
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
			elem, err := json.Marshal(placedElement{ID: id, Name: seg, ParentID: parent})
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
		leaf, parent = id, id
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

// bindCatalogue binds the free tags of a catalogue, under one path and to the
// element there. signal/autobind and the lifecycle trigger both use it, and it
// is idempotent. For each free tag, in this order:
//
//  1. an unbound signal whose bind_intent names this connector and the tag
//     gets the tag, wherever it sits. Two such signals leave the tag unbound:
//     the intents contradict each other, and minting a third would not help;
//  2. an unbound declared signal at the tag's position (see
//     declaredSignals.claim) gets the tag;
//  3. a new signal is minted for it.
//
// Steps 2 and 3 run only for the tags in mint; nil means every tag. Bound
// tags and taken paths are tracked while the set is composed, so a
// declaration and discovery never produce two signals for one tag.
func (c *ConfigExec) bindCatalogue(
	ctx CommandContext, under, element string, conn EntryRef, cat catalogue, mint map[string]bool,
) (int, string, string, []StateWrite) {
	bindings := c.bindings()
	taken := c.takenPaths()
	declared := c.declaredSignals()
	intents := c.intentSignals()
	semantic := c.semanticTags()

	records := make([]StateRecord, 0, len(cat.DataTags))
	skipped, ambiguous := 0, 0
	for _, tag := range cat.DataTags {
		// Skip tags that are already bound; the edit verbs refuse instead (see
		// signalBindings). The empty signal id stands for a signal not created yet.
		if bindings.propose(tag.ID, "") != bindFree {
			skipped++
			continue
		}
		if matches := intents.matching(conn, tag); len(matches) > 0 {
			if len(matches) > 1 {
				ambiguous++
				continue
			}
			waiting := matches[0]
			intents.release(waiting.path)
			encoded, err := json.Marshal(adoptTag(waiting.record, tag, semantic))
			if err != nil {
				return 500, "signal/autobind: encode failed: " + err.Error(), "error", nil
			}
			records = append(records, StateRecord{Topic: c.signalTopic(waiting.path), Payload: encoded})
			bindings.bind(tag.ID, waiting.id)
			continue
		}
		if mint != nil && !mint[tag.ID] {
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
		if existing, existingID, path, ok := declared.claim(under, tag.Name); ok {
			encoded, err := json.Marshal(adoptTag(existing, tag, semantic))
			if err != nil {
				return 500, "signal/autobind: encode failed: " + err.Error(), "error", nil
			}
			records = append(records, StateRecord{Topic: c.signalTopic(path), Payload: encoded})
			bindings.bind(tag.ID, existingID)
			continue
		}
		leaf := sanitize(tag.Name)
		path := joinPath(under, leaf)
		if taken[path] {
			leaf = uniquePath(leaf, under, taken)
			path = joinPath(under, leaf)
		}
		// The signal id is never derived from the binding: metrics carry
		// signal_id, so rebinding must not touch the signal or its history.
		id := c.newID()
		// The raw tag name is not copied onto the signal; data_tag already reaches
		// it, and a copy would drift when a connector renames the tag.
		// is_autobound marks it as minted, not declared: a take_over may retire
		// it in favour of a declared signal.
		signal := map[string]any{
			"id":           id,
			"name":         leaf,
			"data_tag":     tag.ID,
			"is_published": true,
			"is_autobound": true,
		}
		// The binding to the tree. Absent only for an unplaced connector,
		// whose signals are bound to the node itself the way it is.
		if element != "" {
			signal["system_element_id"] = element
		}
		if tag.DataType != "" {
			signal["data_type"] = tag.DataType
		}
		applyTagMeta(signal, tag.Meta.tagMeta, semantic, false)
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
		code, msg, status := commitRefusal("signal/autobind", err)
		return code, msg, status, nil
	}
	if ambiguous > 0 {
		// Tags two waiting signals both name: bound to neither, and reported.
		return 200, fmt.Sprintf(`{"created":%d,"skipped":%d,"ambiguous":%d}`, len(records), skipped, ambiguous), "ok", writes
	}
	return 200, fmt.Sprintf(`{"created":%d,"skipped":%d}`, len(records), skipped), "ok", writes
}

// adoptTag binds a waiting or declared signal record to a tag. What the
// record declares stays; the tag fills in the type and what its meta states,
// and the record's bind_intent, now answered, is dropped.
func adoptTag(record map[string]any, tag catalogueTag, semantic map[string]string) map[string]any {
	record["data_tag"] = tag.ID
	delete(record, "bind_intent")
	if declared, _ := record["data_type"].(string); declared == "" && tag.DataType != "" {
		record["data_type"] = tag.DataType
	}
	applyTagMeta(record, tag.Meta.tagMeta, semantic, false)
	if _, published := record["is_published"]; !published {
		record["is_published"] = true
	}
	return record
}

// commitRefusal answers a refused commit. A tag another signal holds is a
// conflict the caller can resolve, so it is a 409 naming the holder; any other
// refusal is the record's own fault.
func commitRefusal(verb string, err error) (int, string, string) {
	var held *TagHeldError
	if errors.As(err, &held) {
		return 409, verb + ": " + held.Error(), "conflict"
	}
	return 422, verb + ": rejected: " + err.Error(), "invalid"
}

// applyTagMeta writes what a tag states onto a signal record: into empty
// fields only, or over any other value when overwrite is set. semantic maps a
// _SemanticTag name or id to its id. It reports whether the record changed.
func applyTagMeta(signal map[string]any, meta tagMeta, semantic map[string]string, overwrite bool) bool {
	changed := false
	set := func(field, value string) {
		if value == "" {
			return
		}
		current, _ := signal[field].(string)
		if current == value || (current != "" && !overwrite) {
			return
		}
		signal[field] = value
		changed = true
	}
	set("unit", meta.Unit)
	set("description", meta.Description)
	// A name this node does not know leaves the signal unclassified.
	set("semantic_type_id", semantic[meta.SemanticType])
	return changed
}

// semanticTags maps every _SemanticTag this node holds, by name and by id, to
// its id.
func (c *ConfigExec) semanticTags() map[string]string {
	index := map[string]string{}
	for _, rec := range c.store.KVScanAll("_SemanticTag") {
		var tag struct {
			ID   string `json:"id"`
			Name string `json:"name"`
		}
		if json.Unmarshal(rec.Payload, &tag) != nil || tag.ID == "" {
			continue
		}
		index[tag.ID] = tag.ID
		if tag.Name != "" {
			index[tag.Name] = tag.ID
		}
	}
	return index
}

// syncCatalogueMeta brings the signals bound to a catalogue's tags in line with
// what the tags state: empty fields are filled, and the fields of the tags in
// overwrite are replaced. Only records that change are written. It returns how
// many signals changed and the writes.
func (c *ConfigExec) syncCatalogueMeta(ctx CommandContext, cat catalogue, overwrite map[string]bool) (int, []StateWrite, error) {
	metas := make(map[string]tagMeta, len(cat.DataTags))
	for _, tag := range cat.DataTags {
		if tag.Meta.tagMeta != (tagMeta{}) {
			metas[tag.ID] = tag.Meta.tagMeta
		}
	}
	if len(metas) == 0 {
		return 0, nil, nil
	}
	semantic := c.semanticTags()
	var records []StateRecord
	for _, rec := range c.store.KVScan("_Signal", c.store.NodeID()) {
		var signal map[string]any
		if json.Unmarshal(rec.Payload, &signal) != nil {
			continue
		}
		tagID, _ := signal["data_tag"].(string)
		meta, ok := metas[tagID]
		if !ok || !applyTagMeta(signal, meta, semantic, overwrite[tagID]) {
			continue
		}
		encoded, err := json.Marshal(signal)
		if err != nil {
			return 0, nil, err
		}
		records = append(records, StateRecord{Topic: rec.Topic, Payload: encoded})
	}
	writes, err := c.commit(ctx, records)
	if err != nil {
		return 0, nil, err
	}
	return len(records), writes, nil
}

// declaredSignal is an unbound signal record, as the map it was written as,
// so binding it keeps the declared fields.
type declaredSignal struct {
	record map[string]any
	id     string
}

// declaredSignals indexes this node's unbound signals by path. A claimed
// signal leaves the index, so one autobind run never binds two tags to it.
type declaredSignals map[string]declaredSignal

// declaredSignals returns every signal this node holds that has an id and no
// tag yet. A signal bound to any tag is never in it, so autobind cannot take a
// signal from another tag or connector. Nor is a signal with a bind_intent: it
// names the one tag it waits for, and only that tag binds it (intentSignals).
func (c *ConfigExec) declaredSignals() declaredSignals {
	out := declaredSignals{}
	for _, rec := range c.store.KVScan("_Signal", c.store.NodeID()) {
		var record map[string]any
		if json.Unmarshal(rec.Payload, &record) != nil {
			continue
		}
		if tag, _ := record["data_tag"].(string); tag != "" {
			continue
		}
		if _, waiting := intentOf(record); waiting {
			continue
		}
		id, _ := record["id"].(string)
		if id == "" {
			continue
		}
		out[rec.Path] = declaredSignal{record: record, id: id}
	}
	return out
}

// waitingSignal is an unbound signal with a bind_intent.
type waitingSignal struct {
	path   string
	id     string
	record map[string]any
	intent bindIntent
}

// intentSignals indexes this node's unbound signals with a bind_intent by
// path. A bound signal leaves it, so one run never binds two tags to it.
type intentSignals map[string]waitingSignal

func (c *ConfigExec) intentSignals() intentSignals {
	out := intentSignals{}
	for _, rec := range c.store.KVScan("_Signal", c.store.NodeID()) {
		var record map[string]any
		if json.Unmarshal(rec.Payload, &record) != nil {
			continue
		}
		if tag, _ := record["data_tag"].(string); tag != "" {
			continue
		}
		intent, waiting := intentOf(record)
		id, _ := record["id"].(string)
		if !waiting || id == "" {
			continue
		}
		out[rec.Path] = waitingSignal{path: rec.Path, id: id, record: record, intent: intent}
	}
	return out
}

// matching lists the waiting signals whose intent names this tag of this
// connector, by path.
func (w intentSignals) matching(conn EntryRef, tag catalogueTag) []waitingSignal {
	var out []waitingSignal
	for _, s := range w {
		if s.intent.names(conn, tag) {
			out = append(out, s)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].path < out[j].path })
	return out
}

// release removes a signal that has just been bound.
func (w intentSignals) release(path string) { delete(w, path) }

// claim finds the unbound declared signal a tag named name binds to under a
// path, removes it from the index and returns it with its path. The signal
// must sit directly under that path and match the tag in one of these ways,
// tried in order:
//
//  1. its segment is the tag name as autobind spells it (sanitize);
//  2. its segment is the tag name as PREKIT spells it (pascalSegment), the
//     position "prekit dm deploy" declares a signal at;
//  3. its name field is the tag name, when exactly one signal there has it.
//
// No match, or several signals sharing the name, returns false and the caller
// mints a signal as before.
func (d declaredSignals) claim(under, name string) (map[string]any, string, string, bool) {
	for _, segment := range []string{sanitize(name), pascalSegment(name)} {
		path := joinPath(under, segment)
		if s, ok := d[path]; ok {
			delete(d, path)
			return s.record, s.id, path, true
		}
	}
	match := ""
	for path, s := range d {
		if parentPath(path) != under {
			continue
		}
		if declared, _ := s.record["name"].(string); declared != name {
			continue
		}
		if match != "" {
			return nil, "", "", false
		}
		match = path
	}
	if match == "" {
		return nil, "", "", false
	}
	s := d[match]
	delete(d, match)
	return s.record, s.id, match, true
}

// parentPath is the path a local path sits directly under; "" for a top-level
// path.
func parentPath(path string) string {
	if i := strings.LastIndex(path, "/"); i >= 0 {
		return path[:i]
	}
	return ""
}

// pascalSegment spells a name as PREKIT spells a topic segment
// (to_pascal_case since PREKIT 1.33): spaces, "-" and "_" separate words, and
// each word's first character is uppercased with the rest kept as written.
// "execution_context" -> "ExecutionContext", "fillLevel" -> "FillLevel",
// "HMIConfig" -> "HMIConfig", "Line 1" -> "Line1".
func pascalSegment(name string) string {
	var b strings.Builder
	start := true
	for _, r := range name {
		if r == '-' || r == '_' || unicode.IsSpace(r) {
			start = true
			continue
		}
		if start {
			r = unicode.ToUpper(r)
			start = false
		}
		b.WriteRune(r)
	}
	return b.String()
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
		if ref.Contract == "_ClockDefinition" {
			topic := c.definitionTopic(ref.Contract, incoming.ID)
			for _, record := range records {
				if record.Topic == topic {
					return 422, "clock definition repeated in batch", "invalid", nil
				}
			}
			previous, _ := c.store.KVGet(topic)
			if err := checkClockRevision(ref.Definition, previous); err != nil {
				return 409, err.Error(), "conflict", nil
			}
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
	if contract == "_ClockDefinition" {
		_, err := DecodeClockDefinition(raw)
		return err
	}
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
