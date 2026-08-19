package uns

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
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
	if !c.entryOwns(topic) {
		return // no enrolled entry's computed catalogue topic matches: ignore it
	}
	var cat catalogue
	if err := json.Unmarshal(payload, &cat); err != nil {
		return
	}
	bound := c.boundTags()
	for _, tag := range cat.DataTags {
		if bound[tag.ID] {
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
	switch verb {
	case "signal/upsert":
		return c.upsert(payload)
	case "signal/delete":
		return c.delete(payload)
	case "signal/autobind":
		return c.autobind(payload)
	case "element/upsert":
		return c.elementUpsert(payload)
	case "element/delete":
		return c.elementDelete(payload)
	case "definition/upsert":
		return c.definitionUpsert(payload)
	case "definition/delete":
		return c.definitionDelete(payload)
	default:
		return 422, fmt.Sprintf("unknown configure verb %q", verb), "invalid"
	}
}

func (c *ConfigExec) upsert(payload []byte) (int, string, string) {
	var body upsertBody
	if err := json.Unmarshal(payload, &body); err != nil {
		return 422, "signal/upsert: unreadable payload: " + err.Error(), "invalid"
	}
	if len(body.Signals) == 0 {
		return 422, "signal/upsert: no signals given", "invalid"
	}
	for i, ref := range body.Signals {
		if ref.Path == "" {
			return 422, fmt.Sprintf("signal/upsert: entry %d has no path", i), "invalid"
		}
		if len(ref.Signal) == 0 {
			return 422, fmt.Sprintf("signal/upsert: entry %d has no signal", i), "invalid"
		}
		if err := c.store.Publish(c.signalTopic(ref.Path), ref.Signal); err != nil {
			// The bundle rejected it, or the store did. Either way the caller
			// learns which entry and why rather than a bare failure.
			return 422, fmt.Sprintf("signal/upsert: %s rejected: %v", ref.Path, err), "invalid"
		}
	}
	return 200, fmt.Sprintf("upserted %d", len(body.Signals)), "ok"
}

func (c *ConfigExec) delete(payload []byte) (int, string, string) {
	var body deleteBody
	if err := json.Unmarshal(payload, &body); err != nil {
		return 422, "signal/delete: unreadable payload: " + err.Error(), "invalid"
	}
	if len(body.Paths) == 0 {
		return 422, "signal/delete: no paths given", "invalid"
	}
	var missing []string
	for _, path := range body.Paths {
		topic := c.signalTopic(path)
		if _, ok := c.store.KVGet(topic); !ok {
			missing = append(missing, path)
			continue
		}
		// An empty payload is the tombstone: the path is retired, not blanked.
		if err := c.store.Publish(topic, nil); err != nil {
			return 500, fmt.Sprintf("signal/delete: %s failed: %v", path, err), "error"
		}
	}
	if len(missing) > 0 {
		return 404, "signal/delete: no signal at " + strings.Join(missing, ", "), "invalid"
	}
	return 200, fmt.Sprintf("deleted %d", len(body.Paths)), "ok"
}

// autobind creates one signal per unbound tag of a connector's catalogue.
//
// Idempotent by invariant: a tag that already has a binding is skipped and an
// existing binding is never overwritten, so re-running changes nothing. That is
// what lets the same verb be issued by a person, replayed from the commands
// stream after an offline period, or fired by a node lifecycle trigger, without
// any of those paths needing to know about the others.
func (c *ConfigExec) autobind(payload []byte) (int, string, string) {
	var body autobindBody
	if err := json.Unmarshal(payload, &body); err != nil {
		return 422, "signal/autobind: unreadable payload: " + err.Error(), "invalid"
	}
	if body.Connector == "" {
		return 422, "signal/autobind: no connector given", "invalid"
	}

	name, element, ok := c.bound.EntryOf(body.Connector)
	if !ok {
		// Not enrolled here. A parent asked to bind a connector only its child
		// holds must refuse, not guess — the command travels down and executes
		// at the node that owns the identity.
		return 404, "signal/autobind: " + body.Connector + " is not enrolled at this node", "invalid"
	}
	mount, ok := c.mountFor(element)
	if !ok {
		// The entry is enrolled and placed, but this node cannot resolve
		// where — fail closed rather than treat it as unplaced, or a
		// connector this node genuinely cannot locate would read as bound to
		// the node itself and its catalogue topic would be computed wrong.
		return 409, "signal/autobind: " + name + " is bound to an element this node cannot resolve", "conflict"
	}
	catTopic := "colca/v1/_DataTags/" + c.store.NodeID() + "/" + joinPath(mount, name)
	raw, found := c.store.KVGet(catTopic)
	if !found {
		// Nothing to bind against yet — the connector has not published its
		// catalogue. A retry after it does will succeed, so this is a conflict
		// with the current state, not a bad request.
		return 409, "signal/autobind: " + name + " has published no catalogue", "conflict"
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
func (c *ConfigExec) bindCatalogue(under string, raw []byte) (int, string, string) {
	var cat catalogue
	if err := json.Unmarshal(raw, &cat); err != nil {
		return 422, "signal/autobind: unreadable catalogue: " + err.Error(), "invalid"
	}

	bound := c.boundTags()
	taken := c.takenPaths()

	created, skipped := 0, 0
	for _, tag := range cat.DataTags {
		if bound[tag.ID] {
			skipped++
			continue
		}
		leaf := uniquePath(sanitize(tag.Name), under, taken)
		path := under + "/" + leaf
		signal := map[string]any{
			// The signal's own identity: never composed from what it is bound
			// to. Every Metric carries signal_id, so rebinding this signal to
			// a different tag later must leave it — and the whole metric
			// history under it — untouched (design §6).
			"id":           c.newID(),
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
			return 500, "signal/autobind: encode failed: " + err.Error(), "error"
		}
		if err := c.store.Publish(c.signalTopic(path), encoded); err != nil {
			return 422, fmt.Sprintf("signal/autobind: %s rejected: %v", path, err), "invalid"
		}
		taken[path] = true
		created++
	}
	return 200, fmt.Sprintf(`{"created":%d,"skipped":%d}`, created, skipped), "ok"
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

// boundTags is the set of tag ids that already have a signal — the answer to
// "which of this catalogue's tags need no work". A tag's id is its own ULID,
// minted by the connector that discovered it, so this set is exact without
// scoping it to a connector: nothing about a connector appears on a signal any
// more (design §6) — the tag id alone is what a signal points at.
func (c *ConfigExec) boundTags() map[string]bool {
	bound := map[string]bool{}
	for _, rec := range c.store.KVScan("_Signal", c.store.NodeID()) {
		var s boundSignal
		if json.Unmarshal(rec.Payload, &s) != nil {
			continue
		}
		if s.DataTag != "" {
			bound[s.DataTag] = true
		}
	}
	return bound
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
func (c *ConfigExec) elementUpsert(payload []byte) (int, string, string) {
	var body elementUpsertBody
	if err := json.Unmarshal(payload, &body); err != nil {
		return 422, "element/upsert: unreadable payload: " + err.Error(), "invalid"
	}
	if len(body.Elements) == 0 {
		return 422, "element/upsert: no elements given", "invalid"
	}
	for i, ref := range body.Elements {
		if ref.Path == "" {
			return 422, fmt.Sprintf("element/upsert: entry %d has no path", i), "invalid"
		}
		if len(ref.Element) == 0 {
			return 422, fmt.Sprintf("element/upsert: entry %d has no element", i), "invalid"
		}
		var incoming placedElement
		if err := json.Unmarshal(ref.Element, &incoming); err != nil || incoming.ID == "" {
			return 422, fmt.Sprintf("element/upsert: entry %d has no element id — a position "+
				"nothing can name is not addressable", i), "invalid"
		}
		topic := c.elementTopic(ref.Path)
		if existing, ok := c.store.KVGet(topic); ok {
			var held placedElement
			if json.Unmarshal(existing, &held) == nil && held.ID != incoming.ID {
				return 409, fmt.Sprintf("element/upsert: %s is already element %s — two elements "+
					"cannot share one position", ref.Path, held.ID), "conflict"
			}
		}
		if err := c.store.Publish(topic, ref.Element); err != nil {
			return 422, fmt.Sprintf("element/upsert: %s rejected: %v", ref.Path, err), "invalid"
		}
	}
	return 200, fmt.Sprintf("upserted %d", len(body.Elements)), "ok"
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
func (c *ConfigExec) elementDelete(payload []byte) (int, string, string) {
	var body deleteBody
	if err := json.Unmarshal(payload, &body); err != nil {
		return 422, "element/delete: unreadable payload: " + err.Error(), "invalid"
	}
	if len(body.Paths) == 0 {
		return 422, "element/delete: no paths given", "invalid"
	}
	var missing []string
	for _, path := range body.Paths {
		topic := c.elementTopic(path)
		raw, ok := c.store.KVGet(topic)
		if !ok {
			missing = append(missing, path)
			continue
		}
		if held := c.occupantsBelow(path); len(held) > 0 {
			return 409, fmt.Sprintf("element/delete: %s still holds %s", path,
				strings.Join(held, ", ")), "conflict"
		}
		if bound := c.boundIdentities(raw); len(bound) > 0 {
			return 409, fmt.Sprintf("element/delete: %s is still bound by %s", path,
				strings.Join(bound, ", ")), "conflict"
		}
		if err := c.store.Publish(topic, nil); err != nil {
			return 500, fmt.Sprintf("element/delete: %s failed: %v", path, err), "error"
		}
	}
	if len(missing) > 0 {
		return 404, "element/delete: no element at " + strings.Join(missing, ", "), "invalid"
	}
	return 200, fmt.Sprintf("deleted %d", len(body.Paths)), "ok"
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
func (c *ConfigExec) definitionUpsert(payload []byte) (int, string, string) {
	var body definitionUpsertBody
	if err := json.Unmarshal(payload, &body); err != nil {
		return 422, "definition/upsert: unreadable payload: " + err.Error(), "invalid"
	}
	if len(body.Definitions) == 0 {
		return 422, "definition/upsert: no definitions given", "invalid"
	}
	for i, ref := range body.Definitions {
		if code, msg, result := c.checkDefinitionContract(i, ref.Contract); code != 0 {
			return code, msg, result
		}
		if len(ref.Definition) == 0 {
			return 422, fmt.Sprintf("definition/upsert: entry %d has no definition", i), "invalid"
		}
		var incoming identified
		if err := json.Unmarshal(ref.Definition, &incoming); err != nil || incoming.ID == "" {
			return 422, fmt.Sprintf("definition/upsert: entry %d has no id — a definition's id "+
				"is its address, and one without an id cannot be reached", i), "invalid"
		}
		if err := validDefinitionID(incoming.ID); err != nil {
			return 422, fmt.Sprintf("definition/upsert: entry %d: %v", i, err), "invalid"
		}
		if err := checkDefinitionContents(ref.Contract, ref.Definition); err != nil {
			return 422, fmt.Sprintf("definition/upsert: %s %s: %v",
				ref.Contract, incoming.ID, err), "invalid"
		}
		if err := c.store.Publish(c.definitionTopic(ref.Contract, incoming.ID), ref.Definition); err != nil {
			return 422, fmt.Sprintf("definition/upsert: %s %s rejected: %v",
				ref.Contract, incoming.ID, err), "invalid"
		}
	}
	return 200, fmt.Sprintf("upserted %d", len(body.Definitions)), "ok"
}

// definitionDelete retracts definitions with a tombstone, which propagates down
// the same way the definition itself did.
func (c *ConfigExec) definitionDelete(payload []byte) (int, string, string) {
	var body definitionDeleteBody
	if err := json.Unmarshal(payload, &body); err != nil {
		return 422, "definition/delete: unreadable payload: " + err.Error(), "invalid"
	}
	if len(body.Definitions) == 0 {
		return 422, "definition/delete: no definitions given", "invalid"
	}
	var missing []string
	for i, ref := range body.Definitions {
		if code, msg, result := c.checkDefinitionContract(i, ref.Contract); code != 0 {
			return code, msg, result
		}
		if ref.ID == "" {
			return 422, fmt.Sprintf("definition/delete: entry %d has no id", i), "invalid"
		}
		topic := c.definitionTopic(ref.Contract, ref.ID)
		if _, ok := c.store.KVGet(topic); !ok {
			missing = append(missing, ref.Contract+" "+ref.ID)
			continue
		}
		if err := c.store.Publish(topic, nil); err != nil {
			return 500, fmt.Sprintf("definition/delete: %s %s failed: %v",
				ref.Contract, ref.ID, err), "error"
		}
	}
	if len(missing) > 0 {
		return 404, "definition/delete: no definition at " + strings.Join(missing, ", "), "invalid"
	}
	return 200, fmt.Sprintf("deleted %d", len(body.Definitions)), "ok"
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
// name what is in the way instead of just saying no.
func (c *ConfigExec) occupantsBelow(path string) []string {
	var out []string
	for _, rec := range c.store.KVScan("_SystemElement", c.store.NodeID()) {
		if strings.HasPrefix(rec.Path, path+"/") {
			out = append(out, rec.Path)
		}
	}
	sort.Strings(out)
	return out
}
