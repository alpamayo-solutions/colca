// Composing the `model` intent: assigning a data model to a system element,
// and unassigning one.
//
// It is its own file because it is the only intent that composes a SUBTREE.
// Every other composer decides about one entity and the position it occupies;
// this one walks a model's slots, resolves each against what the element
// already has — adopting a matching signal, minting one the caller
// pre-generated an id for, descending into a child element and its own
// model — and answers 409 the moment two slots want the same thing. Like the
// other composers it never writes: it either returns the whole subtree's
// records or none of them, which is what makes a recursive assign atomic.
package uns

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
)

// modelSlot is the part of a _DataModel slot the compose logic needs: what it
// is called, whether it is measured or engineered, its canonical type, and
// the semantic type (a _SemanticTag NAME, not id) it requires. DeclaredBy is
// the model whose YAML originally declared the slot (data_models/loader.py)
// — a model that merely inherits a computed slot via extends: carries its
// parent's declared_by, which is what lets an ancestor/descendant pair share
// one computed slot without that being two independent facts.
type modelSlot struct {
	Key          string `json:"key"`
	Kind         string `json:"kind"`
	DataType     string `json:"data_type"`
	SemanticType string `json:"semantic_type"`
	Unit         string `json:"unit"`
	Required     bool   `json:"required"`
	Description  string `json:"description"`
	DeclaredBy   string `json:"declared_by"`
	// ChildModel is the referenced model's name for a "child" slot authored
	// with children: [{child_model: Model}]; empty for a plain structural
	// child (a children: entry with no child_model).
	ChildModel string `json:"child_model"`
	// EntityName is the child's name: the slot key by default, or the
	// entity_name: override recorded in the YAML. Always non-empty for a
	// "child" slot.
	EntityName string `json:"entity_name"`
}

// dataModelManifest is the part of a _DataModel record composeModel reads.
// The manifest is authored as YAML and compiled by the loader, then carried
// down the definitions stream (colca_data_contracts/data_models/loader.py);
// this struct only names the fields the executor consumes, not every field
// the record carries.
type dataModelManifest struct {
	ID    string      `json:"id"`
	Name  string      `json:"name"`
	Slots []modelSlot `json:"slots"`
}

// slotDataTypes maps a slot's canonical data_type (semantic-types design §3)
// to the _Signal contract's own data_type vocabulary.
var slotDataTypes = map[string]string{
	"boolean": "boolean", "integer": "int", "number": "float", "string": "string", "json": "json",
}

// modelPlanState is the mutable state threaded through one recursive model
// compose (assign or unassign). It is shared across every level of the tree
// so path-collision detection and the pending-write set stay whole-batch,
// not per-element.
type modelPlanState struct {
	entities   map[string]editSnapshot
	manifests  map[string]dataModelManifest
	expected   map[string]uint64
	creates    map[string]string
	queue      func(string, map[string]json.RawMessage) error
	tagsByName map[string]string
	tagsLoaded bool
	// signalPaths/elementPaths are whole-store occupancy sets (every
	// existing signal/constant path, every existing system-element path),
	// grown as this batch queues new creates — a create anywhere in the
	// tree must not collide with a foreign entity OR a sibling create.
	signalPaths  map[string]bool
	elementPaths map[string]bool
	// takenIDs is the same kind of occupancy set for IDENTITY rather than
	// position: every entityVersionKey the store already holds, grown as
	// this batch queues new creates. A create's id comes from the caller's
	// intent.Creates map, and nothing else here checks it is free — so
	// without this an id naming an existing entity is written verbatim at a
	// NEW path, leaving two retained records under one identity. That state
	// is unreachable by any other route and wedges the whole node: snapshot()
	// answers "duplicate retained identity" and every subsequent edit
	// command at that node 409s until an operator removes a record by hand.
	// The projector applies it as a rename+reparent of the victim into the
	// caller's subtree, which no grant anywhere authorized.
	//
	// This is a BACKSTOP, not the primary guard, and it is per-node by
	// construction: `entities` (see below) comes from snapshot(), which scans
	// `w.store.KVScan(contract, w.store.NodeID())` -- this node's own records
	// only. A create id that names an entity at ANOTHER node is invisible to
	// this map and reaches compose unrefused; a client that checks ids across
	// nodes before submitting is what closes that case.
	takenIDs map[string]bool
	// visiting is the cycle guard shared by resolveModelSlots and
	// releaseModelSlots: keyed by elementID+"\x00"+modelName, an entry is
	// present only while that (element, model) pair is on the CURRENT
	// recursion path (pushed on entry, popped via defer on exit) — a
	// hand-authored definition/upsert can create a child_model cycle AND a
	// parent_id cycle in the entity graph at once, which the Python
	// compiler's compile-time check cannot see, so this is the only thing
	// standing between that input and an unbounded recursion.
	visiting map[string]bool
	// stillMandated is populated only for unassign, once, before release
	// walks the removed models: the set of (childID, modelName) pairs the
	// still-DESIRED models independently mandate. releaseModelSlots must
	// never strip a model name (or touch/recurse into its subtree) that
	// this set still names — two desired models can mandate the very same
	// child via the very same child_model, and removing only one of them
	// must not release what the other still requires.
	stillMandated map[string]bool
}

// claimID takes the identity a create slot asked for, or refuses the whole
// command. An id is free only if the store does not already hold an entity of
// that kind under it AND no earlier create in this same batch claimed it —
// exactly the two halves signalPaths/elementPaths cover for position, and for
// the same reason: a batch that half-checks either one can still queue two
// records under one identity.
//
// Refusing is a 409 like every other conflict here, and — because compose
// never writes — it costs zero StateRecords: the command is answered before
// anything is committed, so the node is left exactly as it was.
//
// This is deliberately the same shape composeCreate and composeBinding's
// create_signal_and_bind already use for their own caller-supplied ids. The
// model intent was the one create path that did not check.
func (state *modelPlanState) claimID(kind, id, slotPath string) (int, string) {
	// An element id becomes a grant zone once grantsync registers it, so a
	// slot may not mint one that is a wildcard rather than an identity — see
	// ValidElementID. Signal ids never reach the grant grammar.
	if kind == "system-element" {
		if err := ValidElementID(id); err != nil {
			return 422, fmt.Sprintf("model: slot %q: %v", slotPath, err)
		}
	}
	key := entityVersionKey(kind, id)
	if state.takenIDs[key] {
		return 409, fmt.Sprintf(
			"model: slot %q names id %s, which already identifies an entity", slotPath, id,
		)
	}
	state.takenIDs[key] = true
	return 0, ""
}

func (w *EditExec) resolveSemanticTag(state *modelPlanState, name string) (string, bool) {
	if !state.tagsLoaded {
		for _, rec := range w.store.KVScanAll("_SemanticTag") {
			var tag struct {
				ID   string `json:"id"`
				Name string `json:"name"`
			}
			if json.Unmarshal(rec.Payload, &tag) == nil && tag.Name != "" && tag.ID != "" {
				state.tagsByName[tag.Name] = tag.ID
			}
		}
		state.tagsLoaded = true
	}
	id, ok := state.tagsByName[name]
	return id, ok
}

// modelSlotPath joins a parent slot path with a slot key: "" + "drive_end_bearing"
// -> "drive_end_bearing"; "drive_end_bearing" + "vibration" ->
// "drive_end_bearing/vibration" (spec §4/§5's slot-path convention, shared
// verbatim with the Python plan/creates keying).
func modelSlotPath(path, key string) string {
	if path == "" {
		return key
	}
	return path + "/" + key
}

func stringListFromRaw(raw json.RawMessage) []string {
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return nil
	}
	var values []string
	if err := json.Unmarshal(raw, &values); err != nil {
		return nil
	}
	return values
}

func containsString(list []string, value string) bool {
	for _, item := range list {
		if item == value {
			return true
		}
	}
	return false
}

func removeString(list []string, value string) []string {
	out := make([]string, 0, len(list))
	for _, item := range list {
		if item != value {
			out = append(out, item)
		}
	}
	return out
}

// modelsRemoved returns the names in original that are no longer in desired,
// preserving original's order and deduplicating.
func modelsRemoved(original, desired []string) []string {
	keep := map[string]bool{}
	for _, name := range desired {
		keep[name] = true
	}
	seen := map[string]bool{}
	removed := make([]string, 0)
	for _, name := range original {
		if keep[name] || seen[name] {
			continue
		}
		seen[name] = true
		removed = append(removed, name)
	}
	return removed
}

// directChildrenByName indexes the direct system-element children of
// parentID by name, the lowest id winning a duplicate name (same
// determinism rule as signal-name matching).
func directChildrenByName(entities map[string]editSnapshot, parentID string) map[string]editSnapshot {
	type candidate struct {
		id     string
		entity editSnapshot
	}
	owned := make([]candidate, 0)
	for _, entity := range entities {
		if entity.Kind != "system-element" {
			continue
		}
		parent, _ := rawString(entity.Payload["parent_id"])
		if parent != parentID {
			continue
		}
		id, _ := rawString(entity.Payload["id"])
		owned = append(owned, candidate{id: id, entity: entity})
	}
	sort.Slice(owned, func(i, j int) bool { return owned[i].id < owned[j].id })
	byName := map[string]editSnapshot{}
	for _, item := range owned {
		name, _ := rawString(item.entity.Payload["name"])
		if name == "" {
			continue
		}
		if _, exists := byName[name]; !exists {
			byName[name] = item.entity
		}
	}
	return byName
}

// mandatedChildModels computes, read-only, the full set of (childID,
// modelName) pairs that "models" mandates when walked against whichever
// children currently EXIST under "element" — no writes, no creation of
// missing children (a missing child mandates nothing yet). This is what lets
// releaseModelSlots tell "still mandated by a model that stays desired" apart
// from "no longer mandated by anything", so unassigning one of two models
// that independently name the same child does not release what the other
// still requires (spec §4 unassign). It carries its own cycle guard, local to
// this call, for the same hand-authored-cycle reason resolveModelSlots and
// releaseModelSlots do.
func (w *EditExec) mandatedChildModels(
	element editSnapshot,
	models []string,
	manifests map[string]dataModelManifest,
	entities map[string]editSnapshot,
) (map[string]bool, int, string) {
	mandated := map[string]bool{}
	visiting := map[string]bool{}
	var walk func(element editSnapshot, models []string) (int, string)
	walk = func(element editSnapshot, models []string) (int, string) {
		elementID, _ := rawString(element.Payload["id"])
		for _, name := range models {
			if visiting[elementID+"\x00"+name] {
				return 409, fmt.Sprintf(
					"model: cycle detected while recomputing still-mandated children: %s revisits %s",
					elementID, name,
				)
			}
		}
		for _, name := range models {
			visiting[elementID+"\x00"+name] = true
		}
		defer func() {
			for _, name := range models {
				delete(visiting, elementID+"\x00"+name)
			}
		}()

		childrenByName := directChildrenByName(entities, elementID)
		for _, name := range models {
			manifest, ok := manifests[name]
			if !ok {
				continue
			}
			for _, slot := range manifest.Slots {
				if slot.Kind != "child" || slot.ChildModel == "" {
					continue
				}
				entityName := slot.EntityName
				if entityName == "" {
					entityName = slot.Key
				}
				child, found := childrenByName[entityName]
				if !found {
					continue
				}
				childID, _ := rawString(child.Payload["id"])
				mandated[childID+"\x00"+slot.ChildModel] = true
				if code, message := walk(child, []string{slot.ChildModel}); code != 0 {
					return code, message
				}
			}
		}
		return 0, ""
	}
	if code, message := walk(element, models); code != 0 {
		return nil, code, message
	}
	return mandated, 0, ""
}

// composeModel implements the edit "model" intent: assigning or
// unassigning a system element's desired _DataModel implements list.
//
// "models" always names the FULL desired list (spec §6) — assign sends the
// list with additions, unassign sends it shrunk — so the root element update
// is the same write either way. "assign" recursively resolves every slot of
// every desired model — signals AND mandated children — against the whole
// tree in one atomic batch (child-model design §4); "unassign" recomputes
// the mandated tree of whichever models were REMOVED and strips just those
// model names from the mandated children's own implements, recursively.
// Neither action ever deletes an element or a signal.
func (w *EditExec) composeModel(
	intent editIntent,
	expected map[string]uint64,
	entities map[string]editSnapshot,
) (int, string, string, []StateRecord) {
	if intent.Entity.Kind != "system-element" || intent.Entity.ID == "" {
		return 422, "model: entity must be a system-element", "invalid", nil
	}
	switch intent.Action {
	case "assign", "unassign":
	default:
		return 422, fmt.Sprintf("model: action must be assign or unassign, got %q", intent.Action), "invalid", nil
	}
	element, code, message := requireEntity(expected, entities, "system-element", intent.Entity.ID)
	if code != 0 {
		return code, "model: " + message, resultFor(code), nil
	}

	// Load every _DataModel definition visible at this node (definitions
	// descend from any authoring node — same access pattern groups.go uses
	// for _Group).
	manifests := map[string]dataModelManifest{}
	for _, rec := range w.store.KVScanAll("_DataModel") {
		var m dataModelManifest
		if err := json.Unmarshal(rec.Payload, &m); err != nil || m.Name == "" {
			return 422, "model: retained _DataModel at " + rec.Path + " is unreadable", "invalid", nil
		}
		manifests[m.Name] = m
	}
	for _, name := range intent.Models {
		if _, ok := manifests[name]; !ok {
			return 422, "model: unknown data model " + name, "invalid", nil
		}
	}

	pending := map[string]StateRecord{}
	order := []string{}
	queue := func(topic string, payload map[string]json.RawMessage) error {
		encoded, err := json.Marshal(payload)
		if err != nil {
			return err
		}
		if _, exists := pending[topic]; !exists {
			order = append(order, topic)
		}
		pending[topic] = StateRecord{Topic: topic, Payload: encoded}
		return nil
	}

	desired := intent.Models
	if desired == nil {
		desired = []string{}
	}
	original := stringListFromRaw(element.Payload["implements"])

	state := &modelPlanState{
		entities: entities, manifests: manifests, expected: expected,
		creates: intent.Creates, queue: queue, tagsByName: map[string]string{},
		signalPaths: map[string]bool{}, elementPaths: map[string]bool{},
		takenIDs: map[string]bool{},
		visiting: map[string]bool{},
	}
	for key, entity := range entities {
		state.takenIDs[key] = true
		switch entity.Kind {
		case "signal", "constant":
			state.signalPaths[entity.Record.Path] = true
		case "system-element":
			state.elementPaths[entity.Record.Path] = true
		}
	}

	if intent.Action == "assign" {
		if code, message := w.resolveModelSlots(state, element, desired, ""); code != 0 {
			return code, message, resultFor(code), nil
		}
	} else {
		removed := modelsRemoved(original, desired)
		if len(removed) > 0 {
			stillMandated, code, message := w.mandatedChildModels(element, desired, manifests, entities)
			if code != 0 {
				return code, message, resultFor(code), nil
			}
			state.stillMandated = stillMandated
			if code, message := w.releaseModelSlots(state, element, removed, ""); code != 0 {
				return code, message, resultFor(code), nil
			}
		}
	}

	// The root element update is the same for both actions: "implements"
	// becomes the full desired list the caller sent, verbatim. Queue it only
	// if it actually changes something — an identical re-assign with no slot
	// work must not manufacture a write, mirroring composeUpdate's no-op path.
	elementPayload := cloneRawMap(element.Payload)
	elementPayload["id"] = rawJSON(intent.Entity.ID)
	elementPayload["implements"] = rawJSON(desired)
	if !rawMapsEqual(elementPayload, element.Payload) {
		if err := queue(element.Record.Topic, elementPayload); err != nil {
			return 422, "model: attributes are not encodable", "invalid", nil
		}
	}

	records := make([]StateRecord, 0, len(order))
	for _, topic := range order {
		records = append(records, pending[topic])
	}
	if len(records) > editMutationLimit {
		return 422, fmt.Sprintf("model: at most %d entities may be changed atomically", editMutationLimit), "invalid", nil
	}
	if len(records) == 0 {
		return 409, fmt.Sprintf("model_unchanged: %s already implements %v", intent.Entity.ID, desired), "conflict", nil
	}
	verb := "assigned"
	if intent.Action == "unassign" {
		verb = "unassigned"
	}
	return 200, fmt.Sprintf("model %s: %s now implements %v", verb, intent.Entity.ID, desired), "ok", records
}

// resolveModelSlots is composeModel's recursive assign engine (child-model
// design §4). It matches every slot of every model in "models" against
// "element": signal slots against the element's own signals (exactly as the
// flat implementation did), child slots against the element's direct
// system-element children by exact entity_name. A matched child gains any
// newly-mandated model names in its own "implements" (dedup, list
// semantics); a missing REQUIRED child is created from intent.Creates
// (keyed by the slot path, spec §4.2) with its own initial "implements".
// Either way, resolveModelSlots then recurses into that child with whatever
// models its slot mandated, at "path" extended by the slot key — so a whole
// mandated subtree resolves into ONE pending batch before anything commits.
// It queues nothing and returns a non-zero code on any conflict anywhere in
// the subtree, exactly as the flat implementation did for one element.
func (w *EditExec) resolveModelSlots(
	state *modelPlanState,
	element editSnapshot,
	models []string,
	path string,
) (int, string) {
	elementID, _ := rawString(element.Payload["id"])

	// Cycle guard: a hand-authored child_model reference cycle combined with
	// a parent_id cycle in the entity graph would otherwise recurse forever
	// (the Python compiler's cycle check cannot see this — it only inspects
	// the model graph, never entity data). Revisiting the same (element,
	// model) pair on the CURRENT recursion path is the cycle; pop on exit so
	// a legitimately re-used element/model pair reached via a different,
	// non-cyclic branch is unaffected.
	for _, name := range models {
		if state.visiting[elementID+"\x00"+name] {
			return 409, fmt.Sprintf(
				"model: cycle detected: %s revisits %s while resolving child slot path %q", elementID, name, path,
			)
		}
	}
	for _, name := range models {
		state.visiting[elementID+"\x00"+name] = true
	}
	defer func() {
		for _, name := range models {
			delete(state.visiting, elementID+"\x00"+name)
		}
	}()

	// Rule: every non-child slot of every desired model must name a known
	// canonical data_type — an unrecognized one must not silently become ""
	// and land on a created signal. A "child" slot's data_type is always ""
	// by construction (the loader never sets one for a children: entry) and
	// is not a canonical type at all, so it is exempt from this check
	// (child-model design §3).
	for _, name := range models {
		for _, slot := range state.manifests[name].Slots {
			if slot.Kind == "child" {
				continue
			}
			if _, known := slotDataTypes[slot.DataType]; !known {
				return 422, fmt.Sprintf("model: %s slot %q has unknown data_type %q", name, slot.Key, slot.DataType)
			}
		}
	}

	// Rule: two desired models may not both compute the same slot key (one
	// computer per signal) UNLESS it is the same fact — an ancestor and a
	// descendant that both carry the slot via MRO flattening share one
	// declared_by, and that is one computed slot, not two. Two desired
	// models requiring the same signal slot key must also agree on its
	// data_type and semantic_type.
	type computer struct{ model, declaredBy string }
	computedBy := map[string]computer{}
	type requirement struct {
		slot  modelSlot
		model string
	}
	requiredSignals := map[string]requirement{}
	signalOrder := []string{}

	// Child slots sharing a key merge instead of conflicting (a
	// child slot IS a plain child plus an implements requirement, and
	// implements is a list) — every model that names a child_model at this
	// key gets applied to the same child, all mandating models recursed
	// into together below. Two DIFFERENT keys resolving to the same
	// entity_name is a different case: it is an authoring error, not a
	// merge, because matching two independent childRequirement entries
	// against the very same physical child would let the second one silently
	// overwrite the first's queued implements update (both would key off the
	// SAME topic in "pending", and the loop only ever holds one local copy of
	// the child's payload at a time — nothing propagates the first's addition
	// before the second reads it). childNameOwner catches this before either
	// entry is ever matched or written.
	type childRequirement struct {
		slot        modelSlot
		model       string
		entityName  string
		required    bool
		childModels []string
	}
	requiredChildren := map[string]*childRequirement{}
	childOrder := []string{}
	childNameOwner := map[string]string{}

	for _, name := range models {
		for _, slot := range state.manifests[name].Slots {
			if slot.Kind == "child" {
				entityName := slot.EntityName
				if entityName == "" {
					entityName = slot.Key
				}
				cr, exists := requiredChildren[slot.Key]
				if !exists {
					if owner, taken := childNameOwner[entityName]; taken && owner != slot.Key {
						return 409, fmt.Sprintf(
							"model: child slots %q and %q both resolve to entity name %q",
							owner, slot.Key, entityName,
						)
					}
					childNameOwner[entityName] = slot.Key
					cr = &childRequirement{slot: slot, model: name, entityName: entityName}
					requiredChildren[slot.Key] = cr
					childOrder = append(childOrder, slot.Key)
				} else if cr.entityName != entityName {
					return 409, fmt.Sprintf(
						"model: %s and %s disagree on the name for child slot %q", cr.model, name, slot.Key,
					)
				}
				if slot.Required {
					cr.required = true
				}
				if slot.ChildModel != "" {
					if _, ok := state.manifests[slot.ChildModel]; !ok {
						return 409, fmt.Sprintf(
							"model: %s slot %q references unknown data model %s", name, slot.Key, slot.ChildModel,
						)
					}
					if !containsString(cr.childModels, slot.ChildModel) {
						cr.childModels = append(cr.childModels, slot.ChildModel)
					}
				}
				continue
			}
			if slot.Kind == "computed" {
				if holder, taken := computedBy[slot.Key]; taken && holder.declaredBy != slot.DeclaredBy {
					return 409, fmt.Sprintf("model: %s and %s both compute %q", holder.model, name, slot.Key)
				}
				computedBy[slot.Key] = computer{model: name, declaredBy: slot.DeclaredBy}
			}
			if !slot.Required {
				continue
			}
			if prev, seen := requiredSignals[slot.Key]; seen {
				if prev.slot.DataType != slot.DataType || prev.slot.SemanticType != slot.SemanticType {
					return 409, fmt.Sprintf("model: %s and %s require incompatible %q", prev.model, name, slot.Key)
				}
				continue
			}
			requiredSignals[slot.Key] = requirement{slot: slot, model: name}
			signalOrder = append(signalOrder, slot.Key)
		}
	}
	if len(signalOrder) == 0 && len(childOrder) == 0 {
		return 0, ""
	}

	// Index the element's own signals by name — the slot-matching key — in
	// deterministic (ascending id) order, so a duplicate name under one
	// element always resolves to its LOWEST-id signal regardless of map
	// iteration order.
	type ownedSignal struct {
		id     string
		entity editSnapshot
	}
	owned := make([]ownedSignal, 0)
	otherByName := map[string]editSnapshot{}
	for _, entity := range state.entities {
		if entity.Kind != "signal" && entity.Kind != "constant" {
			continue
		}
		ownerID, _ := rawString(entity.Payload["system_element_id"])
		if ownerID != elementID {
			continue
		}
		name, _ := rawString(entity.Payload["name"])
		if entity.Kind == "signal" {
			id, _ := rawString(entity.Payload["id"])
			owned = append(owned, ownedSignal{id: id, entity: entity})
		}
		if name != "" {
			if _, exists := otherByName[name]; !exists {
				otherByName[name] = entity
			}
		}
	}
	sort.Slice(owned, func(i, j int) bool { return owned[i].id < owned[j].id })
	byName := map[string]editSnapshot{}
	for _, item := range owned {
		name, _ := rawString(item.entity.Payload["name"])
		if name == "" {
			continue
		}
		if _, exists := byName[name]; !exists {
			byName[name] = item.entity
		}
	}

	for _, key := range signalOrder {
		slot := requiredSignals[key].slot
		combinedKey := modelSlotPath(path, key)
		var tagID string
		if slot.SemanticType != "" {
			id, ok := w.resolveSemanticTag(state, slot.SemanticType)
			if !ok {
				return 409, fmt.Sprintf("model: slot %q names unknown semantic type %q", combinedKey, slot.SemanticType)
			}
			tagID = id
		}
		wantType := slotDataTypes[slot.DataType]

		if existing, found := byName[key]; found {
			existingID, _ := rawString(existing.Payload["id"])
			if message := requireExpected(state.expected, entityVersionKey("signal", existingID)); message != "" {
				return 422, "model: " + message
			}
			existingType, _ := rawString(existing.Payload["data_type"])
			if existingType != "" && !compatibleDataTypes(existingType, wantType) {
				return 409, fmt.Sprintf(
					"model: slot %q has incompatible data_type: signal has %q, model requires %q",
					combinedKey, existingType, wantType,
				)
			}
			existingTag, _ := rawString(existing.Payload["semantic_type_id"])
			if tagID != "" && existingTag != "" && existingTag != tagID {
				return 409, fmt.Sprintf("model: slot %q has an incompatible semantic type already assigned", combinedKey)
			}
			if tagID != "" && existingTag == "" {
				payload := cloneRawMap(existing.Payload)
				payload["semantic_type_id"] = rawJSON(tagID)
				if err := state.queue(existing.Record.Topic, payload); err != nil {
					return 422, "model: payload is not encodable"
				}
			}
			continue
		}

		signalID := state.creates[combinedKey]
		if signalID == "" {
			return 422, fmt.Sprintf("model: slot %q requires a new signal but no id was supplied", combinedKey)
		}
		if code, message := state.claimID("signal", signalID, combinedKey); code != 0 {
			return code, message
		}
		signalPath := joinPath(element.Record.Path, sanitize(key))
		if state.signalPaths[signalPath] {
			return 409, fmt.Sprintf("model: slot %q collides with an existing signal path", combinedKey)
		}
		payload := map[string]json.RawMessage{
			"id": rawJSON(signalID), "name": rawJSON(key),
			"system_element_id": rawJSON(elementID),
			"data_type":         rawJSON(wantType),
			"is_published":      rawJSON(true), "index_type": rawJSON("time"),
		}
		if tagID != "" {
			payload["semantic_type_id"] = rawJSON(tagID)
		}
		if slot.Unit != "" {
			payload["unit"] = rawJSON(slot.Unit)
		}
		if slot.Description != "" {
			payload["description"] = rawJSON(slot.Description)
		}
		if slot.Kind == "computed" {
			payload["source"] = rawJSON("dataops")
		}
		topic := editTopic("_Signal", w.store.NodeID(), signalPath)
		if err := state.queue(topic, payload); err != nil {
			return 422, "model: payload is not encodable"
		}
		state.signalPaths[signalPath] = true
	}

	childrenByName := directChildrenByName(state.entities, elementID)
	for _, key := range childOrder {
		cr := requiredChildren[key]
		combinedKey := modelSlotPath(path, key)
		entityName := cr.entityName

		if occupant, found := otherByName[entityName]; found {
			return 409, fmt.Sprintf(
				"model: child slot %q is occupied by a %s named %q, not a system element",
				combinedKey, occupant.Kind, entityName,
			)
		}

		if child, found := childrenByName[entityName]; found {
			childID, _ := rawString(child.Payload["id"])
			if message := requireExpected(state.expected, entityVersionKey("system-element", childID)); message != "" {
				return 422, "model: " + message
			}
			implements := stringListFromRaw(child.Payload["implements"])
			changed := false
			for _, name := range cr.childModels {
				if !containsString(implements, name) {
					implements = append(implements, name)
					changed = true
				}
			}
			if changed {
				payload := cloneRawMap(child.Payload)
				payload["implements"] = rawJSON(implements)
				if err := state.queue(child.Record.Topic, payload); err != nil {
					return 422, "model: attributes are not encodable"
				}
				child.Payload = payload
			}
			if len(cr.childModels) > 0 {
				if code, message := w.resolveModelSlots(state, child, cr.childModels, combinedKey); code != 0 {
					return code, message
				}
			}
			continue
		}

		if !cr.required {
			continue
		}
		childID := state.creates[combinedKey]
		if childID == "" {
			return 422, fmt.Sprintf("model: slot %q requires a new child element but no id was supplied", combinedKey)
		}
		if code, message := state.claimID("system-element", childID, combinedKey); code != 0 {
			return code, message
		}
		childPath := joinPath(element.Record.Path, sanitize(entityName))
		if state.elementPaths[childPath] {
			return 409, fmt.Sprintf("model: slot %q collides with an existing system element path", combinedKey)
		}
		payload := map[string]json.RawMessage{
			"id": rawJSON(childID), "name": rawJSON(entityName), "parent_id": rawJSON(elementID),
		}
		if len(cr.childModels) > 0 {
			payload["implements"] = rawJSON(cr.childModels)
		}
		topic := editTopic("_SystemElement", w.store.NodeID(), childPath)
		if err := state.queue(topic, payload); err != nil {
			return 422, "model: payload is not encodable"
		}
		state.elementPaths[childPath] = true
		newChild := editSnapshot{
			Key: entityVersionKey("system-element", childID), Kind: "system-element",
			Record: KVRecord{Topic: topic, Path: childPath, NodeID: w.store.NodeID()}, Payload: payload,
		}
		if len(cr.childModels) > 0 {
			if code, message := w.resolveModelSlots(state, newChild, cr.childModels, combinedKey); code != 0 {
				return code, message
			}
		}
	}
	return 0, ""
}

// releaseModelSlots is composeModel's recursive unassign engine. "models"
// names the model(s) just REMOVED from element's implements; for each of
// their child slots, it finds the existing matching child (by entity_name)
// and — if present, and not still mandated by a model that stays desired
// (state.stillMandated) — strips that child_model name from the child's own
// implements, then recurses into what THAT child_model itself mandated
// (spec §4 unassign). A child that was never created, or never carried the
// mandated name, is left exactly as it is: unassign only ever shrinks
// implements lists, never creates, adopts, or deletes anything.
func (w *EditExec) releaseModelSlots(
	state *modelPlanState,
	element editSnapshot,
	models []string,
	path string,
) (int, string) {
	elementID, _ := rawString(element.Payload["id"])

	// Same cycle guard as resolveModelSlots (see its comment): a
	// hand-authored child_model cycle plus a parent_id cycle in the entity
	// graph would otherwise recurse forever here too.
	for _, name := range models {
		if state.visiting[elementID+"\x00"+name] {
			return 409, fmt.Sprintf(
				"model: cycle detected: %s revisits %s while releasing child slot path %q", elementID, name, path,
			)
		}
	}
	for _, name := range models {
		state.visiting[elementID+"\x00"+name] = true
	}
	defer func() {
		for _, name := range models {
			delete(state.visiting, elementID+"\x00"+name)
		}
	}()

	// Aggregate every child_model to release per slot KEY before ever
	// touching childrenByName — the same childNameOwner-style pass
	// resolveModelSlots runs before its own childrenByName loop (see its
	// comment above requiredChildren). Without this, childrenByName is read
	// once per REMOVED model's slot straight from a map built once at the
	// top of this call: a second slot key that resolves to the same
	// entity_name as an earlier one reads a snapshot the earlier key's
	// queued write never touched, so its own write recomputes "implements"
	// from that stale, ORIGINAL list and overwrites the first's queued
	// record at the same topic — silently reverting the first model's
	// release while still returning 200 (the exact last-write-wins bug the
	// assign side's childNameOwner guard already prevents). Two DIFFERENT
	// keys resolving to the same entity_name is an authoring error here too,
	// not a merge; two models sharing the SAME key still merge onto one
	// child, released together in one pass below.
	type releaseTarget struct {
		entityName  string
		childModels []string
	}
	releaseTargets := map[string]*releaseTarget{}
	releaseOrder := []string{}
	childNameOwner := map[string]string{}

	for _, name := range models {
		manifest, ok := state.manifests[name]
		if !ok {
			// A previously-assigned model whose definition no longer
			// exists: nothing left to recompute against, so there is
			// nothing to release below it either.
			continue
		}
		for _, slot := range manifest.Slots {
			if slot.Kind != "child" || slot.ChildModel == "" {
				continue
			}
			entityName := slot.EntityName
			if entityName == "" {
				entityName = slot.Key
			}
			rt, exists := releaseTargets[slot.Key]
			if !exists {
				if owner, taken := childNameOwner[entityName]; taken && owner != slot.Key {
					return 409, fmt.Sprintf(
						"model: child slots %q and %q both resolve to entity name %q",
						owner, slot.Key, entityName,
					)
				}
				childNameOwner[entityName] = slot.Key
				rt = &releaseTarget{entityName: entityName}
				releaseTargets[slot.Key] = rt
				releaseOrder = append(releaseOrder, slot.Key)
			}
			if !containsString(rt.childModels, slot.ChildModel) {
				rt.childModels = append(rt.childModels, slot.ChildModel)
			}
		}
	}

	childrenByName := directChildrenByName(state.entities, elementID)
	for _, key := range releaseOrder {
		rt := releaseTargets[key]
		child, found := childrenByName[rt.entityName]
		if !found {
			continue
		}
		childID, _ := rawString(child.Payload["id"])

		toRelease := make([]string, 0, len(rt.childModels))
		for _, childModel := range rt.childModels {
			if state.stillMandated[childID+"\x00"+childModel] {
				// A model that stays desired independently mandates this
				// exact child_model on this exact child — leave it, and everything below it, exactly
				// as it is. Nothing about it is read or written, so it
				// needs no expected version either.
				continue
			}
			toRelease = append(toRelease, childModel)
		}
		if len(toRelease) == 0 {
			continue
		}

		if message := requireExpected(state.expected, entityVersionKey("system-element", childID)); message != "" {
			return 422, "model: " + message
		}
		implements := stringListFromRaw(child.Payload["implements"])
		changed := false
		for _, childModel := range toRelease {
			if containsString(implements, childModel) {
				implements = removeString(implements, childModel)
				changed = true
			}
		}
		if changed {
			payload := cloneRawMap(child.Payload)
			payload["implements"] = rawJSON(implements)
			if err := state.queue(child.Record.Topic, payload); err != nil {
				return 422, "model: attributes are not encodable"
			}
			child.Payload = payload
		}
		combinedKey := modelSlotPath(path, key)
		if code, message := w.releaseModelSlots(state, child, toRelease, combinedKey); code != 0 {
			return code, message
		}
	}
	return 0, ""
}
