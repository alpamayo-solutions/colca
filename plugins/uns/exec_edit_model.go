// The model intent assigns a data model to a system element, or unassigns
// one. It is the only intent that composes a subtree: it walks the model's
// slots, adopts matching signals, creates the ones the caller supplied ids for,
// and descends into child elements and their models. Like every composer it
// never writes, so a recursive assign is all or nothing.

package uns

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// modelSlot is the part of a _DataModel slot compose needs: name, kind,
// canonical type and required semantic tag name. DeclaredBy is the model whose
// YAML declared the slot; a model that inherits it via extends keeps the
// parent's value, so the two share one computed slot.
type modelSlot struct {
	Key          string `json:"key"`
	Kind         string `json:"kind"`
	DataType     string `json:"data_type"`
	SemanticType string `json:"semantic_type"`
	Unit         string `json:"unit"`
	Required     bool   `json:"required"`
	Description  string `json:"description"`
	DeclaredBy   string `json:"declared_by"`
	// ChildModel is the referenced model for a child slot written as
	// children: [{child_model: Model}]; empty for a plain child.
	ChildModel string `json:"child_model"`
	// EntityName is the child's name: the slot key, or the entity_name
	// override from the YAML. Never empty for a child slot.
	EntityName string `json:"entity_name"`
	// Access is the signal slot's access, read or read_write; empty (read)
	// for a child slot and for a manifest compiled before slots carried it.
	Access string `json:"access"`
	// Relationships link this signal slot to other signal slots of the model,
	// by key. Assign gives the signals that fill the slots the same links.
	Relationships []slotRelationship `json:"relationships"`
}

// slotRelationship is one link a data-model slot declares: its type and the
// key of the target slot.
type slotRelationship struct {
	Type   string `json:"type"`
	Target string `json:"target"`
}

// slotLinkTarget is the target key a slot's link of one type names.
func slotLinkTarget(slot modelSlot, kind string) string {
	for _, link := range slot.Relationships {
		if link.Type == kind {
			return link.Target
		}
	}
	return ""
}

// validateSlotDeclarations checks a model's slot access and relationships by
// the rules the Python loader applies after flattening. A manifest the loader
// compiled passes; a hand-written one may not.
func validateSlotDeclarations(model string, slots []modelSlot) []string {
	problems := []string{}
	ends := map[string]relationshipEnd{}
	children := map[string]bool{}
	for _, slot := range slots {
		if slot.Kind == "child" {
			children[slot.Key] = true
			if slot.Access != "" || len(slot.Relationships) > 0 {
				problems = append(problems, fmt.Sprintf("%s slot %q: a child slot declares no access or relationships", model, slot.Key))
			}
			continue
		}
		if slot.Access != "" && !signalAccessValues[slot.Access] {
			problems = append(problems, fmt.Sprintf("%s slot %q: unknown access %q", model, slot.Key, slot.Access))
		}
		links := make([]relationshipLink, len(slot.Relationships))
		for i, link := range slot.Relationships {
			links[i] = relationshipLink(link)
		}
		ends[slot.Key] = relationshipEnd{DataType: slot.DataType, Links: links}
	}
	label := func(key string) string { return fmt.Sprintf("%s slot %q", model, key) }
	return append(problems, relationshipProblems(ends, label, nil, children)...)
}

// dataModelManifest holds the fields of a _DataModel record composeModel
// reads. The loader compiles the YAML, and the record arrives on the
// definitions stream.
type dataModelManifest struct {
	ID    string      `json:"id"`
	Name  string      `json:"name"`
	Slots []modelSlot `json:"slots"`
}

// slotDataTypes maps a slot's canonical data_type to the _Signal data_type
// vocabulary.
var slotDataTypes = map[string]string{
	"boolean": "boolean", "integer": "int", "number": "float", "string": "string", "json": "json",
}

// modelPlanState is the state shared across one recursive model compose, so
// collision checks and pending writes cover the whole batch.
type modelPlanState struct {
	entities   map[string]editSnapshot
	manifests  map[string]dataModelManifest
	expected   map[string]uint64
	creates    map[string]string
	queue      func(string, map[string]json.RawMessage) error
	tagsByName map[string]string
	tagsLoaded bool
	// signalPaths and elementPaths hold every occupied signal, constant and
	// element path, plus this batch's creates, so a create collides with
	// neither an existing entity nor a sibling create.
	signalPaths  map[string]bool
	elementPaths map[string]bool
	// takenIDs does the same for ids: every entity id the store holds plus
	// this batch's creates. Without it a create id naming an existing entity
	// would be written at a new path, leaving one id at two paths, which
	// breaks snapshot() and every later edit at the node. It only sees this
	// node's records; ids at other nodes are the client's to check.
	takenIDs map[string]bool
	// visiting guards resolveModelSlots and releaseModelSlots against cycles:
	// an element+model key is present while that pair is on the current
	// recursion path. A hand-written child_model cycle combined with a
	// parent_id cycle would otherwise recurse forever.
	visiting map[string]bool
	// stillMandated, set only for unassign, holds the (child, model) pairs
	// the remaining models still mandate. releaseModelSlots never releases
	// those, since two models can mandate the same child model.
	stillMandated map[string]bool
}

// claimID takes the id a create slot asked for, or refuses the command with
// a 409. An id is free only if the store has no entity of that kind under it
// and no earlier create in the batch claimed it. Nothing has been written yet,
// so a refusal leaves the node unchanged.
func (state *modelPlanState) claimID(kind, id, slotPath string) (int, string) {
	// Element ids become grant zones, so a slot may not mint a wildcard;
	// see ValidElementID. Signal ids never reach the grant grammar.
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

// modelSlotPath joins a parent slot path and a key: "" + "bearing" is
// "bearing", "bearing" + "vibration" is "bearing/vibration". The Python plan
// keys creates the same way.
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

// directChildrenByName indexes parentID's direct element children by name;
// the lowest id wins a duplicate name.
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

// mandatedChildModels returns the (child, model) pairs models mandate for the
// children that exist under element, without writing or creating anything.
// Unassign uses it so removing one of two models that name the same child keeps
// what the other requires. It has its own cycle guard.
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

// composeModel implements the model intent: assign or unassign models on a
// system element. "models" is always the full desired list, so the element
// update is the same for both. Assign resolves every slot of every model,
// signals and child elements, in one batch; unassign strips the removed models
// from the mandated children. Neither deletes an element or a signal.
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

	// Load every _DataModel definition visible at this node.
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

	// implements becomes the full list the caller sent. Queue the update
	// only if it changes something, so a repeated assign writes nothing.
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

// resolveModelSlots is the recursive assign. Signal slots match the element's
// signals, child slots its direct children by entity_name. A matched child
// gains the newly mandated models in its implements; a missing required child
// is created from intent.Creates, keyed by slot path. It then recurses into the
// child, so the whole subtree ends up in one batch. Any conflict returns a code.
func (w *EditExec) resolveModelSlots(
	state *modelPlanState,
	element editSnapshot,
	models []string,
	path string,
) (int, string) {
	elementID, _ := rawString(element.Payload["id"])

	// Cycle guard: revisiting an (element, model) pair on the current path
	// is a cycle. The Python compiler only checks the model graph, not
	// entity data. The entry is removed on exit, so other branches may
	// reuse the pair.
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

	// Every non-child slot must name a known data_type; an unknown one must
	// not become "" on a created signal. Child slots have no data_type.
	for _, name := range models {
		for _, slot := range state.manifests[name].Slots {
			if slot.Kind == "child" {
				continue
			}
			if _, known := slotDataTypes[slot.DataType]; !known {
				return 422, fmt.Sprintf("model: %s slot %q has unknown data_type %q", name, slot.Key, slot.DataType)
			}
		}
		if problems := validateSlotDeclarations(name, state.manifests[name].Slots); len(problems) > 0 {
			return 422, "model: " + strings.Join(problems, "; ")
		}
	}

	// Two desired models may not compute the same slot unless it is the
	// same slot inherited from one declaration (same declared_by). Models
	// requiring the same signal slot must agree on data_type and
	// semantic_type.
	type computer struct{ model, declaredBy string }
	computedBy := map[string]computer{}
	type requirement struct {
		slot  modelSlot
		model string
	}
	requiredSignals := map[string]requirement{}
	signalOrder := []string{}
	// What the models declare for each required signal slot beyond its type:
	// the access it needs and the links it holds, by target slot key.
	slotAccess := map[string]requirement{}
	slotLinks := map[string]map[string]requirement{} // key -> type -> the declaring slot

	// Child slots with the same key merge: every child_model at that key is
	// applied to the same child. Two different keys with the same
	// entity_name are an authoring error, because the second would
	// overwrite the first's queued update of that child; childNameOwner
	// catches it first.
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
			if slot.Access == "read_write" {
				slotAccess[slot.Key] = requirement{slot: slot, model: name}
			}
			for _, link := range slot.Relationships {
				if slotLinks[slot.Key] == nil {
					slotLinks[slot.Key] = map[string]requirement{}
				}
				if prev, seen := slotLinks[slot.Key][link.Type]; seen && slotLinkTarget(prev.slot, link.Type) != link.Target {
					return 409, fmt.Sprintf(
						"model: %s and %s give slot %q different %s targets", prev.model, name, slot.Key, link.Type,
					)
				}
				slotLinks[slot.Key][link.Type] = requirement{slot: slot, model: name}
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

	// Index the element's signals by name in ascending id order, so a
	// duplicate name always resolves to its lowest-id signal.
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

	// The signal filling each required slot after this plan: its id, topic
	// and payload (the queued one where the plan changes it).
	type filled struct {
		id      string
		topic   string
		payload map[string]json.RawMessage
	}
	resolved := map[string]*filled{}
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
			payload := existing.Payload
			if tagID != "" && existingTag == "" {
				payload = cloneRawMap(existing.Payload)
				payload["semantic_type_id"] = rawJSON(tagID)
				if err := state.queue(existing.Record.Topic, payload); err != nil {
					return 422, "model: payload is not encodable"
				}
			}
			resolved[key] = &filled{id: existingID, topic: existing.Record.Topic, payload: payload}
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
		resolved[key] = &filled{id: signalID, topic: topic, payload: payload}
	}

	// Access and links the slots declare land on the signals that fill them.
	// A read_write slot makes its signal read_write; a signal that is
	// read_write where the slot is read does not implement it. A link whose
	// target slot no signal fills here (an optional slot) is left out.
	for _, key := range signalOrder {
		fill := resolved[key]
		combinedKey := modelSlotPath(path, key)
		payload := cloneRawMap(fill.payload)
		current, _ := rawString(payload["access"])
		if current == "" {
			current = "read"
		}
		_, wantsWrite := slotAccess[key]
		switch {
		case wantsWrite && current != "read_write":
			payload["access"] = rawJSON("read_write")
		case !wantsWrite && current == "read_write":
			return 409, fmt.Sprintf(
				"model: slot %q is read, but its signal %s is read_write; a read slot is implemented by a read signal",
				combinedKey, fill.id,
			)
		}
		var links []relationshipLink
		if raw, ok := payload["relationships"]; ok && len(raw) > 0 && string(raw) != "null" {
			if err := json.Unmarshal(raw, &links); err != nil {
				return 422, fmt.Sprintf("model: signal %s has unreadable relationships", fill.id)
			}
		}
		kinds := make([]string, 0, len(slotLinks[key]))
		for kind := range slotLinks[key] {
			kinds = append(kinds, kind)
		}
		sort.Strings(kinds)
		addedLinks := false
		for _, kind := range kinds {
			targetFill := resolved[slotLinkTarget(slotLinks[key][kind].slot, kind)]
			if targetFill == nil {
				continue
			}
			held := -1
			for i, link := range links {
				if link.Type == kind {
					held = i
				}
			}
			switch {
			case held < 0:
				links = append(links, relationshipLink{Type: kind, Target: targetFill.id})
				addedLinks = true
			case links[held].Target != targetFill.id:
				return 409, fmt.Sprintf(
					"model: slot %q needs %s target %s, but its signal %s already names %s",
					combinedKey, kind, targetFill.id, fill.id, links[held].Target,
				)
			}
		}
		if addedLinks {
			payload["relationships"] = rawJSON(links)
		}
		if rawMapsEqual(payload, fill.payload) {
			continue
		}
		if err := state.queue(fill.topic, payload); err != nil {
			return 422, "model: payload is not encodable"
		}
		fill.payload = payload
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

// releaseModelSlots is the recursive unassign. For each child slot of the
// removed models it finds the existing child and, unless a remaining model
// still mandates it, strips that child_model from the child's implements and
// recurses. It only shrinks implements lists; it never creates or deletes.
func (w *EditExec) releaseModelSlots(
	state *modelPlanState,
	element editSnapshot,
	models []string,
	path string,
) (int, string) {
	elementID, _ := rawString(element.Payload["id"])

	// Same cycle guard as resolveModelSlots.
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

	// Group the child models to release by slot key before touching
	// childrenByName, as resolveModelSlots does. Otherwise a second key with
	// the same entity_name would recompute implements from the original
	// list and silently revert the first release. Different keys with the
	// same entity_name are an authoring error; the same key merges.
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
			// The model definition no longer exists, so there is nothing to
			// release below it.
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
				// A remaining model still mandates this child_model on this child:
				// leave it and everything below it alone.
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
