// Composing entity mutations: create, update, delete, and the external
// references that hang off them.
//
// "Compose" is the word on purpose — these functions never write. They turn one
// validated intent into the exact set of state records the command would
// produce, and hand them back to ExecuteWithWrites, which commits them as one
// batch alongside the receipt. Nothing here can half-apply, because nothing
// here applies at all.
package uns

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

func (w *EditExec) compose(
	intent editIntent,
	expected map[string]uint64,
	entities map[string]editSnapshot,
	catalogues map[string]editCatalogueSnapshot,
	externalSystems map[string]bool,
) (int, string, string, []StateRecord) {
	switch intent.Type {
	case "create":
		return w.composeCreate(intent, expected, entities)
	case "update":
		return w.composeUpdate(intent, expected, entities, externalSystems)
	case "delete":
		return w.composeDelete(intent, expected, entities)
	case "placement":
		return w.composePlacement(intent, expected, entities)
	case "binding":
		return w.composeBinding(intent, expected, entities, catalogues)
	case "model":
		return w.composeModel(intent, expected, entities)
	case "annotation":
		return w.composeAnnotation(intent)
	case "resource":
		return w.composeResource(intent)
	case "alarm", "alarm_acknowledgement":
		return w.composeAlarm(intent)
	default:
		return 422, fmt.Sprintf("unknown edit intent %q", intent.Type), "invalid", nil
	}
}

func (w *EditExec) composeCreate(
	intent editIntent,
	expected map[string]uint64,
	entities map[string]editSnapshot,
) (int, string, string, []StateRecord) {
	contract, key, err := validateEntityKey(intent.Entity)
	if err != nil {
		return 422, "create: " + err.Error(), "invalid", nil
	}
	if _, exists := entities[key]; exists {
		return 409, "entity_already_exists: " + key, "conflict", nil
	}
	if len(intent.Attributes) == 0 {
		return 422, "create: attributes are required", "invalid", nil
	}
	attributes := cloneRawMap(intent.Attributes)
	attributes["id"] = rawJSON(intent.Entity.ID)

	var path string
	switch intent.Entity.Kind {
	case "system-element", "signal", "constant":
		parent, code, message := requireEntity(expected, entities, "system-element", intent.ParentID)
		if code != 0 {
			return code, "create: " + message, resultFor(code), nil
		}
		name, nameErr := rawString(attributes["name"])
		if nameErr != nil || name == "" {
			return 422, "create: attributes.name is required", "invalid", nil
		}
		segment := intent.Segment
		if segment == "" {
			segment = sanitize(name)
		}
		path = joinPath(parent.Record.Path, segment)
		if intent.Entity.Kind == "system-element" {
			// An element id becomes a grant zone once grantsync registers it,
			// so it must be an identity and not a wildcard — see ValidElementID.
			if err := ValidElementID(intent.Entity.ID); err != nil {
				return 422, "create: " + err.Error(), "invalid", nil
			}
			attributes["parent_id"] = rawJSON(intent.ParentID)
		} else {
			attributes["system_element_id"] = rawJSON(intent.ParentID)
		}
	case "external-reference":
		path = "_colca/external-references/" + intent.Entity.ID
	default:
		return 422, "create: unsupported entity kind", "invalid", nil
	}
	if err := validatePositionPath(path); err != nil {
		return 422, "create: " + err.Error(), "invalid", nil
	}
	topic := editTopic(contract, w.store.NodeID(), path)
	if _, occupied := w.store.KVGet(topic); occupied {
		return 409, "duplicate_name: position is already occupied", "conflict", nil
	}
	payload, err := json.Marshal(attributes)
	if err != nil {
		return 422, "create: attributes are not encodable", "invalid", nil
	}
	return 200, "created " + key, "ok", []StateRecord{{Topic: topic, Payload: payload}}
}

func (w *EditExec) composeUpdate(
	intent editIntent,
	expected map[string]uint64,
	entities map[string]editSnapshot,
	externalSystems map[string]bool,
) (int, string, string, []StateRecord) {
	_, key, err := validateEntityKey(intent.Entity)
	if err != nil {
		return 422, "update: " + err.Error(), "invalid", nil
	}
	current, code, message := requireEntity(expected, entities, intent.Entity.Kind, intent.Entity.ID)
	if code != 0 {
		return code, "update: " + message, resultFor(code), nil
	}
	if len(intent.Attributes) == 0 && intent.ExternalReferences == nil {
		return 422, "update: attributes or external_references are required", "invalid", nil
	}
	records := []StateRecord{}
	merged := cloneRawMap(current.Payload)
	for name, value := range intent.Attributes {
		merged[name] = append(json.RawMessage(nil), value...)
	}
	merged["id"] = rawJSON(intent.Entity.ID)
	if current.Kind == "system-element" {
		delete(merged, "parent_id")
		if parent, ok := current.Payload["parent_id"]; ok {
			merged["parent_id"] = append(json.RawMessage(nil), parent...)
		}
	}
	if current.Kind == "signal" || current.Kind == "constant" {
		delete(merged, "system_element_id")
		if parent, ok := current.Payload["system_element_id"]; ok {
			merged["system_element_id"] = append(json.RawMessage(nil), parent...)
		}
	}
	if len(intent.Attributes) > 0 && !rawMapsEqual(merged, current.Payload) {
		payload, err := json.Marshal(merged)
		if err != nil {
			return 422, "update: attributes are not encodable", "invalid", nil
		}
		records = append(records, StateRecord{Topic: current.Record.Topic, Payload: payload})
	}
	if intent.ExternalReferences != nil {
		code, message, referenceRecords := w.composeExternalReferences(
			intent, expected, entities, externalSystems,
		)
		if code != 200 {
			return code, "update: " + message, resultFor(code), nil
		}
		records = append(records, referenceRecords...)
	}
	if len(records) == 0 {
		return 409, "update_unchanged: " + key, "conflict", nil
	}
	if len(records) > editMutationLimit {
		return 422, fmt.Sprintf("update: at most %d entities may be changed atomically", editMutationLimit), "invalid", nil
	}
	return 200, fmt.Sprintf("updated %s with %d state changes", key, len(records)), "ok", records
}

func (w *EditExec) composeExternalReferences(
	intent editIntent,
	expected map[string]uint64,
	entities map[string]editSnapshot,
	externalSystems map[string]bool,
) (int, string, []StateRecord) {
	sourceEntity := editSourceEntity(intent.Entity.Kind)
	if sourceEntity == "" {
		return 422, "external references are unsupported for this entity kind", nil
	}
	current := map[string]editSnapshot{}
	for _, candidate := range entities {
		if candidate.Kind != "external-reference" {
			continue
		}
		heldSource, _ := rawString(candidate.Payload["source_entity"])
		heldObject, _ := rawString(candidate.Payload["source_object_id"])
		if heldSource == sourceEntity && heldObject == intent.Entity.ID {
			current[strings.TrimPrefix(candidate.Key, "external-reference:")] = candidate
			if message := requireExpected(expected, candidate.Key); message != "" {
				return 422, message, nil
			}
		}
	}

	desiredIDs := map[string]bool{}
	clientIDs := map[string]bool{}
	semanticKeys := map[string]bool{}
	newRecords := map[string]StateRecord{}
	for index, reference := range *intent.ExternalReferences {
		if reference.ClientID == "" || clientIDs[reference.ClientID] {
			return 422, fmt.Sprintf("external reference %d has a missing or duplicate client_id", index), nil
		}
		clientIDs[reference.ClientID] = true
		if reference.ID == "" || desiredIDs[reference.ID] {
			return 422, fmt.Sprintf("external reference %d has a missing or duplicate id", index), nil
		}
		desiredIDs[reference.ID] = true
		if reference.SourceEntity != sourceEntity || reference.SourceObjectID != intent.Entity.ID {
			return 422, fmt.Sprintf("foreign_reference: %s has invalid source ownership", reference.ID), nil
		}
		if reference.RelationshipType == "" || reference.ExternalSystemID == "" || reference.ExternalTable == "" || reference.ExternalRowID == "" {
			return 422, fmt.Sprintf("external reference %s is incomplete", reference.ID), nil
		}
		if !externalSystems[reference.ExternalSystemID] {
			return 422, "external_system_not_found: " + reference.ExternalSystemID, nil
		}
		semanticKey := strings.Join([]string{
			reference.RelationshipType, reference.ExternalSystemID,
			reference.ExternalTable, reference.ExternalRowID,
		}, "\x00")
		if semanticKeys[semanticKey] {
			return 422, "duplicate_external_reference", nil
		}
		semanticKeys[semanticKey] = true

		key := entityVersionKey("external-reference", reference.ID)
		existing, belongsToSource := current[reference.ID]
		if held, exists := entities[key]; exists && !belongsToSource {
			return 422, "foreign_reference: " + held.Key, nil
		}
		if belongsToSource {
			if reference.Version == "" || reference.Version != strconv.FormatUint(editRecordVersion(existing.Record), 10) {
				return 409, "stale_version: " + key, nil
			}
		} else if reference.Version != "" {
			return 422, "foreign_reference: " + key, nil
		}

		payload := externalReferencePayload(reference)
		if belongsToSource && externalReferenceMatches(existing.Payload, payload) {
			continue
		}
		encoded, err := json.Marshal(payload)
		if err != nil {
			return 422, "external reference payload is not encodable", nil
		}
		topic := editTopic(
			"_ExternalReference", w.store.NodeID(), "_colca/external-references/"+reference.ID,
		)
		newRecords[topic] = StateRecord{Topic: topic, Payload: encoded}
	}

	records := []StateRecord{}
	removedTopics := []string{}
	for id, existing := range current {
		if !desiredIDs[id] {
			removedTopics = append(removedTopics, existing.Record.Topic)
		}
	}
	sort.Strings(removedTopics)
	for _, topic := range removedTopics {
		records = append(records, StateRecord{Topic: topic})
	}
	newTopics := make([]string, 0, len(newRecords))
	for topic := range newRecords {
		newTopics = append(newTopics, topic)
	}
	sort.Strings(newTopics)
	for _, topic := range newTopics {
		records = append(records, newRecords[topic])
	}
	return 200, "external references validated", records
}

func (w *EditExec) composeDelete(
	intent editIntent,
	expected map[string]uint64,
	entities map[string]editSnapshot,
) (int, string, string, []StateRecord) {
	_, key, err := validateEntityKey(intent.Entity)
	if err != nil {
		return 422, "delete: " + err.Error(), "invalid", nil
	}
	current, code, message := requireEntity(expected, entities, intent.Entity.Kind, intent.Entity.ID)
	if code != 0 {
		return code, "delete: " + message, resultFor(code), nil
	}
	selected := []editSnapshot{current}
	if intent.Entity.Kind == "system-element" {
		selected = selected[:0]
		for _, candidate := range entities {
			if candidate.Record.Path == current.Record.Path || strings.HasPrefix(candidate.Record.Path, current.Record.Path+"/") {
				selected = append(selected, candidate)
			}
		}
		if len(selected) > 1 && !intent.Cascade {
			return 409, fmt.Sprintf("delete_impact: %s still contains %d entities", key, len(selected)-1), "conflict", nil
		}
		// Occupancy, judged over the WHOLE selected subtree and independent of
		// cascade. An identity — a child node, a connector — names an element
		// to get its place, so retiring that element leaves it authenticating
		// with nowhere to write: the child is refused at the replication door,
		// the connector's autobind can no longer resolve a mount. Cascade says
		// the caller accepts taking the children with it; it says nothing about
		// participants, which are not entities in this snapshot and are not the
		// caller's to strand. This is the same rule and the same port the
		// `_CmdConfigure` element/delete verb applies (occupantsOf), because two
		// doors retiring the same positions under two rules is how an Edit
		// cascade cut off a node the configure verb refused to touch.
		// Every occupied position is named, sorted, so the refusal reads the
		// same however the snapshot map happened to iterate.
		var occupied []string
		for _, candidate := range selected {
			if candidate.Kind != "system-element" {
				continue
			}
			elementID, _ := rawString(candidate.Payload["id"])
			held := occupantsOf(w.bound, elementID)
			if len(held) == 0 {
				continue
			}
			sort.Strings(held)
			occupied = append(occupied,
				fmt.Sprintf("%s by %s", candidate.Record.Path, strings.Join(held, ", ")))
		}
		if len(occupied) > 0 {
			sort.Strings(occupied)
			return 409, "delete_impact: still bound — " + strings.Join(occupied, "; "), "conflict", nil
		}
	}
	ownedSources := map[string]bool{}
	for _, candidate := range selected {
		sourceEntity := editSourceEntity(candidate.Kind)
		sourceID, _ := rawString(candidate.Payload["id"])
		if sourceEntity != "" && sourceID != "" {
			ownedSources[sourceEntity+"\x00"+sourceID] = true
		}
	}
	for _, candidate := range entities {
		if candidate.Kind != "external-reference" {
			continue
		}
		sourceEntity, _ := rawString(candidate.Payload["source_entity"])
		sourceID, _ := rawString(candidate.Payload["source_object_id"])
		if ownedSources[sourceEntity+"\x00"+sourceID] {
			selected = append(selected, candidate)
		}
	}
	for _, candidate := range selected {
		if message := requireExpected(expected, candidate.Key); message != "" {
			return 422, "delete: " + message, "invalid", nil
		}
	}
	if len(selected) > editMutationLimit {
		return 422, fmt.Sprintf("delete: at most %d entities may be changed atomically", editMutationLimit), "invalid", nil
	}
	sort.Slice(selected, func(i, j int) bool { return selected[i].Record.Topic < selected[j].Record.Topic })
	records := make([]StateRecord, len(selected))
	for i, candidate := range selected {
		records[i] = StateRecord{Topic: candidate.Record.Topic}
	}
	return 200, fmt.Sprintf("deleted %d entities", len(records)), "ok", records
}

func editSourceEntity(kind string) string {
	return map[string]string{
		"system-element": "SystemElement",
		"signal":         "Signal",
		"constant":       "Constant",
		"colca-node":    "Node",
	}[kind]
}

func externalReferencePayload(reference editExternalReference) map[string]json.RawMessage {
	return map[string]json.RawMessage{
		"id":                 rawJSON(reference.ID),
		"source_entity":      rawJSON(reference.SourceEntity),
		"source_object_id":   rawJSON(reference.SourceObjectID),
		"relationship_type":  rawJSON(reference.RelationshipType),
		"external_system_id": rawJSON(reference.ExternalSystemID),
		"external_table":     rawJSON(reference.ExternalTable),
		"external_column":    rawJSON(reference.ExternalColumn),
		"external_row_id":    rawJSON(reference.ExternalRowID),
		"description":        rawJSON(reference.Description),
	}
}

func externalReferenceMatches(
	current map[string]json.RawMessage,
	desired map[string]json.RawMessage,
) bool {
	for _, field := range []string{
		"id", "source_entity", "source_object_id", "relationship_type",
		"external_system_id", "external_table", "external_column",
		"external_row_id", "description",
	} {
		currentValue, currentErr := rawString(current[field])
		desiredValue, desiredErr := rawString(desired[field])
		if currentErr != nil || desiredErr != nil || currentValue != desiredValue {
			return false
		}
	}
	return true
}

func validateEntityKey(entity editEntityKey) (contract, key string, err error) {
	if entity.Kind == "" || entity.ID == "" {
		return "", "", fmt.Errorf("entity kind and id are required")
	}
	contract, ok := editContracts[entity.Kind]
	if !ok {
		return "", "", fmt.Errorf("unknown entity kind %q", entity.Kind)
	}
	return contract, entityVersionKey(entity.Kind, entity.ID), nil
}

func rawMapsEqual(left, right map[string]json.RawMessage) bool {
	leftJSON, leftErr := json.Marshal(left)
	rightJSON, rightErr := json.Marshal(right)
	return leftErr == nil && rightErr == nil && bytes.Equal(leftJSON, rightJSON)
}

var editContracts = map[string]string{
	"system-element":     "_SystemElement",
	"signal":             "_Signal",
	"constant":           "_Constant",
	"colca-node":        "_Node",
	"external-reference": "_ExternalReference",
}
