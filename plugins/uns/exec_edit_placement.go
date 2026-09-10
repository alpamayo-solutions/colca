// Composing the two position intents: moving an entity and binding a signal to
// a connector's tag. Both check what already occupies the position and answer
// 409 instead of overwriting. Like the entity intents, they never write.

package uns

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

func (w *EditExec) composePlacement(
	intent editIntent,
	expected map[string]uint64,
	entities map[string]editSnapshot,
) (int, string, string, []StateRecord) {
	if intent.Entity.Kind != "system-element" && intent.Entity.Kind != "signal" && intent.Entity.Kind != "constant" {
		return 422, "placement: entity kind is not positionable", "invalid", nil
	}
	current, code, message := requireEntity(expected, entities, intent.Entity.Kind, intent.Entity.ID)
	if code != 0 {
		return code, "placement: " + message, resultFor(code), nil
	}
	target, code, message := requireEntity(expected, entities, "system-element", intent.TargetParentID)
	if code != 0 {
		return code, "placement: " + message, resultFor(code), nil
	}
	if current.Kind == "system-element" &&
		(target.Record.Path == current.Record.Path || strings.HasPrefix(target.Record.Path, current.Record.Path+"/")) {
		return 409, "descendant_cycle", "conflict", nil
	}
	leaf := current.Record.Path
	if index := strings.LastIndexByte(leaf, '/'); index >= 0 {
		leaf = leaf[index+1:]
	}
	newRoot := joinPath(target.Record.Path, leaf)
	if newRoot == current.Record.Path {
		return 409, "placement_unchanged", "conflict", nil
	}

	selected := []editSnapshot{current}
	if current.Kind == "system-element" {
		selected = selected[:0]
		for _, candidate := range entities {
			if candidate.Record.Path == current.Record.Path || strings.HasPrefix(candidate.Record.Path, current.Record.Path+"/") {
				selected = append(selected, candidate)
			}
		}
	}
	for _, candidate := range selected {
		if message := requireExpected(expected, candidate.Key); message != "" {
			return 422, "placement: " + message, "invalid", nil
		}
	}
	if len(selected) > editMutationLimit {
		return 422, fmt.Sprintf("placement: at most %d entities may be changed atomically", editMutationLimit), "invalid", nil
	}
	sort.Slice(selected, func(i, j int) bool { return selected[i].Record.Topic < selected[j].Record.Topic })
	movingTopics := map[string]bool{}
	for _, candidate := range selected {
		movingTopics[candidate.Record.Topic] = true
	}

	records := make([]StateRecord, 0, len(selected)*2)
	for _, candidate := range selected {
		records = append(records, StateRecord{Topic: candidate.Record.Topic})
	}
	for _, candidate := range selected {
		suffix := strings.TrimPrefix(candidate.Record.Path, current.Record.Path)
		newPath := newRoot + suffix
		parsed, err := Parse(candidate.Record.Topic)
		if err != nil {
			return 409, "placement: retained topic is invalid", "conflict", nil
		}
		newTopic := editTopic(parsed.Contract, w.store.NodeID(), newPath)
		if _, occupied := w.store.KVGet(newTopic); occupied && !movingTopics[newTopic] {
			return 409, "duplicate_name: destination is occupied", "conflict", nil
		}
		payload := cloneRawMap(candidate.Payload)
		if candidate.Record.Topic == current.Record.Topic {
			if current.Kind == "system-element" {
				payload["parent_id"] = rawJSON(intent.TargetParentID)
			} else {
				payload["system_element_id"] = rawJSON(intent.TargetParentID)
			}
		}
		encoded, err := json.Marshal(payload)
		if err != nil {
			return 422, "placement: payload is not encodable", "invalid", nil
		}
		records = append(records, StateRecord{Topic: newTopic, Payload: encoded})
	}
	return 200, fmt.Sprintf("moved %d entities", len(selected)), "ok", records
}

func (w *EditExec) composeBinding(
	intent editIntent,
	expected map[string]uint64,
	entities map[string]editSnapshot,
	catalogues map[string]editCatalogueSnapshot,
) (int, string, string, []StateRecord) {
	if intent.ConnectorID == "" {
		return 422, "binding: connector_id is required", "invalid", nil
	}
	if len(intent.Operations) == 0 {
		return 422, "binding: operations are required", "invalid", nil
	}
	if len(intent.Operations) > editMutationLimit {
		return 422, fmt.Sprintf("binding: at most %d operations are allowed", editMutationLimit), "invalid", nil
	}
	catalogue, ok := catalogues[intent.ConnectorID]
	if !ok {
		return 409, "binding: connector catalogue not found", "conflict", nil
	}
	if message := requireExpected(expected, "catalogue:"+intent.ConnectorID); message != "" {
		return 422, "binding: " + message, "invalid", nil
	}

	type bindingTag struct {
		dataType string
		stale    bool
	}
	tags := map[string]bindingTag{}
	for _, tag := range catalogue.Catalogue.DataTags {
		tags[tag.ID] = bindingTag{dataType: tag.DataType, stale: tag.IsStale}
	}
	signals := map[string]editSnapshot{}
	// The binding rule lives in signalBindings. Autobind skips what it may
	// not bind; the edit refuses, so the person sees which binding is in the
	// way.
	bindings := newSignalBindings()
	takenPaths := map[string]bool{}
	for key, entity := range entities {
		if entity.Kind != "signal" {
			continue
		}
		id := strings.TrimPrefix(key, "signal:")
		signals[id] = entity
		takenPaths[entity.Record.Path] = true
		if tagID, _ := rawString(entity.Payload["data_tag"]); tagID != "" {
			bindings.bind(tagID, id)
		}
	}

	pending := map[string]StateRecord{}
	order := []string{}
	operationIDs := map[string]bool{}
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

	for index, operation := range intent.Operations {
		if operation.ID == "" || operationIDs[operation.ID] {
			return 422, fmt.Sprintf("binding: operation %d has a missing or duplicate id", index), "invalid", nil
		}
		operationIDs[operation.ID] = true
		tag, found := tags[operation.TagID]
		if !found {
			return 422, fmt.Sprintf("binding: operation %s names unknown tag %s", operation.ID, operation.TagID), "invalid", nil
		}
		if tag.stale {
			return 409, fmt.Sprintf("binding: tag %s is stale", operation.TagID), "conflict", nil
		}

		switch operation.Kind {
		case "bind":
			signal, found := signals[operation.SignalID]
			if !found {
				return 422, fmt.Sprintf("binding: operation %s names unknown signal %s", operation.ID, operation.SignalID), "invalid", nil
			}
			if message := requireExpected(expected, entityVersionKey("signal", operation.SignalID)); message != "" {
				return 422, "binding: " + message, "invalid", nil
			}
			switch bindings.propose(operation.TagID, operation.SignalID) {
			case bindTagHeld:
				return 409, fmt.Sprintf("binding: tag %s is already bound to %s",
					operation.TagID, bindings.signalHolding(operation.TagID)), "conflict", nil
			case bindSignalHeld:
				return 409, fmt.Sprintf("binding: signal %s is already bound to %s",
					operation.SignalID, bindings.tagHeldBy(operation.SignalID)), "conflict", nil
			}
			// Data type compatibility and staleness are checked by the edit only,
			// not by provisioning.
			signalType, _ := rawString(signal.Payload["data_type"])
			if signalType != "" && tag.dataType != "" && !compatibleDataTypes(signalType, tag.dataType) {
				return 409, fmt.Sprintf("binding: datatype mismatch for operation %s", operation.ID), "conflict", nil
			}
			payload := cloneRawMap(signal.Payload)
			payload["data_tag"] = rawJSON(operation.TagID)
			if err := queue(signal.Record.Topic, payload); err != nil {
				return 422, "binding: payload is not encodable", "invalid", nil
			}
			bindings.bind(operation.TagID, operation.SignalID)
			signal.Payload = payload
			signals[operation.SignalID] = signal

		case "unbind":
			signal, found := signals[operation.SignalID]
			if !found {
				return 422, fmt.Sprintf("binding: operation %s names unknown signal %s", operation.ID, operation.SignalID), "invalid", nil
			}
			if message := requireExpected(expected, entityVersionKey("signal", operation.SignalID)); message != "" {
				return 422, "binding: " + message, "invalid", nil
			}
			if bindings.tagHeldBy(operation.SignalID) != operation.TagID {
				return 409, fmt.Sprintf("binding: signal %s is not bound to tag %s", operation.SignalID, operation.TagID), "conflict", nil
			}
			payload := cloneRawMap(signal.Payload)
			delete(payload, "data_tag")
			if err := queue(signal.Record.Topic, payload); err != nil {
				return 422, "binding: payload is not encodable", "invalid", nil
			}
			bindings.unbind(operation.TagID)
			signal.Payload = payload
			signals[operation.SignalID] = signal

		case "create_signal_and_bind":
			if operation.SignalID == "" || operation.ParentID == "" || operation.Name == "" {
				return 422, fmt.Sprintf("binding: operation %s requires signal_id, parent_id and name", operation.ID), "invalid", nil
			}
			if _, exists := signals[operation.SignalID]; exists {
				return 409, fmt.Sprintf("binding: signal %s already exists", operation.SignalID), "conflict", nil
			}
			// The signal is new (the guard above refused existing ids), so only
			// the tag side can refuse.
			if bindings.propose(operation.TagID, operation.SignalID) != bindFree {
				return 409, fmt.Sprintf("binding: tag %s is already bound to %s",
					operation.TagID, bindings.signalHolding(operation.TagID)), "conflict", nil
			}
			parent, code, message := requireEntity(expected, entities, "system-element", operation.ParentID)
			if code != 0 {
				return code, "binding: " + message, resultFor(code), nil
			}
			path := joinPath(parent.Record.Path, sanitize(operation.Name))
			if takenPaths[path] {
				return 409, fmt.Sprintf("binding: signal name %q collides below target", operation.Name), "conflict", nil
			}
			payload := map[string]json.RawMessage{
				"id": rawJSON(operation.SignalID), "name": rawJSON(operation.Name),
				"system_element_id": rawJSON(operation.ParentID), "data_tag": rawJSON(operation.TagID),
				"is_published": rawJSON(true), "index_type": rawJSON("time"),
			}
			if tag.dataType != "" {
				payload["data_type"] = rawJSON(tag.dataType)
			}
			topic := editTopic("_Signal", w.store.NodeID(), path)
			if err := queue(topic, payload); err != nil {
				return 422, "binding: payload is not encodable", "invalid", nil
			}
			bindings.bind(operation.TagID, operation.SignalID)
			takenPaths[path] = true
			signals[operation.SignalID] = editSnapshot{
				Key: entityVersionKey("signal", operation.SignalID), Kind: "signal",
				Record: KVRecord{Topic: topic, Path: path, NodeID: w.store.NodeID()}, Payload: payload,
			}

		default:
			return 422, fmt.Sprintf("binding: operation %s has unknown kind %q", operation.ID, operation.Kind), "invalid", nil
		}
	}

	records := make([]StateRecord, 0, len(order))
	for _, topic := range order {
		records = append(records, pending[topic])
	}
	return 200, fmt.Sprintf("applied %d binding operations", len(intent.Operations)), "ok", records
}

func compatibleDataTypes(left, right string) bool {
	normalize := func(value string) string {
		value = strings.ToLower(strings.TrimSpace(value))
		value = strings.NewReplacer("_", "", "-", "", " ", "").Replace(value)
		switch value {
		case "int", "int64", "integer":
			return "integer"
		case "float", "float64", "double":
			return "float"
		case "bool", "boolean":
			return "boolean"
		case "str", "string", "text":
			return "string"
		default:
			return value
		}
	}
	return normalize(left) != "" && normalize(left) == normalize(right)
}
