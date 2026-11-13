package uns

import (
	"encoding/json"
	"maps"
)

// A data-model signal slot that declares value_definitions owns them: every
// signal filling the slot carries exactly the slot's definition. Assigning the
// model gives the filling signals the slot's definition, creating a signal
// that fills such a slot without one gives it the slot's, and a create or an
// edit of value_definitions that would leave it with any other definition is
// refused with
//
//	value_definitions_owned_by_model: <model>
//
// A signal fills a slot of a model its element implements when its name is
// the slot key. Slots that declare no definition leave the signal's own alone.

// valueDefinitionsOwnedRefusal is the message an edit is refused with when it
// would change a definition a data model owns.
func valueDefinitionsOwnedRefusal(model string) string {
	return "value_definitions_owned_by_model: " + model
}

// slotValueDefinitions names the data model that owns the value definitions
// of a signal called name under elementID, and the definition it declares: the
// first model in the element's implements whose signal slot keyed name
// declares one. Empty when no model owns them, including when the element is
// not held here.
func (w *EditExec) slotValueDefinitions(
	entities map[string]editSnapshot, elementID, name string,
) (string, map[string]string) {
	element, ok := entities[entityVersionKey("system-element", elementID)]
	if !ok || name == "" {
		return "", nil
	}
	implements := stringListFromRaw(element.Payload["implements"])
	if len(implements) == 0 {
		return "", nil
	}
	manifests := map[string]dataModelManifest{}
	for _, rec := range w.store.KVScanAll("_DataModel") {
		var m dataModelManifest
		if json.Unmarshal(rec.Payload, &m) == nil && m.Name != "" {
			manifests[m.Name] = m
		}
	}
	for _, model := range implements {
		for _, slot := range manifests[model].Slots {
			if slot.Kind != "child" && slot.Key == name && len(slot.ValueDefinitions) > 0 {
				return model, slot.ValueDefinitions
			}
		}
	}
	return "", nil
}

// heldValueDefinitions reads a record's value_definitions; nil when absent,
// null or unreadable (the node's record check refuses an unreadable one).
func heldValueDefinitions(raw json.RawMessage) map[string]string {
	if len(raw) == 0 {
		return nil
	}
	var held map[string]string
	if json.Unmarshal(raw, &held) != nil {
		return nil
	}
	return held
}

// sameValueDefinitions compares two definitions; absent and empty are equal.
func sameValueDefinitions(left, right map[string]string) bool {
	if len(left) == 0 && len(right) == 0 {
		return true
	}
	return maps.Equal(left, right)
}

// ownValueDefinitions applies the slot's ownership to a signal record about
// to be written under elementID. A record without a definition gets the
// slot's when fill is true (a signal being created or adopted); a record whose
// definition differs from the slot's is refused. It answers the refusal, or ""
// when the record may be written as it now stands.
func (w *EditExec) ownValueDefinitions(
	entities map[string]editSnapshot, elementID string, payload map[string]json.RawMessage, fill bool,
) string {
	name, _ := rawString(payload["name"])
	model, owned := w.slotValueDefinitions(entities, elementID, name)
	if model == "" {
		return ""
	}
	held := heldValueDefinitions(payload["value_definitions"])
	switch {
	case sameValueDefinitions(held, owned):
		return ""
	case fill && len(held) == 0:
		payload["value_definitions"] = rawJSON(owned)
		return ""
	default:
		return valueDefinitionsOwnedRefusal(model)
	}
}
