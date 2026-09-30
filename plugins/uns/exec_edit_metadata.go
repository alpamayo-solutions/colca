// Composing a single-key metadata write. An update intent replaces a record's
// whole metadata map and needs the whole record's version, so two writers of
// different keys on one record refuse or overwrite each other. The metadata
// intent compares and sets one key instead:
//
//	{"type": "metadata", "entity": {"kind": "colca-node" | "system-element" |
//	                                "signal" | "constant" | "resource", "id": "…"},
//	 "key": "<metadata definition id>",
//	 "expect": {"absent": true} | {"value": <json>},
//	 "value": <json> | "remove": true}
//
// The owning node checks only that key against expect, then writes the
// record with that key set or removed. Every other key, and every other
// attribute, is taken from the record as it stands, so a concurrent change to
// another key survives. The record's own version is not required; a caller
// that also sends it in expected_versions still has it checked.

package uns

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
)

// editMetadataExpect is what the caller believes the key holds: absent, or a
// value compared as decoded JSON, so a client need not reproduce Go's
// encoding byte for byte.
type editMetadataExpect struct {
	Absent bool            `json:"absent"`
	Value  json.RawMessage `json:"value"`
}

func (w *EditExec) composeMetadata(
	intent editIntent,
	entities map[string]editSnapshot,
) (int, string, string, []StateRecord) {
	key, err := validateMetadataIntent(intent)
	if err != "" {
		return 422, "metadata: " + err, "invalid", nil
	}
	current, ok := w.metadataTarget(intent, entities)
	if !ok {
		return 409, "metadata: entity_not_found: " + key, "conflict", nil
	}
	metadata := map[string]json.RawMessage{}
	if raw, held := current.Payload["metadata"]; held && !isJSONNull(raw) {
		if err := json.Unmarshal(raw, &metadata); err != nil {
			return 409, "metadata: retained metadata of " + key + " is not an object", "conflict", nil
		}
	}
	held, present := metadata[intent.Key]
	if intent.Expect.Absent {
		if present {
			return 409, "stale_metadata: " + intent.Key, "conflict", nil
		}
	} else if !present || !jsonValuesEqual(held, intent.Expect.Value) {
		return 409, "stale_metadata: " + intent.Key, "conflict", nil
	}

	if intent.Remove {
		if !present {
			return 200, "metadata_unchanged: " + intent.Key, "ok", nil
		}
		delete(metadata, intent.Key)
	} else {
		if present && jsonValuesEqual(held, intent.Value) {
			return 200, "metadata_unchanged: " + intent.Key, "ok", nil
		}
		metadata[intent.Key] = append(json.RawMessage(nil), intent.Value...)
	}
	merged := cloneRawMap(current.Payload)
	merged["metadata"] = rawJSON(metadata)
	payload, marshalErr := json.Marshal(merged)
	if marshalErr != nil {
		return 422, "metadata: value is not encodable", "invalid", nil
	}
	return 200, "set metadata " + intent.Key + " on " + key, "ok",
		[]StateRecord{{Topic: current.Record.Topic, Payload: payload}}
}

// metadataTarget is the record the intent names. Entities come from the
// snapshot; a resource is found by id among this node's _Resource records,
// since the resource intent addresses records by path instead.
func (w *EditExec) metadataTarget(
	intent editIntent, entities map[string]editSnapshot,
) (editSnapshot, bool) {
	key := entityVersionKey(intent.Entity.Kind, intent.Entity.ID)
	if intent.Entity.Kind != "resource" {
		entity, ok := entities[key]
		return entity, ok
	}
	for _, record := range w.store.KVScan(ResourceContract, w.store.NodeID()) {
		if id, ok := ResourceID(record.Payload); !ok || id != intent.Entity.ID {
			continue
		}
		var payload map[string]json.RawMessage
		if err := json.Unmarshal(record.Payload, &payload); err != nil {
			return editSnapshot{}, false
		}
		return editSnapshot{Key: key, Kind: "resource", Record: record, Payload: payload}, true
	}
	return editSnapshot{}, false
}

// validateMetadataIntent checks the intent's shape and returns the entity's
// version key, or why the intent is malformed.
func validateMetadataIntent(intent editIntent) (string, string) {
	entity := intent.Entity
	switch {
	case entity.Kind == "" || entity.ID == "":
		return "", "entity kind and id are required"
	case entity.Kind == "external-reference":
		return "", "external references carry no metadata"
	case entity.Kind != "resource" && editContracts[entity.Kind] == "":
		return "", fmt.Sprintf("unknown entity kind %q", entity.Kind)
	}
	if intent.Key == "" {
		return "", "key is required"
	}
	if intent.Expect == nil {
		return "", "expect is required: {\"absent\": true} or {\"value\": …}"
	}
	expectsValue := len(intent.Expect.Value) > 0
	if intent.Expect.Absent == expectsValue {
		return "", "expect must be exactly one of absent or value"
	}
	setsValue := len(intent.Value) > 0
	if intent.Remove == setsValue {
		return "", "exactly one of value or remove is required"
	}
	if setsValue && isJSONNull(intent.Value) {
		return "", "value must not be null: remove the key instead"
	}
	return entityVersionKey(entity.Kind, entity.ID), ""
}

// metadataPositions is where a metadata intent writes: the entity's own
// position, or for a resource the element it sits on, as for the resource
// intent. A param grant covers it on a constant, as it covers an update of a
// constant's metadata.
func (w *EditExec) metadataPositions(intent editIntent, entities map[string]editSnapshot) []editTouched {
	key := entityVersionKey(intent.Entity.Kind, intent.Entity.ID)
	target, ok := w.metadataTarget(intent, entities)
	if !ok {
		return []editTouched{{path: "", key: key}}
	}
	path := target.Record.Path
	if intent.Entity.Kind == "resource" {
		if i := strings.LastIndex(path, "/"); i >= 0 {
			path = path[:i]
		} else {
			path = ""
		}
	}
	return []editTouched{{path: path, key: key, param: intent.Entity.Kind == "constant"}}
}

func jsonValuesEqual(left, right json.RawMessage) bool {
	var leftValue, rightValue any
	if json.Unmarshal(left, &leftValue) != nil || json.Unmarshal(right, &rightValue) != nil {
		return false
	}
	return reflect.DeepEqual(leftValue, rightValue)
}

func isJSONNull(raw json.RawMessage) bool {
	return bytes.Equal(bytes.TrimSpace(raw), []byte("null"))
}
