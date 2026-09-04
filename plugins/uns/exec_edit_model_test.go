package uns

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// vocabularyVectorPath is the golden vocabulary dataset (schema-bundle design
// §2, tier 2), the same mechanism vectors_test.go uses for the topic grammar:
// one checked-in file both native copies of the slot data_type vocabulary
// answer to.
const vocabularyVectorPath = "../../contracts/src/colca_data_contracts/vectors/data_model_vocabulary.json"

// Vocabulary pin (architecture principle 2: one owner per fact). slotDataTypes
// is this package's copy of the slot canonical-data_type vocabulary; the one
// definition lives in colca_data_contracts.data_models.loader.CANONICAL_DATA_TYPES.
// This test judges Go against the golden vectors, the contracts suite judges
// the vectors against that definition (tests/test_data_models_vocabulary.py),
// and api's edge/tests/test_model_rules.py judges MODEL_SLOT_DATA_TYPES
// against it too -- so adding a sixth canonical type really does break all
// three sites until they move together.
func TestSlotDataTypesMatchesTheGoldenVocabulary(t *testing.T) {
	raw, err := os.ReadFile(filepath.Clean(vocabularyVectorPath))
	if err != nil {
		t.Fatalf("read vocabulary vectors: %v", err)
	}
	var vectors struct {
		CanonicalDataTypes []string `json:"canonical_data_types"`
	}
	if err := json.Unmarshal(raw, &vectors); err != nil {
		t.Fatalf("decode vocabulary vectors: %v", err)
	}
	if len(vectors.CanonicalDataTypes) == 0 {
		t.Fatal("vocabulary vectors carry no canonical_data_types")
	}
	if len(slotDataTypes) != len(vectors.CanonicalDataTypes) {
		t.Fatalf(
			"slotDataTypes has %d entries, vectors have %d: %+v vs %v",
			len(slotDataTypes), len(vectors.CanonicalDataTypes), slotDataTypes, vectors.CanonicalDataTypes,
		)
	}
	for _, key := range vectors.CanonicalDataTypes {
		if _, ok := slotDataTypes[key]; !ok {
			t.Fatalf("slotDataTypes missing canonical type %q: %+v", key, slotDataTypes)
		}
	}
}

// Test 1: assign creates missing required signals under the element and
// updates implements. Both slots declare a semantic type; the computed slot
// gets "source":"dataops", the measured slot omits "source" entirely.
func TestModelAssignCreatesMissingSignals(t *testing.T) {
	f := newStore("n-edge1")
	elementVersion := seedEditEntity(t, f, "_SystemElement", "press", map[string]any{
		"id": "el-press", "name": "Press",
	})
	seedEditEntity(t, f, "_SemanticTag", "_colca/semantic-tags/device-online", map[string]any{
		"id": "tag-online", "name": "device-online",
	})
	seedEditEntity(t, f, "_SemanticTag", "_colca/semantic-tags/device-connectivity", map[string]any{
		"id": "tag-connectivity", "name": "device-connectivity",
	})
	seedEditEntity(t, f, "_DataModel", "_colca/data-models/machine", map[string]any{
		"id": "dm-machine", "name": "Machine", "version": "1.0",
		"slots": []map[string]any{
			{"key": "heartbeat", "kind": "measured", "data_type": "boolean", "semantic_type": "device-online", "required": true},
			{"key": "is_connected", "kind": "computed", "data_type": "boolean", "semantic_type": "device-connectivity", "required": true},
		},
	})
	exec := NewEditExec(f, nil)

	payload := editBody(t, "op-model-assign", map[string]uint64{
		"system-element:el-press": elementVersion,
	}, map[string]any{
		"type": "model", "action": "assign",
		"entity": map[string]any{"kind": "system-element", "id": "el-press"},
		"models": []string{"Machine"},
		"creates": map[string]string{
			"heartbeat": "sig-heartbeat", "is_connected": "sig-isconnected",
		},
	})
	code, msg, _, writes := exec.ExecuteWithWrites(asHuman, "_CmdEdit", "apply", payload)
	if code != 200 || len(writes) != 3 || f.batchCalls != 1 {
		t.Fatalf("assign create = %d %q writes=%+v batches=%d", code, msg, writes, f.batchCalls)
	}

	heartbeat, ok := f.KVGet("colca/v1/_Signal/n-edge1/press/heartbeat")
	if !ok {
		t.Fatal("assign did not create the heartbeat signal")
	}
	var heartbeatPayload map[string]any
	if err := json.Unmarshal(heartbeat, &heartbeatPayload); err != nil {
		t.Fatal(err)
	}
	if heartbeatPayload["id"] != "sig-heartbeat" || heartbeatPayload["data_type"] != "boolean" ||
		heartbeatPayload["semantic_type_id"] != "tag-online" {
		t.Fatalf("heartbeat signal = %+v", heartbeatPayload)
	}
	if _, hasSource := heartbeatPayload["source"]; hasSource {
		t.Fatalf("measured slot must not set source: %+v", heartbeatPayload)
	}

	connected, ok := f.KVGet("colca/v1/_Signal/n-edge1/press/is_connected")
	if !ok {
		t.Fatal("assign did not create the is_connected signal")
	}
	var connectedPayload map[string]any
	if err := json.Unmarshal(connected, &connectedPayload); err != nil {
		t.Fatal(err)
	}
	if connectedPayload["id"] != "sig-isconnected" || connectedPayload["data_type"] != "boolean" ||
		connectedPayload["semantic_type_id"] != "tag-connectivity" || connectedPayload["source"] != "dataops" {
		t.Fatalf("is_connected signal = %+v", connectedPayload)
	}

	element, _ := f.KVGet("colca/v1/_SystemElement/n-edge1/press")
	var elementPayload map[string]any
	if err := json.Unmarshal(element, &elementPayload); err != nil {
		t.Fatal(err)
	}
	implements, _ := elementPayload["implements"].([]any)
	if len(implements) != 1 || implements[0] != "Machine" {
		t.Fatalf("element implements = %+v", elementPayload["implements"])
	}
}

// Test 2: adopt — an existing signal with the slot name and a compatible
// type is left alone (never recreated), and gains the slot's semantic type.
func TestModelAssignAdoptsCompatibleSignal(t *testing.T) {
	f := newStore("n-edge1")
	elementVersion := seedEditEntity(t, f, "_SystemElement", "press", map[string]any{
		"id": "el-press", "name": "Press",
	})
	signalVersion := seedEditEntity(t, f, "_Signal", "press/heartbeat", map[string]any{
		"id": "sig-heartbeat", "name": "heartbeat", "system_element_id": "el-press", "data_type": "boolean",
	})
	seedEditEntity(t, f, "_SemanticTag", "_colca/semantic-tags/device-online", map[string]any{
		"id": "tag-online", "name": "device-online",
	})
	seedEditEntity(t, f, "_DataModel", "_colca/data-models/machine", map[string]any{
		"id": "dm-machine", "name": "Machine", "version": "1.0",
		"slots": []map[string]any{
			{"key": "heartbeat", "kind": "measured", "data_type": "boolean", "semantic_type": "device-online", "required": true},
		},
	})
	exec := NewEditExec(f, nil)

	payload := editBody(t, "op-model-adopt", map[string]uint64{
		"system-element:el-press": elementVersion,
		"signal:sig-heartbeat":    signalVersion,
	}, map[string]any{
		"type": "model", "action": "assign",
		"entity": map[string]any{"kind": "system-element", "id": "el-press"},
		"models": []string{"Machine"},
	})
	code, msg, _, writes := exec.ExecuteWithWrites(asHuman, "_CmdEdit", "apply", payload)
	if code != 200 || len(writes) != 2 || f.batchCalls != 1 {
		t.Fatalf("assign adopt = %d %q writes=%+v batches=%d", code, msg, writes, f.batchCalls)
	}

	signal, ok := f.KVGet("colca/v1/_Signal/n-edge1/press/heartbeat")
	if !ok {
		t.Fatal("adopt removed the existing signal instead of leaving it in place")
	}
	var signalPayload map[string]any
	if err := json.Unmarshal(signal, &signalPayload); err != nil {
		t.Fatal(err)
	}
	if signalPayload["id"] != "sig-heartbeat" {
		t.Fatalf("adopt must not replace the signal's identity: %+v", signalPayload)
	}
	if signalPayload["semantic_type_id"] != "tag-online" {
		t.Fatalf("adopt did not fill the slot's semantic type: %+v", signalPayload)
	}
}

// Test 3: conflict — name match with the wrong data_type aborts with 409 and
// writes nothing at all (atomicity).
func TestModelAssignTypeConflictAborts(t *testing.T) {
	f := newStore("n-edge1")
	elementVersion := seedEditEntity(t, f, "_SystemElement", "press", map[string]any{
		"id": "el-press", "name": "Press",
	})
	signalVersion := seedEditEntity(t, f, "_Signal", "press/heartbeat", map[string]any{
		"id": "sig-heartbeat", "name": "heartbeat", "system_element_id": "el-press", "data_type": "string",
	})
	seedEditEntity(t, f, "_DataModel", "_colca/data-models/machine", map[string]any{
		"id": "dm-machine", "name": "Machine", "version": "1.0",
		"slots": []map[string]any{
			{"key": "heartbeat", "kind": "measured", "data_type": "boolean", "required": true},
		},
	})
	exec := NewEditExec(f, nil)

	before := f.offset
	payload := editBody(t, "op-model-type-conflict", map[string]uint64{
		"system-element:el-press": elementVersion,
		"signal:sig-heartbeat":    signalVersion,
	}, map[string]any{
		"type": "model", "action": "assign",
		"entity": map[string]any{"kind": "system-element", "id": "el-press"},
		"models": []string{"Machine"},
	})
	code, msg, _, writes := exec.ExecuteWithWrites(asHuman, "_CmdEdit", "apply", payload)
	if code != 409 || len(writes) != 0 || f.offset != before || f.batchCalls != 0 {
		t.Fatalf("type conflict = %d %q writes=%+v offset=%d batches=%d", code, msg, writes, f.offset, f.batchCalls)
	}
	if !strings.Contains(msg, "heartbeat") {
		t.Fatalf("conflict message must name the slot: %q", msg)
	}
}

// Test 4: two desired models computing the same slot key conflict — one
// computer per signal. The two models declare it independently (distinct
// declared_by), which is the dividing line from an inherited slot
// (see TestModelAssignInheritedComputedSlotIsNotAConflict).
func TestModelAssignTwoComputersConflict(t *testing.T) {
	f := newStore("n-edge1")
	elementVersion := seedEditEntity(t, f, "_SystemElement", "press", map[string]any{
		"id": "el-press", "name": "Press",
	})
	seedEditEntity(t, f, "_DataModel", "_colca/data-models/model-a", map[string]any{
		"id": "dm-a", "name": "ModelA", "version": "1.0",
		"slots": []map[string]any{
			{"key": "power", "kind": "computed", "data_type": "number", "required": true, "declared_by": "ModelA"},
		},
	})
	seedEditEntity(t, f, "_DataModel", "_colca/data-models/model-b", map[string]any{
		"id": "dm-b", "name": "ModelB", "version": "1.0",
		"slots": []map[string]any{
			{"key": "power", "kind": "computed", "data_type": "number", "required": true, "declared_by": "ModelB"},
		},
	})
	exec := NewEditExec(f, nil)

	before := f.offset
	payload := editBody(t, "op-model-two-computers", map[string]uint64{
		"system-element:el-press": elementVersion,
	}, map[string]any{
		"type": "model", "action": "assign",
		"entity": map[string]any{"kind": "system-element", "id": "el-press"},
		"models": []string{"ModelA", "ModelB"},
	})
	code, msg, _, writes := exec.ExecuteWithWrites(asHuman, "_CmdEdit", "apply", payload)
	if code != 409 || len(writes) != 0 || f.offset != before || f.batchCalls != 0 {
		t.Fatalf("two computers conflict = %d %q writes=%+v offset=%d batches=%d", code, msg, writes, f.offset, f.batchCalls)
	}
	if !strings.Contains(msg, "power") {
		t.Fatalf("conflict message must name the slot: %q", msg)
	}
}

// Test 5: two desired models sharing a compatible required slot are
// satisfied by one signal — no duplicate creation, no false conflict.
func TestModelAssignSharedRequirementIsOneSignal(t *testing.T) {
	f := newStore("n-edge1")
	elementVersion := seedEditEntity(t, f, "_SystemElement", "press", map[string]any{
		"id": "el-press", "name": "Press",
	})
	signalVersion := seedEditEntity(t, f, "_Signal", "press/heartbeat", map[string]any{
		"id": "sig-heartbeat", "name": "heartbeat", "system_element_id": "el-press", "data_type": "boolean",
	})
	seedEditEntity(t, f, "_SemanticTag", "_colca/semantic-tags/device-online", map[string]any{
		"id": "tag-online", "name": "device-online",
	})
	seedEditEntity(t, f, "_DataModel", "_colca/data-models/model-a", map[string]any{
		"id": "dm-a", "name": "ModelA", "version": "1.0",
		"slots": []map[string]any{
			{"key": "heartbeat", "kind": "measured", "data_type": "boolean", "semantic_type": "device-online", "required": true},
		},
	})
	seedEditEntity(t, f, "_DataModel", "_colca/data-models/model-b", map[string]any{
		"id": "dm-b", "name": "ModelB", "version": "1.0",
		"slots": []map[string]any{
			{"key": "heartbeat", "kind": "measured", "data_type": "boolean", "semantic_type": "device-online", "required": true},
		},
	})
	exec := NewEditExec(f, nil)

	payload := editBody(t, "op-model-shared", map[string]uint64{
		"system-element:el-press": elementVersion,
		"signal:sig-heartbeat":    signalVersion,
	}, map[string]any{
		"type": "model", "action": "assign",
		"entity": map[string]any{"kind": "system-element", "id": "el-press"},
		"models": []string{"ModelA", "ModelB"},
	})
	code, msg, _, writes := exec.ExecuteWithWrites(asHuman, "_CmdEdit", "apply", payload)
	if code != 200 || len(writes) != 2 || f.batchCalls != 1 {
		t.Fatalf("shared requirement = %d %q writes=%+v batches=%d", code, msg, writes, f.batchCalls)
	}

	signal, _ := f.KVGet("colca/v1/_Signal/n-edge1/press/heartbeat")
	var signalPayload map[string]any
	if err := json.Unmarshal(signal, &signalPayload); err != nil {
		t.Fatal(err)
	}
	if signalPayload["semantic_type_id"] != "tag-online" {
		t.Fatalf("shared requirement was not adopted: %+v", signalPayload)
	}

	element, _ := f.KVGet("colca/v1/_SystemElement/n-edge1/press")
	var elementPayload map[string]any
	if err := json.Unmarshal(element, &elementPayload); err != nil {
		t.Fatal(err)
	}
	implements, _ := elementPayload["implements"].([]any)
	if len(implements) != 2 {
		t.Fatalf("element implements = %+v", elementPayload["implements"])
	}
}

// Test 6: an unknown model name in the desired list is a 422.
func TestModelAssignUnknownModel(t *testing.T) {
	f := newStore("n-edge1")
	elementVersion := seedEditEntity(t, f, "_SystemElement", "press", map[string]any{
		"id": "el-press", "name": "Press",
	})
	exec := NewEditExec(f, nil)

	before := f.offset
	payload := editBody(t, "op-model-unknown", map[string]uint64{
		"system-element:el-press": elementVersion,
	}, map[string]any{
		"type": "model", "action": "assign",
		"entity": map[string]any{"kind": "system-element", "id": "el-press"},
		"models": []string{"Nonexistent"},
	})
	code, msg, _, writes := exec.ExecuteWithWrites(asHuman, "_CmdEdit", "apply", payload)
	if code != 422 || len(writes) != 0 || f.offset != before || f.batchCalls != 0 {
		t.Fatalf("unknown model = %d %q writes=%+v offset=%d batches=%d", code, msg, writes, f.offset, f.batchCalls)
	}
	if !strings.Contains(msg, "Nonexistent") {
		t.Fatalf("unknown-model message must name it: %q", msg)
	}
}

// Test 7: unassign shrinks implements and touches nothing else — no signal
// is created, adopted, or otherwise disturbed.
func TestModelUnassignReleasesOnly(t *testing.T) {
	f := newStore("n-edge1")
	elementVersion := seedEditEntity(t, f, "_SystemElement", "press", map[string]any{
		"id": "el-press", "name": "Press", "implements": []string{"Machine", "Other"},
	})
	seedEditEntity(t, f, "_Signal", "press/temperature", map[string]any{
		"id": "sig-temperature", "name": "temperature", "system_element_id": "el-press", "data_type": "float",
	})
	seedEditEntity(t, f, "_DataModel", "_colca/data-models/other", map[string]any{
		"id": "dm-other", "name": "Other", "version": "1.0",
		"slots": []map[string]any{},
	})
	exec := NewEditExec(f, nil)

	payload := editBody(t, "op-model-unassign", map[string]uint64{
		"system-element:el-press": elementVersion,
	}, map[string]any{
		"type": "model", "action": "unassign",
		"entity": map[string]any{"kind": "system-element", "id": "el-press"},
		"models": []string{"Other"},
	})
	code, msg, _, writes := exec.ExecuteWithWrites(asHuman, "_CmdEdit", "apply", payload)
	if code != 200 || len(writes) != 1 || f.batchCalls != 1 {
		t.Fatalf("unassign = %d %q writes=%+v batches=%d", code, msg, writes, f.batchCalls)
	}

	// Presence pins the denominator (the query mechanism finds a real signal
	// when one exists) before the negative claim below is trusted.
	if _, ok := f.KVGet("colca/v1/_Signal/n-edge1/press/temperature"); !ok {
		t.Fatal("unassign must not touch a signal unrelated to the released model")
	}
	if _, ok := f.KVGet("colca/v1/_Signal/n-edge1/press/heartbeat"); ok {
		t.Fatal("unassign must never create or adopt signals")
	}

	element, _ := f.KVGet("colca/v1/_SystemElement/n-edge1/press")
	var elementPayload map[string]any
	if err := json.Unmarshal(element, &elementPayload); err != nil {
		t.Fatal(err)
	}
	implements, _ := elementPayload["implements"].([]any)
	if len(implements) != 1 || implements[0] != "Other" {
		t.Fatalf("element implements = %+v", elementPayload["implements"])
	}
}

// Test 8: a stale expected version on the element aborts with 409, reusing
// the same version-staleness path every other edit intent goes through.
func TestModelAssignRacedElement(t *testing.T) {
	f := newStore("n-edge1")
	elementVersion := seedEditEntity(t, f, "_SystemElement", "press", map[string]any{
		"id": "el-press", "name": "Press",
	})
	seedEditEntity(t, f, "_DataModel", "_colca/data-models/machine", map[string]any{
		"id": "dm-machine", "name": "Machine", "version": "1.0",
		"slots": []map[string]any{
			{"key": "heartbeat", "kind": "measured", "data_type": "boolean", "required": true},
		},
	})
	exec := NewEditExec(f, nil)

	before := f.offset
	payload := editBody(t, "op-model-raced", map[string]uint64{
		"system-element:el-press": elementVersion + 1,
	}, map[string]any{
		"type": "model", "action": "assign",
		"entity":  map[string]any{"kind": "system-element", "id": "el-press"},
		"models":  []string{"Machine"},
		"creates": map[string]string{"heartbeat": "sig-heartbeat"},
	})
	code, msg, _, writes := exec.ExecuteWithWrites(asHuman, "_CmdEdit", "apply", payload)
	if code != 409 || !strings.Contains(msg, "stale_version") || len(writes) != 0 || f.offset != before || f.batchCalls != 0 {
		t.Fatalf("raced element = %d %q writes=%+v offset=%d batches=%d", code, msg, writes, f.offset, f.batchCalls)
	}
}

// The adopt path must use the same compatibility rule
// as composeBinding (compatibleDataTypes), not exact string equality. A
// signal stored as "integer" is what composeBinding accepts for a tag typed
// "int" — a slot mapping canonical "integer" to _Signal type "int" must
// adopt the same signal instead of treating it as a conflict.
func TestModelAssignAdoptsCompatibleButDifferentlySpelledDataType(t *testing.T) {
	f := newStore("n-edge1")
	elementVersion := seedEditEntity(t, f, "_SystemElement", "press", map[string]any{
		"id": "el-press", "name": "Press",
	})
	signalVersion := seedEditEntity(t, f, "_Signal", "press/cycles", map[string]any{
		"id": "sig-cycles", "name": "cycles", "system_element_id": "el-press", "data_type": "integer",
	})
	seedEditEntity(t, f, "_DataModel", "_colca/data-models/machine", map[string]any{
		"id": "dm-machine", "name": "Machine", "version": "1.0",
		"slots": []map[string]any{
			{"key": "cycles", "kind": "measured", "data_type": "integer", "required": true, "declared_by": "Machine"},
		},
	})
	exec := NewEditExec(f, nil)

	payload := editBody(t, "op-model-compatible-spelling", map[string]uint64{
		"system-element:el-press": elementVersion,
		"signal:sig-cycles":       signalVersion,
	}, map[string]any{
		"type": "model", "action": "assign",
		"entity": map[string]any{"kind": "system-element", "id": "el-press"},
		"models": []string{"Machine"},
	})
	code, msg, _, writes := exec.ExecuteWithWrites(asHuman, "_CmdEdit", "apply", payload)
	if code != 200 || len(writes) != 1 || f.batchCalls != 1 {
		t.Fatalf("compatible spelling adopt = %d %q writes=%+v batches=%d", code, msg, writes, f.batchCalls)
	}
	signal, _ := f.KVGet("colca/v1/_Signal/n-edge1/press/cycles")
	var signalPayload map[string]any
	if err := json.Unmarshal(signal, &signalPayload); err != nil {
		t.Fatal(err)
	}
	if signalPayload["id"] != "sig-cycles" {
		t.Fatalf("adopt must not replace the signal: %+v", signalPayload)
	}
}

// A manifest naming an unrecognized canonical
// data_type must be rejected outright, not silently mapped to "" and written
// onto a created signal.
func TestModelAssignUnknownSlotDataTypeIsRejected(t *testing.T) {
	f := newStore("n-edge1")
	elementVersion := seedEditEntity(t, f, "_SystemElement", "press", map[string]any{
		"id": "el-press", "name": "Press",
	})
	seedEditEntity(t, f, "_DataModel", "_colca/data-models/machine", map[string]any{
		"id": "dm-machine", "name": "Machine", "version": "1.0",
		"slots": []map[string]any{
			{"key": "precision", "kind": "measured", "data_type": "decimal", "required": true, "declared_by": "Machine"},
		},
	})
	exec := NewEditExec(f, nil)

	before := f.offset
	payload := editBody(t, "op-model-bad-datatype", map[string]uint64{
		"system-element:el-press": elementVersion,
	}, map[string]any{
		"type": "model", "action": "assign",
		"entity":  map[string]any{"kind": "system-element", "id": "el-press"},
		"models":  []string{"Machine"},
		"creates": map[string]string{"precision": "sig-precision"},
	})
	code, msg, _, writes := exec.ExecuteWithWrites(asHuman, "_CmdEdit", "apply", payload)
	if code != 422 || len(writes) != 0 || f.offset != before || f.batchCalls != 0 {
		t.Fatalf("unknown data_type = %d %q writes=%+v offset=%d batches=%d", code, msg, writes, f.offset, f.batchCalls)
	}
	for _, want := range []string{"Machine", "precision", "decimal"} {
		if !strings.Contains(msg, want) {
			t.Fatalf("message must name the model, slot and value: %q (missing %q)", msg, want)
		}
	}
	if _, ok := f.KVGet("colca/v1/_Signal/n-edge1/press/precision"); ok {
		t.Fatal("a rejected manifest must not create a signal with data_type \"\"")
	}
}

// A slot's unit and description land on a created
// signal when non-empty (Signal has those columns); enum does not (no Signal
// column for it).
func TestModelAssignCreateCarriesUnitAndDescription(t *testing.T) {
	f := newStore("n-edge1")
	elementVersion := seedEditEntity(t, f, "_SystemElement", "press", map[string]any{
		"id": "el-press", "name": "Press",
	})
	seedEditEntity(t, f, "_DataModel", "_colca/data-models/machine", map[string]any{
		"id": "dm-machine", "name": "Machine", "version": "1.0",
		"slots": []map[string]any{
			{
				"key": "temperature", "kind": "measured", "data_type": "number", "required": true,
				"declared_by": "Machine", "unit": "degC", "description": "Bearing temperature",
				"enum": []string{"low", "high"},
			},
		},
	})
	exec := NewEditExec(f, nil)

	payload := editBody(t, "op-model-unit-description", map[string]uint64{
		"system-element:el-press": elementVersion,
	}, map[string]any{
		"type": "model", "action": "assign",
		"entity":  map[string]any{"kind": "system-element", "id": "el-press"},
		"models":  []string{"Machine"},
		"creates": map[string]string{"temperature": "sig-temperature"},
	})
	code, msg, _, writes := exec.ExecuteWithWrites(asHuman, "_CmdEdit", "apply", payload)
	if code != 200 || len(writes) != 2 || f.batchCalls != 1 {
		t.Fatalf("unit/description create = %d %q writes=%+v batches=%d", code, msg, writes, f.batchCalls)
	}
	signal, _ := f.KVGet("colca/v1/_Signal/n-edge1/press/temperature")
	var signalPayload map[string]any
	if err := json.Unmarshal(signal, &signalPayload); err != nil {
		t.Fatal(err)
	}
	if signalPayload["unit"] != "degC" || signalPayload["description"] != "Bearing temperature" {
		t.Fatalf("created signal missing unit/description: %+v", signalPayload)
	}
	if _, hasEnum := signalPayload["enum"]; hasEnum {
		t.Fatalf("enum has no Signal column and must not be written: %+v", signalPayload)
	}
}

// _DataModel (and the _SemanticTag it names) are read
// with KVScanAll, so a definition authored by a DIFFERENT node must still be
// usable — this is the cross-node mechanic the definitions stream relies on
// (groups.go's _Group read is the same shape). Every other test in this file
// seeds through seedEditEntity, which always publishes under this
// node's own id, so swapping KVScanAll for a same-node KVScan would leave
// them green; this test seeds the manifest and tag under "n-authority"
// instead, so only the real cross-node scan can find them.
func TestModelAssignUsesDefinitionsFromAForeignAuthoringNode(t *testing.T) {
	f := newStore("n-edge1")
	elementVersion := seedEditEntity(t, f, "_SystemElement", "press", map[string]any{
		"id": "el-press", "name": "Press",
	})
	foreignSemanticTag := body(t, map[string]any{"id": "tag-online", "name": "device-online"})
	if _, err := f.seed("colca/v1/_SemanticTag/n-authority/_colca/semantic-tags/device-online", foreignSemanticTag); err != nil {
		t.Fatal(err)
	}
	foreignManifest := body(t, map[string]any{
		"id": "dm-machine", "name": "Machine", "version": "1.0",
		"slots": []map[string]any{
			{"key": "heartbeat", "kind": "measured", "data_type": "boolean", "semantic_type": "device-online", "required": true, "declared_by": "Machine"},
		},
	})
	if _, err := f.seed("colca/v1/_DataModel/n-authority/_colca/data-models/machine", foreignManifest); err != nil {
		t.Fatal(err)
	}
	exec := NewEditExec(f, nil)

	payload := editBody(t, "op-model-foreign-authority", map[string]uint64{
		"system-element:el-press": elementVersion,
	}, map[string]any{
		"type": "model", "action": "assign",
		"entity":  map[string]any{"kind": "system-element", "id": "el-press"},
		"models":  []string{"Machine"},
		"creates": map[string]string{"heartbeat": "sig-heartbeat"},
	})
	code, msg, _, writes := exec.ExecuteWithWrites(asHuman, "_CmdEdit", "apply", payload)
	if code != 200 || len(writes) != 2 || f.batchCalls != 1 {
		t.Fatalf("foreign-authored definitions = %d %q writes=%+v batches=%d", code, msg, writes, f.batchCalls)
	}
	signal, ok := f.KVGet("colca/v1/_Signal/n-edge1/press/heartbeat")
	if !ok {
		t.Fatal("assign did not create the heartbeat signal from the foreign-authored manifest")
	}
	var signalPayload map[string]any
	if err := json.Unmarshal(signal, &signalPayload); err != nil {
		t.Fatal(err)
	}
	if signalPayload["semantic_type_id"] != "tag-online" {
		t.Fatalf("did not resolve the foreign-authored semantic tag: %+v", signalPayload)
	}
}

// Creates+updates are capped at editMutationLimit,
// like composeUpdate and composeBinding.
func TestModelAssignRejectsOverTheMutationLimit(t *testing.T) {
	f := newStore("n-edge1")
	elementVersion := seedEditEntity(t, f, "_SystemElement", "press", map[string]any{
		"id": "el-press", "name": "Press",
	})
	slots := make([]map[string]any, 0, editMutationLimit+1)
	creates := map[string]string{}
	for i := 0; i <= editMutationLimit; i++ {
		key := fmt.Sprintf("slot-%03d", i)
		slots = append(slots, map[string]any{
			"key": key, "kind": "measured", "data_type": "boolean", "required": true, "declared_by": "Machine",
		})
		creates[key] = fmt.Sprintf("sig-%03d", i)
	}
	seedEditEntity(t, f, "_DataModel", "_colca/data-models/machine", map[string]any{
		"id": "dm-machine", "name": "Machine", "version": "1.0", "slots": slots,
	})
	exec := NewEditExec(f, nil)

	before := f.offset
	payload := editBody(t, "op-model-over-limit", map[string]uint64{
		"system-element:el-press": elementVersion,
	}, map[string]any{
		"type": "model", "action": "assign",
		"entity":  map[string]any{"kind": "system-element", "id": "el-press"},
		"models":  []string{"Machine"},
		"creates": creates,
	})
	code, msg, _, writes := exec.ExecuteWithWrites(asHuman, "_CmdEdit", "apply", payload)
	if code != 422 || len(writes) != 0 || f.offset != before || f.batchCalls != 0 {
		t.Fatalf("over the mutation limit = %d %q writes=%+v offset=%d batches=%d", code, msg, writes, f.offset, f.batchCalls)
	}
}

// Re-assigning an identical implements list with
// nothing else to change is a no-op — no element write — mirroring
// composeUpdate's update_unchanged path.
func TestModelAssignUnchangedIsANoOp(t *testing.T) {
	f := newStore("n-edge1")
	elementVersion := seedEditEntity(t, f, "_SystemElement", "press", map[string]any{
		"id": "el-press", "name": "Press",
	})
	seedEditEntity(t, f, "_DataModel", "_colca/data-models/other", map[string]any{
		"id": "dm-other", "name": "Other", "version": "1.0",
		"slots": []map[string]any{},
	})
	exec := NewEditExec(f, nil)

	first := editBody(t, "op-model-first", map[string]uint64{
		"system-element:el-press": elementVersion,
	}, map[string]any{
		"type": "model", "action": "assign",
		"entity": map[string]any{"kind": "system-element", "id": "el-press"},
		"models": []string{"Other"},
	})
	code, msg, _, writes := exec.ExecuteWithWrites(asHuman, "_CmdEdit", "apply", first)
	if code != 200 || len(writes) != 1 || f.batchCalls != 1 {
		t.Fatalf("first assign = %d %q writes=%+v batches=%d", code, msg, writes, f.batchCalls)
	}

	second := editBody(t, "op-model-second", map[string]uint64{
		"system-element:el-press": writes[0].Offset,
	}, map[string]any{
		"type": "model", "action": "assign",
		"entity": map[string]any{"kind": "system-element", "id": "el-press"},
		"models": []string{"Other"},
	})
	code, msg, _, writes = exec.ExecuteWithWrites(asHuman, "_CmdEdit", "apply", second)
	if code != 409 || len(writes) != 0 || f.batchCalls != 1 {
		t.Fatalf("unchanged re-assign = %d %q writes=%+v batches=%d", code, msg, writes, f.batchCalls)
	}
	if !strings.Contains(msg, "unchanged") {
		t.Fatalf("no-op message must say so: %q", msg)
	}
}

// Adoption must be deterministic when two signals
// under one element share a slot's name — the LOWEST signal id wins,
// regardless of map iteration order.
func TestModelAssignAdoptsLowestSignalIdOnDuplicateNames(t *testing.T) {
	f := newStore("n-edge1")
	elementVersion := seedEditEntity(t, f, "_SystemElement", "press", map[string]any{
		"id": "el-press", "name": "Press",
	})
	// Seed several duplicate-named signals so the winner cannot be an
	// accident of insertion or map-iteration order.
	seedEditEntity(t, f, "_Signal", "press/heartbeat-c", map[string]any{
		"id": "sig-c-heartbeat", "name": "heartbeat", "system_element_id": "el-press", "data_type": "boolean",
	})
	versionA := seedEditEntity(t, f, "_Signal", "press/heartbeat-a", map[string]any{
		"id": "sig-a-heartbeat", "name": "heartbeat", "system_element_id": "el-press", "data_type": "boolean",
	})
	seedEditEntity(t, f, "_Signal", "press/heartbeat-b", map[string]any{
		"id": "sig-b-heartbeat", "name": "heartbeat", "system_element_id": "el-press", "data_type": "boolean",
	})
	seedEditEntity(t, f, "_DataModel", "_colca/data-models/machine", map[string]any{
		"id": "dm-machine", "name": "Machine", "version": "1.0",
		"slots": []map[string]any{
			{"key": "heartbeat", "kind": "measured", "data_type": "boolean", "required": true, "declared_by": "Machine"},
		},
	})
	exec := NewEditExec(f, nil)

	// Only sig-a-heartbeat's expected version is supplied. If adoption picked
	// any other duplicate, requireExpected would reject it with 422 (missing
	// expected version for that signal) — so a 200 here is only possible
	// because the lowest id was the one matched, deterministically.
	payload := editBody(t, "op-model-duplicate-names", map[string]uint64{
		"system-element:el-press": elementVersion,
		"signal:sig-a-heartbeat":  versionA,
	}, map[string]any{
		"type": "model", "action": "assign",
		"entity": map[string]any{"kind": "system-element", "id": "el-press"},
		"models": []string{"Machine"},
	})
	code, msg, _, writes := exec.ExecuteWithWrites(asHuman, "_CmdEdit", "apply", payload)
	if code != 200 || len(writes) != 1 || f.batchCalls != 1 {
		t.Fatalf("duplicate-name adopt = %d %q writes=%+v batches=%d", code, msg, writes, f.batchCalls)
	}
	if writes[0].Topic != "colca/v1/_SystemElement/n-edge1/press" {
		t.Fatalf("the matched slot needed no change, only the element write was expected: %+v", writes)
	}
}

// Final-fix-wave finding: the adopt path requires an expected version for a
// matched EXISTING signal, same as any other edit mutation of it — a
// caller that omits it must get a 422, not silently skip the check.
func TestModelAssignAdoptMissingExpectedVersionIsRejected(t *testing.T) {
	f := newStore("n-edge1")
	elementVersion := seedEditEntity(t, f, "_SystemElement", "press", map[string]any{
		"id": "el-press", "name": "Press",
	})
	seedEditEntity(t, f, "_Signal", "press/heartbeat", map[string]any{
		"id": "sig-heartbeat", "name": "heartbeat", "system_element_id": "el-press", "data_type": "boolean",
	})
	seedEditEntity(t, f, "_DataModel", "_colca/data-models/machine", map[string]any{
		"id": "dm-machine", "name": "Machine", "version": "1.0",
		"slots": []map[string]any{
			{"key": "heartbeat", "kind": "measured", "data_type": "boolean", "required": true, "declared_by": "Machine"},
		},
	})
	exec := NewEditExec(f, nil)

	before := f.offset
	// Note: no "signal:sig-heartbeat" entry in expected_versions.
	payload := editBody(t, "op-model-adopt-missing-version", map[string]uint64{
		"system-element:el-press": elementVersion,
	}, map[string]any{
		"type": "model", "action": "assign",
		"entity": map[string]any{"kind": "system-element", "id": "el-press"},
		"models": []string{"Machine"},
	})
	code, msg, _, writes := exec.ExecuteWithWrites(asHuman, "_CmdEdit", "apply", payload)
	if code != 422 || len(writes) != 0 || f.offset != before || f.batchCalls != 0 {
		t.Fatalf("adopt missing expected version = %d %q writes=%+v offset=%d batches=%d", code, msg, writes, f.offset, f.batchCalls)
	}
	if !strings.Contains(msg, "sig-heartbeat") {
		t.Fatalf("message must name the signal missing its expected version: %q", msg)
	}
}

// Final-fix-wave finding: a created slot's path is derived only from the
// target element's own path and the slot key -- it must still be rejected as
// a 409 when a signal from a DIFFERENT element already occupies that exact
// path (takenPaths registers every signal's path, not just the target
// element's own, per queueModelSignals' doc comment).
func TestModelAssignCreatePathCollidesWithForeignElementSignal(t *testing.T) {
	f := newStore("n-edge1")
	elementVersion := seedEditEntity(t, f, "_SystemElement", "press", map[string]any{
		"id": "el-press", "name": "Press",
	})
	// A signal owned by a DIFFERENT element ("el-other"), but published at
	// the exact path a "heartbeat" create under el-press would use.
	seedEditEntity(t, f, "_Signal", "press/heartbeat", map[string]any{
		"id": "sig-foreign-heartbeat", "name": "heartbeat", "system_element_id": "el-other", "data_type": "boolean",
	})
	seedEditEntity(t, f, "_DataModel", "_colca/data-models/machine", map[string]any{
		"id": "dm-machine", "name": "Machine", "version": "1.0",
		"slots": []map[string]any{
			{"key": "heartbeat", "kind": "measured", "data_type": "boolean", "required": true, "declared_by": "Machine"},
		},
	})
	exec := NewEditExec(f, nil)

	before := f.offset
	payload := editBody(t, "op-model-path-collision", map[string]uint64{
		"system-element:el-press": elementVersion,
	}, map[string]any{
		"type": "model", "action": "assign",
		"entity":  map[string]any{"kind": "system-element", "id": "el-press"},
		"models":  []string{"Machine"},
		"creates": map[string]string{"heartbeat": "sig-heartbeat"},
	})
	code, msg, _, writes := exec.ExecuteWithWrites(asHuman, "_CmdEdit", "apply", payload)
	if code != 409 || len(writes) != 0 || f.offset != before || f.batchCalls != 0 {
		t.Fatalf("path collision = %d %q writes=%+v offset=%d batches=%d", code, msg, writes, f.offset, f.batchCalls)
	}
	if !strings.Contains(msg, "heartbeat") {
		t.Fatalf("conflict message must name the slot: %q", msg)
	}
}

// Final-fix-wave finding: a slot naming a semantic type with no live
// _SemanticTag entity must abort with 409, naming the slot and the unknown
// tag -- not silently create the signal with an empty semantic_type_id.
func TestModelAssignSlotNamesUnknownSemanticType(t *testing.T) {
	f := newStore("n-edge1")
	elementVersion := seedEditEntity(t, f, "_SystemElement", "press", map[string]any{
		"id": "el-press", "name": "Press",
	})
	// Deliberately no _SemanticTag seeded for "device-online".
	seedEditEntity(t, f, "_DataModel", "_colca/data-models/machine", map[string]any{
		"id": "dm-machine", "name": "Machine", "version": "1.0",
		"slots": []map[string]any{
			{"key": "heartbeat", "kind": "measured", "data_type": "boolean", "semantic_type": "device-online", "required": true, "declared_by": "Machine"},
		},
	})
	exec := NewEditExec(f, nil)

	before := f.offset
	payload := editBody(t, "op-model-unknown-semantic-type", map[string]uint64{
		"system-element:el-press": elementVersion,
	}, map[string]any{
		"type": "model", "action": "assign",
		"entity":  map[string]any{"kind": "system-element", "id": "el-press"},
		"models":  []string{"Machine"},
		"creates": map[string]string{"heartbeat": "sig-heartbeat"},
	})
	code, msg, _, writes := exec.ExecuteWithWrites(asHuman, "_CmdEdit", "apply", payload)
	if code != 409 || len(writes) != 0 || f.offset != before || f.batchCalls != 0 {
		t.Fatalf("unknown semantic type = %d %q writes=%+v offset=%d batches=%d", code, msg, writes, f.offset, f.batchCalls)
	}
	for _, want := range []string{"heartbeat", "device-online"} {
		if !strings.Contains(msg, want) {
			t.Fatalf("message must name the slot and the unknown tag: %q (missing %q)", msg, want)
		}
	}
}

// an ancestor and a descendant model that
// flatten the SAME computed slot (identical declared_by) are one fact, not a
// conflict — contrast TestModelAssignTwoComputersConflict, where the two
// models declare the slot independently.
func TestModelAssignInheritedComputedSlotIsNotAConflict(t *testing.T) {
	f := newStore("n-edge1")
	elementVersion := seedEditEntity(t, f, "_SystemElement", "press", map[string]any{
		"id": "el-press", "name": "Press",
	})
	seedEditEntity(t, f, "_DataModel", "_colca/data-models/machine", map[string]any{
		"id": "dm-machine", "name": "Machine", "version": "1.0",
		"slots": []map[string]any{
			{"key": "state", "kind": "computed", "data_type": "string", "required": true, "declared_by": "Machine"},
		},
	})
	// MachineState extends Machine and does not redeclare "state" — the
	// compiled manifest still carries the slot (MRO flattening), with
	// declared_by unchanged at the base class that actually defined it.
	seedEditEntity(t, f, "_DataModel", "_colca/data-models/machine-state", map[string]any{
		"id": "dm-machine-state", "name": "MachineState", "version": "1.0", "extends": []string{"Machine"},
		"slots": []map[string]any{
			{"key": "state", "kind": "computed", "data_type": "string", "required": true, "declared_by": "Machine"},
		},
	})
	exec := NewEditExec(f, nil)

	payload := editBody(t, "op-model-inherited-computed", map[string]uint64{
		"system-element:el-press": elementVersion,
	}, map[string]any{
		"type": "model", "action": "assign",
		"entity":  map[string]any{"kind": "system-element", "id": "el-press"},
		"models":  []string{"Machine", "MachineState"},
		"creates": map[string]string{"state": "sig-state"},
	})
	code, msg, _, writes := exec.ExecuteWithWrites(asHuman, "_CmdEdit", "apply", payload)
	if code != 200 || len(writes) != 2 || f.batchCalls != 1 {
		t.Fatalf("inherited computed slot = %d %q writes=%+v batches=%d", code, msg, writes, f.batchCalls)
	}
	if _, ok := f.KVGet("colca/v1/_Signal/n-edge1/press/state"); !ok {
		t.Fatal("the inherited computed slot must still be created once")
	}
	element, _ := f.KVGet("colca/v1/_SystemElement/n-edge1/press")
	var elementPayload map[string]any
	if err := json.Unmarshal(element, &elementPayload); err != nil {
		t.Fatal(err)
	}
	implements, _ := elementPayload["implements"].([]any)
	if len(implements) != 2 {
		t.Fatalf("element implements = %+v", elementPayload["implements"])
	}
}

// ---------------------------------------------------------------------------
// Child-model design (the data model children design
// §4): recursive composeModel — child elements and sub-models resolve in one
// atomic batch, and unassign releases them recursively.
// ---------------------------------------------------------------------------

// Task-2 brief test 1: assigning a two-level model creates the mandated
// child element (correct parent path, name from entity_name, "implements"
// carrying the child model), then recurses to create the child model's own
// signal under the CHILD, all in one batch — plus the root's own implements
// update.
func TestModelAssignRecursesIntoChildModel(t *testing.T) {
	f := newStore("n-edge1")
	motorVersion := seedEditEntity(t, f, "_SystemElement", "motor", map[string]any{
		"id": "el-motor", "name": "Motor1",
	})
	seedEditEntity(t, f, "_DataModel", "_colca/data-models/bearing", map[string]any{
		"id": "dm-bearing", "name": "BearingModel", "version": "1.0",
		"slots": []map[string]any{
			{
				"key": "vibration", "kind": "measured", "data_type": "number", "required": true,
				"declared_by": "BearingModel", "child_model": "", "entity_name": "vibration",
			},
		},
	})
	seedEditEntity(t, f, "_DataModel", "_colca/data-models/motor", map[string]any{
		"id": "dm-motor", "name": "Motor", "version": "1.0",
		"slots": []map[string]any{
			{
				"key": "drive_end_bearing", "kind": "child", "data_type": "", "required": true,
				"declared_by": "Motor", "child_model": "BearingModel", "entity_name": "drive_end_bearing",
			},
		},
	})
	exec := NewEditExec(f, nil)

	payload := editBody(t, "op-model-child-assign", map[string]uint64{
		"system-element:el-motor": motorVersion,
	}, map[string]any{
		"type": "model", "action": "assign",
		"entity": map[string]any{"kind": "system-element", "id": "el-motor"},
		"models": []string{"Motor"},
		"creates": map[string]string{
			"drive_end_bearing":           "el-bearing",
			"drive_end_bearing/vibration": "sig-vibration",
		},
	})
	code, msg, _, writes := exec.ExecuteWithWrites(asHuman, "_CmdEdit", "apply", payload)
	if code != 200 || len(writes) != 3 || f.batchCalls != 1 {
		t.Fatalf("recursive child assign = %d %q writes=%+v batches=%d", code, msg, writes, f.batchCalls)
	}

	child, ok := f.KVGet("colca/v1/_SystemElement/n-edge1/motor/drive_end_bearing")
	if !ok {
		t.Fatal("assign did not create the mandated child element at the parent's path")
	}
	var childPayload map[string]any
	if err := json.Unmarshal(child, &childPayload); err != nil {
		t.Fatal(err)
	}
	if childPayload["id"] != "el-bearing" || childPayload["name"] != "drive_end_bearing" || childPayload["parent_id"] != "el-motor" {
		t.Fatalf("child element = %+v", childPayload)
	}
	childImplements, _ := childPayload["implements"].([]any)
	if len(childImplements) != 1 || childImplements[0] != "BearingModel" {
		t.Fatalf("child element implements = %+v", childPayload["implements"])
	}

	grandchild, ok := f.KVGet("colca/v1/_Signal/n-edge1/motor/drive_end_bearing/vibration")
	if !ok {
		t.Fatal("assign did not recurse into the child model's own signal slot")
	}
	var grandchildPayload map[string]any
	if err := json.Unmarshal(grandchild, &grandchildPayload); err != nil {
		t.Fatal(err)
	}
	if grandchildPayload["id"] != "sig-vibration" || grandchildPayload["system_element_id"] != "el-bearing" ||
		grandchildPayload["data_type"] != "float" {
		t.Fatalf("grandchild signal = %+v", grandchildPayload)
	}

	root, _ := f.KVGet("colca/v1/_SystemElement/n-edge1/motor")
	var rootPayload map[string]any
	if err := json.Unmarshal(root, &rootPayload); err != nil {
		t.Fatal(err)
	}
	rootImplements, _ := rootPayload["implements"].([]any)
	if len(rootImplements) != 1 || rootImplements[0] != "Motor" {
		t.Fatalf("root implements = %+v", rootPayload["implements"])
	}
}

// Task-2 brief test 2: adopt — a pre-existing child element matching
// entity_name (a name= override) is adopted, not duplicated; it gains the
// child model in its own implements, and its pre-existing compatible signal
// adopts too.
func TestModelAssignAdoptsExistingChildByName(t *testing.T) {
	f := newStore("n-edge1")
	motorVersion := seedEditEntity(t, f, "_SystemElement", "motor", map[string]any{
		"id": "el-motor", "name": "Motor1",
	})
	bearingVersion := seedEditEntity(t, f, "_SystemElement", "motor/m-101", map[string]any{
		"id": "el-bearing", "name": "M-101 DE Bearing", "parent_id": "el-motor",
	})
	signalVersion := seedEditEntity(t, f, "_Signal", "motor/m-101/vibration", map[string]any{
		"id": "sig-vibration", "name": "vibration", "system_element_id": "el-bearing", "data_type": "float",
	})
	seedEditEntity(t, f, "_DataModel", "_colca/data-models/bearing", map[string]any{
		"id": "dm-bearing", "name": "BearingModel", "version": "1.0",
		"slots": []map[string]any{
			{"key": "vibration", "kind": "measured", "data_type": "number", "required": true, "declared_by": "BearingModel"},
		},
	})
	seedEditEntity(t, f, "_DataModel", "_colca/data-models/motor", map[string]any{
		"id": "dm-motor", "name": "Motor", "version": "1.0",
		"slots": []map[string]any{
			{
				"key": "drive_end_bearing", "kind": "child", "data_type": "", "required": true,
				"declared_by": "Motor", "child_model": "BearingModel", "entity_name": "M-101 DE Bearing",
			},
		},
	})
	exec := NewEditExec(f, nil)

	payload := editBody(t, "op-model-child-adopt", map[string]uint64{
		"system-element:el-motor":   motorVersion,
		"system-element:el-bearing": bearingVersion,
		"signal:sig-vibration":      signalVersion,
	}, map[string]any{
		"type": "model", "action": "assign",
		"entity": map[string]any{"kind": "system-element", "id": "el-motor"},
		"models": []string{"Motor"},
	})
	code, msg, _, writes := exec.ExecuteWithWrites(asHuman, "_CmdEdit", "apply", payload)
	if code != 200 || len(writes) != 2 || f.batchCalls != 1 {
		t.Fatalf("child adopt = %d %q writes=%+v batches=%d", code, msg, writes, f.batchCalls)
	}

	child, ok := f.KVGet("colca/v1/_SystemElement/n-edge1/motor/m-101")
	if !ok {
		t.Fatal("adopt removed the pre-existing child element instead of leaving it in place")
	}
	var childPayload map[string]any
	if err := json.Unmarshal(child, &childPayload); err != nil {
		t.Fatal(err)
	}
	if childPayload["id"] != "el-bearing" {
		t.Fatalf("adopt must not replace the child's identity: %+v", childPayload)
	}
	childImplements, _ := childPayload["implements"].([]any)
	if len(childImplements) != 1 || childImplements[0] != "BearingModel" {
		t.Fatalf("adopted child implements = %+v", childPayload["implements"])
	}

	// The ONLY path a create would ever have used is
	// sanitize(entityName) under the parent's path (sanitize replaces
	// separators/spaces with "_" but does not lowercase or dash) — so the
	// reachable would-be duplicate is "motor/M-101_DE_Bearing", not the two
	// unreachable paths previously asserted here. The presence check above
	// already proves KVGet finds a real record when one exists, so this
	// absence check is not testing a broken query.
	if _, ok := f.KVGet("colca/v1/_SystemElement/n-edge1/motor/M-101_DE_Bearing"); ok {
		t.Fatal("adopt must not additionally create a second child element at the create-derived path")
	}

	signal, ok := f.KVGet("colca/v1/_Signal/n-edge1/motor/m-101/vibration")
	if !ok {
		t.Fatal("adopt removed the child's pre-existing signal")
	}
	var signalPayload map[string]any
	if err := json.Unmarshal(signal, &signalPayload); err != nil {
		t.Fatal(err)
	}
	if signalPayload["id"] != "sig-vibration" {
		t.Fatalf("adopt must not replace the signal's identity: %+v", signalPayload)
	}
}

// Task-2 brief test 3: a wrong-kind name match — a SIGNAL named like the
// mandated child — is a 409 conflict, with zero writes (atomicity).
func TestModelAssignChildSlotWrongKindNameConflict(t *testing.T) {
	f := newStore("n-edge1")
	motorVersion := seedEditEntity(t, f, "_SystemElement", "motor", map[string]any{
		"id": "el-motor", "name": "Motor1",
	})
	seedEditEntity(t, f, "_Signal", "motor/drive_end_bearing", map[string]any{
		"id": "sig-imposter", "name": "drive_end_bearing", "system_element_id": "el-motor", "data_type": "float",
	})
	seedEditEntity(t, f, "_DataModel", "_colca/data-models/bearing", map[string]any{
		"id": "dm-bearing", "name": "BearingModel", "version": "1.0",
		"slots": []map[string]any{},
	})
	seedEditEntity(t, f, "_DataModel", "_colca/data-models/motor", map[string]any{
		"id": "dm-motor", "name": "Motor", "version": "1.0",
		"slots": []map[string]any{
			{
				"key": "drive_end_bearing", "kind": "child", "data_type": "", "required": true,
				"declared_by": "Motor", "child_model": "BearingModel", "entity_name": "drive_end_bearing",
			},
		},
	})
	exec := NewEditExec(f, nil)

	before := f.offset
	payload := editBody(t, "op-model-child-wrong-kind", map[string]uint64{
		"system-element:el-motor": motorVersion,
	}, map[string]any{
		"type": "model", "action": "assign",
		"entity":  map[string]any{"kind": "system-element", "id": "el-motor"},
		"models":  []string{"Motor"},
		"creates": map[string]string{"drive_end_bearing": "el-bearing"},
	})
	code, msg, _, writes := exec.ExecuteWithWrites(asHuman, "_CmdEdit", "apply", payload)
	if code != 409 || len(writes) != 0 || f.offset != before || f.batchCalls != 0 {
		t.Fatalf("child wrong-kind conflict = %d %q writes=%+v offset=%d batches=%d", code, msg, writes, f.offset, f.batchCalls)
	}
	if !strings.Contains(msg, "drive_end_bearing") {
		t.Fatalf("conflict message must name the slot: %q", msg)
	}
}

// Task-2 brief test 4: a child slot referencing a child_model with no live
// _DataModel definition is a 409 naming the missing model.
func TestModelAssignChildSlotUnknownChildModel(t *testing.T) {
	f := newStore("n-edge1")
	motorVersion := seedEditEntity(t, f, "_SystemElement", "motor", map[string]any{
		"id": "el-motor", "name": "Motor1",
	})
	seedEditEntity(t, f, "_DataModel", "_colca/data-models/motor", map[string]any{
		"id": "dm-motor", "name": "Motor", "version": "1.0",
		"slots": []map[string]any{
			{
				"key": "drive_end_bearing", "kind": "child", "data_type": "", "required": true,
				"declared_by": "Motor", "child_model": "MissingBearingModel", "entity_name": "drive_end_bearing",
			},
		},
	})
	exec := NewEditExec(f, nil)

	before := f.offset
	payload := editBody(t, "op-model-child-unknown-model", map[string]uint64{
		"system-element:el-motor": motorVersion,
	}, map[string]any{
		"type": "model", "action": "assign",
		"entity":  map[string]any{"kind": "system-element", "id": "el-motor"},
		"models":  []string{"Motor"},
		"creates": map[string]string{"drive_end_bearing": "el-bearing"},
	})
	code, msg, _, writes := exec.ExecuteWithWrites(asHuman, "_CmdEdit", "apply", payload)
	if code != 409 || len(writes) != 0 || f.offset != before || f.batchCalls != 0 {
		t.Fatalf("unknown child model = %d %q writes=%+v offset=%d batches=%d", code, msg, writes, f.offset, f.batchCalls)
	}
	if !strings.Contains(msg, "MissingBearingModel") {
		t.Fatalf("conflict message must name the missing model: %q", msg)
	}
}

// Task-2 brief test 5: a required missing child with no supplied creates id
// is a 422 naming the slot path.
func TestModelAssignChildSlotMissingCreatesID(t *testing.T) {
	f := newStore("n-edge1")
	motorVersion := seedEditEntity(t, f, "_SystemElement", "motor", map[string]any{
		"id": "el-motor", "name": "Motor1",
	})
	seedEditEntity(t, f, "_DataModel", "_colca/data-models/bearing", map[string]any{
		"id": "dm-bearing", "name": "BearingModel", "version": "1.0",
		"slots": []map[string]any{},
	})
	seedEditEntity(t, f, "_DataModel", "_colca/data-models/motor", map[string]any{
		"id": "dm-motor", "name": "Motor", "version": "1.0",
		"slots": []map[string]any{
			{
				"key": "drive_end_bearing", "kind": "child", "data_type": "", "required": true,
				"declared_by": "Motor", "child_model": "BearingModel", "entity_name": "drive_end_bearing",
			},
		},
	})
	exec := NewEditExec(f, nil)

	before := f.offset
	payload := editBody(t, "op-model-child-missing-creates-id", map[string]uint64{
		"system-element:el-motor": motorVersion,
	}, map[string]any{
		"type": "model", "action": "assign",
		"entity": map[string]any{"kind": "system-element", "id": "el-motor"},
		"models": []string{"Motor"},
	})
	code, msg, _, writes := exec.ExecuteWithWrites(asHuman, "_CmdEdit", "apply", payload)
	if code != 422 || len(writes) != 0 || f.offset != before || f.batchCalls != 0 {
		t.Fatalf("missing creates id = %d %q writes=%+v offset=%d batches=%d", code, msg, writes, f.offset, f.batchCalls)
	}
	if !strings.Contains(msg, "drive_end_bearing") {
		t.Fatalf("message must name the slot path: %q", msg)
	}
}

// Task-2 brief test 6: unassign recomputes what the removed model mandated
// and recursively strips just those model names from the mandated
// children's implements — the child element and its signal always stay
// (presence-pinned). Because implements has no stored provenance, this is
// the SAME list-entry case as a direct assignment of the same model on the
// same child (spec §1 5's documented caveat): a parent unassign
// releases the entry regardless of how it got there.
func TestModelUnassignReleasesRecursively(t *testing.T) {
	f := newStore("n-edge1")
	motorVersion := seedEditEntity(t, f, "_SystemElement", "motor", map[string]any{
		"id": "el-motor", "name": "Motor1",
	})
	seedEditEntity(t, f, "_DataModel", "_colca/data-models/bearing", map[string]any{
		"id": "dm-bearing", "name": "BearingModel", "version": "1.0",
		"slots": []map[string]any{
			{"key": "vibration", "kind": "measured", "data_type": "number", "required": true, "declared_by": "BearingModel"},
		},
	})
	seedEditEntity(t, f, "_DataModel", "_colca/data-models/motor", map[string]any{
		"id": "dm-motor", "name": "Motor", "version": "1.0",
		"slots": []map[string]any{
			{
				"key": "drive_end_bearing", "kind": "child", "data_type": "", "required": true,
				"declared_by": "Motor", "child_model": "BearingModel", "entity_name": "drive_end_bearing",
			},
		},
	})
	exec := NewEditExec(f, nil)

	assignPayload := editBody(t, "op-model-unassign-setup", map[string]uint64{
		"system-element:el-motor": motorVersion,
	}, map[string]any{
		"type": "model", "action": "assign",
		"entity": map[string]any{"kind": "system-element", "id": "el-motor"},
		"models": []string{"Motor"},
		"creates": map[string]string{
			"drive_end_bearing":           "el-bearing",
			"drive_end_bearing/vibration": "sig-vibration",
		},
	})
	code, msg, _, writes := exec.ExecuteWithWrites(asHuman, "_CmdEdit", "apply", assignPayload)
	if code != 200 || len(writes) != 3 || f.batchCalls != 1 {
		t.Fatalf("assign setup = %d %q writes=%+v batches=%d", code, msg, writes, f.batchCalls)
	}
	var childVersion, rootVersion uint64
	for _, write := range writes {
		switch write.Topic {
		case "colca/v1/_SystemElement/n-edge1/motor/drive_end_bearing":
			childVersion = write.Offset
		case "colca/v1/_SystemElement/n-edge1/motor":
			rootVersion = write.Offset
		}
	}
	if childVersion == 0 || rootVersion == 0 {
		t.Fatalf("could not locate child/root versions in assign writes: %+v", writes)
	}

	unassignPayload := editBody(t, "op-model-unassign", map[string]uint64{
		"system-element:el-motor":   rootVersion,
		"system-element:el-bearing": childVersion,
	}, map[string]any{
		"type": "model", "action": "unassign",
		"entity": map[string]any{"kind": "system-element", "id": "el-motor"},
		"models": []string{},
	})
	code, msg, _, writes = exec.ExecuteWithWrites(asHuman, "_CmdEdit", "apply", unassignPayload)
	if code != 200 || len(writes) != 2 || f.batchCalls != 2 {
		t.Fatalf("recursive unassign = %d %q writes=%+v batches=%d", code, msg, writes, f.batchCalls)
	}

	// Presence pins the denominator before the implements-emptied claim below.
	child, ok := f.KVGet("colca/v1/_SystemElement/n-edge1/motor/drive_end_bearing")
	if !ok {
		t.Fatal("unassign must never delete the mandated child element")
	}
	var childPayload map[string]any
	if err := json.Unmarshal(child, &childPayload); err != nil {
		t.Fatal(err)
	}
	if implements, _ := childPayload["implements"].([]any); len(implements) != 0 {
		t.Fatalf("child implements must be released: %+v", childPayload["implements"])
	}
	if _, ok := f.KVGet("colca/v1/_Signal/n-edge1/motor/drive_end_bearing/vibration"); !ok {
		t.Fatal("unassign must never delete the child's signal")
	}

	root, _ := f.KVGet("colca/v1/_SystemElement/n-edge1/motor")
	var rootPayload map[string]any
	if err := json.Unmarshal(root, &rootPayload); err != nil {
		t.Fatal(err)
	}
	if implements, _ := rootPayload["implements"].([]any); len(implements) != 0 {
		t.Fatalf("root implements must be released: %+v", rootPayload["implements"])
	}
}

// Task-2 brief test 7: a model tree whose pending records exceed the
// 200-entity mutation limit is rejected atomically, exactly as a flat
// over-the-limit assign already was.
func TestModelAssignChildTreeRejectsOverTheMutationLimit(t *testing.T) {
	f := newStore("n-edge1")
	motorVersion := seedEditEntity(t, f, "_SystemElement", "motor", map[string]any{
		"id": "el-motor", "name": "Motor1",
	})
	slots := make([]map[string]any, 0, editMutationLimit+1)
	creates := map[string]string{}
	for i := 0; i <= editMutationLimit; i++ {
		key := fmt.Sprintf("child-%03d", i)
		slots = append(slots, map[string]any{
			"key": key, "kind": "child", "data_type": "", "required": true,
			"declared_by": "Big", "child_model": "", "entity_name": key,
		})
		creates[key] = fmt.Sprintf("el-%03d", i)
	}
	seedEditEntity(t, f, "_DataModel", "_colca/data-models/big", map[string]any{
		"id": "dm-big", "name": "Big", "version": "1.0", "slots": slots,
	})
	exec := NewEditExec(f, nil)

	before := f.offset
	payload := editBody(t, "op-model-child-over-limit", map[string]uint64{
		"system-element:el-motor": motorVersion,
	}, map[string]any{
		"type": "model", "action": "assign",
		"entity":  map[string]any{"kind": "system-element", "id": "el-motor"},
		"models":  []string{"Big"},
		"creates": creates,
	})
	code, msg, _, writes := exec.ExecuteWithWrites(asHuman, "_CmdEdit", "apply", payload)
	if code != 422 || len(writes) != 0 || f.offset != before || f.batchCalls != 0 {
		t.Fatalf("child tree over the mutation limit = %d %q writes=%+v offset=%d batches=%d", code, msg, writes, f.offset, f.batchCalls)
	}
}

// Task-2 brief test 8: the vocabulary/kind pin — a "child" slot's empty
// data_type is legal (it is not a canonical type at all), while a non-child
// slot still must name one of the canonical types. Both claims are pinned
// in the SAME test so a change that accidentally exempts every slot, or
// stops exempting "child" slots, breaks it.
func TestModelSlotKindExemptsOnlyChildFromCanonicalDataType(t *testing.T) {
	f := newStore("n-edge1")
	motorVersion := seedEditEntity(t, f, "_SystemElement", "motor", map[string]any{
		"id": "el-motor", "name": "Motor1",
	})

	// Negative: a child slot with data_type "" alongside a non-child slot
	// with an unrecognized data_type is still rejected — for the NON-CHILD
	// slot's sake, not the child slot's empty type.
	seedEditEntity(t, f, "_DataModel", "_colca/data-models/bad-motor", map[string]any{
		"id": "dm-bad-motor", "name": "BadMotor", "version": "1.0",
		"slots": []map[string]any{
			{
				"key": "drive_end_bearing", "kind": "child", "data_type": "", "required": false,
				"declared_by": "BadMotor", "child_model": "", "entity_name": "drive_end_bearing",
			},
			{
				"key": "precision", "kind": "measured", "data_type": "decimal", "required": true,
				"declared_by": "BadMotor",
			},
		},
	})
	exec := NewEditExec(f, nil)

	before := f.offset
	badPayload := editBody(t, "op-model-kind-pin-bad", map[string]uint64{
		"system-element:el-motor": motorVersion,
	}, map[string]any{
		"type": "model", "action": "assign",
		"entity":  map[string]any{"kind": "system-element", "id": "el-motor"},
		"models":  []string{"BadMotor"},
		"creates": map[string]string{"precision": "sig-precision"},
	})
	code, msg, _, writes := exec.ExecuteWithWrites(asHuman, "_CmdEdit", "apply", badPayload)
	if code != 422 || len(writes) != 0 || f.offset != before || f.batchCalls != 0 {
		t.Fatalf("non-child unknown data_type = %d %q writes=%+v offset=%d batches=%d", code, msg, writes, f.offset, f.batchCalls)
	}
	for _, want := range []string{"precision", "decimal"} {
		if !strings.Contains(msg, want) {
			t.Fatalf("rejection must name the NON-CHILD slot, not the child slot: %q (missing %q)", msg, want)
		}
	}
	if strings.Contains(msg, "drive_end_bearing") {
		t.Fatalf("the child slot's empty data_type must not be the reason for rejection: %q", msg)
	}

	// Positive: a manifest with ONLY a child slot (empty data_type) assigns
	// cleanly — an empty data_type is not, by itself, a rejection.
	seedEditEntity(t, f, "_DataModel", "_colca/data-models/bearing", map[string]any{
		"id": "dm-bearing", "name": "BearingModel", "version": "1.0",
		"slots": []map[string]any{},
	})
	seedEditEntity(t, f, "_DataModel", "_colca/data-models/good-motor", map[string]any{
		"id": "dm-good-motor", "name": "GoodMotor", "version": "1.0",
		"slots": []map[string]any{
			{
				"key": "drive_end_bearing", "kind": "child", "data_type": "", "required": true,
				"declared_by": "GoodMotor", "child_model": "BearingModel", "entity_name": "drive_end_bearing",
			},
		},
	})
	goodPayload := editBody(t, "op-model-kind-pin-good", map[string]uint64{
		"system-element:el-motor": motorVersion,
	}, map[string]any{
		"type": "model", "action": "assign",
		"entity":  map[string]any{"kind": "system-element", "id": "el-motor"},
		"models":  []string{"GoodMotor"},
		"creates": map[string]string{"drive_end_bearing": "el-bearing"},
	})
	code, msg, _, writes = exec.ExecuteWithWrites(asHuman, "_CmdEdit", "apply", goodPayload)
	if code != 200 || len(writes) != 2 || f.batchCalls != 1 {
		t.Fatalf("child-only assign with empty data_type = %d %q writes=%+v batches=%d", code, msg, writes, f.batchCalls)
	}
}

// ---------------------------------------------------------------------------
// A same-key merge across models must still work;
// two DIFFERENT keys resolving to the same entity_name must 409 instead of
// silently losing an update; a hand-authored cycle must 409 instead of
// recursing forever; releasing one of two models that independently mandate
// the same child must not release what the other still requires.
// ---------------------------------------------------------------------------

// Regression: two desired models sharing the SAME child slot KEY still
// merge — both child_models land on the one adopted/created child, and both
// get recursed into. This is the case the fix above must NOT turn
// into a conflict.
func TestModelAssignSameChildKeyAcrossModelsMerges(t *testing.T) {
	f := newStore("n-edge1")
	motorVersion := seedEditEntity(t, f, "_SystemElement", "motor", map[string]any{
		"id": "el-motor", "name": "Motor1",
	})
	seedEditEntity(t, f, "_DataModel", "_colca/data-models/bearing-a", map[string]any{
		"id": "dm-bearing-a", "name": "BearingModelA", "version": "1.0",
		"slots": []map[string]any{},
	})
	seedEditEntity(t, f, "_DataModel", "_colca/data-models/bearing-b", map[string]any{
		"id": "dm-bearing-b", "name": "BearingModelB", "version": "1.0",
		"slots": []map[string]any{},
	})
	seedEditEntity(t, f, "_DataModel", "_colca/data-models/motor-a", map[string]any{
		"id": "dm-motor-a", "name": "MotorA", "version": "1.0",
		"slots": []map[string]any{
			{
				"key": "bearing", "kind": "child", "data_type": "", "required": true,
				"declared_by": "MotorA", "child_model": "BearingModelA", "entity_name": "Bearing",
			},
		},
	})
	seedEditEntity(t, f, "_DataModel", "_colca/data-models/motor-b", map[string]any{
		"id": "dm-motor-b", "name": "MotorB", "version": "1.0",
		"slots": []map[string]any{
			{
				"key": "bearing", "kind": "child", "data_type": "", "required": true,
				"declared_by": "MotorB", "child_model": "BearingModelB", "entity_name": "Bearing",
			},
		},
	})
	exec := NewEditExec(f, nil)

	payload := editBody(t, "op-model-same-key-merge", map[string]uint64{
		"system-element:el-motor": motorVersion,
	}, map[string]any{
		"type": "model", "action": "assign",
		"entity":  map[string]any{"kind": "system-element", "id": "el-motor"},
		"models":  []string{"MotorA", "MotorB"},
		"creates": map[string]string{"bearing": "el-bearing"},
	})
	code, msg, _, writes := exec.ExecuteWithWrites(asHuman, "_CmdEdit", "apply", payload)
	if code != 200 || len(writes) != 2 || f.batchCalls != 1 {
		t.Fatalf("same-key merge = %d %q writes=%+v batches=%d", code, msg, writes, f.batchCalls)
	}

	child, ok := f.KVGet("colca/v1/_SystemElement/n-edge1/motor/Bearing")
	if !ok {
		t.Fatal("merge did not create the one shared child")
	}
	var childPayload map[string]any
	if err := json.Unmarshal(child, &childPayload); err != nil {
		t.Fatal(err)
	}
	implements, _ := childPayload["implements"].([]any)
	if len(implements) != 2 {
		t.Fatalf("merged child must carry BOTH mandated models: %+v", childPayload["implements"])
	}
	want := map[string]bool{"BearingModelA": true, "BearingModelB": true}
	for _, name := range implements {
		delete(want, fmt.Sprint(name))
	}
	if len(want) != 0 {
		t.Fatalf("merged child implements missing %v: got %+v", want, childPayload["implements"])
	}
}

// Two DIFFERENT child slot keys that resolve to the
// SAME entity_name is an authoring error, not a merge — it must 409 naming
// both keys and the colliding name, with zero writes. The child ALREADY
// EXISTS here (the adopt path, not the create path): the create path already
// 409s on this via elementPaths (both keys would try to create at the same
// sanitized path), so it does not exercise the bug this guards against.
// Before the fix, the second key's childrenByName lookup returned a STALE
// local copy of the child (adopt mutates only a local struct copy, never the
// childrenByName map), so the second slot's implements addition silently
// overwrote the first's write to the very same topic in "pending" — losing
// the first model name entirely while still returning 200.
func TestModelAssignChildSlotsCollideOnEntityName(t *testing.T) {
	f := newStore("n-edge1")
	motorVersion := seedEditEntity(t, f, "_SystemElement", "motor", map[string]any{
		"id": "el-motor", "name": "Motor1",
	})
	bearingVersion := seedEditEntity(t, f, "_SystemElement", "motor/bearing", map[string]any{
		"id": "el-bearing", "name": "Bearing", "parent_id": "el-motor", "implements": []string{},
	})
	seedEditEntity(t, f, "_DataModel", "_colca/data-models/bearing-a", map[string]any{
		"id": "dm-bearing-a", "name": "BearingModelA", "version": "1.0",
		"slots": []map[string]any{},
	})
	seedEditEntity(t, f, "_DataModel", "_colca/data-models/bearing-b", map[string]any{
		"id": "dm-bearing-b", "name": "BearingModelB", "version": "1.0",
		"slots": []map[string]any{},
	})
	seedEditEntity(t, f, "_DataModel", "_colca/data-models/motor", map[string]any{
		"id": "dm-motor", "name": "Motor", "version": "1.0",
		"slots": []map[string]any{
			{
				"key": "primary_bearing", "kind": "child", "data_type": "", "required": true,
				"declared_by": "Motor", "child_model": "BearingModelA", "entity_name": "Bearing",
			},
			{
				"key": "secondary_bearing", "kind": "child", "data_type": "", "required": true,
				"declared_by": "Motor", "child_model": "BearingModelB", "entity_name": "Bearing",
			},
		},
	})
	exec := NewEditExec(f, nil)

	before := f.offset
	payload := editBody(t, "op-model-child-name-collision", map[string]uint64{
		"system-element:el-motor":   motorVersion,
		"system-element:el-bearing": bearingVersion,
	}, map[string]any{
		"type": "model", "action": "assign",
		"entity": map[string]any{"kind": "system-element", "id": "el-motor"},
		"models": []string{"Motor"},
	})
	code, msg, _, writes := exec.ExecuteWithWrites(asHuman, "_CmdEdit", "apply", payload)
	if code != 409 || len(writes) != 0 || f.offset != before || f.batchCalls != 0 {
		t.Fatalf("colliding child entity names = %d %q writes=%+v offset=%d batches=%d", code, msg, writes, f.offset, f.batchCalls)
	}
	for _, want := range []string{"primary_bearing", "secondary_bearing", "Bearing"} {
		if !strings.Contains(msg, want) {
			t.Fatalf("message must name both keys and the colliding entity name: %q (missing %q)", msg, want)
		}
	}
}

// A hand-authored child_model cycle (ModelX's child
// mandates ModelY, ModelY's child mandates ModelX back) combined with a
// parent_id cycle in the entity graph (el-a's parent is el-b, el-b's parent
// is el-a) must 409 as a cycle, not recurse forever. The Python compiler's
// cycle check cannot see this: it only walks the model graph, never entity
// parent_id data written straight to the KV store (as this test does, same
// as every other test in this file).
func TestModelAssignChildModelCycleWithEntityParentCycleIsRejected(t *testing.T) {
	f := newStore("n-edge1")
	aVersion := seedEditEntity(t, f, "_SystemElement", "a", map[string]any{
		"id": "el-a", "name": "A", "parent_id": "el-b",
	})
	bVersion := seedEditEntity(t, f, "_SystemElement", "b", map[string]any{
		"id": "el-b", "name": "B", "parent_id": "el-a",
	})
	seedEditEntity(t, f, "_DataModel", "_colca/data-models/model-x", map[string]any{
		"id": "dm-model-x", "name": "ModelX", "version": "1.0",
		"slots": []map[string]any{
			{
				"key": "child_of_x", "kind": "child", "data_type": "", "required": false,
				"declared_by": "ModelX", "child_model": "ModelY", "entity_name": "B",
			},
		},
	})
	seedEditEntity(t, f, "_DataModel", "_colca/data-models/model-y", map[string]any{
		"id": "dm-model-y", "name": "ModelY", "version": "1.0",
		"slots": []map[string]any{
			{
				"key": "child_of_y", "kind": "child", "data_type": "", "required": false,
				"declared_by": "ModelY", "child_model": "ModelX", "entity_name": "A",
			},
		},
	})
	exec := NewEditExec(f, nil)

	before := f.offset
	payload := editBody(t, "op-model-cycle", map[string]uint64{
		"system-element:el-a": aVersion,
		"system-element:el-b": bVersion,
	}, map[string]any{
		"type": "model", "action": "assign",
		"entity": map[string]any{"kind": "system-element", "id": "el-a"},
		"models": []string{"ModelX"},
	})
	code, msg, _, writes := exec.ExecuteWithWrites(asHuman, "_CmdEdit", "apply", payload)
	if code != 409 || len(writes) != 0 || f.offset != before || f.batchCalls != 0 {
		t.Fatalf("cyclic child model = %d %q writes=%+v offset=%d batches=%d", code, msg, writes, f.offset, f.batchCalls)
	}
	if !strings.Contains(msg, "cycle") {
		t.Fatalf("message must say cycle: %q", msg)
	}
}

// Unassigning one of two models that independently
// mandate the SAME child_model on the SAME child must not release it while
// the other model stays desired. Only unassigning both actually releases it.
func TestModelUnassignPreservesStillMandatedChild(t *testing.T) {
	f := newStore("n-edge1")
	motorVersion := seedEditEntity(t, f, "_SystemElement", "motor", map[string]any{
		"id": "el-motor", "name": "Motor1", "implements": []string{"ModelA", "ModelB"},
	})
	bearingVersion := seedEditEntity(t, f, "_SystemElement", "motor/bearing", map[string]any{
		"id": "el-bearing", "name": "Bearing", "parent_id": "el-motor", "implements": []string{"BearingModel"},
	})
	seedEditEntity(t, f, "_DataModel", "_colca/data-models/bearing", map[string]any{
		"id": "dm-bearing", "name": "BearingModel", "version": "1.0",
		"slots": []map[string]any{},
	})
	seedEditEntity(t, f, "_DataModel", "_colca/data-models/model-a", map[string]any{
		"id": "dm-model-a", "name": "ModelA", "version": "1.0",
		"slots": []map[string]any{
			{
				"key": "primary_bearing", "kind": "child", "data_type": "", "required": true,
				"declared_by": "ModelA", "child_model": "BearingModel", "entity_name": "Bearing",
			},
		},
	})
	seedEditEntity(t, f, "_DataModel", "_colca/data-models/model-b", map[string]any{
		"id": "dm-model-b", "name": "ModelB", "version": "1.0",
		"slots": []map[string]any{
			{
				"key": "monitored_bearing", "kind": "child", "data_type": "", "required": true,
				"declared_by": "ModelB", "child_model": "BearingModel", "entity_name": "Bearing",
			},
		},
	})
	exec := NewEditExec(f, nil)

	// Unassign ModelA only (desired keeps ModelB) — Bearing must stay
	// exactly as it is: no expected version for it is even supplied, so a
	// wrongly-touched write would 422, not silently succeed.
	firstPayload := editBody(t, "op-model-preserve-first", map[string]uint64{
		"system-element:el-motor": motorVersion,
	}, map[string]any{
		"type": "model", "action": "unassign",
		"entity": map[string]any{"kind": "system-element", "id": "el-motor"},
		"models": []string{"ModelB"},
	})
	code, msg, _, writes := exec.ExecuteWithWrites(asHuman, "_CmdEdit", "apply", firstPayload)
	if code != 200 || len(writes) != 1 || f.batchCalls != 1 {
		t.Fatalf("unassign ModelA (ModelB stays) = %d %q writes=%+v batches=%d", code, msg, writes, f.batchCalls)
	}
	rootVersion := writes[0].Offset

	// Presence pins the denominator before the two claims below: the child
	// still exists, and its implements is untouched.
	child, ok := f.KVGet("colca/v1/_SystemElement/n-edge1/motor/bearing")
	if !ok {
		t.Fatal("unassigning ModelA must never delete the child ModelB still mandates")
	}
	var childPayload map[string]any
	if err := json.Unmarshal(child, &childPayload); err != nil {
		t.Fatal(err)
	}
	implements, _ := childPayload["implements"].([]any)
	if len(implements) != 1 || implements[0] != "BearingModel" {
		t.Fatalf("BearingModel must stay on the child while ModelB still mandates it: %+v", childPayload["implements"])
	}

	root, _ := f.KVGet("colca/v1/_SystemElement/n-edge1/motor")
	var rootPayload map[string]any
	if err := json.Unmarshal(root, &rootPayload); err != nil {
		t.Fatal(err)
	}
	rootImplements, _ := rootPayload["implements"].([]any)
	if len(rootImplements) != 1 || rootImplements[0] != "ModelB" {
		t.Fatalf("root implements after first unassign = %+v", rootPayload["implements"])
	}

	// Now unassign ModelB too (desired becomes empty) — nothing mandates
	// BearingModel on the child any more, so it IS released this time.
	secondPayload := editBody(t, "op-model-preserve-second", map[string]uint64{
		"system-element:el-motor":   rootVersion,
		"system-element:el-bearing": bearingVersion,
	}, map[string]any{
		"type": "model", "action": "unassign",
		"entity": map[string]any{"kind": "system-element", "id": "el-motor"},
		"models": []string{},
	})
	code, msg, _, writes = exec.ExecuteWithWrites(asHuman, "_CmdEdit", "apply", secondPayload)
	if code != 200 || len(writes) != 2 || f.batchCalls != 2 {
		t.Fatalf("unassign ModelB too = %d %q writes=%+v batches=%d", code, msg, writes, f.batchCalls)
	}

	child, ok = f.KVGet("colca/v1/_SystemElement/n-edge1/motor/bearing")
	if !ok {
		t.Fatal("unassign must never delete the child even when fully released")
	}
	if err := json.Unmarshal(child, &childPayload); err != nil {
		t.Fatal(err)
	}
	if implements, _ := childPayload["implements"].([]any); len(implements) != 0 {
		t.Fatalf("BearingModel must now be released: %+v", childPayload["implements"])
	}
}

// Task-5 parked item 1: the release-side stale-copy. releaseModelSlots built
// childrenByName ONCE and never refreshed it, so two REMOVED models whose
// child slots resolve to the SAME entity_name (via different slot keys —
// ModelP's "primary_bearing" and ModelQ's "secondary_bearing" both name
// "Bearing") hit the exact last-write-wins pattern childNameOwner already
// guards against on the assign side (TestModelAssignChildSlotsCollideOnEntityName):
// the second key's childrenByName lookup returns a snapshot the first key's
// queued write never touched, so the second write recomputes "implements"
// from the ORIGINAL list and overwrites the first's queued record at the
// same topic — silently reverting the first model's release while still
// returning 200. Before the fix, this test's own assertions caught it
// directly: code came back 200 (not 409), and the survivor's implements was
// ["BearingModelA"] (BearingModelA's removal reverted) rather than the
// collision this must now report.
func TestModelUnassignChildSlotsCollideOnEntityName(t *testing.T) {
	f := newStore("n-edge1")
	motorVersion := seedEditEntity(t, f, "_SystemElement", "motor", map[string]any{
		"id": "el-motor", "name": "Motor1", "implements": []string{"ModelP", "ModelQ"},
	})
	bearingVersion := seedEditEntity(t, f, "_SystemElement", "motor/bearing", map[string]any{
		"id": "el-bearing", "name": "Bearing", "parent_id": "el-motor",
		"implements": []string{"BearingModelA", "BearingModelB"},
	})
	seedEditEntity(t, f, "_DataModel", "_colca/data-models/bearing-a", map[string]any{
		"id": "dm-bearing-a", "name": "BearingModelA", "version": "1.0",
		"slots": []map[string]any{},
	})
	seedEditEntity(t, f, "_DataModel", "_colca/data-models/bearing-b", map[string]any{
		"id": "dm-bearing-b", "name": "BearingModelB", "version": "1.0",
		"slots": []map[string]any{},
	})
	seedEditEntity(t, f, "_DataModel", "_colca/data-models/model-p", map[string]any{
		"id": "dm-model-p", "name": "ModelP", "version": "1.0",
		"slots": []map[string]any{
			{
				"key": "primary_bearing", "kind": "child", "data_type": "", "required": true,
				"declared_by": "ModelP", "child_model": "BearingModelA", "entity_name": "Bearing",
			},
		},
	})
	seedEditEntity(t, f, "_DataModel", "_colca/data-models/model-q", map[string]any{
		"id": "dm-model-q", "name": "ModelQ", "version": "1.0",
		"slots": []map[string]any{
			{
				"key": "secondary_bearing", "kind": "child", "data_type": "", "required": true,
				"declared_by": "ModelQ", "child_model": "BearingModelB", "entity_name": "Bearing",
			},
		},
	})
	exec := NewEditExec(f, nil)

	before := f.offset
	payload := editBody(t, "op-model-unassign-name-collision", map[string]uint64{
		"system-element:el-motor":   motorVersion,
		"system-element:el-bearing": bearingVersion,
	}, map[string]any{
		"type": "model", "action": "unassign",
		"entity": map[string]any{"kind": "system-element", "id": "el-motor"},
		"models": []string{},
	})
	code, msg, _, writes := exec.ExecuteWithWrites(asHuman, "_CmdEdit", "apply", payload)
	if code != 409 || len(writes) != 0 || f.offset != before || f.batchCalls != 0 {
		t.Fatalf("colliding child entity names on unassign = %d %q writes=%+v offset=%d batches=%d", code, msg, writes, f.offset, f.batchCalls)
	}
	for _, want := range []string{"primary_bearing", "secondary_bearing", "Bearing"} {
		if !strings.Contains(msg, want) {
			t.Fatalf("message must name both keys and the colliding entity name: %q (missing %q)", msg, want)
		}
	}
}

// A create slot's id comes from the caller's own intent.Creates map, and it
// is the one create path in this package that never checked the id was FREE.
// Naming an existing entity's id wrote a second retained record under one
// identity, which no other route can produce: the projector then applies it
// as a rename+reparent of the victim into the caller's subtree (no grant
// anywhere authorized that), and snapshot() answers "duplicate retained
// identity" for every LATER edit command at the node until an operator
// removes a record by hand.
//
// So the refusal is checked from both ends here: the hijack is a 409 that
// writes nothing, and the node still answers the next command normally — the
// node-wide wedge is unreachable, not merely unlikely. The same command with
// a free id then succeeds, which is what stops this test from passing on a
// setup that never reaches the create site at all.
func TestModelAssignRefusesACreateIDThatAlreadyIdentifiesAnEntity(t *testing.T) {
	f := newStore("n-edge1")
	pressVersion := seedEditEntity(t, f, "_SystemElement", "press", map[string]any{
		"id": "el-press", "name": "Press",
	})
	// The victim: an element the caller's command has no business touching.
	seedEditEntity(t, f, "_SystemElement", "vault", map[string]any{
		"id": "el-vault", "name": "Vault",
	})
	seedEditEntity(t, f, "_DataModel", "_colca/data-models/bearing", map[string]any{
		"id": "dm-bearing", "name": "BearingModel", "version": "1.0",
		"slots": []map[string]any{},
	})
	seedEditEntity(t, f, "_DataModel", "_colca/data-models/motor", map[string]any{
		"id": "dm-motor", "name": "Motor", "version": "1.0",
		"slots": []map[string]any{
			{
				"key": "gearbox", "kind": "child", "data_type": "", "required": true,
				"declared_by": "Motor", "child_model": "BearingModel", "entity_name": "Gearbox",
			},
		},
	})
	exec := NewEditExec(f, nil)

	before := f.offset
	hijack := editBody(t, "op-model-create-id-hijack", map[string]uint64{
		"system-element:el-press": pressVersion,
	}, map[string]any{
		"type": "model", "action": "assign",
		"entity":  map[string]any{"kind": "system-element", "id": "el-press"},
		"models":  []string{"Motor"},
		"creates": map[string]string{"gearbox": "el-vault"},
	})
	code, msg, _, writes := exec.ExecuteWithWrites(asHuman, "_CmdEdit", "apply", hijack)
	if code != 409 || len(writes) != 0 || f.offset != before || f.batchCalls != 0 {
		t.Fatalf("create-id hijack = %d %q writes=%+v offset=%d batches=%d", code, msg, writes, f.offset, f.batchCalls)
	}
	if !strings.Contains(msg, "el-vault") || !strings.Contains(msg, "gearbox") {
		t.Fatalf("conflict message must name the slot and the id: %q", msg)
	}
	if held, ok := f.KVGet("colca/v1/_SystemElement/n-edge1/vault"); !ok {
		t.Fatal("the victim's own record must be untouched")
	} else if !strings.Contains(string(held), "Vault") {
		t.Fatalf("victim record = %s", held)
	}

	// The node is not wedged: it still composes a snapshot and answers.
	free := editBody(t, "op-model-create-id-free", map[string]uint64{
		"system-element:el-press": pressVersion,
	}, map[string]any{
		"type": "model", "action": "assign",
		"entity":  map[string]any{"kind": "system-element", "id": "el-press"},
		"models":  []string{"Motor"},
		"creates": map[string]string{"gearbox": "el-gearbox"},
	})
	code, msg, _, writes = exec.ExecuteWithWrites(asHuman, "_CmdEdit", "apply", free)
	if code != 200 || len(writes) != 2 {
		t.Fatalf("free create id = %d %q writes=%+v", code, msg, writes)
	}
	if _, ok := f.KVGet("colca/v1/_SystemElement/n-edge1/press/Gearbox"); !ok {
		t.Fatal("a free create id must still create the mandated child")
	}
}

// The other half of "free": an id nothing holds YET, claimed twice inside one
// batch. Both slots would be written at different paths under one identity —
// the same duplicate-identity state as the hijack above, reached without any
// existing entity being named. The occupancy set therefore grows as the batch
// queues creates, exactly as signalPaths/elementPaths do for position.
func TestModelAssignRefusesTheSameCreateIDTwiceInOneBatch(t *testing.T) {
	f := newStore("n-edge1")
	motorVersion := seedEditEntity(t, f, "_SystemElement", "motor", map[string]any{
		"id": "el-motor", "name": "Motor1",
	})
	seedEditEntity(t, f, "_DataModel", "_colca/data-models/machine", map[string]any{
		"id": "dm-machine", "name": "Machine", "version": "1.0",
		"slots": []map[string]any{
			{"key": "heartbeat", "kind": "measured", "data_type": "boolean", "required": true, "declared_by": "Machine"},
			{"key": "part_counter", "kind": "measured", "data_type": "integer", "required": true, "declared_by": "Machine"},
		},
	})
	exec := NewEditExec(f, nil)

	before := f.offset
	payload := editBody(t, "op-model-create-id-reused", map[string]uint64{
		"system-element:el-motor": motorVersion,
	}, map[string]any{
		"type": "model", "action": "assign",
		"entity":  map[string]any{"kind": "system-element", "id": "el-motor"},
		"models":  []string{"Machine"},
		"creates": map[string]string{"heartbeat": "sig-shared", "part_counter": "sig-shared"},
	})
	code, msg, _, writes := exec.ExecuteWithWrites(asHuman, "_CmdEdit", "apply", payload)
	if code != 409 || len(writes) != 0 || f.offset != before || f.batchCalls != 0 {
		t.Fatalf("reused create id = %d %q writes=%+v offset=%d batches=%d", code, msg, writes, f.offset, f.batchCalls)
	}
	if !strings.Contains(msg, "sig-shared") {
		t.Fatalf("conflict message must name the id: %q", msg)
	}
}
