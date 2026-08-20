package uns

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func editBody(t *testing.T, operationID string, expected map[string]uint64, intent map[string]any) []byte {
	t.Helper()
	wireVersions := map[string]string{}
	for key, version := range expected {
		wireVersions[key] = fmt.Sprint(version)
	}
	return body(t, map[string]any{
		"operation_id":      operationID,
		"correlation_id":    "correlation-" + operationID,
		"expires_at":        9999999999999,
		"expected_versions": wireVersions,
		"intent":            intent,
	})
}

func seedEditEntity(t *testing.T, f *fakeStore, contract, path string, payload map[string]any) uint64 {
	t.Helper()
	write, err := f.Publish("colca/v1/"+contract+"/"+f.NodeID()+"/"+path, body(t, payload))
	if err != nil {
		t.Fatal(err)
	}
	return write.Offset
}

func TestEditVersionUsesOwnerCoordinate(t *testing.T) {
	record := KVRecord{Offset: 91, OriginOffset: 7}
	if got := editRecordVersion(record); got != 7 {
		t.Fatalf("edit version = %d, want owner offset 7", got)
	}
	record.OriginOffset = 0
	if got := editRecordVersion(record); got != 91 {
		t.Fatalf("legacy edit version = %d, want local offset 91", got)
	}
}

func TestEditCreateUpdateDeleteUsesTypedEntityIntent(t *testing.T) {
	f := newStore("n-edge1")
	parentVersion := seedEditEntity(t, f, "_SystemElement", "line1", map[string]any{
		"id": "el-line1", "name": "Line 1",
	})
	exec := NewEditExec(f)

	create := editBody(t, "op-create", map[string]uint64{
		"system-element:el-line1": parentVersion,
	}, map[string]any{
		"type":      "create",
		"entity":    map[string]any{"kind": "constant", "id": "const-speed"},
		"parent_id": "el-line1",
		"attributes": map[string]any{
			"name": "Target speed", "data_type": "int64", "value": 18000,
		},
	})
	code, msg, _, writes := exec.ExecuteWithWrites("_CmdEdit", "apply", create)
	if code != 200 || len(writes) != 1 {
		t.Fatalf("create = %d %q writes=%+v", code, msg, writes)
	}
	constantTopic := "colca/v1/_Constant/n-edge1/line1/Target_speed"
	created, ok := f.KVGet(constantTopic)
	if !ok {
		t.Fatalf("create did not materialize %s", constantTopic)
	}
	var constant map[string]any
	if err := json.Unmarshal(created, &constant); err != nil {
		t.Fatal(err)
	}
	if constant["id"] != "const-speed" || constant["system_element_id"] != "el-line1" {
		t.Fatalf("composed constant = %+v", constant)
	}

	update := editBody(t, "op-update", map[string]uint64{
		"constant:const-speed": writes[0].Offset,
	}, map[string]any{
		"type":   "update",
		"entity": map[string]any{"kind": "constant", "id": "const-speed"},
		"attributes": map[string]any{
			"name": "Target speed", "description": "Nominal line speed",
		},
	})
	code, msg, _, writes = exec.ExecuteWithWrites("_CmdEdit", "apply", update)
	if code != 200 || len(writes) != 1 {
		t.Fatalf("update = %d %q writes=%+v", code, msg, writes)
	}
	updated, _ := f.KVGet(constantTopic)
	if err := json.Unmarshal(updated, &constant); err != nil {
		t.Fatal(err)
	}
	if constant["description"] != "Nominal line speed" || constant["value"] != float64(18000) {
		t.Fatalf("update did not merge with authoritative state: %+v", constant)
	}

	remove := editBody(t, "op-delete", map[string]uint64{
		"constant:const-speed": writes[0].Offset,
	}, map[string]any{
		"type": "delete", "entity": map[string]any{"kind": "constant", "id": "const-speed"},
	})
	code, msg, _, writes = exec.ExecuteWithWrites("_CmdEdit", "apply", remove)
	if code != 200 || len(writes) != 1 {
		t.Fatalf("delete = %d %q writes=%+v", code, msg, writes)
	}
	if _, ok := f.KVGet(constantTopic); ok {
		t.Fatal("delete left the constant retained")
	}
}

func TestEditUpdateComposesEntityAndExternalReferencesInOneBatch(t *testing.T) {
	f := newStore("n-edge1")
	entityVersion := seedEditEntity(t, f, "_SystemElement", "line1", map[string]any{
		"id": "el-line1", "name": "Line 1",
	})
	seedEditEntity(t, f, "_ExternalSystem", "tcdb", map[string]any{
		"id": "ext-tcdb", "key": "tcdb", "name": "TCDB", "system_type": "database",
	})
	keepVersion := seedEditEntity(t, f, "_ExternalReference", "_colca/external-references/ref-keep", map[string]any{
		"id": "ref-keep", "source_entity": "SystemElement", "source_object_id": "el-line1",
		"relationship_type": "access:equipment", "external_system_id": "ext-tcdb",
		"external_table": "equipment", "external_column": "id", "external_row_id": "7",
	})
	removeVersion := seedEditEntity(t, f, "_ExternalReference", "_colca/external-references/ref-remove", map[string]any{
		"id": "ref-remove", "source_entity": "SystemElement", "source_object_id": "el-line1",
		"relationship_type": "maintenance:asset", "external_system_id": "ext-tcdb",
		"external_table": "assets", "external_column": "id", "external_row_id": "old",
	})
	exec := NewEditExec(f)
	payload := editBody(t, "op-reference-compose", map[string]uint64{
		"system-element:el-line1":       entityVersion,
		"external-reference:ref-keep":   keepVersion,
		"external-reference:ref-remove": removeVersion,
	}, map[string]any{
		"type": "update", "entity": map[string]any{"kind": "system-element", "id": "el-line1"},
		"attributes": map[string]any{"description": "Packaging line"},
		"external_references": []map[string]any{
			{
				"client_id": "keep", "id": "ref-keep", "version": fmt.Sprint(keepVersion),
				"source_entity": "SystemElement", "source_object_id": "el-line1",
				"relationship_type": "access:equipment", "external_system_id": "ext-tcdb",
				"external_table": "equipment", "external_column": "id", "external_row_id": "7",
				"description": "Primary equipment",
			},
			{
				"client_id": "new", "id": "ref-new",
				"source_entity": "SystemElement", "source_object_id": "el-line1",
				"relationship_type": "maintenance:asset", "external_system_id": "ext-tcdb",
				"external_table": "assets", "external_column": "id", "external_row_id": "new",
			},
		},
	})

	code, msg, _, writes := exec.ExecuteWithWrites("_CmdEdit", "apply", payload)

	if code != 200 || len(writes) != 4 || f.batchCalls != 1 {
		t.Fatalf("reference composition = %d %q writes=%+v batches=%d", code, msg, writes, f.batchCalls)
	}
	if _, ok := f.KVGet("colca/v1/_ExternalReference/n-edge1/_colca/external-references/ref-remove"); ok {
		t.Fatal("removed reference remains retained")
	}
	for _, topic := range []string{
		"colca/v1/_ExternalReference/n-edge1/_colca/external-references/ref-keep",
		"colca/v1/_ExternalReference/n-edge1/_colca/external-references/ref-new",
	} {
		if _, ok := f.KVGet(topic); !ok {
			t.Fatalf("desired reference missing at %s", topic)
		}
	}
	entity, _ := f.KVGet("colca/v1/_SystemElement/n-edge1/line1")
	var entityPayload map[string]any
	if err := json.Unmarshal(entity, &entityPayload); err != nil {
		t.Fatal(err)
	}
	if entityPayload["description"] != "Packaging line" {
		t.Fatalf("entity update was not in the batch: %+v", entityPayload)
	}

	batchCalls := f.batchCalls
	code, _, _, replayWrites := exec.ExecuteWithWrites("_CmdEdit", "apply", payload)
	if code != 200 || len(replayWrites) != len(writes) || f.batchCalls != batchCalls {
		t.Fatalf("reference replay wrote again: code=%d writes=%+v batches=%d", code, replayWrites, f.batchCalls)
	}
}

func TestEditDeleteTombstonesOwnedExternalReferencesInOneBatch(t *testing.T) {
	f := newStore("n-edge1")
	signalVersion := seedEditEntity(t, f, "_Signal", "line1/temp", map[string]any{
		"id": "sig-temp", "name": "Temperature", "system_element_id": "el-line1",
	})
	referenceVersion := seedEditEntity(t, f, "_ExternalReference", "_colca/external-references/ref-temp", map[string]any{
		"id": "ref-temp", "source_entity": "Signal", "source_object_id": "sig-temp",
		"relationship_type": "maintenance:asset", "external_system_id": "ext-cmms",
		"external_table": "assets", "external_row_id": "A-7",
	})
	exec := NewEditExec(f)
	intent := map[string]any{
		"type": "delete", "entity": map[string]any{"kind": "signal", "id": "sig-temp"},
		"cascade": false,
	}

	before := f.offset
	code, _, _, writes := exec.ExecuteWithWrites("_CmdEdit", "apply", editBody(
		t, "op-delete-missing-reference-version", map[string]uint64{
			"signal:sig-temp": signalVersion,
		}, intent,
	))
	if code != 422 || len(writes) != 0 || f.offset != before || f.batchCalls != 0 {
		t.Fatalf("missing reference version = %d writes=%+v offset=%d batches=%d", code, writes, f.offset, f.batchCalls)
	}

	code, msg, _, writes := exec.ExecuteWithWrites("_CmdEdit", "apply", editBody(
		t, "op-delete-with-reference", map[string]uint64{
			"signal:sig-temp":             signalVersion,
			"external-reference:ref-temp": referenceVersion,
		}, intent,
	))
	if code != 200 || len(writes) != 2 || f.batchCalls != 1 {
		t.Fatalf("delete with reference = %d %q writes=%+v batches=%d", code, msg, writes, f.batchCalls)
	}
	for _, topic := range []string{
		"colca/v1/_Signal/n-edge1/line1/temp",
		"colca/v1/_ExternalReference/n-edge1/_colca/external-references/ref-temp",
	} {
		if _, ok := f.KVGet(topic); ok {
			t.Fatalf("delete left retained state at %s", topic)
		}
	}
}

func TestEditReferenceOnlyUpdateRejectsInvalidAndNoopWithoutWriting(t *testing.T) {
	f := newStore("n-edge1")
	entityVersion := seedEditEntity(t, f, "_Signal", "line1/temp", map[string]any{
		"id": "sig-temp", "name": "Temperature", "system_element_id": "el-line1",
	})
	seedEditEntity(t, f, "_ExternalSystem", "tcdb", map[string]any{
		"id": "ext-tcdb", "key": "tcdb", "name": "TCDB", "system_type": "database",
	})
	exec := NewEditExec(f)
	baseIntent := map[string]any{
		"type": "update", "entity": map[string]any{"kind": "signal", "id": "sig-temp"},
	}

	invalid := map[string]any{}
	for key, value := range baseIntent {
		invalid[key] = value
	}
	invalid["external_references"] = []map[string]any{{
		"client_id": "new", "id": "ref-new", "source_entity": "Signal", "source_object_id": "other",
		"relationship_type": "access:equipment", "external_system_id": "ext-missing",
		"external_table": "equipment", "external_row_id": "7",
	}}
	before := f.offset
	code, _, _, writes := exec.ExecuteWithWrites("_CmdEdit", "apply", editBody(
		t, "op-invalid-reference", map[string]uint64{"signal:sig-temp": entityVersion}, invalid,
	))
	if code != 422 || len(writes) != 0 || f.offset != before || f.batchCalls != 0 {
		t.Fatalf("invalid reference = %d writes=%+v offset=%d batches=%d", code, writes, f.offset, f.batchCalls)
	}

	noop := map[string]any{}
	for key, value := range baseIntent {
		noop[key] = value
	}
	noop["external_references"] = []map[string]any{}
	code, _, _, writes = exec.ExecuteWithWrites("_CmdEdit", "apply", editBody(
		t, "op-reference-noop", map[string]uint64{"signal:sig-temp": entityVersion}, noop,
	))
	if code != 409 || len(writes) != 0 || f.offset != before || f.batchCalls != 0 {
		t.Fatalf("reference no-op = %d writes=%+v offset=%d batches=%d", code, writes, f.offset, f.batchCalls)
	}
}

func TestEditPlacementMovesAWholeEntitySubtreeInOneBatch(t *testing.T) {
	f := newStore("n-edge1")
	seedEditEntity(t, f, "_SystemElement", "line1", map[string]any{"id": "el-line1", "name": "Line 1"})
	machineVersion := seedEditEntity(t, f, "_SystemElement", "line1/machine", map[string]any{
		"id": "el-machine", "name": "Machine", "parent_id": "el-line1",
	})
	signalVersion := seedEditEntity(t, f, "_Signal", "line1/machine/temp", map[string]any{
		"id": "sig-temp", "name": "Temperature", "system_element_id": "el-machine",
	})
	targetVersion := seedEditEntity(t, f, "_SystemElement", "line2", map[string]any{
		"id": "el-line2", "name": "Line 2",
	})
	exec := NewEditExec(f)

	code, msg, _, writes := exec.ExecuteWithWrites("_CmdEdit", "apply", editBody(
		t, "op-move", map[string]uint64{
			"system-element:el-machine": machineVersion,
			"system-element:el-line2":   targetVersion,
			"signal:sig-temp":           signalVersion,
		}, map[string]any{
			"type": "placement", "entity": map[string]any{"kind": "system-element", "id": "el-machine"},
			"target_parent_id": "el-line2",
		},
	))
	if code != 200 || len(writes) != 4 || f.batchCalls != 1 {
		t.Fatalf("placement = %d %q writes=%+v batch_calls=%d", code, msg, writes, f.batchCalls)
	}
	for _, topic := range []string{
		"colca/v1/_SystemElement/n-edge1/line2/machine",
		"colca/v1/_Signal/n-edge1/line2/machine/temp",
	} {
		if _, ok := f.KVGet(topic); !ok {
			t.Errorf("moved state missing at %s", topic)
		}
	}
	if _, ok := f.KVGet("colca/v1/_SystemElement/n-edge1/line1/machine"); ok {
		t.Fatal("placement left the old element topic retained")
	}
}

func TestEditBindingValidatesWholeMixedBatchBeforeWriting(t *testing.T) {
	f := newStore("n-edge1")
	parentVersion := seedEditEntity(t, f, "_SystemElement", "line1", map[string]any{
		"id": "el-line1", "name": "Line 1",
	})
	catalogVersion := seedEditEntity(t, f, "_DataTags", "connector-1", map[string]any{
		"connector": "connector-1",
		"data_tags": []map[string]any{
			{"id": "tag-1", "name": "Temperature", "data_type": "float", "is_stale": false},
			{"id": "tag-2", "name": "Speed", "data_type": "int", "is_stale": false},
		},
	})
	signalVersion := seedEditEntity(t, f, "_Signal", "line1/existing", map[string]any{
		"id": "sig-existing", "name": "Existing", "system_element_id": "el-line1", "data_type": "float",
	})
	exec := NewEditExec(f)

	valid := editBody(t, "op-binding", map[string]uint64{
		"catalogue:connector-1":   catalogVersion,
		"system-element:el-line1": parentVersion,
		"signal:sig-existing":     signalVersion,
	}, map[string]any{
		"type": "binding", "connector_id": "connector-1",
		"operations": []map[string]any{
			{"id": "bind-existing", "kind": "bind", "tag_id": "tag-1", "signal_id": "sig-existing"},
			{"id": "create-speed", "kind": "create_signal_and_bind", "tag_id": "tag-2", "signal_id": "sig-speed", "parent_id": "el-line1", "name": "Speed"},
		},
	})
	code, msg, _, writes := exec.ExecuteWithWrites("_CmdEdit", "apply", valid)
	if code != 200 || len(writes) != 2 || f.batchCalls != 1 {
		t.Fatalf("binding = %d %q writes=%+v batch_calls=%d", code, msg, writes, f.batchCalls)
	}

	before := f.offset
	invalid := editBody(t, "op-binding-invalid", map[string]uint64{
		"catalogue:connector-1": catalogVersion,
		"signal:sig-existing":   writes[0].Offset,
	}, map[string]any{
		"type": "binding", "connector_id": "connector-1",
		"operations": []map[string]any{
			{"id": "unbind-existing", "kind": "unbind", "tag_id": "tag-1", "signal_id": "sig-existing"},
			{"id": "late-invalid", "kind": "bind", "tag_id": "missing", "signal_id": "sig-existing"},
		},
	})
	code, _, _, _ = exec.ExecuteWithWrites("_CmdEdit", "apply", invalid)
	if code != 422 || f.offset != before || f.batchCalls != 1 {
		t.Fatalf("late invalid binding = %d offset=%d batch_calls=%d, want no write", code, f.offset, f.batchCalls)
	}
}

func TestEditOperationReplayIsExactAndConflictingReuseIsRejected(t *testing.T) {
	f := newStore("n-edge1")
	parentVersion := seedEditEntity(t, f, "_SystemElement", "line1", map[string]any{
		"id": "el-line1", "name": "Line 1",
	})
	exec := NewEditExec(f)
	payload := editBody(t, "op-replay", map[string]uint64{
		"system-element:el-line1": parentVersion,
	}, map[string]any{
		"type": "create", "entity": map[string]any{"kind": "constant", "id": "const-1"},
		"parent_id":  "el-line1",
		"attributes": map[string]any{"name": "Target", "data_type": "int64", "value": 1},
	})

	firstCode, firstMsg, firstResult, firstWrites := exec.ExecuteWithWrites("_CmdEdit", "apply", payload)
	firstBatchCalls := f.batchCalls
	// Reconstruct through retained state, not the executor's process cache.
	exec = NewEditExec(f)
	code, msg, result, writes := exec.ExecuteWithWrites("_CmdEdit", "apply", payload)
	if code != firstCode || msg != firstMsg || result != firstResult || len(writes) != len(firstWrites) || f.batchCalls != firstBatchCalls {
		t.Fatalf("exact replay changed outcome or wrote again: first=%d/%q/%q/%+v replay=%d/%q/%q/%+v batches=%d",
			firstCode, firstMsg, firstResult, firstWrites, code, msg, result, writes, f.batchCalls)
	}

	conflict := editBody(t, "op-replay", map[string]uint64{
		"system-element:el-line1": parentVersion,
	}, map[string]any{
		"type": "create", "entity": map[string]any{"kind": "constant", "id": "const-2"},
		"parent_id":  "el-line1",
		"attributes": map[string]any{"name": "Other", "data_type": "int64", "value": 2},
	})
	code, msg, result, _ = exec.ExecuteWithWrites("_CmdEdit", "apply", conflict)
	if code != 409 || msg != "idempotency_conflict" || result != "conflict" || f.batchCalls != firstBatchCalls {
		t.Fatalf("operation reuse = %d %q %q batches=%d", code, msg, result, f.batchCalls)
	}
}

func TestEditDurableReplayReceiptsStayBounded(t *testing.T) {
	f := newStore("n-edge1")
	parentVersion := seedEditEntity(t, f, "_SystemElement", "line1", map[string]any{
		"id": "el-line1", "name": "Line 1",
	})
	for index := range editReplayLimit {
		operationID := fmt.Sprintf("old-%04d", index)
		_, err := f.Publish(editOperationTopic(f.NodeID(), operationID), body(t, editOperationReceipt{
			ID: operationID, Digest: strings.Repeat("a", sha256.Size*2), Message: "old", Result: "ok",
			Topics: []string{"colca/v1/_Constant/n-edge1/line1/old"},
		}))
		if err != nil {
			t.Fatal(err)
		}
	}
	exec := NewEditExec(f)
	payload := editBody(t, "new-operation", map[string]uint64{
		"system-element:el-line1": parentVersion,
	}, map[string]any{
		"type": "create", "entity": map[string]any{"kind": "constant", "id": "const-new"},
		"parent_id":  "el-line1",
		"attributes": map[string]any{"name": "New", "data_type": "int64", "value": 1},
	})

	code, msg, _, writes := exec.ExecuteWithWrites("_CmdEdit", "apply", payload)
	if code != 200 || len(writes) != 1 {
		t.Fatalf("bounded receipt command = %d %q writes=%+v", code, msg, writes)
	}
	if got := f.KVScan("_EditOperation", f.NodeID()); len(got) != editReplayLimit {
		t.Fatalf("durable receipt count = %d, want %d", len(got), editReplayLimit)
	}
	if _, found := f.KVGet(editOperationTopic(f.NodeID(), "old-0000")); found {
		t.Fatal("oldest durable replay receipt was not pruned atomically")
	}
}

func TestEditRejectsMalformedIntentKindsWithoutWriting(t *testing.T) {
	for _, tc := range []struct {
		name   string
		intent map[string]any
	}{
		{"create", map[string]any{"type": "create"}},
		{"update", map[string]any{"type": "update", "entity": map[string]any{"kind": "constant", "id": "missing"}}},
		{"delete", map[string]any{"type": "delete", "entity": map[string]any{"kind": "unknown", "id": "x"}}},
		{"placement", map[string]any{"type": "placement", "entity": map[string]any{"kind": "system-element", "id": "x"}}},
		{"binding", map[string]any{"type": "binding", "connector_id": "connector-1"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newStore("n-edge1")
			exec := NewEditExec(f)
			code, _, _, writes := exec.ExecuteWithWrites("_CmdEdit", "apply", editBody(t, "op-"+tc.name, nil, tc.intent))
			if code != 422 || len(writes) != 0 || f.batchCalls != 0 || f.offset != 0 {
				t.Fatalf("malformed %s = %d writes=%+v batches=%d offset=%d", tc.name, code, writes, f.batchCalls, f.offset)
			}
		})
	}
}
