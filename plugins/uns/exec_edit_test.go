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
