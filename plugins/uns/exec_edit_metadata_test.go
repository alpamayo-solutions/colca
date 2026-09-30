package uns

import (
	"encoding/json"
	"strings"
	"testing"
)

func metadataIntent(kind, id, key string, expect, change map[string]any) map[string]any {
	intent := map[string]any{
		"type": "metadata", "entity": map[string]any{"kind": kind, "id": id},
		"key": key, "expect": expect,
	}
	for name, value := range change {
		intent[name] = value
	}
	return intent
}

func metadataOf(t *testing.T, f *fakeStore, topic string) map[string]any {
	t.Helper()
	raw, ok := f.KVGet(topic)
	if !ok {
		t.Fatalf("%s is not retained", topic)
	}
	var record map[string]any
	if err := json.Unmarshal(raw, &record); err != nil {
		t.Fatal(err)
	}
	metadata, _ := record["metadata"].(map[string]any)
	return metadata
}

const nodeRecordTopic = "colca/v1/_Node/n-edge1/_colca/nodes/n-edge1"

func seedNodeRecord(t *testing.T, f *fakeStore, metadata map[string]any) {
	t.Helper()
	seedEditEntity(t, f, "_Node", "_colca/nodes/n-edge1", map[string]any{
		"id": "n-edge1", "name": "Edge 1", "metadata": metadata,
	})
}

// Two writers that each read the record before either wrote, and change
// different keys: both apply, and neither erases the other's key or a key
// nobody touched. No record version is needed, only the key's own state.
func TestMetadataWritersOfDifferentKeysBothSurvive(t *testing.T) {
	f := newStore("n-edge1")
	seedNodeRecord(t, f, map[string]any{"meta-site": "Basel"})
	exec := NewEditExec(f, nil)

	first := editBody(t, "op-app-config", map[string]uint64{}, metadataIntent(
		"colca-node", "n-edge1", "meta-app-config",
		map[string]any{"absent": true},
		map[string]any{"value": map[string]any{"theme": "dark", "zones": []any{1, 2}}},
	))
	second := editBody(t, "op-owner", map[string]uint64{}, metadataIntent(
		"colca-node", "n-edge1", "meta-owner",
		map[string]any{"absent": true},
		map[string]any{"value": "ops"},
	))
	for _, payload := range [][]byte{first, second} {
		code, message, _, writes := exec.ExecuteWithWrites(asHuman, "_CmdEdit", "apply", payload)
		if code != 200 || len(writes) != 1 {
			t.Fatalf("metadata write = %d %q writes=%+v", code, message, writes)
		}
	}

	metadata := metadataOf(t, f, nodeRecordTopic)
	if metadata["meta-site"] != "Basel" || metadata["meta-owner"] != "ops" {
		t.Fatalf("a concurrent key was lost: %+v", metadata)
	}
	config, _ := metadata["meta-app-config"].(map[string]any)
	if config["theme"] != "dark" {
		t.Fatalf("app config = %+v", metadata["meta-app-config"])
	}
	raw, _ := f.KVGet(nodeRecordTopic)
	var record map[string]any
	_ = json.Unmarshal(raw, &record)
	if record["name"] != "Edge 1" || record["id"] != "n-edge1" {
		t.Fatalf("other attributes changed: %+v", record)
	}
}

// expect is compared against the key alone. A mismatch, a missing key where a
// value was expected, or a present key where absence was expected is
// stale_metadata with nothing written.
func TestMetadataStaleExpectIsRefusedWithoutWriting(t *testing.T) {
	f := newStore("n-edge1")
	seedNodeRecord(t, f, map[string]any{"meta-app-config": map[string]any{"rev": 2}})
	exec := NewEditExec(f, nil)
	before := f.batchCalls

	cases := map[string]map[string]any{
		"value differs":   {"value": map[string]any{"rev": 1}},
		"expected absent": {"absent": true},
	}
	index := 0
	for name, expect := range cases {
		index++
		payload := editBody(t, "op-stale-"+string(rune('a'+index)), map[string]uint64{}, metadataIntent(
			"colca-node", "n-edge1", "meta-app-config", expect, map[string]any{"value": map[string]any{"rev": 3}},
		))
		code, message, _, writes := exec.ExecuteWithWrites(asHuman, "_CmdEdit", "apply", payload)
		if code != 409 || message != "stale_metadata: meta-app-config" || len(writes) != 0 {
			t.Fatalf("%s: = %d %q writes=%+v", name, code, message, writes)
		}
	}
	missing := editBody(t, "op-stale-missing", map[string]uint64{}, metadataIntent(
		"colca-node", "n-edge1", "meta-unset", map[string]any{"value": "x"}, map[string]any{"value": "y"},
	))
	if code, message, _, _ := exec.ExecuteWithWrites(asHuman, "_CmdEdit", "apply", missing); code != 409 ||
		message != "stale_metadata: meta-unset" {
		t.Fatalf("expected value on a missing key = %d %q", code, message)
	}
	if f.batchCalls != before {
		t.Fatalf("a refused compare wrote %d batches", f.batchCalls-before)
	}
}

// Values compare as decoded JSON: key order and number spelling do not
// matter, so a client need not reproduce the node's encoding.
func TestMetadataExpectComparesDecodedJSON(t *testing.T) {
	f := newStore("n-edge1")
	seedNodeRecord(t, f, map[string]any{"meta-app-config": map[string]any{"a": 1, "b": []any{"x", true}}})
	exec := NewEditExec(f, nil)
	payload := []byte(`{"operation_id":"op-json","correlation_id":"c","expires_at":9999999999999,` +
		`"expected_versions":{},"intent":{"type":"metadata",` +
		`"entity":{"kind":"colca-node","id":"n-edge1"},"key":"meta-app-config",` +
		`"expect":{"value":{"b":["x",true],"a":1.0}},"value":{"a":2}}}`)
	code, message, _, writes := exec.ExecuteWithWrites(asHuman, "_CmdEdit", "apply", payload)
	if code != 200 || len(writes) != 1 {
		t.Fatalf("equivalent JSON = %d %q", code, message)
	}
	config, _ := metadataOf(t, f, nodeRecordTopic)["meta-app-config"].(map[string]any)
	if config["a"] != float64(2) {
		t.Fatalf("value not set: %+v", config)
	}
}

// Removing takes the key out and leaves the rest; setting a key to what it
// already holds, or removing an absent key, succeeds without writing.
func TestMetadataRemoveAndNoOp(t *testing.T) {
	f := newStore("n-edge1")
	seedNodeRecord(t, f, map[string]any{"meta-app-config": "v1", "meta-site": "Basel"})
	exec := NewEditExec(f, nil)

	remove := editBody(t, "op-remove", map[string]uint64{}, metadataIntent(
		"colca-node", "n-edge1", "meta-app-config", map[string]any{"value": "v1"}, map[string]any{"remove": true},
	))
	if code, message, _, writes := exec.ExecuteWithWrites(asHuman, "_CmdEdit", "apply", remove); code != 200 || len(writes) != 1 {
		t.Fatalf("remove = %d %q", code, message)
	}
	metadata := metadataOf(t, f, nodeRecordTopic)
	if _, held := metadata["meta-app-config"]; held || metadata["meta-site"] != "Basel" {
		t.Fatalf("after remove = %+v", metadata)
	}

	before := f.batchCalls
	for op, intent := range map[string]map[string]any{
		"op-remove-absent": metadataIntent("colca-node", "n-edge1", "meta-app-config",
			map[string]any{"absent": true}, map[string]any{"remove": true}),
		"op-same-value": metadataIntent("colca-node", "n-edge1", "meta-site",
			map[string]any{"value": "Basel"}, map[string]any{"value": "Basel"}),
	} {
		code, message, _, writes := exec.ExecuteWithWrites(asHuman, "_CmdEdit", "apply",
			editBody(t, op, map[string]uint64{}, intent))
		if code != 200 || len(writes) != 0 || !strings.HasPrefix(message, "metadata_unchanged: ") {
			t.Fatalf("%s = %d %q writes=%+v", op, code, message, writes)
		}
	}
	if f.batchCalls != before {
		t.Fatal("a no-op metadata write published a batch")
	}
}

// A retried operation returns its first outcome and writes nothing more, even
// though the key no longer matches the retry's expect.
func TestMetadataWriteReplaysByOperationID(t *testing.T) {
	f := newStore("n-edge1")
	seedNodeRecord(t, f, map[string]any{})
	exec := NewEditExec(f, nil)
	payload := editBody(t, "op-once", map[string]uint64{}, metadataIntent(
		"colca-node", "n-edge1", "meta-app-config", map[string]any{"absent": true}, map[string]any{"value": 1},
	))
	code, _, _, writes := exec.ExecuteWithWrites(asHuman, "_CmdEdit", "apply", payload)
	if code != 200 || len(writes) != 1 {
		t.Fatalf("first = %d", code)
	}
	batches := f.batchCalls
	code, _, _, replay := exec.ExecuteWithWrites(asHuman, "_CmdEdit", "apply", payload)
	if code != 200 || len(replay) != 1 || replay[0] != writes[0] || f.batchCalls != batches {
		t.Fatalf("replay = %d %+v batches=%d", code, replay, f.batchCalls-batches)
	}

	// A fresh executor has no cache: the durable receipt answers.
	fresh := NewEditExec(f, nil)
	code, _, _, replay = fresh.ExecuteWithWrites(asHuman, "_CmdEdit", "apply", payload)
	if code != 200 || len(replay) != 1 || f.batchCalls != batches {
		t.Fatalf("durable replay = %d %+v", code, replay)
	}
}

// A record version the caller chooses to send is still checked.
func TestMetadataStillChecksSuppliedRecordVersions(t *testing.T) {
	f := newStore("n-edge1")
	seedNodeRecord(t, f, map[string]any{})
	exec := NewEditExec(f, nil)
	payload := editBody(t, "op-versioned", map[string]uint64{"colca-node:n-edge1": 999}, metadataIntent(
		"colca-node", "n-edge1", "meta-app-config", map[string]any{"absent": true}, map[string]any{"value": 1},
	))
	code, message, _, _ := exec.ExecuteWithWrites(asHuman, "_CmdEdit", "apply", payload)
	if code != 409 || !strings.HasPrefix(message, "stale_version: colca-node:n-edge1") {
		t.Fatalf("stale record version = %d %q", code, message)
	}
}

func TestMetadataRejectsMalformedIntents(t *testing.T) {
	f := newStore("n-edge1")
	seedNodeRecord(t, f, map[string]any{})
	exec := NewEditExec(f, nil)
	absent := map[string]any{"absent": true}
	cases := map[string]map[string]any{
		"no key":           metadataIntent("colca-node", "n-edge1", "", absent, map[string]any{"value": 1}),
		"no expect":        {"type": "metadata", "entity": map[string]any{"kind": "colca-node", "id": "n-edge1"}, "key": "k", "value": 1},
		"both expects":     metadataIntent("colca-node", "n-edge1", "k", map[string]any{"absent": true, "value": 1}, map[string]any{"value": 1}),
		"empty expect":     metadataIntent("colca-node", "n-edge1", "k", map[string]any{}, map[string]any{"value": 1}),
		"value and remove": metadataIntent("colca-node", "n-edge1", "k", absent, map[string]any{"value": 1, "remove": true}),
		"neither":          metadataIntent("colca-node", "n-edge1", "k", absent, nil),
		"null value":       metadataIntent("colca-node", "n-edge1", "k", absent, map[string]any{"value": nil}),
		"unknown kind":     metadataIntent("gadget", "n-edge1", "k", absent, map[string]any{"value": 1}),
		"reference":        metadataIntent("external-reference", "r", "k", absent, map[string]any{"value": 1}),
	}
	index := 0
	for name, intent := range cases {
		index++
		payload := editBody(t, "op-bad-"+string(rune('a'+index)), map[string]uint64{}, intent)
		code, message, _, writes := exec.ExecuteWithWrites(asHuman, "_CmdEdit", "apply", payload)
		if code != 422 || len(writes) != 0 {
			t.Fatalf("%s = %d %q", name, code, message)
		}
	}
	missing := editBody(t, "op-missing", map[string]uint64{}, metadataIntent(
		"signal", "sig-none", "k", absent, map[string]any{"value": 1},
	))
	if code, message, _, _ := exec.ExecuteWithWrites(asHuman, "_CmdEdit", "apply", missing); code != 409 ||
		!strings.Contains(message, "entity_not_found: signal:sig-none") {
		t.Fatalf("missing entity = %d %q", code, message)
	}
}

// Authorized like an update of the entity: configure over its position, or
// param on a constant. A refused caller gets entity_not_found even when its
// expect is wrong, so the refusal says nothing about the key's value.
func TestMetadataIsAuthorizedBeforeTheCompare(t *testing.T) {
	f, exec, _ := twoLines(t)
	seedEditEntity(t, f, "_Constant", "line2/sta1/target", map[string]any{
		"id": "const-2", "name": "Target", "data_type": "float64", "value": 1.0,
		"system_element_id": "el-line2", "metadata": map[string]any{"meta-k": "held"},
	})
	seedEditEntity(t, f, "_Constant", "line1/sta1/target", map[string]any{
		"id": "const-1", "name": "Target", "data_type": "float64", "value": 1.0,
		"system_element_id": "el-line1", "metadata": map[string]any{"meta-k": "held"},
	})

	wrongExpect := editBody(t, "op-probe", map[string]uint64{}, metadataIntent(
		"constant", "const-2", "meta-k", map[string]any{"value": "guess"}, map[string]any{"value": "x"},
	))
	code, message, _, writes := exec.ExecuteWithWrites(scopedTo("el-line1"), "_CmdEdit", "apply", wrongExpect)
	if code != 409 || message != "entity_not_found: constant:const-2" || len(writes) != 0 {
		t.Fatalf("outside the grant = %d %q", code, message)
	}

	operator := CommandContext{Actor: &Entry{
		ULID: "kc-sub-op", Kind: KindHuman, Grants: []string{"cmd:el-line1/#:param"},
	}}
	set := editBody(t, "op-param", map[string]uint64{}, metadataIntent(
		"constant", "const-1", "meta-k", map[string]any{"value": "held"}, map[string]any{"value": "operator"},
	))
	if code, message, _, writes := exec.ExecuteWithWrites(operator, "_CmdEdit", "apply", set); code != 200 || len(writes) != 1 {
		t.Fatalf("param on a constant = %d %q", code, message)
	}
	onElement := editBody(t, "op-param-element", map[string]uint64{}, metadataIntent(
		"system-element", "el-line1", "meta-k", map[string]any{"absent": true}, map[string]any{"value": "x"},
	))
	if code, message, _, _ := exec.ExecuteWithWrites(operator, "_CmdEdit", "apply", onElement); code != 409 ||
		message != "entity_not_found: system-element:el-line1" {
		t.Fatalf("param on an element = %d %q", code, message)
	}
}

// A resource is addressed by id and authorized at the element it sits on, as
// the resource intent is; its other fields stay as they are.
func TestMetadataOnAResourceIsAuthorizedAtItsElement(t *testing.T) {
	f, exec, _ := twoLines(t)
	const topic = "colca/v1/_Resource/n-edge1/line1/res-1"
	seedEditEntity(t, f, "_Resource", "line1/res-1", map[string]any{
		"id": "res-1", "system_element_id": "el-line1", "filename": "manual.pdf",
		"content_type": "application/pdf", "sha256": strings.Repeat("a", 64), "size_bytes": 10,
	})
	set := func(op string) []byte {
		return editBody(t, op, map[string]uint64{}, metadataIntent(
			"resource", "res-1", "meta-doc-kind", map[string]any{"absent": true}, map[string]any{"value": "manual"},
		))
	}
	code, message, _, _ := exec.ExecuteWithWrites(scopedTo("el-line2"), "_CmdEdit", "apply", set("op-other-line"))
	if code != 409 || message != "entity_not_found: resource:res-1" {
		t.Fatalf("outside the grant = %d %q", code, message)
	}
	code, message, _, writes := exec.ExecuteWithWrites(scopedTo("el-line1"), "_CmdEdit", "apply", set("op-own-line"))
	if code != 200 || len(writes) != 1 {
		t.Fatalf("inside the grant = %d %q", code, message)
	}
	if got := metadataOf(t, f, topic)["meta-doc-kind"]; got != "manual" {
		t.Fatalf("resource metadata = %v", got)
	}
	raw, _ := f.KVGet(topic)
	if _, err := validateResourcePayload(raw); err != nil || !strings.Contains(string(raw), "manual.pdf") {
		t.Fatalf("resource record = %s (%v)", raw, err)
	}
}
