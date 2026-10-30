package uns

import (
	"encoding/json"
	"strings"
	"testing"
)

// What an assign gives an existing entity that does not carry it yet: a
// signal or constant takes the slot's unit, a child element the category the
// child slot names. What an entity already carries is kept; a child of
// another category is refused.

func payloadAt(t *testing.T, f *fakeStore, topic string) map[string]any {
	t.Helper()
	raw, ok := f.KVGet(topic)
	if !ok {
		t.Fatalf("no record at %s", topic)
	}
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatal(err)
	}
	return payload
}

func seedMeterModel(t *testing.T, f *fakeStore) {
	t.Helper()
	seedEditEntity(t, f, "_DataModel", "_colca/data-models/meter", map[string]any{
		"id": "dm-meter", "name": "Meter", "version": "1.0",
		"slots": []map[string]any{
			{"key": "power", "kind": "measured", "data_type": "number", "unit": "kW", "required": true},
			{"key": "energy", "kind": "measured", "data_type": "number", "unit": "kWh", "required": true},
			{"key": "rated_power", "kind": "constant", "data_type": "number", "unit": "kW", "required": true},
		},
	})
}

func TestModelAssignGivesAnUnlabelledSignalAndConstantTheSlotsUnit(t *testing.T) {
	f := newStore("n-edge1")
	elementVersion := seedEditEntity(t, f, "_SystemElement", "press", map[string]any{"id": "el-press", "name": "Press"})
	powerVersion := seedEditEntity(t, f, "_Signal", "press/power", map[string]any{
		"id": "sig-power", "name": "power", "system_element_id": "el-press", "data_type": "float",
	})
	energyVersion := seedEditEntity(t, f, "_Signal", "press/energy", map[string]any{
		"id": "sig-energy", "name": "energy", "system_element_id": "el-press", "data_type": "float", "unit": "MWh",
	})
	ratedVersion := seedEditEntity(t, f, "_Constant", "press/rated_power", map[string]any{
		"id": "c-rated", "name": "rated_power", "system_element_id": "el-press", "data_type": "float64", "value": 55.0,
	})
	seedMeterModel(t, f)

	payload := editBody(t, "op-meter-assign", map[string]uint64{
		"system-element:el-press": elementVersion, "signal:sig-power": powerVersion,
		"signal:sig-energy": energyVersion, "constant:c-rated": ratedVersion,
	}, map[string]any{
		"type": "model", "action": "assign",
		"entity": map[string]any{"kind": "system-element", "id": "el-press"},
		"models": []string{"Meter"},
	})
	code, msg, _, _ := NewEditExec(f, nil).ExecuteWithWrites(asHuman, "_CmdEdit", "apply", payload)
	if code != 200 {
		t.Fatalf("assign = %d %q", code, msg)
	}
	if got := payloadAt(t, f, "colca/v1/_Signal/n-edge1/press/power")["unit"]; got != "kW" {
		t.Fatalf("a signal without a unit should take the slot's, got %v", got)
	}
	if got := payloadAt(t, f, "colca/v1/_Constant/n-edge1/press/rated_power")["unit"]; got != "kW" {
		t.Fatalf("a constant without a unit should take the slot's, got %v", got)
	}
	// A unit the signal carries is the command boundary's to judge, never
	// overwritten here.
	if got := payloadAt(t, f, "colca/v1/_Signal/n-edge1/press/energy")["unit"]; got != "MWh" {
		t.Fatalf("a signal's own unit must be kept, got %v", got)
	}
}

func seedLineModel(t *testing.T, f *fakeStore, slots ...map[string]any) {
	t.Helper()
	seedEditEntity(t, f, "_SemanticTag", "_colca/semantic-tags/machine", map[string]any{"id": "tag-machine", "name": "machine"})
	seedEditEntity(t, f, "_SemanticTag", "_colca/semantic-tags/subsystem", map[string]any{
		"id": "tag-subsystem", "name": "subsystem",
	})
	if len(slots) == 0 {
		slots = []map[string]any{{
			"key": "filler", "kind": "child", "data_type": "", "required": true,
			"declared_by": "Line", "entity_name": "filler", "semantic_type": "machine",
		}}
	}
	seedEditEntity(t, f, "_DataModel", "_colca/data-models/line", map[string]any{
		"id": "dm-line", "name": "Line", "version": "1.0", "slots": slots,
	})
}

func assignLine(t *testing.T, f *fakeStore, expected map[string]uint64, models ...string) (int, string) {
	t.Helper()
	if len(models) == 0 {
		models = []string{"Line"}
	}
	payload := editBody(t, "op-line-assign", expected, map[string]any{
		"type": "model", "action": "assign",
		"entity":  map[string]any{"kind": "system-element", "id": "el-line"},
		"models":  models,
		"creates": map[string]string{"filler": "el-filler"},
	})
	code, msg, _, _ := NewEditExec(f, nil).ExecuteWithWrites(asHuman, "_CmdEdit", "apply", payload)
	return code, msg
}

func TestModelAssignCreatesAChildWithTheSlotsCategory(t *testing.T) {
	f := newStore("n-edge1")
	lineVersion := seedEditEntity(t, f, "_SystemElement", "line", map[string]any{"id": "el-line", "name": "Line1"})
	seedLineModel(t, f)

	if code, msg := assignLine(t, f, map[string]uint64{"system-element:el-line": lineVersion}); code != 200 {
		t.Fatalf("assign = %d %q", code, msg)
	}
	if got := payloadAt(t, f, "colca/v1/_SystemElement/n-edge1/line/filler")["semantic_type_id"]; got != "tag-machine" {
		t.Fatalf("created child category = %v", got)
	}
}

func TestModelAssignGivesAnUnclassifiedChildTheSlotsCategory(t *testing.T) {
	f := newStore("n-edge1")
	lineVersion := seedEditEntity(t, f, "_SystemElement", "line", map[string]any{"id": "el-line", "name": "Line1"})
	fillerVersion := seedEditEntity(t, f, "_SystemElement", "line/filler", map[string]any{
		"id": "el-filler", "name": "filler", "parent_id": "el-line",
	})
	seedLineModel(t, f)

	code, msg := assignLine(t, f, map[string]uint64{
		"system-element:el-line": lineVersion, "system-element:el-filler": fillerVersion,
	})
	if code != 200 {
		t.Fatalf("assign = %d %q", code, msg)
	}
	if got := payloadAt(t, f, "colca/v1/_SystemElement/n-edge1/line/filler")["semantic_type_id"]; got != "tag-machine" {
		t.Fatalf("adopted child category = %v", got)
	}
}

func TestModelAssignRefusesAChildOfAnotherCategory(t *testing.T) {
	f := newStore("n-edge1")
	lineVersion := seedEditEntity(t, f, "_SystemElement", "line", map[string]any{"id": "el-line", "name": "Line1"})
	fillerVersion := seedEditEntity(t, f, "_SystemElement", "line/filler", map[string]any{
		"id": "el-filler", "name": "filler", "parent_id": "el-line", "semantic_type_id": "tag-subsystem",
	})
	seedLineModel(t, f)
	before := f.offset

	code, msg := assignLine(t, f, map[string]uint64{
		"system-element:el-line": lineVersion, "system-element:el-filler": fillerVersion,
	})
	if code != 409 || !strings.Contains(msg, `child slot "filler" has an incompatible category`) {
		t.Fatalf("assign = %d %q", code, msg)
	}
	if f.offset != before {
		t.Fatal("a refused assign must write nothing")
	}
}

func TestModelAssignRefusesTwoModelsAskingOneChildForTwoCategories(t *testing.T) {
	f := newStore("n-edge1")
	lineVersion := seedEditEntity(t, f, "_SystemElement", "line", map[string]any{"id": "el-line", "name": "Line1"})
	seedLineModel(t, f)
	seedEditEntity(t, f, "_DataModel", "_colca/data-models/cell", map[string]any{
		"id": "dm-cell", "name": "Cell", "version": "1.0",
		"slots": []map[string]any{{
			"key": "filler", "kind": "child", "data_type": "", "required": true,
			"declared_by": "Cell", "entity_name": "filler", "semantic_type": "subsystem",
		}},
	})

	code, msg := assignLine(t, f, map[string]uint64{"system-element:el-line": lineVersion}, "Line", "Cell")
	if code != 409 || !strings.Contains(msg, `require different categories for child slot "filler"`) {
		t.Fatalf("assign = %d %q", code, msg)
	}
}
