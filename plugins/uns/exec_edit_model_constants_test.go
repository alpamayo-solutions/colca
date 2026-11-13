package uns

import (
	"encoding/json"
	"strings"
	"testing"
)

// energyConsumerModel seeds a model with one measured and one constant slot,
// plus the rated-power tag.
func energyConsumerModel(t *testing.T, f *fakeStore) {
	t.Helper()
	seedEditEntity(t, f, "_SemanticTag", "_colca/semantic-tags/rated-power", map[string]any{
		"id": "tag-rated-power", "name": "rated-power",
	})
	seedEditEntity(t, f, "_DataModel", "_colca/data-models/energy-consumer", map[string]any{
		"id": "dm-energy", "name": "EnergyConsumer", "version": "1.0",
		"slots": []map[string]any{
			{"key": "active_power", "kind": "measured", "data_type": "number", "required": true},
			{"key": "rated_power", "kind": "constant", "data_type": "number", "semantic_type": "rated-power", "required": true},
		},
	})
}

func assignEnergyConsumer(t *testing.T, f *fakeStore, expected map[string]uint64) (int, string, []StateWrite) {
	t.Helper()
	payload := editBody(t, "op-energy-assign", expected, map[string]any{
		"type": "model", "action": "assign",
		"entity":  map[string]any{"kind": "system-element", "id": "el-press"},
		"models":  []string{"EnergyConsumer"},
		"creates": map[string]string{"active_power": "sig-power"},
	})
	code, msg, _, writes := NewEditExec(f, nil).ExecuteWithWrites(asHuman, "_CmdEdit", "apply", payload)
	return code, msg, writes
}

// An existing constant fills the constant slot and gains the slot's semantic
// type; the signal slot is created as usual.
func TestModelAssignAdoptsConstantForConstantSlot(t *testing.T) {
	f := newStore("n-edge1")
	elementVersion := seedEditEntity(t, f, "_SystemElement", "press", map[string]any{"id": "el-press", "name": "Press"})
	constantVersion := seedEditEntity(t, f, "_Constant", "press/rated_power", map[string]any{
		"id": "c-rated", "name": "rated_power", "system_element_id": "el-press", "data_type": "float64", "value": 55.0,
	})
	energyConsumerModel(t, f)

	code, msg, writes := assignEnergyConsumer(t, f, map[string]uint64{
		"system-element:el-press": elementVersion, "constant:c-rated": constantVersion,
	})
	if code != 200 || len(writes) != 3 {
		t.Fatalf("assign = %d %q writes=%+v", code, msg, writes)
	}
	raw, _ := f.KVGet("colca/v1/_Constant/n-edge1/press/rated_power")
	var constant map[string]any
	if err := json.Unmarshal(raw, &constant); err != nil {
		t.Fatal(err)
	}
	if constant["semantic_type_id"] != "tag-rated-power" || constant["value"] != 55.0 {
		t.Fatalf("constant after assign = %+v", constant)
	}
	if _, ok := f.KVGet("colca/v1/_Signal/n-edge1/press/rated_power"); ok {
		t.Fatal("a constant slot must never create a signal")
	}
}

// Assign never creates a constant: its value is authored. The assign succeeds
// and leaves the slot for coverage to report as missing.
func TestModelAssignLeavesMissingConstantUncreated(t *testing.T) {
	f := newStore("n-edge1")
	elementVersion := seedEditEntity(t, f, "_SystemElement", "press", map[string]any{"id": "el-press", "name": "Press"})
	energyConsumerModel(t, f)

	code, msg, writes := assignEnergyConsumer(t, f, map[string]uint64{"system-element:el-press": elementVersion})
	if code != 200 || len(writes) != 2 {
		t.Fatalf("assign = %d %q writes=%+v", code, msg, writes)
	}
	for _, write := range writes {
		if strings.Contains(write.Topic, "_Constant/") {
			t.Fatalf("assign created a constant: %+v", write)
		}
	}
}

// A matched constant is written, so its version must be expected.
func TestModelAssignConstantNeedsItsVersion(t *testing.T) {
	f := newStore("n-edge1")
	elementVersion := seedEditEntity(t, f, "_SystemElement", "press", map[string]any{"id": "el-press", "name": "Press"})
	seedEditEntity(t, f, "_Constant", "press/rated_power", map[string]any{
		"id": "c-rated", "name": "rated_power", "system_element_id": "el-press", "data_type": "float64", "value": 55.0,
	})
	energyConsumerModel(t, f)

	code, msg, _ := assignEnergyConsumer(t, f, map[string]uint64{"system-element:el-press": elementVersion})
	if code != 422 || !strings.Contains(msg, "missing_expected_version: constant:c-rated") {
		t.Fatalf("assign = %d %q", code, msg)
	}
}

func TestModelAssignConstantConflicts(t *testing.T) {
	cases := []struct {
		name     string
		contract string
		payload  map[string]any
		key      string
		want     string
	}{
		{
			name: "data type", contract: "_Constant", key: "constant:c-rated",
			payload: map[string]any{
				"id": "c-rated", "name": "rated_power", "system_element_id": "el-press", "data_type": "string", "value": "55 kW",
			},
			want: `slot "rated_power" has incompatible data_type: constant has "string"`,
		},
		{
			name: "semantic type", contract: "_Constant", key: "constant:c-rated",
			payload: map[string]any{
				"id": "c-rated", "name": "rated_power", "system_element_id": "el-press", "data_type": "float64",
				"value": 55.0, "semantic_type_id": "tag-other",
			},
			want: `slot "rated_power" has an incompatible semantic type`,
		},
		{
			name: "occupied by a signal", contract: "_Signal", key: "signal:sig-rated",
			payload: map[string]any{
				"id": "sig-rated", "name": "rated_power", "system_element_id": "el-press", "data_type": "float",
			},
			want: `constant slot "rated_power" is occupied by signal sig-rated`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newStore("n-edge1")
			elementVersion := seedEditEntity(t, f, "_SystemElement", "press", map[string]any{"id": "el-press", "name": "Press"})
			version := seedEditEntity(t, f, tc.contract, "press/rated_power", tc.payload)
			energyConsumerModel(t, f)
			before := f.offset

			code, msg, _ := assignEnergyConsumer(t, f, map[string]uint64{
				"system-element:el-press": elementVersion, tc.key: version,
			})
			if code != 409 || !strings.Contains(msg, tc.want) {
				t.Fatalf("assign = %d %q, want 409 %q", code, msg, tc.want)
			}
			if f.offset != before {
				t.Fatal("a refused assign wrote something")
			}
		})
	}
}

// One key cannot be a signal in one model and a constant in another.
func TestModelAssignRefusesSignalAndConstantUnderOneKey(t *testing.T) {
	f := newStore("n-edge1")
	elementVersion := seedEditEntity(t, f, "_SystemElement", "press", map[string]any{"id": "el-press", "name": "Press"})
	seedEditEntity(t, f, "_DataModel", "_colca/data-models/a", map[string]any{
		"id": "dm-a", "name": "A", "version": "1.0",
		"slots": []map[string]any{{"key": "rated_power", "kind": "measured", "data_type": "number", "required": true}},
	})
	seedEditEntity(t, f, "_DataModel", "_colca/data-models/b", map[string]any{
		"id": "dm-b", "name": "B", "version": "1.0",
		"slots": []map[string]any{{"key": "rated_power", "kind": "constant", "data_type": "number", "required": true}},
	})
	payload := editBody(t, "op-kinds", map[string]uint64{"system-element:el-press": elementVersion}, map[string]any{
		"type": "model", "action": "assign",
		"entity":  map[string]any{"kind": "system-element", "id": "el-press"},
		"models":  []string{"A", "B"},
		"creates": map[string]string{"rated_power": "sig-rated"},
	})
	code, msg, _, _ := NewEditExec(f, nil).ExecuteWithWrites(asHuman, "_CmdEdit", "apply", payload)
	if code != 409 || !strings.Contains(msg, `A and B disagree on whether "rated_power" is a signal or a constant`) {
		t.Fatalf("assign = %d %q", code, msg)
	}
}

// A hand-written constant slot that declares signal-only fields is refused,
// and a signal link to a constant slot names it as no signal.
func TestValidateSlotDeclarationsConstantSlots(t *testing.T) {
	problems := validateSlotDeclarations("M", []modelSlot{
		{Key: "c", Kind: "constant", DataType: "number", Access: "read_write"},
		{Key: "s", Kind: "measured", DataType: "number", Relationships: []slotRelationship{{Type: "upper_limit", Target: "c"}}},
	})
	joined := strings.Join(problems, "; ")
	if !strings.Contains(joined, `M slot "c": a constant slot declares no access`) ||
		!strings.Contains(joined, "is not a signal") {
		t.Fatalf("problems = %v", problems)
	}
}
