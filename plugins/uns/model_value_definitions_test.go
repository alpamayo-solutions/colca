package uns

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
)

var stateDefinitions = map[string]any{"0": "UNKNOWN", "1": "OFFLINE", "2": "DOWN", "3": "IDLE", "4": "EXECUTING"}

// stateModel seeds a State model: a required machine_state and an optional
// operating_mode that own their value definitions, and an optional
// fault_code that declares none.
func stateModel(t *testing.T, f *fakeStore) {
	t.Helper()
	seedEditEntity(t, f, "_DataModel", "_colca/data-models/state", map[string]any{
		"id": "dm-state", "name": "State", "version": "1.0",
		"slots": []map[string]any{
			{
				"key": "machine_state", "kind": "computed", "data_type": "integer", "required": true,
				"declared_by": "State", "value_definitions": stateDefinitions,
			},
			{
				"key": "operating_mode", "kind": "measured", "data_type": "integer", "required": false,
				"declared_by": "State", "value_definitions": map[string]any{"0": "UNKNOWN", "1": "PRODUCTION"},
			},
			{"key": "fault_code", "kind": "measured", "data_type": "integer", "required": false, "declared_by": "State"},
		},
	})
}

func assignState(t *testing.T, exec *EditExec, op string, expected map[string]uint64, creates map[string]string) (int, string) {
	t.Helper()
	code, msg, _, _ := exec.ExecuteWithWrites(asHuman, "_CmdEdit", "apply", editBody(t, op, expected, map[string]any{
		"type": "model", "action": "assign",
		"entity":  map[string]any{"kind": "system-element", "id": "el-press"},
		"models":  []string{"State"},
		"creates": creates,
	}))
	return code, msg
}

// Assign gives every signal filling a slot that owns value definitions
// exactly the slot's: a created one, an adopted one holding others, and an
// existing signal of an optional slot. A slot without a definition leaves its
// signal's own alone.
func TestModelAssignGivesSignalsTheSlotsValueDefinitions(t *testing.T) {
	f := newStore("n-edge1")
	elementVersion := seedEditEntity(t, f, "_SystemElement", "press", map[string]any{"id": "el-press", "name": "Press"})
	modeVersion := seedEditEntity(t, f, "_Signal", "press/operating_mode", map[string]any{
		"id": "sig-mode", "name": "operating_mode", "system_element_id": "el-press", "data_type": "int",
		"value_definitions": map[string]any{"1": "Automatic"},
	})
	faultVersion := seedEditEntity(t, f, "_Signal", "press/fault_code", map[string]any{
		"id": "sig-fault", "name": "fault_code", "system_element_id": "el-press", "data_type": "int",
		"value_definitions": map[string]any{"202": "Sieve blocked"},
	})
	stateModel(t, f)
	exec := NewEditExec(f, nil)

	code, msg := assignState(t, exec, "op-assign-definitions", map[string]uint64{
		"system-element:el-press": elementVersion, "signal:sig-mode": modeVersion, "signal:sig-fault": faultVersion,
	}, map[string]string{"machine_state": "sig-state"})
	if code != 200 {
		t.Fatalf("assign = %d %q", code, msg)
	}
	if got := signalPayloadAt(t, f, "colca/v1/_Signal/n-edge1/press/machine_state")["value_definitions"]; !reflect.DeepEqual(got, stateDefinitions) {
		t.Fatalf("created machine_state carries %v", got)
	}
	if got := signalPayloadAt(t, f, "colca/v1/_Signal/n-edge1/press/operating_mode")["value_definitions"]; !reflect.DeepEqual(got, map[string]any{"0": "UNKNOWN", "1": "PRODUCTION"}) {
		t.Fatalf("operating_mode kept %v instead of the slot's definition", got)
	}
	if got := signalPayloadAt(t, f, "colca/v1/_Signal/n-edge1/press/fault_code")["value_definitions"]; !reflect.DeepEqual(got, map[string]any{"202": "Sieve blocked"}) {
		t.Fatalf("a slot without a definition changed its signal's: %v", got)
	}
}

// Adopting a signal whose definition differs is a write to it, so assign
// needs the signal's expected version like any adopt.
func TestModelAssignNeedsTheVersionOfASignalWhoseDefinitionItReplaces(t *testing.T) {
	f := newStore("n-edge1")
	elementVersion := seedEditEntity(t, f, "_SystemElement", "press", map[string]any{"id": "el-press", "name": "Press"})
	seedEditEntity(t, f, "_Signal", "press/operating_mode", map[string]any{
		"id": "sig-mode", "name": "operating_mode", "system_element_id": "el-press", "data_type": "int",
	})
	stateModel(t, f)
	before := f.offset

	code, msg := assignState(t, NewEditExec(f, nil), "op-assign-unversioned",
		map[string]uint64{"system-element:el-press": elementVersion}, map[string]string{"machine_state": "sig-state"})
	if code != 422 || !strings.Contains(msg, "missing_expected_version: signal:sig-mode") || f.offset != before {
		t.Fatalf("assign = %d %q", code, msg)
	}
}

// Two models giving one slot different definitions cannot both be implemented.
func TestModelAssignRefusesModelsDisagreeingOnASlotsDefinition(t *testing.T) {
	f := newStore("n-edge1")
	elementVersion := seedEditEntity(t, f, "_SystemElement", "press", map[string]any{"id": "el-press", "name": "Press"})
	stateModel(t, f)
	seedEditEntity(t, f, "_DataModel", "_colca/data-models/other", map[string]any{
		"id": "dm-other", "name": "Other", "version": "1.0",
		"slots": []map[string]any{{
			"key": "machine_state", "kind": "measured", "data_type": "integer", "required": true,
			"declared_by": "Other", "value_definitions": map[string]any{"0": "OFF"},
		}},
	})
	code, msg, _, writes := NewEditExec(f, nil).ExecuteWithWrites(asHuman, "_CmdEdit", "apply", editBody(t, "op-disagree",
		map[string]uint64{"system-element:el-press": elementVersion},
		map[string]any{
			"type": "model", "action": "assign",
			"entity":  map[string]any{"kind": "system-element", "id": "el-press"},
			"models":  []string{"State", "Other"},
			"creates": map[string]string{"machine_state": "sig-state"},
		}))
	if code != 409 || len(writes) != 0 || !strings.Contains(msg, `give slot "machine_state" different value definitions`) {
		t.Fatalf("assign = %d %q", code, msg)
	}
}

// A model whose slot declares a definition its data type cannot take is
// refused, as the Python compiler would have refused it.
func TestModelAssignRefusesASlotDefinitionTheRulesRefuse(t *testing.T) {
	f := newStore("n-edge1")
	elementVersion := seedEditEntity(t, f, "_SystemElement", "press", map[string]any{"id": "el-press", "name": "Press"})
	seedEditEntity(t, f, "_DataModel", "_colca/data-models/state", map[string]any{
		"id": "dm-state", "name": "State", "version": "1.0",
		"slots": []map[string]any{{
			"key": "speed", "kind": "measured", "data_type": "number", "required": true,
			"declared_by": "State", "value_definitions": map[string]any{"1": "fast"},
		}},
	})
	code, msg := assignState(t, NewEditExec(f, nil), "op-bad-slot",
		map[string]uint64{"system-element:el-press": elementVersion}, map[string]string{"speed": "sig-speed"})
	if code != 422 || !strings.Contains(msg, `State slot "speed": _Signal: value_definitions are only taken by`) {
		t.Fatalf("assign = %d %q", code, msg)
	}
}

// ownedPress seeds a press implementing State with a machine_state that
// carries the slot's definition, and returns the element's and the signal's
// versions.
func ownedPress(t *testing.T, f *fakeStore) (uint64, uint64) {
	t.Helper()
	elementVersion := seedEditEntity(t, f, "_SystemElement", "press", map[string]any{
		"id": "el-press", "name": "Press", "implements": []string{"State"},
	})
	signalVersion := seedEditEntity(t, f, "_Signal", "press/machine_state", map[string]any{
		"id": "sig-state", "name": "machine_state", "system_element_id": "el-press", "data_type": "int",
		"value_definitions": stateDefinitions,
	})
	stateModel(t, f)
	return elementVersion, signalVersion
}

func updateSignal(t *testing.T, exec *EditExec, op, id string, version uint64, attributes map[string]any) (int, string) {
	t.Helper()
	code, msg, _, _ := exec.ExecuteWithWrites(asHuman, "_CmdEdit", "apply", editBody(t, op,
		map[string]uint64{"signal:" + id: version},
		map[string]any{"type": "update", "entity": map[string]any{"kind": "signal", "id": id}, "attributes": attributes}))
	return code, msg
}

// An edit of a definition the model owns is refused naming the model, while
// edits of the signal's other fields go through, and a slot that owns no
// definition leaves the signal's free to edit.
func TestAnEditOfAModelOwnedDefinitionIsRefusedNamingTheModel(t *testing.T) {
	f := newStore("n-edge1")
	_, stateVersion := ownedPress(t, f)
	faultVersion := seedEditEntity(t, f, "_Signal", "press/fault_code", map[string]any{
		"id": "sig-fault", "name": "fault_code", "system_element_id": "el-press", "data_type": "int",
	})
	exec := NewEditExec(f, nil)
	before := f.offset

	for i, definitions := range []any{map[string]any{"0": "Unknown"}, map[string]any{}, nil} {
		code, msg := updateSignal(t, exec, fmt.Sprintf("op-edit-owned-%d", i), "sig-state", stateVersion, map[string]any{"value_definitions": definitions})
		if code != 409 || msg != "value_definitions_owned_by_model: State" || f.offset != before {
			t.Fatalf("edit of the owned definition to %v = %d %q", definitions, code, msg)
		}
	}
	if code, msg := updateSignal(t, exec, "op-describe", "sig-state", stateVersion, map[string]any{"description": "Roll-up"}); code != 200 {
		t.Fatalf("edit of another field = %d %q", code, msg)
	}
	if code, msg := updateSignal(t, exec, "op-edit-free", "sig-fault", faultVersion, map[string]any{
		"value_definitions": map[string]any{"202": "Sieve blocked"},
	}); code != 200 {
		t.Fatalf("edit of a definition no model owns = %d %q", code, msg)
	}
}

// Creating a signal that fills an owning slot gives it the slot's definition;
// creating it with another one is refused naming the model.
func TestCreatingASignalForAnOwningSlotTakesTheSlotsDefinition(t *testing.T) {
	f := newStore("n-edge1")
	elementVersion := seedEditEntity(t, f, "_SystemElement", "press", map[string]any{
		"id": "el-press", "name": "Press", "implements": []string{"State"},
	})
	stateModel(t, f)
	exec := NewEditExec(f, nil)
	create := func(op string, attributes map[string]any) (int, string) {
		code, msg, _, _ := exec.ExecuteWithWrites(asHuman, "_CmdEdit", "apply", editBody(t, op,
			map[string]uint64{"system-element:el-press": elementVersion},
			map[string]any{
				"type": "create", "entity": map[string]any{"kind": "signal", "id": "sig-mode"},
				"parent_id": "el-press", "attributes": attributes,
			}))
		return code, msg
	}

	code, msg := create("op-create-other", map[string]any{
		"name": "operating_mode", "data_type": "int", "value_definitions": map[string]any{"1": "Automatic"},
	})
	if code != 409 || msg != "value_definitions_owned_by_model: State" {
		t.Fatalf("create with another definition = %d %q", code, msg)
	}
	if code, msg := create("op-create", map[string]any{"name": "operating_mode", "data_type": "int"}); code != 200 {
		t.Fatalf("create = %d %q", code, msg)
	}
	got := signalPayloadAt(t, f, "colca/v1/_Signal/n-edge1/press/operating_mode")["value_definitions"]
	if !reflect.DeepEqual(got, map[string]any{"0": "UNKNOWN", "1": "PRODUCTION"}) {
		t.Fatalf("created operating_mode carries %v", got)
	}
}
