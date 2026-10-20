package engine

import (
	"strings"
	"testing"

	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// The node holds the value definition rules for every write, under the
// generated bundle as the node runs: a Workbench edit giving a float signal a
// definition, or changing a defined int signal to float, is refused with the
// field refusal and writes nothing; a valid definition is applied.
func TestAnEditBreakingAValueDefinitionRuleIsRefusedNamingTheField(t *testing.T) {
	e := execEngine(t, nil)
	withGeneratedBundle(t, e)
	parent, err := e.IngestAdmin("colca/v1/_SystemElement/n-edge1/line1",
		[]byte(`{"id":"`+limitsElement+`","name":"Line 1"}`))
	if err != nil {
		t.Fatal(err)
	}
	e.SetExecutor(uns.NewEditExec(e.EntityStore(), nil))
	entitiesBefore := e.Store().NextOffset("entities")

	refused := runEdit(t, e, editCommand(t, "op-create-float", map[string]uint64{
		"system-element:" + limitsElement: parent.Offset,
	}, map[string]any{
		"type":      "create",
		"entity":    map[string]any{"kind": "signal", "id": limitsSignal},
		"parent_id": limitsElement,
		"attributes": map[string]any{
			"name": "Temperature", "data_type": "float", "value_definitions": map[string]any{"1": "hot"},
		},
	}))
	if refused.ResultCode != 422 || refused.Message != "invalid_field: value_definitions: data_type" {
		t.Fatalf("float signal with a definition = %d %q", refused.ResultCode, refused.Message)
	}
	if got := e.Store().NextOffset("entities"); got != entitiesBefore {
		t.Fatalf("a refused create appended entity state: next=%d want=%d", got, entitiesBefore)
	}

	created := runEdit(t, e, editCommand(t, "op-create-int", map[string]uint64{
		"system-element:" + limitsElement: parent.Offset,
	}, map[string]any{
		"type":      "create",
		"entity":    map[string]any{"kind": "signal", "id": limitsSignal},
		"parent_id": limitsElement,
		"attributes": map[string]any{
			"name": "fault_code", "data_type": "int",
			"value_definitions": map[string]any{"0": "no fault", "202": "Sieve blocked"},
		},
	}))
	if created.ResultCode != 200 || len(created.StateWrites) != 1 {
		t.Fatalf("int signal with a definition = %d %q", created.ResultCode, created.Message)
	}
	raw, ok := e.EntityStore().KVGet("colca/v1/_Signal/n-edge1/line1/fault_code")
	if !ok || !strings.Contains(string(raw), `"202":"Sieve blocked"`) {
		t.Fatalf("the signal does not carry its definition: %s", raw)
	}

	retyped := runEdit(t, e, editCommand(t, "op-retype", map[string]uint64{
		"signal:" + limitsSignal: created.StateWrites[0].Offset,
	}, map[string]any{
		"type":       "update",
		"entity":     map[string]any{"kind": "signal", "id": limitsSignal},
		"attributes": map[string]any{"data_type": "float"},
	}))
	if retyped.ResultCode != 422 || retyped.Message != "invalid_field: value_definitions: data_type" {
		t.Fatalf("retyping a defined signal to float = %d %q", retyped.ResultCode, retyped.Message)
	}
}
