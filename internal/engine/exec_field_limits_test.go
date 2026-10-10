package engine

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/alpamayo-solutions/colca/internal/contracts"
	"github.com/alpamayo-solutions/colca/internal/contracts/contractstest"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

const (
	limitsElement = "01M4JPH8CN21VK2MT9BM6S2N4V"
	limitsSignal  = "01M4JPYZ25MPQZVBVFYHKQYA38"
)

func withGeneratedBundle(t *testing.T, e *Engine) {
	t.Helper()
	tbl, err := contracts.Load(contractstest.GeneratedBundlePath(t), "")
	if err != nil {
		t.Fatal(err)
	}
	e.SetContracts(tbl)
}

func editCommand(t *testing.T, op string, expected map[string]uint64, intent map[string]any) []byte {
	t.Helper()
	versions := map[string]string{}
	for key, version := range expected {
		versions[key] = fmt.Sprint(version)
	}
	payload, err := json.Marshal(map[string]any{
		"operation_id": op, "correlation_id": "correlation-" + op, "expires_at": futureMS(),
		"expected_versions": versions, "intent": intent,
	})
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

func runEdit(t *testing.T, e *Engine, payload []byte) *CommandOutcome {
	t.Helper()
	result, err := e.IngestHuman(humanEntry(t, "cmd:#:configure"), "colca/v1/_CmdEdit/n-edge1/apply", payload)
	if err != nil {
		t.Fatal(err)
	}
	if result.Command == nil {
		t.Fatal("the edit command produced no outcome")
	}
	return result.Command
}

// A Workbench create that carries a value its contract does not allow is
// refused by the node with the field it broke, and nothing is written. Before,
// the node applied a 5000-character unit and the read side set the record
// aside, so the signal existed at the node and nowhere a person could see it.
func TestAnEditCreateBreakingAFieldLimitIsRefusedNamingTheField(t *testing.T) {
	e := execEngine(t, nil)
	withGeneratedBundle(t, e)
	parent, err := e.IngestAdmin("colca/v1/_SystemElement/n-edge1/line1",
		[]byte(`{"id":"`+limitsElement+`","name":"Line 1"}`))
	if err != nil {
		t.Fatal(err)
	}
	e.SetExecutor(uns.NewEditExec(e.EntityStore(), nil))
	entitiesBefore := e.Store().NextOffset("entities")

	for _, c := range []struct {
		attribute string
		value     any
		refusal   string
	}{
		{"unit", strings.Repeat("u", 5000), "invalid_field: unit: maxLength 50"},
		{"precision", -1, "invalid_field: precision: minimum 0"},
	} {
		outcome := runEdit(t, e, editCommand(t, "op-create-"+c.attribute, map[string]uint64{
			"system-element:" + limitsElement: parent.Offset,
		}, map[string]any{
			"type":       "create",
			"entity":     map[string]any{"kind": "signal", "id": limitsSignal},
			"parent_id":  limitsElement,
			"attributes": map[string]any{"name": "Temperature", "data_type": "float", c.attribute: c.value},
		}))
		if outcome.ResultCode != 422 || outcome.Message != c.refusal {
			t.Fatalf("create with an out-of-limit %s = %d %q, want 422 %q", c.attribute, outcome.ResultCode, outcome.Message, c.refusal)
		}
	}
	if got := e.Store().NextOffset("entities"); got != entitiesBefore {
		t.Fatalf("a refused create appended entity state: next=%d want=%d", got, entitiesBefore)
	}
	if got := mustKVScan(t, e.Store(), "line1/"); len(got) != 0 {
		t.Fatalf("a refused create left records: %+v", got)
	}
}

// A record a node accepted before its contract bounded the field stays the
// entity's current state. An edit that leaves the field alone is refused naming
// it, because the composed record would carry the held value forward; an edit
// that corrects it is applied, and that later record is what the read side then
// applies in place of the one it set aside.
func TestAnEntityHoldingAnOutOfLimitValueIsRepairedByTheEditThatCorrectsIt(t *testing.T) {
	e := execEngine(t, nil)
	if _, err := e.IngestAdmin("colca/v1/_SystemElement/n-edge1/line1",
		[]byte(`{"id":"`+limitsElement+`","name":"Line 1"}`)); err != nil {
		t.Fatal(err)
	}
	// Written under the built-in floor, which bounds no field: what an older
	// node stored.
	held, err := e.IngestAdmin("colca/v1/_Signal/n-edge1/line1/temp", []byte(`{"id":"`+limitsSignal+
		`","name":"Temp","system_element_id":"`+limitsElement+`","data_type":"float","precision":-1}`))
	if err != nil {
		t.Fatal(err)
	}
	withGeneratedBundle(t, e)
	e.SetExecutor(uns.NewEditExec(e.EntityStore(), nil))

	untouched := runEdit(t, e, editCommand(t, "op-describe", map[string]uint64{"signal:" + limitsSignal: held.Offset},
		map[string]any{
			"type":       "update",
			"entity":     map[string]any{"kind": "signal", "id": limitsSignal},
			"attributes": map[string]any{"description": "Inlet"},
		}))
	if untouched.ResultCode != 422 || untouched.Message != "invalid_field: precision: minimum 0" {
		t.Fatalf("edit leaving the held value = %d %q, want 422 naming precision", untouched.ResultCode, untouched.Message)
	}

	corrected := runEdit(t, e, editCommand(t, "op-correct", map[string]uint64{"signal:" + limitsSignal: held.Offset},
		map[string]any{
			"type":       "update",
			"entity":     map[string]any{"kind": "signal", "id": limitsSignal},
			"attributes": map[string]any{"precision": 2},
		}))
	if corrected.ResultCode != 200 || len(corrected.StateWrites) != 1 {
		t.Fatalf("edit correcting the held value = %d %q", corrected.ResultCode, corrected.Message)
	}
	raw, ok := e.EntityStore().KVGet("colca/v1/_Signal/n-edge1/line1/temp")
	if !ok || !strings.Contains(string(raw), `"precision":2`) {
		t.Fatalf("the corrected record is not the current state: %s", raw)
	}
}
