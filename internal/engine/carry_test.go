package engine

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/store"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// carryEngine is execEngine with the bus recorded, a Workbench executor, and
// a machine line, line2 and a signal on the machine whose producer published
// a value. A service placed at the machine keeps its catalogue there.
func carryEngine(t *testing.T) (*Engine, *[]delivery, map[string]uint64) {
	t.Helper()
	s, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	var bus []delivery
	e := New(s, &config.Config{ULID: "n-edge1"}, testIDs(), func(topic string, payload []byte, retain bool) {
		bus = append(bus, delivery{topic, string(payload), retain})
	}, nil, nil)
	e.SetContracts(writeBundle(t, map[string]any{
		"_Metric":             obj("data", true, nil, map[string]any{}),
		"_SystemElement":      obj("entity", true, nil, map[string]any{}),
		"_Signal":             obj("entity", true, nil, map[string]any{}),
		"_DataTags":           obj("entity", true, nil, map[string]any{}),
		"_EditOperation":      obj("entity", true, nil, map[string]any{}),
		"_WorkbenchOperation": obj("entity", true, nil, map[string]any{}),
		"_CmdEdit":            obj("cmd", false, nil, map[string]any{}),
		"_Ack":                obj("ack", false, nil, map[string]any{}),
	}))
	edit := uns.NewEditExec(e.EntityStore(), nil)
	edit.SetScope(e.Scope())
	e.SetExecutor(edit)

	versions := map[string]uint64{}
	for _, seed := range []struct{ key, topic, payload string }{
		{"system-element:el-line1", "colca/v1/_SystemElement/n-edge1/line1", `{"id":"el-line1","name":"Line 1"}`},
		{"system-element:el-line2", "colca/v1/_SystemElement/n-edge1/line2", `{"id":"el-line2","name":"Line 2"}`},
		{"system-element:el-machine", "colca/v1/_SystemElement/n-edge1/line1/machine", `{"id":"el-machine","name":"Machine","parent_id":"el-line1"}`},
		{"signal:sig-temp", "colca/v1/_Signal/n-edge1/line1/machine/temp", `{"id":"sig-temp","name":"temp","system_element_id":"el-machine"}`},
		{"", "colca/v1/_DataTags/n-edge1/line1/machine/dataops", `{"connector":"dataops","data_tags":[{"id":"TAG-OEE"}]}`},
		{"", "colca/v1/_Metric/n-edge1/line1/machine/temp", `{"value":21.5,"signal_id":"sig-temp","timestamp":1790000000}`},
	} {
		res, err := e.IngestAdmin(seed.topic, []byte(seed.payload))
		if err != nil {
			t.Fatalf("seed %s: %v", seed.topic, err)
		}
		if seed.key != "" {
			versions[seed.key] = res.Offset
		}
	}
	bus = nil
	return e, &bus, versions
}

func moveMachineTo(t *testing.T, e *Engine, versions map[string]uint64, operation string) {
	t.Helper()
	expected := map[string]string{}
	for key, version := range versions {
		expected[key] = fmt.Sprint(version)
	}
	payload, err := json.Marshal(map[string]any{
		"operation_id": operation, "correlation_id": "corr-" + operation, "expires_at": futureMS(),
		"expected_versions": expected,
		"intent": map[string]any{
			"type": "placement", "entity": map[string]any{"kind": "system-element", "id": "el-machine"},
			"target_parent_id": "el-line2",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := e.IngestHuman(humanEntry(t, "cmd:#:configure"), "colca/v1/_CmdEdit/n-edge1/apply", payload)
	if err != nil {
		t.Fatal(err)
	}
	if result.Command == nil || result.Command.ResultCode != 200 {
		t.Fatalf("placement = %+v", result.Command)
	}
}

func kvPayload(t *testing.T, e *Engine, topic string) ([]byte, int64, bool) {
	t.Helper()
	p, err := uns.Parse(topic)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range mustKVScan(t, e.Store(), p.Path) {
		if entry.Topic == topic {
			return entry.Payload, entry.TS, true
		}
	}
	return nil, 0, false
}

// A Workbench reparent moves the element and everything under it in one
// batch. The signal's value and the placed service's catalogue follow, with
// their payload and timestamp, and the bus clears the old retained messages.
func TestAWorkbenchReparentCarriesTheValueAndThePlacedCatalogue(t *testing.T) {
	e, bus, versions := carryEngine(t)
	const (
		oldMetric = "colca/v1/_Metric/n-edge1/line1/machine/temp"
		newMetric = "colca/v1/_Metric/n-edge1/line2/machine/temp"
		oldCat    = "colca/v1/_DataTags/n-edge1/line1/machine/dataops"
		newCat    = "colca/v1/_DataTags/n-edge1/line2/machine/dataops"
	)
	value, valueTS, _ := kvPayload(t, e, oldMetric)

	moveMachineTo(t, e, versions, "op-reparent")

	got, ts, ok := kvPayload(t, e, newMetric)
	if !ok || string(got) != string(value) || ts != valueTS {
		t.Fatalf("value at the new path = %s @%d, want %s @%d", got, ts, value, valueTS)
	}
	if _, _, ok := kvPayload(t, e, oldMetric); ok {
		t.Fatal("the value is still retained at the old path")
	}
	if _, _, ok := kvPayload(t, e, newCat); !ok {
		t.Fatal("the placed catalogue did not follow its element")
	}
	if _, _, ok := kvPayload(t, e, oldCat); ok {
		t.Fatal("the placed catalogue is still at the old path")
	}
	want := map[string]string{newMetric: string(value), oldMetric: "", oldCat: ""}
	for _, d := range *bus {
		if expected, tracked := want[d.Topic]; tracked && d.Retain && d.Payload == expected {
			delete(want, d.Topic)
		}
	}
	if len(want) != 0 {
		t.Fatalf("not delivered as retained: %v (bus %+v)", want, *bus)
	}
	if holders := e.catalogues.Holders("TAG-OEE"); len(holders) != 1 || holders[0] != newCat {
		t.Fatalf("catalogue index holders = %v, want only %s", holders, newCat)
	}
}

// A value the producer already published at the new path is newer than the
// one at the old path: it stays, and the old one is still retired.
func TestACarryDoesNotOverwriteANewerValue(t *testing.T) {
	e, _, versions := carryEngine(t)
	const newMetric = "colca/v1/_Metric/n-edge1/line2/machine/temp"
	newer := `{"value":30,"signal_id":"sig-temp","timestamp":1790000100}`
	if _, err := e.IngestAdmin(newMetric, []byte(newer)); err != nil {
		t.Fatal(err)
	}

	moveMachineTo(t, e, versions, "op-newer")

	if got, _, _ := kvPayload(t, e, newMetric); string(got) != newer {
		t.Fatalf("value at the new path = %s, want the newer %s", got, newer)
	}
	if _, _, ok := kvPayload(t, e, "colca/v1/_Metric/n-edge1/line1/machine/temp"); ok {
		t.Fatal("the old value is still retained")
	}
}

// A batch that writes a signal where it already stands moves nothing.
func TestAnUpsertInPlaceCarriesNothing(t *testing.T) {
	e, _, _ := carryEngine(t)
	metrics := e.Store().NextOffset("metrics")
	if _, err := e.EntityStore().PublishBatch(uns.CommandContext{}, []uns.StateRecord{{
		Topic:   "colca/v1/_Signal/n-edge1/line1/machine/temp",
		Payload: []byte(`{"id":"sig-temp","name":"temp","system_element_id":"el-machine"}`),
	}}); err != nil {
		t.Fatal(err)
	}
	if got := e.Store().NextOffset("metrics"); got != metrics {
		t.Fatalf("an upsert in place wrote %d metrics records", got-metrics)
	}
}
