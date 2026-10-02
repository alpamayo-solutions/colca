package uns

import (
	"encoding/json"
	"os"
	"testing"
)

func TestApplicationTimeVectors(t *testing.T) {
	raw, err := os.ReadFile("../../contracts/src/colca_data_contracts/vectors/application_time.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []struct {
		Name     string          `json:"name"`
		Clock    json.RawMessage `json:"clock"`
		Real     float64         `json:"real"`
		Expected float64         `json:"expected"`
	}
	if err := json.Unmarshal(raw, &cases); err != nil {
		t.Fatal(err)
	}
	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			clock, err := DecodeClockDefinition(c.Clock)
			if err != nil {
				t.Fatal(err)
			}
			if got := clock.At(c.Real); got != c.Expected {
				t.Fatalf("got %v, want %v", got, c.Expected)
			}
		})
	}
}

func TestClockDefinitionRevisionsAndContinuity(t *testing.T) {
	f := newStore("hub")
	c := NewConfigExec(f, nil, nil, nil, nil, nil)
	d := map[string]any{"id": "factory", "run_id": "run1", "revision": 1, "real_anchor": 10000, "factory_anchor": 1000, "start_at": 1000, "rate": 100}
	apply := func(want int) {
		t.Helper()
		code, msg, _ := c.Execute(asHuman, "_CmdConfigure", "definition/upsert", definitionBody(t, "_ClockDefinition", d))
		if code != want {
			t.Fatalf("got %d (%s), want %d", code, msg, want)
		}
	}
	apply(200)
	apply(200) // duplicate delivery is idempotent
	d["rate"] = 200
	apply(409) // competing controller using the same revision
	d["revision"], d["real_anchor"], d["factory_anchor"], d["rate"] = 2, 10005, 1500, 0
	apply(200) // pause without a jump
	d["revision"], d["real_anchor"], d["factory_anchor"], d["rate"] = 3, 10100, 1500, 10
	apply(200) // resume; pause duration does not age the factory
	d["start_at"] = 900
	apply(409) // restarting a worker must still recover the original start
	d["start_at"] = 1000
	d["revision"], d["real_anchor"], d["factory_anchor"] = 4, 10101, 1400
	apply(409) // no rewind of historian keys
	d["factory_anchor"], d["run_id"] = 1510, "new-run"
	apply(409) // new runs select a new topic explicitly
}

func TestClockDefinitionValidationAndRouting(t *testing.T) {
	if ClassOf("_ClockDefinition") != ClassDefinition {
		t.Fatal("clock must descend as a definition")
	}
	for _, raw := range []string{
		`{"id":"x"}`, `{"id":"x","run_id":"r","revision":1,"real_anchor":0,"factory_anchor":0,"rate":-1}`,
		`{"id":"x","run_id":"r","revision":1.5,"real_anchor":0,"factory_anchor":0,"rate":1}`,
		`{"id":"x","run_id":"r","revision":1,"real_anchor":0,"factory_anchor":0,"rate":1001}`,
		`{"id":"x","run_id":"r","revision":1,"real_anchor":0,"factory_anchor":0,"rate":1,"stop_at":-1}`,
		`{"id":"x","run_id":"r","revision":2,"real_anchor":0,"factory_anchor":0,"rate":0,"previous":{}}`,
	} {
		if err := Validate("_ClockDefinition", []byte(raw)); err == nil {
			t.Fatalf("accepted %s", raw)
		}
	}
}

func TestClockProjectionStopsAndCatchesUp(t *testing.T) {
	d := ClockDefinition{RealAnchor: 10000, FactoryAnchor: 1000, Rate: 1000, CatchUp: true}
	if d.At(10020) != 10020 {
		t.Fatal("catch-up overshot now")
	}
	stop := 2000.0
	d.StopAt = &stop
	if d.At(10020) != stop {
		t.Fatal("duration overshot stop")
	}
}
