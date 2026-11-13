package uns

import (
	"encoding/json"
	"os"
	"testing"
)

func TestSignalReplicationPolicyVocabulary(t *testing.T) {
	data, err := os.ReadFile("../../contracts/src/colca_data_contracts/vectors/replication_policy.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []struct {
		Policy any  `json:"policy"`
		Valid  bool `json:"valid"`
	}
	if err := json.Unmarshal(data, &cases); err != nil {
		t.Fatal(err)
	}
	for _, c := range cases {
		payload, _ := json.Marshal(map[string]any{"id": "s1", "replication_policy": c.Policy})
		if err := Validate("_Signal", payload); (err == nil) != c.Valid {
			t.Errorf("policy %v: %v", c.Policy, err)
		}
	}
	if err := Validate("_Signal", []byte(`{"id":"s1"}`)); err != nil {
		t.Fatal(err)
	}
}

func TestSignalSourceLocalOnlyReadsThePolicy(t *testing.T) {
	for payload, want := range map[string]bool{
		`{"id":"s1"}`: false,
		`{"id":"s1","replication_policy":"replicate_to_parents"}`: false,
		`{"id":"s1","replication_policy":"source_local_only"}`:    true,
	} {
		if got, err := SignalSourceLocalOnly([]byte(payload)); err != nil || got != want {
			t.Errorf("%s: %v %v, want %v", payload, got, err, want)
		}
	}
	for _, payload := range []string{`{"replication_policy":"elsewhere"}`, `{"replication_policy":1}`, `[]`, `not json`} {
		if _, err := SignalSourceLocalOnly([]byte(payload)); err == nil {
			t.Errorf("%s: no error", payload)
		}
	}
	if path, node, signal, ok := MetricSignalKey("colca/v1/_Metric/n1/line/temp"); !ok || path != "line/temp" || node != "n1" || signal != Prefix()+"_Signal/n1/line/temp" {
		t.Errorf("MetricSignalKey = %q %q %q %v", path, node, signal, ok)
	}
	if _, _, _, ok := MetricSignalKey("colca/v1/_Signal/n1/line/temp"); ok {
		t.Error("a signal has no signal key")
	}
	if !DecidesReplication("colca/v1/_Signal/n1/a") || DecidesReplication("colca/v1/_Metric/n1/a") {
		t.Error("DecidesReplication")
	}
}
