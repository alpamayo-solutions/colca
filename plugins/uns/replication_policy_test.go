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
