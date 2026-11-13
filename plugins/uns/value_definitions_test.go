package uns

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

const valueDefinitionsVectorPath = "../../contracts/src/colca_data_contracts/vectors/value_definitions.json"

type valueDefinitionVectors struct {
	KeyKinds   map[string]string `json:"key_kinds"`
	MaxEntries int               `json:"max_entries"`
	MaxText    int               `json:"max_text"`
	Cases      []struct {
		Name             string  `json:"name"`
		DataType         *string `json:"data_type"`
		Definitions      any     `json:"definitions"`
		GeneratedEntries int     `json:"generated_entries"`
		Refusal          *string `json:"refusal"`
	} `json:"cases"`
}

func readValueDefinitionVectors(t *testing.T) valueDefinitionVectors {
	t.Helper()
	raw, err := os.ReadFile(filepath.Clean(valueDefinitionsVectorPath))
	if err != nil {
		t.Fatalf("read value definition vectors: %v", err)
	}
	var vectors valueDefinitionVectors
	if err := json.Unmarshal(raw, &vectors); err != nil {
		t.Fatalf("decode value definition vectors: %v", err)
	}
	if len(vectors.Cases) == 0 {
		t.Fatal("value definition vectors carry no cases")
	}
	return vectors
}

// The Go key table and limits are the Python ones, which the contracts suite
// checks against its definition.
func TestValueDefinitionRulesMatchTheGoldenVectors(t *testing.T) {
	vectors := readValueDefinitionVectors(t)
	if len(vectors.KeyKinds) != len(valueDefinitionKeyKinds) {
		t.Fatalf("key kinds: vectors %v, Go %v", vectors.KeyKinds, valueDefinitionKeyKinds)
	}
	for dataType, kind := range vectors.KeyKinds {
		if valueDefinitionKeyKinds[dataType] != kind {
			t.Fatalf("key kind of %q: vectors %q, Go %q", dataType, kind, valueDefinitionKeyKinds[dataType])
		}
	}
	if vectors.MaxEntries != valueDefinitionMaxEntries || vectors.MaxText != valueDefinitionMaxText {
		t.Fatalf("limits: vectors %d/%d, Go %d/%d",
			vectors.MaxEntries, vectors.MaxText, valueDefinitionMaxEntries, valueDefinitionMaxText)
	}
}

// Every judged definition gets the refusal the vectors name, through the
// batch check the engine runs, so the node and every client that preflights
// with the Python rules refuse the same definitions with the same words.
func TestValueDefinitionCasesAreJudgedAsTheVectorsSay(t *testing.T) {
	for _, tc := range readValueDefinitionVectors(t).Cases {
		t.Run(tc.Name, func(t *testing.T) {
			definitions := tc.Definitions
			if tc.GeneratedEntries > 0 {
				generated := map[string]any{}
				for i := 0; i < tc.GeneratedEntries; i++ {
					generated[strconv.Itoa(i)] = "v"
				}
				definitions = generated
			}
			record := map[string]any{"id": "01KZ0000000000000000000001", "name": "code"}
			if tc.DataType != nil {
				record["data_type"] = *tc.DataType
			}
			if definitions != nil {
				record["value_definitions"] = definitions
			}
			payload, err := json.Marshal(record)
			if err != nil {
				t.Fatal(err)
			}
			err = CheckSignalValueDefinitions([]StateRecord{{Topic: "colca/v1/_Signal/n-1/line/code", Payload: payload}})
			if tc.Refusal == nil {
				if err != nil {
					t.Fatalf("valid definition refused: %v", err)
				}
				return
			}
			var field *FieldError
			if !errors.As(err, &field) {
				t.Fatalf("want refusal %q, got %v", *tc.Refusal, err)
			}
			if field.Refusal() != *tc.Refusal {
				t.Fatalf("refusal = %q, want %q", field.Refusal(), *tc.Refusal)
			}
			if Validate("_Signal", payload) == nil {
				t.Fatal("the floor validator must refuse what the batch check refuses")
			}
		})
	}
}
