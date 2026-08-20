package historian

import (
	"encoding/json"
	"os"
	"testing"
)

// The Go half of the shared metric-row vectors (the Python half is
// api/src/historian/tests/test_metric_row_vectors.py). Which column a
// measurement lands in is decided here and read back there, in another
// language: a value written to value_number and read from value_text simply
// disappears from a chart, and nothing would fail. One dataset, judged twice.
const vectorPath = "../../contracts/src/colca_data_contracts/vectors/metric_rows.json"

type metricVectors struct {
	Cases []struct {
		Why     string          `json:"why"`
		Payload json.RawMessage `json:"payload"`
		Column  string          `json:"column"`
		Stored  json.RawMessage `json:"stored"`
	} `json:"cases"`
	NotMeasurements []struct {
		Why     string          `json:"why"`
		Payload json.RawMessage `json:"payload"`
	} `json:"not_measurements"`
	Refused []struct {
		Why     string          `json:"why"`
		Payload json.RawMessage `json:"payload"`
	} `json:"refused"`
}

func loadVectors(t *testing.T) metricVectors {
	t.Helper()
	raw, err := os.ReadFile(vectorPath)
	if err != nil {
		t.Fatalf("reading %s: %v", vectorPath, err)
	}
	var vectors metricVectors
	if err := json.Unmarshal(raw, &vectors); err != nil {
		t.Fatalf("parsing %s: %v", vectorPath, err)
	}
	if len(vectors.Cases) == 0 {
		t.Fatalf("%s carries no cases — the vectors would pass vacuously", vectorPath)
	}
	return vectors
}

func columnOf(row Row) (string, any) {
	switch {
	case row.Number != nil:
		return "value_number", *row.Number
	case row.Text != nil:
		return "value_text", *row.Text
	case row.Bool != nil:
		return "value_bool", *row.Bool
	case row.JSON != nil:
		return "value_json", string(row.JSON)
	}
	return "", nil
}

func TestGoldenVectorsLandInTheirColumn(t *testing.T) {
	vectors := loadVectors(t)
	for _, tc := range vectors.Cases {
		t.Run(tc.Why, func(t *testing.T) {
			row, err := RowFrom("colca/v1/_Metric/m1/press3/temp", tc.Payload, 1755600000000)
			if err != nil {
				t.Fatalf("RowFrom: %v", err)
			}
			column, value := columnOf(row)
			if column != tc.Column {
				t.Fatalf("landed in %s, want %s (value %v)", column, tc.Column, value)
			}
			if !sameValue(t, tc.Stored, value) {
				t.Fatalf("%s = %v, want %s", column, value, tc.Stored)
			}
		})
	}
}

func TestGoldenVectorsThatAreNotMeasurements(t *testing.T) {
	vectors := loadVectors(t)
	for _, tc := range vectors.NotMeasurements {
		t.Run(tc.Why, func(t *testing.T) {
			if _, err := RowFrom("colca/v1/_Metric/m1/t", tc.Payload, 1); err != ErrNotAMeasurement {
				t.Fatalf("err = %v, want ErrNotAMeasurement", err)
			}
		})
	}
}

func TestGoldenVectorsThatAreRefused(t *testing.T) {
	vectors := loadVectors(t)
	for _, tc := range vectors.Refused {
		t.Run(tc.Why, func(t *testing.T) {
			if _, err := RowFrom("colca/v1/_Metric/m1/t", tc.Payload, 1); err == nil {
				t.Fatal("accepted")
			}
		})
	}
}

// sameValue compares the stored value against the vector's expectation through
// JSON, so 42 and 42.0 agree without the test caring which Go type carried it.
func sameValue(t *testing.T, want json.RawMessage, got any) bool {
	t.Helper()
	gotJSON, err := json.Marshal(got)
	if err != nil {
		t.Fatalf("marshalling %v: %v", got, err)
	}
	var a, b any
	if err := json.Unmarshal(want, &a); err != nil {
		t.Fatalf("vector value %s: %v", want, err)
	}
	// A JSON column round-trips as a string of JSON; compare its content.
	if s, ok := got.(string); ok {
		if err := json.Unmarshal([]byte(s), &b); err == nil {
			return jsonEqual(a, b)
		}
	}
	if err := json.Unmarshal(gotJSON, &b); err != nil {
		t.Fatalf("re-parsing %s: %v", gotJSON, err)
	}
	return jsonEqual(a, b)
}

func jsonEqual(a, b any) bool {
	left, _ := json.Marshal(a)
	right, _ := json.Marshal(b)
	return string(left) == string(right)
}
