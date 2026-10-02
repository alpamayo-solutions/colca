package historian

import (
	"encoding/json"
	"errors"
	"testing"
	"time"
)

// Level 4 of the topic is the publishing node, never the signal's owner, so the
// signal must come from the payload.
func TestRowTakesTheSignalFromThePayloadNotTheTopic(t *testing.T) {
	row, err := RowFrom("colca/v1/_Metric/m1/press3/temp",
		[]byte(`{"signal_id":"sig-1","value":21.5}`), 1755600000000)
	if err != nil {
		t.Fatalf("RowFrom: %v", err)
	}
	if row.SignalID != "sig-1" {
		t.Fatalf("SignalID = %q, want %q (from the payload, not the topic)", row.SignalID, "sig-1")
	}
	if row.Number == nil || *row.Number != 21.5 {
		t.Fatalf("Number = %v, want 21.5", row.Number)
	}
}

func TestEachValueKindLandsInItsOwnColumn(t *testing.T) {
	cases := []struct {
		name    string
		payload string
		check   func(Row) bool
	}{
		{"number", `{"signal_id":"s","value":1.5}`, func(r Row) bool { return r.Number != nil && r.Text == nil }},
		{"text", `{"signal_id":"s","value":"warm"}`, func(r Row) bool { return r.Text != nil && r.Number == nil }},
		{"bool", `{"signal_id":"s","value":true}`, func(r Row) bool { return r.Bool != nil && r.Number == nil }},
		{"object", `{"signal_id":"s","value":{"x":1}}`, func(r Row) bool { return r.JSON != nil }},
		{"array", `{"signal_id":"s","value":[1,2]}`, func(r Row) bool { return r.JSON != nil }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			row, err := RowFrom("colca/v1/_Metric/m1/t", []byte(tc.payload), 1)
			if err != nil {
				t.Fatalf("RowFrom: %v", err)
			}
			if !tc.check(row) {
				t.Fatalf("%s landed wrong: number=%v text=%v bool=%v json=%s",
					tc.name, row.Number, row.Text, row.Bool, row.JSON)
			}
		})
	}
}

// A number not representable as a float64 must still historise exactly.
func TestALargeIntegerKeepsItsValue(t *testing.T) {
	row, err := RowFrom("colca/v1/_Metric/m1/t", []byte(`{"signal_id":"s","value":9007199254740993}`), 1)
	if err != nil {
		t.Fatalf("RowFrom: %v", err)
	}
	if row.JSON == nil {
		t.Fatalf("a value beyond float64 precision was stored as a float: %v", row.Number)
	}
	if string(row.JSON) != "9007199254740993" {
		t.Fatalf("JSON = %s, want the digits unchanged", row.JSON)
	}
}

func TestARecordWithoutASignalIsRefused(t *testing.T) {
	// Without a signal the row would be unattributable, so it is refused.
	if _, err := RowFrom("colca/v1/_Metric/m1/t", []byte(`{"value":1}`), 1); err == nil {
		t.Fatal("a metric with no signal_id was accepted")
	}
}

func TestTheRecordsOwnTimestampWins(t *testing.T) {
	// A metric carries when it was measured, in unix seconds; the store's ts is
	// when it was ingested.
	row, err := RowFrom("colca/v1/_Metric/m1/t",
		[]byte(`{"signal_id":"s","value":1,"timestamp":1700000000}`), 1755600000000)
	if err != nil {
		t.Fatalf("RowFrom: %v", err)
	}
	if got := row.Timestamp.UnixMilli(); got != 1700000000000 {
		t.Fatalf("Timestamp = %d, want the payload's 1700000000 (seconds) as 1700000000000ms", got)
	}
}

// TestARecordsFractionalSecondTimestampSurvives: sub-second precision must
// round-trip.
func TestARecordsFractionalSecondTimestampSurvives(t *testing.T) {
	row, err := RowFrom("colca/v1/_Metric/m1/t",
		[]byte(`{"signal_id":"s","value":1,"timestamp":1700000000.25}`), 1)
	if err != nil {
		t.Fatalf("RowFrom: %v", err)
	}
	if got := row.Timestamp.UnixNano(); got != 1700000000250000000 {
		t.Fatalf("Timestamp = %d ns, want 1700000000250000000 (1700000000.25s)", got)
	}
}

func TestWithoutAPayloadTimestampTheStoresIsUsed(t *testing.T) {
	row, err := RowFrom("colca/v1/_Metric/m1/t", []byte(`{"signal_id":"s","value":1}`), 1755600000000)
	if err != nil {
		t.Fatalf("RowFrom: %v", err)
	}
	if got := row.Timestamp.UnixMilli(); got != 1755600000000 {
		t.Fatalf("Timestamp = %d, want the record's 1755600000000", got)
	}
}

func TestTheNodeIdComesFromTheTopicWhenThePayloadOmitsIt(t *testing.T) {
	row, err := RowFrom("colca/v1/_Metric/n-edge1/press3/temp", []byte(`{"signal_id":"s","value":1}`), 1)
	if err != nil {
		t.Fatalf("RowFrom: %v", err)
	}
	if row.NodeID != "n-edge1" {
		t.Fatalf("NodeID = %q, want n-edge1 (level 4)", row.NodeID)
	}
}

func TestAMalformedPayloadIsRefusedRatherThanGuessed(t *testing.T) {
	if _, err := RowFrom("colca/v1/_Metric/m1/t", []byte(`{"signal_id":`), 1); err == nil {
		t.Fatal("a truncated payload was accepted")
	}
}

func TestATombstoneIsNotARow(t *testing.T) {
	// _Metric is tombstone-capable (contracts bundle). A tombstone deletes the
	// retained value; it is not a measurement and must not become one.
	row, err := RowFrom("colca/v1/_Metric/m1/t", []byte(`{"signal_id":"s","deleted":true}`), 1)
	if !errors.Is(err, ErrNotAMeasurement) {
		t.Fatalf("err = %v (row %+v), want ErrNotAMeasurement", err, row)
	}
}

func TestANullValueIsARetraction(t *testing.T) {
	row, err := RowFrom("colca/v1/_Metric/n1/m1/t",
		[]byte(`{"signal_id":"s","colca_node_id":"n1","timestamp":1755600000.5,"value":null}`), 1)
	if err != nil {
		t.Fatalf("RowFrom: %v — a null value says the value went missing, history must keep that", err)
	}
	if !row.Missing() || row.SignalID != "s" || row.NodeID != "n1" {
		t.Fatalf("row = %+v, want a retraction of s on n1", row)
	}
	if want := time.Unix(1755600000, 5e8).UTC(); !row.Timestamp.Equal(want) {
		t.Fatalf("Timestamp = %s, want %s", row.Timestamp, want)
	}
}

func TestRowJSONStaysValidJSON(t *testing.T) {
	row, err := RowFrom("colca/v1/_Metric/m1/t", []byte(`{"signal_id":"s","value":{"a":[1,2]}}`), 1)
	if err != nil {
		t.Fatalf("RowFrom: %v", err)
	}
	var back any
	if err := json.Unmarshal(row.JSON, &back); err != nil {
		t.Fatalf("stored JSON does not round-trip: %v", err)
	}
}

// _Log records on the metrics stream are skipped without an error or a warning.
func TestANonMetricRecordIsPassedBySilently(t *testing.T) {
	_, err := RowFrom("colca/v1/_Log/n1/line1/dataops/INFO",
		[]byte(`{"timestamp":"2026-08-25T19:38:09+00:00","level":"INFO","message":"hello"}`), 1)
	if !errors.Is(err, ErrNotAMeasurement) {
		t.Fatalf("err = %v, want ErrNotAMeasurement — a _Log record is not undecodable, it is not a metric", err)
	}
}
