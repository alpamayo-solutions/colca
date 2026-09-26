// Package historian turns _Metric records from a node's metrics stream into rows
// of historian_metric.
package historian

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// ErrNotAMeasurement means the record carries nothing to historise: a
// tombstone, or a record that is not a _Metric. A null value is not this: it
// says the value went missing, and it is stored as a row without a value.
var ErrNotAMeasurement = errors.New("historian: record carries no measurement")

// Row is one `historian_metric` row.
//
// At most one of Number/Text/Bool/JSON is set: the table has a column per kind
// and the API reads them in that order, so putting a value in two would make
// which one wins depend on the reader. None set is a retraction: the value went
// missing at Timestamp, and history shows a gap from there to the next value.
type Row struct {
	Timestamp time.Time
	SignalID  string
	NodeID    string

	Number *float64
	Text   *string
	Bool   *bool
	JSON   json.RawMessage

	// Offset and Topic are not written; they identify a refused row in logs and
	// metrics.
	Offset int64
	Topic  string
}

// Missing reports whether the row is a retraction, with no value column set.
func (r Row) Missing() bool {
	return r.Number == nil && r.Text == nil && r.Bool == nil && len(r.JSON) == 0
}

// RowFrom decodes one stored record. ts, the store's ingest time, is used only
// when the payload has no timestamp: a metric records when it was measured.
func RowFrom(topic string, payload []byte, ts int64) (Row, error) {
	// _Log records share the metrics stream; only _Metric rows are stored.
	if parsed, err := uns.Parse(topic); err == nil && parsed.Contract != "_Metric" {
		return Row{}, ErrNotAMeasurement
	}
	// json.Number keeps the digits as written, so values beyond float64 precision
	// survive.
	decoder := json.NewDecoder(strings.NewReader(string(payload)))
	decoder.UseNumber()

	var body struct {
		SignalID  string          `json:"signal_id"`
		NodeID    string          `json:"colca_node_id"`
		Timestamp *json.Number    `json:"timestamp"`
		Value     json.RawMessage `json:"value"`
		Deleted   bool            `json:"deleted"`
	}
	if err := decoder.Decode(&body); err != nil {
		return Row{}, fmt.Errorf("historian: undecodable _Metric payload: %w", err)
	}
	if body.SignalID == "" {
		return Row{}, errors.New(
			"historian: _Metric without signal_id — the row would be unattributable, and " +
				"nothing downstream could repair it")
	}
	if body.Deleted || len(body.Value) == 0 {
		return Row{}, ErrNotAMeasurement
	}

	row := Row{
		SignalID:  body.SignalID,
		NodeID:    body.NodeID,
		Timestamp: timestampOf(body.Timestamp, ts),
	}
	if row.NodeID == "" {
		row.NodeID = nodeFromTopic(topic)
	}
	if err := setValue(&row, body.Value); err != nil {
		return Row{}, err
	}
	return row, nil
}

// nodeFromTopic reads the publishing node from level 4. The signal always comes
// from the payload.
func nodeFromTopic(topic string) string {
	parts := strings.Split(topic, "/")
	if len(parts) < 4 {
		return ""
	}
	return parts[3]
}

// timestampOf decodes the payload's timestamp in unix seconds (a float), the
// unit publishers use. The fallback is the store's ingest time in milliseconds.
func timestampOf(payload *json.Number, storeTS int64) time.Time {
	if payload != nil {
		if f, err := payload.Float64(); err == nil {
			sec := math.Floor(f)
			nsec := int64(math.Round((f - sec) * float64(time.Second)))
			return time.Unix(int64(sec), nsec).UTC()
		}
	}
	return time.UnixMilli(storeTS).UTC()
}

func setValue(row *Row, raw json.RawMessage) error {
	trimmed := strings.TrimSpace(string(raw))
	switch {
	case trimmed == "null":
		return nil // a retraction: every value column stays NULL
	case trimmed == "true" || trimmed == "false":
		b := trimmed == "true"
		row.Bool = &b
		return nil
	case strings.HasPrefix(trimmed, "\""):
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return fmt.Errorf("historian: undecodable string value: %w", err)
		}
		row.Text = &s
		return nil
	case strings.HasPrefix(trimmed, "{"), strings.HasPrefix(trimmed, "["):
		row.JSON = json.RawMessage(trimmed)
		return nil
	}

	// A bare number. Keep it numeric when a float64 holds it exactly, and fall
	// back to JSON when it does not, so the digits are never rounded away.
	number := json.Number(trimmed)
	f, err := number.Float64()
	if err != nil {
		return fmt.Errorf("historian: undecodable numeric value %q: %w", trimmed, err)
	}
	if json.Number(fmt.Sprintf("%v", f)) != number && !representsExactly(number, f) {
		row.JSON = json.RawMessage(trimmed)
		return nil
	}
	row.Number = &f
	return nil
}

// representsExactly reports whether the float round-trips to the same digits.
func representsExactly(n json.Number, f float64) bool {
	if i, err := n.Int64(); err == nil {
		return float64(i) == f && int64(f) == i
	}
	return true // a decimal literal: float64 is the intended representation
}
