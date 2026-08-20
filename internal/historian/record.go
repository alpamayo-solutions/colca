// Package historian turns `_Metric` records into hypertable rows.
//
// The sink is the table the deleted Kafka pipeline used to fill —
// `historian_metric`, with its unique index on (signal_id, timestamp) — so
// Grafana and the API's history endpoints read what they always read. What
// changed is where the records come from: colca's `metrics` stream, followed
// with a cursor, instead of a topic on a broker nobody runs any more.
package historian

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// ErrNotAMeasurement means the record is well-formed but carries no value to
// historise — a tombstone, which deletes a retained value rather than
// measuring one.
var ErrNotAMeasurement = errors.New("historian: record carries no measurement")

// Row is one `historian_metric` row.
//
// Exactly one of Number/Text/Bool/JSON is set: the table has a column per kind
// and the API reads them in that order, so putting a value in two would make
// which one wins depend on the reader.
type Row struct {
	Timestamp time.Time
	SignalID  string
	NodeID    string

	Number *float64
	Text   *string
	Bool   *bool
	JSON   json.RawMessage
}

// RowFrom decodes one stored record.
//
// `ts` is the store's ingest timestamp, used only when the payload carries none
// of its own: a metric records when it was MEASURED, and historising the ingest
// time would silently re-date everything that arrives after an outage.
func RowFrom(topic string, payload []byte, ts int64) (Row, error) {
	// json.Number keeps the digits as written, so a value beyond float64
	// precision survives — the door hands payloads through verbatim for
	// exactly this reason, and this is the last place it could be lost.
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

// nodeFromTopic reads level 4 — `colca/v1/_Metric/<node>/…`.
//
// It is the PUBLISHER's id, which is why the signal is never taken from here:
// a machine publishes its own metrics under its own id, and the signal they
// belong to lives in the payload.
func nodeFromTopic(topic string) string {
	parts := strings.Split(topic, "/")
	if len(parts) < 4 {
		return ""
	}
	return parts[3]
}

func timestampOf(payload *json.Number, storeTS int64) time.Time {
	if payload != nil {
		if ms, err := payload.Int64(); err == nil {
			return time.UnixMilli(ms).UTC()
		}
		if f, err := payload.Float64(); err == nil {
			return time.UnixMilli(int64(f)).UTC()
		}
	}
	return time.UnixMilli(storeTS).UTC()
}

func setValue(row *Row, raw json.RawMessage) error {
	trimmed := strings.TrimSpace(string(raw))
	switch {
	case trimmed == "null":
		return ErrNotAMeasurement
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
