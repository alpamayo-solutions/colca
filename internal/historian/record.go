// Package historian turns `_Metric` records into hypertable rows.
//
// The sink is `historian_metric`, with its unique index on (signal_id,
// timestamp). The records come from a node's `metrics` stream, followed with a
// cursor.
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

	// Offset and Topic are NOT written to historian_metric — they are the
	// stream position this row was decoded from, carried through purely so a
	// row the schema permanently refuses (sink.go's poison classification)
	// can be logged and counted with enough context to find the offending
	// publisher. Set by the bridge in Once; zero-valued and harmless for any
	// caller that predates per-row rejection (the boundary suite included).
	Offset int64
	Topic  string
}

// RowFrom decodes one stored record.
//
// `ts` is the store's ingest timestamp, used only when the payload carries none
// of its own: a metric records when it was MEASURED, and historising the ingest
// time would silently re-date everything that arrives after an outage.
func RowFrom(topic string, payload []byte, ts int64) (Row, error) {
	// The metrics stream carries more than measurements -- `_Log` records
	// ride the same lane -- and only a `_Metric` is one. Anything else is
	// not a decoding failure worth a warning per record; it is simply not
	// this bridge's to historise.
	if parsed, err := uns.Parse(topic); err == nil && parsed.Contract != "_Metric" {
		return Row{}, ErrNotAMeasurement
	}
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

// timestampOf decodes the payload's own timestamp, which is the wire unit
// every publisher uses: unix seconds, float, fractional part allowed
// (franzmq's default `datetime.now(UTC).timestamp()`; connector and dataops
// both encode this way too). The store-TS fallback is a different clock
// entirely — colca's own ingest time, in epoch-MILLISECONDS — and keeps its
// own unit rather than being coerced to match.
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
