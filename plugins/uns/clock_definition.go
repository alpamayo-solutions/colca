package uns

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
)

// ClockSegment maps real UTC seconds to application UTC seconds.
type ClockSegment struct {
	RealAnchor    float64  `json:"real_anchor"`
	FactoryAnchor float64  `json:"factory_anchor"`
	Rate          float64  `json:"rate"`
	StopAt        *float64 `json:"stop_at"`
	CatchUp       bool     `json:"catch_up"`
}

// ClockDefinition is application time, never the node's operational clock.
// Consumers explicitly select one authority and id. Epochs are UTC seconds.
type ClockDefinition struct {
	ID            string        `json:"id"`
	RunID         string        `json:"run_id"`
	Revision      int64         `json:"revision"`
	RealAnchor    float64       `json:"real_anchor"`
	FactoryAnchor float64       `json:"factory_anchor"`
	Rate          float64       `json:"rate"`
	StopAt        *float64      `json:"stop_at"`
	CatchUp       bool          `json:"catch_up"`
	Previous      *ClockSegment `json:"previous"`
	StartAt       *float64      `json:"start_at"`
}

// DecodeClockDefinition decodes and validates a complete application timeline.
func DecodeClockDefinition(raw []byte) (ClockDefinition, error) {
	var d ClockDefinition
	if err := json.Unmarshal(raw, &d); err != nil {
		return d, err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return d, err
	}
	for _, field := range []string{"id", "run_id", "revision", "real_anchor", "factory_anchor", "rate"} {
		if value, ok := fields[field]; !ok || string(value) == "null" {
			return d, fmt.Errorf("clock %s is required", field)
		}
	}
	if d.ID == "" || d.RunID == "" || d.Revision < 1 {
		return d, fmt.Errorf("clock id, run_id and positive integer revision are required")
	}
	if d.Rate < 0 || d.Rate > 1000 {
		return d, fmt.Errorf("clock rate must be between 0 and 1000")
	}
	if d.StopAt != nil && *d.StopAt < d.FactoryAnchor {
		return d, fmt.Errorf("clock stop_at precedes factory_anchor")
	}
	if d.StartAt != nil && *d.StartAt > d.FactoryAnchor {
		return d, fmt.Errorf("clock start_at follows factory_anchor")
	}
	if d.CatchUp && (d.FactoryAnchor > d.RealAnchor || (d.Rate > 0 && d.Rate < 1)) {
		return d, fmt.Errorf("catch-up requires a past anchor and rate 0 or >= 1")
	}
	if d.Previous != nil {
		var previousFields map[string]json.RawMessage
		if err := json.Unmarshal(fields["previous"], &previousFields); err != nil {
			return d, err
		}
		for _, field := range []string{"real_anchor", "factory_anchor", "rate"} {
			if value, ok := previousFields[field]; !ok || string(value) == "null" {
				return d, fmt.Errorf("previous clock %s is required", field)
			}
		}
		p := d.Previous
		if p.Rate < 0 || p.Rate > 1000 || p.RealAnchor > d.RealAnchor ||
			(p.StopAt != nil && *p.StopAt < p.FactoryAnchor) ||
			(p.CatchUp && (p.FactoryAnchor > p.RealAnchor || (p.Rate > 0 && p.Rate < 1))) ||
			math.Abs(p.At(d.RealAnchor)-d.FactoryAnchor) > 0.000001 {
			return d, fmt.Errorf("invalid previous clock segment")
		}
	}
	return d, nil
}

// At evaluates a definition. A consumer must separately enforce revision,
// synchronization freshness and monotonic emission; this is a pure projection.
func (d ClockDefinition) At(realNow float64) float64 {
	if d.Previous != nil && realNow < d.RealAnchor {
		return d.Previous.At(realNow)
	}
	return d.Segment().At(realNow)
}

// Segment returns the active segment without its revision or prior segment.
func (d ClockDefinition) Segment() ClockSegment {
	return ClockSegment{d.RealAnchor, d.FactoryAnchor, d.Rate, d.StopAt, d.CatchUp}
}

// At projects real UTC seconds onto this segment, including end/catch-up caps.
func (d ClockSegment) At(realNow float64) float64 {
	result := d.FactoryAnchor + math.Max(0, realNow-d.RealAnchor)*d.Rate
	if d.CatchUp {
		result = math.Min(result, realNow)
	}
	if d.StopAt != nil {
		result = math.Min(result, *d.StopAt)
	}
	return result
}

func checkClockRevision(raw, previous []byte) error {
	next, err := DecodeClockDefinition(raw)
	if err != nil {
		return err
	}
	if len(previous) == 0 {
		if next.Previous != nil {
			return fmt.Errorf("new clock must not contain a previous segment")
		}
		if next.Revision != 1 {
			return fmt.Errorf("new clock must begin at revision 1")
		}
		return nil
	}
	old, err := DecodeClockDefinition(previous)
	if err != nil {
		return fmt.Errorf("stored clock is invalid: %w", err)
	}
	if next.ID != old.ID || next.RunID != old.RunID {
		return fmt.Errorf("a new run requires a new clock id")
	}
	if !reflect.DeepEqual(next.StartAt, old.StartAt) {
		return fmt.Errorf("clock start_at is immutable within a run")
	}
	sameStop := (old.StopAt == nil && next.StopAt == nil) || (old.StopAt != nil && next.StopAt != nil && *old.StopAt == *next.StopAt)
	if reflect.DeepEqual(next.Previous, old.Previous) && next.Revision == old.Revision && next.RealAnchor == old.RealAnchor && next.FactoryAnchor == old.FactoryAnchor && next.Rate == old.Rate && next.CatchUp == old.CatchUp && sameStop {
		return nil
	}
	if next.Revision != old.Revision+1 {
		return fmt.Errorf("clock revision conflict: expected %d", old.Revision+1)
	}
	if next.Previous != nil && !reflect.DeepEqual(*next.Previous, old.Segment()) {
		return fmt.Errorf("previous clock segment must match the stored definition")
	}
	if next.RealAnchor < old.RealAnchor || math.Abs(next.FactoryAnchor-old.At(next.RealAnchor)) > 0.000001 {
		return fmt.Errorf("clock update must preserve timeline continuity")
	}
	return nil
}
