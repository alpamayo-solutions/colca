package bench

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
)

type Bound struct {
	Min *float64 `json:"min"`
	Max *float64 `json:"max"`
}

// Check compares the newest report of each scenario in resultsPath against
// thresholdsPath. Metrics with null/absent bounds are printed but never gate —
// that is how the harness ships BEFORE target-hardware numbers exist. A missing
// file (either side) is an error: a gate that cannot read its inputs must fail,
// not pass.
func Check(resultsPath, thresholdsPath string, w io.Writer) error {
	rawR, err := os.ReadFile(resultsPath)
	if err != nil {
		return fmt.Errorf("read results: %w", err)
	}
	var reports []Report
	if err := json.Unmarshal(rawR, &reports); err != nil {
		return fmt.Errorf("parse results: %w", err)
	}
	rawT, err := os.ReadFile(thresholdsPath)
	if err != nil {
		return fmt.Errorf("read thresholds: %w", err)
	}
	var thresholds map[string]map[string]Bound
	if err := json.Unmarshal(rawT, &thresholds); err != nil {
		return fmt.Errorf("parse thresholds: %w", err)
	}

	// newest report per scenario wins (results files are append-only)
	latest := map[string]Report{}
	for _, r := range reports {
		latest[r.Scenario] = r
	}

	var violations []string
	for scenario, bounds := range thresholds {
		rep, ok := latest[scenario]
		if !ok {
			continue // scenario not in this run; gate only what was measured
		}
		for metric, b := range bounds {
			v, ok := rep.Metrics[metric]
			if !ok {
				violations = append(violations, fmt.Sprintf("%s: metric %q missing from results", scenario, metric))
				continue
			}
			status := "ok (ungated)"
			if b.Min != nil && v < *b.Min {
				violations = append(violations, fmt.Sprintf("%s: %s = %.1f < min %.1f", scenario, metric, v, *b.Min))
				status = "VIOLATION"
			} else if b.Max != nil && v > *b.Max {
				violations = append(violations, fmt.Sprintf("%s: %s = %.1f > max %.1f", scenario, metric, v, *b.Max))
				status = "VIOLATION"
			} else if b.Min != nil || b.Max != nil {
				status = "ok"
			}
			fmt.Fprintf(w, "check %-11s %-32s %12.1f  %s\n", scenario, metric, v, status)
		}
	}
	if len(violations) > 0 {
		return fmt.Errorf("%d threshold violation(s):\n  %s", len(violations), strings.Join(violations, "\n  "))
	}
	return nil
}
