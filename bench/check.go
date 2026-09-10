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
// thresholdsPath. Metrics with null bounds are printed but never gate. Missing
// files are errors, and so is a scenario with a real bound that has no result;
// a scenario whose bounds are all null may be absent.
func Check(resultsPath, thresholdsPath string, w io.Writer) error {
	reports, err := ReadRecords(resultsPath)
	if err != nil {
		return fmt.Errorf("read results: %w", err)
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
			if hasActiveBound(bounds) {
				violations = append(violations, fmt.Sprintf("scenario %q with active bounds missing from results", scenario))
			}
			// Every bound is null (report-only), so an absent scenario is fine.
			continue
		}
		for metric, b := range bounds {
			v, ok := rep.Metrics[metric]
			if !ok {
				violations = append(violations, fmt.Sprintf("%s: metric %q missing from results", scenario, metric))
				continue
			}
			status := "ok (ungated)"
			switch {
			case b.Min != nil && v < *b.Min:
				violations = append(violations, fmt.Sprintf("%s: %s = %.1f < min %.1f", scenario, metric, v, *b.Min))
				status = "VIOLATION"
			case b.Max != nil && v > *b.Max:
				violations = append(violations, fmt.Sprintf("%s: %s = %.1f > max %.1f", scenario, metric, v, *b.Max))
				status = "VIOLATION"
			case b.Min != nil || b.Max != nil:
				status = "ok"
			}
			_, _ = fmt.Fprintf(w, "check %-11s %-32s %12.1f  %s\n", scenario, metric, v, status)
		}
	}
	if len(violations) > 0 {
		return fmt.Errorf("%d threshold violation(s):\n  %s", len(violations), strings.Join(violations, "\n  "))
	}
	return nil
}

// hasActiveBound reports whether any metric in bounds has a non-null min or
// max. A scenario whose thresholds are all null is report-only by design and
// must not gate even when it's absent from the results being checked.
func hasActiveBound(bounds map[string]Bound) bool {
	for _, b := range bounds {
		if b.Min != nil || b.Max != nil {
			return true
		}
	}
	return false
}
