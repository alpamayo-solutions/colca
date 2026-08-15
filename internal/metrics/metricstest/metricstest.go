// Package metricstest is a test-only helper for reading back individual
// metric values through *metrics.Metrics' public HTTP contract.
//
// *metrics.Metrics exposes no Collector/Gatherer accessor (its registry is
// unexported, by design — the only production surface is Handler() serving
// the Prometheus text format), so packages outside internal/metrics cannot
// use prometheus/client_golang/prometheus/testutil.ToFloat64 directly. This
// package scrapes through the same Handler() every real Prometheus client
// uses, then parses the one exposition line the caller asked for — exercising
// the actual served contract rather than reaching into internals.
package metricstest

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/alpamayo-solutions/colca/internal/metrics"
)

// Value scrapes m and returns the value of one exact family+labels line, e.g.
// `colca_rejected_publishes_total{reason="identity"}`. It fails t if the line
// is not present — every family in the contract is pre-created and
// zero-valued from construction, so a missing line means the wrong
// family/label was asked for, not that the metric hasn't fired yet.
func Value(t testing.TB, m *metrics.Metrics, line string) float64 {
	t.Helper()
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	for _, l := range strings.Split(rec.Body.String(), "\n") {
		if rest, ok := strings.CutPrefix(l, line+" "); ok {
			v, err := strconv.ParseFloat(strings.TrimSpace(rest), 64)
			if err != nil {
				t.Fatalf("parse metric line %q: %v", l, err)
			}
			return v
		}
	}
	t.Fatalf("metric line %q not found in scrape:\n%s", line, rec.Body.String())
	return 0
}
