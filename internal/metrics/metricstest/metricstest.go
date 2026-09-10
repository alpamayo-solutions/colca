// Package metricstest reads metric values in tests through the served Prometheus
// text format, since *metrics.Metrics does not expose its registry.
package metricstest

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/alpamayo-solutions/colca/internal/metrics"
)

// Value scrapes m and returns the value of one exact line, such as
// `colca_rejected_publishes_total{reason="identity"}`. Every family is
// pre-created, so a missing line means the wrong name or labels were asked for.
func Value(t testing.TB, m *metrics.Metrics, line string) float64 {
	t.Helper()
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/metrics", nil))
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
