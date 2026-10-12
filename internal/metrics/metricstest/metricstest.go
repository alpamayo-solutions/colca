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
// `colca_rejected_publishes_total{reason="identity"}`. A missing line fails the
// test: it means the wrong name or labels were asked for.
func Value(t testing.TB, m *metrics.Metrics, line string) float64 {
	t.Helper()
	v, ok, body := lookup(t, m, line)
	if !ok {
		t.Fatalf("metric line %q not found in scrape:\n%s", line, body)
	}
	return v
}

// Lookup is Value for a series that is exported only while its value is known:
// ok is false when the scrape does not carry the line.
func Lookup(t testing.TB, m *metrics.Metrics, line string) (v float64, ok bool) {
	t.Helper()
	v, ok, _ = lookup(t, m, line)
	return v, ok
}

func lookup(t testing.TB, m *metrics.Metrics, line string) (v float64, ok bool, body string) {
	t.Helper()
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/metrics", nil))
	body = rec.Body.String()
	for _, l := range strings.Split(body, "\n") {
		if rest, found := strings.CutPrefix(l, line+" "); found {
			v, err := strconv.ParseFloat(strings.TrimSpace(rest), 64)
			if err != nil {
				t.Fatalf("parse metric line %q: %v", l, err)
			}
			return v, true, body
		}
	}
	return 0, false, body
}
