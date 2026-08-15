package engine

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/metrics"
	"github.com/alpamayo-solutions/colca/internal/store"
)

// scrapeMetric parses the Prometheus text exposition from m.Handler() and
// returns the value of one exact family+labels line, e.g.
// `colca_rejected_publishes_total{reason="identity"}`. It fails the test if
// the line is not present — every family here is pre-created and zero-valued,
// so a missing line means the wrong family/label was asked for, not that the
// metric hasn't fired yet.
func scrapeMetric(t *testing.T, m *metrics.Metrics, line string) float64 {
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

// newMetricsEngine builds an engine wired to a live *metrics.Metrics (unlike
// newEngine/newRecordingEngine, which pass nil to keep the plain behavioral
// tests metrics-agnostic). It has the same two clients as newRecordingEngine:
// "m1" mounted at "m1", and "observer" with no mount (a read-only client).
func newMetricsEngine(t *testing.T) (*Engine, *metrics.Metrics) {
	t.Helper()
	s, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	m := metrics.New(s)
	cfg := &config.Config{ULID: "n-edge1", Clients: []config.Client{
		{ULID: "m1", Token: "tok", Mount: "m1"},
		{ULID: "observer", Token: "observer-secret"},
	}}
	return New(s, cfg, nil, m), m
}

// TestRejectPublishByReason pins the reason mapping for every reject branch in
// IngestClient, IngestAdmin and IngestDownlink: each trigger below must
// increment exactly its own reason child by exactly one, and touch no other
// reason.
func TestRejectPublishByReason(t *testing.T) {
	cases := []struct {
		name   string
		reason string
		invoke func(e *Engine) error
	}{
		{
			name:   "IngestClient: level-4 != identity",
			reason: metrics.ReasonIdentity,
			invoke: func(e *Engine) error {
				_, err := e.IngestClient("m1", "colca/v1/_Metric/OTHER/temp", []byte(`{"v":1}`))
				return err
			},
		},
		{
			name:   "IngestClient: unparseable topic (too few segments)",
			reason: metrics.ReasonGrammar,
			invoke: func(e *Engine) error {
				_, err := e.IngestClient("m1", "colca/v1/_Metric", []byte(`{"v":1}`))
				return err
			},
		},
		{
			name:   "IngestClient: unknown contract (ClassNone)",
			reason: metrics.ReasonGrammar,
			invoke: func(e *Engine) error {
				_, err := e.IngestClient("m1", "colca/v1/_Unknown/m1/x", []byte(`{"v":1}`))
				return err
			},
		},
		{
			name:   "IngestClient: client publishes a command",
			reason: metrics.ReasonNotCommand,
			invoke: func(e *Engine) error {
				_, err := e.IngestClient("m1", "colca/v1/_CmdParam/m1/x", []byte(`{"correlation_id":"c","expires_at":1}`))
				return err
			},
		},
		{
			name:   "IngestClient: payload fails validation",
			reason: metrics.ReasonValidation,
			invoke: func(e *Engine) error {
				_, err := e.IngestClient("m1", "colca/v1/_Metric/m1/temp", []byte(`{"v":"bad"}`))
				return err
			},
		},
		{
			name:   "IngestClient: mount-less client (observer)",
			reason: metrics.ReasonNoMount,
			invoke: func(e *Engine) error {
				_, err := e.IngestClient("observer", "colca/v1/_Metric/observer/temp", []byte(`{"v":1}`))
				return err
			},
		},
		{
			name:   "IngestAdmin: topic outside colca/#",
			reason: metrics.ReasonGrammar,
			invoke: func(e *Engine) error {
				_, err := e.IngestAdmin("not-uns-at-all", []byte(`{}`))
				return err
			},
		},
		{
			name:   "IngestAdmin: unparseable topic",
			reason: metrics.ReasonGrammar,
			invoke: func(e *Engine) error {
				_, err := e.IngestAdmin("colca/v1/_Metric", []byte(`{}`))
				return err
			},
		},
		{
			name:   "IngestAdmin: unknown contract",
			reason: metrics.ReasonGrammar,
			invoke: func(e *Engine) error {
				_, err := e.IngestAdmin("colca/v1/_Unknown/m1/x", []byte(`{}`))
				return err
			},
		},
		{
			name:   "IngestAdmin: payload fails validation",
			reason: metrics.ReasonValidation,
			invoke: func(e *Engine) error {
				_, err := e.IngestAdmin("colca/v1/_Metric/m1/temp", []byte(`{"v":"bad"}`))
				return err
			},
		},
		{
			name:   "IngestDownlink: unparseable topic",
			reason: metrics.ReasonGrammar,
			invoke: func(e *Engine) error {
				_, err := e.IngestDownlink("colca/v1/_Metric", []byte(`{}`), 1)
				return err
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e, m := newMetricsEngine(t)
			line := `colca_rejected_publishes_total{reason="` + tc.reason + `"}`
			before := scrapeMetric(t, m, line)
			if before != 0 {
				t.Fatalf("precondition: %s = %v, want 0 on a fresh engine", line, before)
			}
			if err := tc.invoke(e); err == nil {
				t.Fatal("trigger must return an error (the publish must be rejected)")
			}
			after := scrapeMetric(t, m, line)
			if after != 1 {
				t.Fatalf("%s = %v after one trigger, want exactly 1", line, after)
			}
			// No other reason may have moved.
			for _, r := range []string{metrics.ReasonIdentity, metrics.ReasonGrammar, metrics.ReasonValidation,
				metrics.ReasonNoMount, metrics.ReasonNotCommand, metrics.ReasonAuth} {
				if r == tc.reason {
					continue
				}
				other := `colca_rejected_publishes_total{reason="` + r + `"}`
				if v := scrapeMetric(t, m, other); v != 0 {
					t.Fatalf("%s moved to %v while triggering reason %q — cross-reason contamination", other, v, tc.reason)
				}
			}
		})
	}
}

// TestIngestRecordOnPersistNotOnReject pins the other half of the ingest
// contract: a successful persist increments colca_ingest_records_total on the
// record's stream, and a rejected publish must not touch it at all.
func TestIngestRecordOnPersistNotOnReject(t *testing.T) {
	e, m := newMetricsEngine(t)

	const line = `colca_ingest_records_total{stream="metrics"}`
	if v := scrapeMetric(t, m, line); v != 0 {
		t.Fatalf("%s = %v before any publish, want 0", line, v)
	}

	if _, err := e.IngestClient("m1", "colca/v1/_Metric/m1/temp", []byte(`{"v":7}`)); err != nil {
		t.Fatal(err)
	}
	if v := scrapeMetric(t, m, line); v != 1 {
		t.Fatalf("%s = %v after one successful persist, want 1", line, v)
	}

	// A rejected publish (identity violation) must not move the ingest counter.
	if _, err := e.IngestClient("m1", "colca/v1/_Metric/OTHER/temp", []byte(`{"v":1}`)); err == nil {
		t.Fatal("identity violation must be rejected")
	}
	if v := scrapeMetric(t, m, line); v != 1 {
		t.Fatalf("%s = %v after a rejected publish, want unchanged 1", line, v)
	}
	if v := scrapeMetric(t, m, `colca_rejected_publishes_total{reason="identity"}`); v != 1 {
		t.Fatalf("rejected/identity = %v, want 1", v)
	}
}

// TestIngestRecordCountsReplicatedApplies pins the decision: replication
// is a fourth entry path into the store, so colca_ingest_records_total counts
// records applied via IngestReplicated exactly like client/admin/downlink
// writes — once per NEWLY applied record, never for a deduplicated re-push.
func TestIngestRecordCountsReplicatedApplies(t *testing.T) {
	e, m := newMetricsEngine(t)
	const line = `colca_ingest_records_total{stream="metrics"}`

	batch := []store.ReplRecord{
		{ChildOffset: 1, Topic: "colca/v1/_Metric/m1/edge1/m1/a", Payload: []byte(`{"v":1}`), TS: 1, KVPath: "edge1/m1/a", KVNode: "m1"},
		{ChildOffset: 2, Topic: "colca/v1/_Metric/m1/edge1/m1/b", Payload: []byte(`{"v":2}`), TS: 2, KVPath: "edge1/m1/b", KVNode: "m1"},
	}
	applied, _, err := e.IngestReplicated("n-edge1", "metrics", batch)
	if err != nil {
		t.Fatal(err)
	}
	if applied != 2 {
		t.Fatalf("applied = %d, want 2", applied)
	}
	if v := scrapeMetric(t, m, line); v != 2 {
		t.Fatalf("%s = %v after replicating 2 new records, want 2", line, v)
	}

	// Re-push the same batch: fully deduplicated, nothing new counted.
	applied, _, err = e.IngestReplicated("n-edge1", "metrics", batch)
	if err != nil {
		t.Fatal(err)
	}
	if applied != 0 {
		t.Fatalf("second apply: applied = %d, want 0 (dedupe)", applied)
	}
	if v := scrapeMetric(t, m, line); v != 2 {
		t.Fatalf("%s = %v after a fully-deduplicated re-push, want unchanged 2", line, v)
	}
}

// TestNilMetricsIsSafe pins the nil-safety contract every caller (including
// every other test in this package) relies on: an engine built with a nil
// *metrics.Metrics must not panic on any reject or persist path.
func TestNilMetricsIsSafe(t *testing.T) {
	e := newEngine(t) // built with New(..., nil, nil)
	if _, err := e.IngestClient("m1", "colca/v1/_Metric/OTHER/temp", []byte(`{"v":1}`)); err == nil {
		t.Fatal("want identity rejection")
	}
	if _, err := e.IngestClient("m1", "colca/v1/_Metric/m1/temp", []byte(`{"v":9}`)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := e.IngestReplicated("n-edge1", "metrics", []store.ReplRecord{
		{ChildOffset: 1, Topic: "colca/v1/_Metric/m1/edge1/m1/a", Payload: []byte(`{"v":1}`), TS: 1},
	}); err != nil {
		t.Fatal(err)
	}
}
