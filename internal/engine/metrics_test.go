package engine

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/metrics"
	"github.com/alpamayo-solutions/colca/internal/metrics/metricstest"
	"github.com/alpamayo-solutions/colca/internal/store"
)

// scrapeMetric reads back one metric value through the shared test helper
// (metricstest.Value) — see that package's doc comment for why this goes
// through Handler() rather than a Collector/Gatherer accessor.
var scrapeMetric = metricstest.Value

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
	m := metrics.New(s, config.Retention{}, nil)
	cfg := &config.Config{ULID: "n-edge1"}
	return New(s, cfg, testIDs(), nil, m, nil), m
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
			name:   "IngestClient: command without a covering cmd grant",
			reason: metrics.ReasonCmdDenied,
			invoke: func(e *Engine) error {
				_, err := e.IngestClient("m1", "colca/v1/_CmdParam/m1/x", []byte(`{"correlation_id":"c","expires_at":1}`))
				return err
			},
		},
		{
			name:   "IngestClient: _EdgeNode at an ordinary door",
			reason: metrics.ReasonRegistryContract,
			invoke: func(e *Engine) error {
				_, err := e.IngestClient("m1", "colca/v1/_EdgeNode/m1/x", []byte(`{"ulid":"m1"}`))
				return err
			},
		},
		{
			name:   "IngestAdmin: _EdgeNode at the admin door",
			reason: metrics.ReasonRegistryContract,
			invoke: func(e *Engine) error {
				_, err := e.IngestAdmin("colca/v1/_EdgeNode/x/y", []byte(`{"ulid":"x"}`))
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
				metrics.ReasonNoMount, metrics.ReasonCmdDenied, metrics.ReasonRegistryContract, metrics.ReasonHumanWrite} {
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

// scrapeBody returns m's full exposition text — used for colca_repl_gap_applied_total,
// whose (child, stream) labels are dynamic (no pre-created children, unlike
// the fixed-label counters), so ABSENCE has to be checked by substring rather
// than metricstest.Value (which fails the test when a line is missing).
func scrapeBody(t *testing.T, m *metrics.Metrics) string {
	t.Helper()
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	return rec.Body.String()
}

// TestGapAppliedCountsOffsetJumpsOnly is the design §8 companion to
// TestIngestReplicatedLogsOffsetJumps (engine_test.go): colca_repl_gap_applied_total
// must move on exactly the same event that triggers the ERROR log — a
// contiguous apply must never create the series, and the commands-stream
// exemption (filtered uplink, not data loss) must hold for the counter too.
func TestGapAppliedCountsOffsetJumpsOnly(t *testing.T) {
	e, m := newMetricsEngine(t)
	metric := "colca/v1/_Metric/m1/edge1/m1/t"
	const line = `colca_repl_gap_applied_total{child="n-edge1",stream="metrics"}`

	if _, _, err := e.IngestReplicated("n-edge1", "metrics", []store.ReplRecord{
		{ChildOffset: 1, Topic: metric, Payload: []byte(`{"v":1}`), TS: 1},
		{ChildOffset: 2, Topic: metric, Payload: []byte(`{"v":2}`), TS: 2},
	}); err != nil {
		t.Fatal(err)
	}
	if body := scrapeBody(t, m); strings.Contains(body, line) {
		t.Fatalf("%s present after a contiguous apply, want no series at all:\n%s", line, body)
	}

	if _, _, err := e.IngestReplicated("n-edge1", "metrics", []store.ReplRecord{
		{ChildOffset: 5, Topic: metric, Payload: []byte(`{"v":5}`), TS: 5},
	}); err != nil {
		t.Fatal(err)
	}
	if v := scrapeMetric(t, m, line); v != 1 {
		t.Fatalf("%s = %v after the 2→5 jump, want 1", line, v)
	}

	// The commands stream is exempt (filtered uplink): no series at all, no
	// matter how large the child-offset hole.
	const commandsLine = `colca_repl_gap_applied_total{child="n-edge1",stream="commands"}`
	if _, _, err := e.IngestReplicated("n-edge1", "commands", []store.ReplRecord{
		{ChildOffset: 9, Topic: "colca/v1/_Ack/m1/edge1/m1/go", Payload: []byte(`{"v":1}`), TS: 9},
	}); err != nil {
		t.Fatal(err)
	}
	if body := scrapeBody(t, m); strings.Contains(body, commandsLine) {
		t.Fatalf("%s present, want no series (commands stream is exempt from jump detection):\n%s", commandsLine, body)
	}
}

// TestIngestRefreshFailuresDoNotCountAsRejectedPublishes is the mutation
// guard for the reroute: IngestRefresh used to fall through to
// e.metrics.RejectPublish on every failure branch, conflating the pruner's
// internal §6.5 repair traffic with a client's rejected publish. Refresh
// failures are now the caller's (retention.Pruner.refreshEntities) concern —
// this engine-level method must leave colca_rejected_publishes_total alone
// entirely, on every one of its own failure branches.
func TestIngestRefreshFailuresDoNotCountAsRejectedPublishes(t *testing.T) {
	e, m := newMetricsEngine(t)
	topic := "colca/v1/_SystemElement/n-edge1/line1/press"
	if _, err := e.IngestAdmin(topic, []byte(`{"id":"P1"}`)); err != nil { // entities offset 1
		t.Fatal(err)
	}
	before := map[string]float64{}
	for _, reason := range []string{metrics.ReasonIdentity, metrics.ReasonGrammar, metrics.ReasonValidation,
		metrics.ReasonNoMount, metrics.ReasonCmdDenied, metrics.ReasonRegistryContract, metrics.ReasonHumanWrite} {
		before[reason] = scrapeMetric(t, m, `colca_rejected_publishes_total{reason="`+reason+`"}`)
	}

	// Non-UNS topic, root-prefixed but unparseable (too few segments —
	// uns.Parse's own grammar error, distinct from IsUns's prefix check),
	// non-KV class (grammar), empty payload (validation), and
	// schema-invalid payload (validation) — every failure branch
	// IngestRefresh has.
	if _, _, err := e.IngestRefresh("not-uns-at-all", []byte(`{}`), 1); err == nil {
		t.Fatal("non-UNS topic must be rejected")
	}
	if _, _, err := e.IngestRefresh("colca/v1/x", []byte(`{}`), 1); err == nil {
		t.Fatal("root-prefixed but unparseable topic (too few segments) must be rejected")
	}
	if _, _, err := e.IngestRefresh("colca/v1/_Ack/n-edge1/line1/x", []byte(`{"correlation_id":"c","result_code":0}`), 1); err == nil {
		t.Fatal("non-KV class must be rejected")
	}
	if _, _, err := e.IngestRefresh(topic, nil, 1); err == nil {
		t.Fatal("empty payload must be rejected")
	}
	if _, _, err := e.IngestRefresh(topic, []byte(`{"not":"a valid SystemElement"}`), 1); err == nil {
		t.Fatal("schema-invalid payload must be rejected")
	}

	for _, reason := range []string{metrics.ReasonIdentity, metrics.ReasonGrammar, metrics.ReasonValidation,
		metrics.ReasonNoMount, metrics.ReasonCmdDenied, metrics.ReasonRegistryContract, metrics.ReasonHumanWrite} {
		if v := scrapeMetric(t, m, `colca_rejected_publishes_total{reason="`+reason+`"}`); v != before[reason] {
			t.Fatalf("colca_rejected_publishes_total{reason=%s} moved from %v to %v after refresh failures — must stay untouched", reason, before[reason], v)
		}
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
