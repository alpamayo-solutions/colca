package engine

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/metrics"
	"github.com/alpamayo-solutions/colca/internal/metrics/metricstest"
	"github.com/alpamayo-solutions/colca/internal/registry"
	"github.com/alpamayo-solutions/colca/internal/store"
)

// scrapeMetric reads one metric value through metricstest.Value.
var scrapeMetric = metricstest.Value

// newMetricsEngine builds an engine with live metrics and the same clients as
// newRecordingEngine: m1 with a write grant, hmi with only a cmd grant.
func newMetricsEngine(t *testing.T) (*Engine, *metrics.Metrics) {
	t.Helper()
	s, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	m := metrics.New(s, config.Retention{}, nil)
	cfg := &config.Config{ULID: "n-edge1"}
	e := New(s, cfg, testIDs(), nil, m, nil)
	placeTestElements(t, e)
	return e, m
}

// Each reject branch in IngestClient, IngestAdmin and IngestDownlink increments
// exactly its own reason by one.
func TestRejectPublishByReason(t *testing.T) {
	cases := []struct {
		name   string
		reason string
		invoke func(e *Engine) error
	}{
		{
			name:   "IngestClient: level-4 is not this node",
			reason: metrics.ReasonNodeID,
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
			name:   "IngestClient: _EnrolledIdentity at an ordinary door",
			reason: metrics.ReasonRegistryContract,
			invoke: func(e *Engine) error {
				_, err := e.IngestClient("m1", "colca/v1/_EnrolledIdentity/m1/_colca/identities/m1", []byte(`{"ulid":"m1"}`))
				return err
			},
		},
		{
			name:   "IngestAdmin: _EnrolledIdentity at the admin door",
			reason: metrics.ReasonRegistryContract,
			invoke: func(e *Engine) error {
				_, err := e.IngestAdmin("colca/v1/_EnrolledIdentity/n1/_colca/identities/x", []byte(`{"ulid":"x"}`))
				return err
			},
		},
		{
			name:   "IngestClient: payload fails validation",
			reason: metrics.ReasonValidation,
			invoke: func(e *Engine) error {
				_, err := e.IngestClient("m1", "colca/v1/_Metric/n-edge1/m1/temp", []byte(`{"v":"bad"}`))
				return err
			},
		},
		{
			name:   "IngestClient: no write scope covers the topic",
			reason: metrics.ReasonWriteDenied,
			invoke: func(e *Engine) error {
				_, err := e.IngestClient("hmi", "colca/v1/_Metric/n-edge1/hmi/temp", []byte(`{"v":1}`))
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
			for _, r := range []string{metrics.ReasonNodeID, metrics.ReasonGrammar, metrics.ReasonValidation,
				metrics.ReasonWriteDenied, metrics.ReasonCmdDenied, metrics.ReasonRegistryContract, metrics.ReasonHumanWrite} {
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

// A persist increments colca_ingest_records_total for its stream; a rejected
// publish does not.
func TestIngestRecordOnPersistNotOnReject(t *testing.T) {
	e, m := newMetricsEngine(t)

	const line = `colca_ingest_records_total{stream="metrics"}`
	if v := scrapeMetric(t, m, line); v != 0 {
		t.Fatalf("%s = %v before any publish, want 0", line, v)
	}

	if _, err := e.IngestClient("m1", "colca/v1/_Metric/n-edge1/m1/temp", []byte(`{"v":7}`)); err != nil {
		t.Fatal(err)
	}
	if v := scrapeMetric(t, m, line); v != 1 {
		t.Fatalf("%s = %v after one successful persist, want 1", line, v)
	}

	// A rejected publish (wrong level-4) must not move the ingest counter.
	if _, err := e.IngestClient("m1", "colca/v1/_Metric/OTHER/temp", []byte(`{"v":1}`)); err == nil {
		t.Fatal("wrong level-4 must be rejected")
	}
	if v := scrapeMetric(t, m, line); v != 1 {
		t.Fatalf("%s = %v after a rejected publish, want unchanged 1", line, v)
	}
	if v := scrapeMetric(t, m, `colca_rejected_publishes_total{reason="node_id"}`); v != 1 {
		t.Fatalf("rejected/node_id = %v, want 1", v)
	}
}

// Replicated applies count toward colca_ingest_records_total once per newly
// applied record, never for a deduplicated re-push.
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

// scrapeBody returns m's full exposition text. colca_repl_gap_applied_total has
// dynamic labels, so its absence is checked by substring.
func scrapeBody(t *testing.T, m *metrics.Metrics) string {
	t.Helper()
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	return rec.Body.String()
}

// colca_repl_gap_applied_total moves on exactly the event that logs an offset
// jump: never for a contiguous apply, never on a filtered uplink.
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

	// The filtered uplinks (commands and entities) create no series, however large
	// the hole.
	for stream, topic := range map[string]string{
		"commands": "colca/v1/_Ack/m1/edge1/m1/go",
		"entities": "colca/v1/_SystemElement/m1/edge1/m1/a",
	} {
		exemptLine := `colca_repl_gap_applied_total{child="n-edge1",stream="` + stream + `"}`
		if _, _, err := e.IngestReplicated("n-edge1", stream, []store.ReplRecord{
			{ChildOffset: 9, Topic: topic, Payload: []byte(`{"v":1}`), TS: 9},
		}); err != nil {
			t.Fatal(err)
		}
		if body := scrapeBody(t, m); strings.Contains(body, exemptLine) {
			t.Fatalf("%s present, want no series (%s stream is exempt from jump detection):\n%s", exemptLine, stream, body)
		}
	}
}

// IngestRefresh failures are the pruner's to count; the engine must leave
// colca_rejected_publishes_total alone on every failure branch.
func TestIngestRefreshFailuresDoNotCountAsRejectedPublishes(t *testing.T) {
	e, m := newMetricsEngine(t)
	topic := "colca/v1/_SystemElement/n-edge1/line1/press"
	if _, err := e.IngestAdmin(topic, []byte(`{"id":"P1"}`)); err != nil { // entities offset 1
		t.Fatal(err)
	}
	before := map[string]float64{}
	for _, reason := range []string{metrics.ReasonNodeID, metrics.ReasonGrammar, metrics.ReasonValidation,
		metrics.ReasonWriteDenied, metrics.ReasonCmdDenied, metrics.ReasonRegistryContract, metrics.ReasonHumanWrite} {
		before[reason] = scrapeMetric(t, m, `colca_rejected_publishes_total{reason="`+reason+`"}`)
	}

	// Every IngestRefresh failure branch: non-UNS topic, unparseable topic, non-KV
	// class, empty payload and schema-invalid payload.
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

	for _, reason := range []string{metrics.ReasonNodeID, metrics.ReasonGrammar, metrics.ReasonValidation,
		metrics.ReasonWriteDenied, metrics.ReasonCmdDenied, metrics.ReasonRegistryContract, metrics.ReasonHumanWrite} {
		if v := scrapeMetric(t, m, `colca_rejected_publishes_total{reason="`+reason+`"}`); v != before[reason] {
			t.Fatalf("colca_rejected_publishes_total{reason=%s} moved from %v to %v after refresh failures — must stay untouched", reason, before[reason], v)
		}
	}
}

// An engine with nil metrics does not panic on any reject or persist path.
func TestNilMetricsIsSafe(t *testing.T) {
	e := newEngine(t) // built with New(..., nil, nil)
	if _, err := e.IngestClient("m1", "colca/v1/_Metric/OTHER/temp", []byte(`{"v":1}`)); err == nil {
		t.Fatal("want level-4 rejection")
	}
	if _, err := e.IngestClient("m1", "colca/v1/_Metric/n-edge1/m1/temp", []byte(`{"v":9}`)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := e.IngestReplicated("n-edge1", "metrics", []store.ReplRecord{
		{ChildOffset: 1, Topic: "colca/v1/_Metric/m1/edge1/m1/a", Payload: []byte(`{"v":1}`), TS: 1},
	}); err != nil {
		t.Fatal(err)
	}
}

// newRoutedMetricsEngine is newMetricsEngine with one child node enrolled at
// site1/edge1, enough to ask the routability question.
func newRoutedMetricsEngine(t *testing.T) (*Engine, *metrics.Metrics) {
	t.Helper()
	s, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	m := metrics.New(s, config.Retention{}, nil)
	ids := testIDs()
	ids.routes = []string{"site1/edge1"}
	e := New(s, &config.Config{ULID: "n-edge1"}, ids, nil, m, nil)
	placeTestElements(t, e)
	return e, m
}

// The real registry must implement routableMounts, or
// colca_command_unroutable_total silently stays at zero while the fakes keep the
// tests green. This turns a mismatch into a compile error. A decorator around the
// registry in node assembly would still slip past.
var _ routableMounts = (*registry.Manager)(nil)

// A downward command no child's mount covers produces no other signal: it is
// never delivered, executed, acked or visibly expired. The counter must count it,
// and stay silent for the cases other mechanisms own.
func TestCommandUnroutableCountsOnlyTheCommandsNothingCanReach(t *testing.T) {
	const line = "colca_command_unroutable_total"
	e, m := newRoutedMetricsEngine(t)

	publish := func(topic string) {
		t.Helper()
		if _, err := e.IngestAdmin(topic, cmdPayload("c-"+topic)); err != nil {
			t.Fatalf("publish %s: %v", topic, err)
		}
	}

	// Presence first: an uncovered route does count.
	before := scrapeMetric(t, m, line)
	publish("colca/v1/_CmdConfigure/n-elsewhere/nowhere/resource/upsert")
	if got := scrapeMetric(t, m, line); got != before+1 {
		t.Fatalf("%s = %v, want %v — an unroutable command must be counted", line, got, before+1)
	}

	// The three exclusions, each owned by a different mechanism.
	for _, c := range []struct{ why, topic string }{
		{"a command for a child's subtree is routable", "colca/v1/_CmdConfigure/n-child/site1/edge1/resource/upsert"},
		{"a command for THIS node executes in-process", "colca/v1/_CmdConfigure/n-edge1/resource/upsert"},
		{"a command for a machine enrolled here rides the local bus", "colca/v1/_CmdParam/m1/m1/set-speed"},
	} {
		at := scrapeMetric(t, m, line)
		publish(c.topic)
		if got := scrapeMetric(t, m, line); got != at {
			t.Errorf("%s: %s rose from %v to %v, but %s", line, c.topic, at, got, c.why)
		}
	}

	// A sibling mount that only shares a string prefix does not cover the path, so it
	// counts.
	at := scrapeMetric(t, m, line)
	publish("colca/v1/_CmdConfigure/n-other/site1/edge10/resource/upsert")
	if got := scrapeMetric(t, m, line); got != at+1 {
		t.Fatalf("%s = %v, want %v — site1/edge10 is not under site1/edge1", line, got, at+1)
	}
}
