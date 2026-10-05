package engine

import (
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/alpamayo-solutions/colca/internal/clock"
	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/contracts"
	"github.com/alpamayo-solutions/colca/internal/contracts/contractstest"
	"github.com/alpamayo-solutions/colca/internal/metrics"
	"github.com/alpamayo-solutions/colca/internal/metrics/metricstest"
	"github.com/alpamayo-solutions/colca/internal/store"
)

// fakeNow is a settable clock for the log gate's windows.
type fakeNow struct {
	mu sync.Mutex
	t  time.Time
}

func (f *fakeNow) now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.t
}

func (f *fakeNow) advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.t = f.t.Add(d)
}

// newLogEngine is an engine with a fake clock, metrics and the given logs:
// block.
func newLogEngine(t *testing.T, logs config.Logs) (*Engine, *fakeNow, *metrics.Metrics) {
	t.Helper()
	s, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	clk := &fakeNow{t: time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)}
	m := metrics.New(s, config.Retention{}, nil)
	e := New(s, &config.Config{ULID: "n-edge1", Logs: logs}, testIDs(), nil, m, clock.New(true, clk.now))
	placeTestElements(t, e)
	e.SetContracts(logBundle(t))
	return e, clk, m
}

// logBundle is the builtin floor plus _Log as the generated bundle defines it.
func logBundle(t *testing.T) *contracts.Table {
	t.Helper()
	str := map[string]any{"type": "string", "minLength": 1}
	numeric := map[string]any{"type": "number"}
	entries := map[string]any{
		"_Metric":        obj("data", true, []string{"v"}, map[string]any{"v": numeric}),
		"_SystemElement": obj("entity", true, []string{"id"}, map[string]any{"id": str}),
		"_Log": obj("log", false,
			[]string{"function", "level", "line_no", "logger_name", "message", "module", "timestamp"},
			map[string]any{
				"exc_info": map[string]any{"type": []string{"null", "string"}},
				"extra":    map[string]any{"type": []string{"null", "object"}},
				"function": str, "level": str, "line_no": numeric, "logger_name": str,
				"message": str, "module": str, "timestamp": str,
			}),
	}
	return writeBundle(t, entries)
}

func logLine(message string) []byte {
	raw, _ := json.Marshal(map[string]any{
		"timestamp": "2026-10-05T12:00:00Z", "level": "ERROR", "message": message,
		"logger_name": "driver", "module": "driver", "function": "connect", "line_no": 7,
	})
	return raw
}

func logRecords(t *testing.T, e *Engine) []store.StoredRecord {
	t.Helper()
	recs, _, err := e.Store().Read("logs", 1, 100000, nil)
	if err != nil {
		t.Fatal(err)
	}
	return recs
}

func intPtr(n int) *int { return &n }

const m1Errors = "colca/v1/_Log/n-edge1/m1/ERROR"

func TestRepeatedClientLogsAreStoredOnceAndSummarizedAtTheWindowEnd(t *testing.T) {
	e, clk, m := newLogEngine(t, config.Logs{})
	for i := range 1000 {
		res, err := e.IngestClient("m1", m1Errors, logLine("connection refused"))
		if err != nil {
			t.Fatal(err)
		}
		if want := i > 0; (res.Withheld == "collapsed") != want || res.Persisted == want {
			t.Fatalf("record %d: %+v", i, res)
		}
		clk.advance(10 * time.Millisecond)
	}
	if got := len(logRecords(t, e)); got != 1 {
		t.Fatalf("%d records stored inside the window, want 1", got)
	}
	clk.advance(time.Minute)
	e.writeLogGate(e.logs.Due(clk.now()))
	recs := logRecords(t, e)
	if len(recs) != 2 {
		t.Fatalf("%d records after the window, want the first and one summary", len(recs))
	}
	summary := recs[1]
	if summary.Topic != m1Errors || summary.WrittenBy != "m1" {
		t.Fatalf("summary at %s by %s, want the original topic and writer", summary.Topic, summary.WrittenBy)
	}
	var body map[string]any
	if err := json.Unmarshal(summary.Payload, &body); err != nil {
		t.Fatal(err)
	}
	if body["message"] != "connection refused (×999 in 60 s)" || body["extra"].(map[string]any)["repeated"] != float64(999) {
		t.Fatalf("summary %v", body)
	}
	if got := metricstest.Value(t, m, `colca_log_withheld_total{reason="collapsed"}`); got != 999 {
		t.Fatalf("colca_log_withheld_total{collapsed} = %v, want 999", got)
	}
}

func TestAFloodFromOneServiceIsCappedWithOneDropNotice(t *testing.T) {
	e, clk, m := newLogEngine(t, config.Logs{MaxPerService: intPtr(10)})
	for i := range 25 {
		if _, err := e.IngestClient("m1", "colca/v1/_Log/n-edge1/m1/INFO", logLine(fmt.Sprintf("line %d", i))); err != nil {
			t.Fatal(err)
		}
	}
	// Another service on the same node keeps its own budget.
	if res, err := e.IngestAdminAttributed("colca/v1/_Log/n-edge1/colca/INFO", logLine("node line"),
		Attribution{WrittenBy: "colca", ActorID: "colca", ActorKind: "system"}); err != nil || !res.Persisted {
		t.Fatalf("another service: %+v %v", res, err)
	}
	clk.advance(time.Minute)
	e.writeLogGate(e.logs.Due(clk.now()))
	recs := logRecords(t, e)
	if len(recs) != 12 {
		t.Fatalf("%d records, want 10 stored, the other service's 1 and one drop notice", len(recs))
	}
	notice := recs[11]
	if notice.Topic != "colca/v1/_Log/n-edge1/m1/WARNING" || notice.WrittenBy != LogGateAuthor || notice.ActorKind != "system" {
		t.Fatalf("notice %s by %s/%s", notice.Topic, notice.WrittenBy, notice.ActorKind)
	}
	var body map[string]any
	if err := json.Unmarshal(notice.Payload, &body); err != nil {
		t.Fatal(err)
	}
	if body["message"] != "15 log record(s) dropped: m1 exceeded 10 records in 60 s" {
		t.Fatalf("notice message %q", body["message"])
	}
	if err := e.validateContract("_Log", notice.Payload); err != nil {
		t.Fatalf("the drop notice must be a valid _Log: %v", err)
	}
	if got := metricstest.Value(t, m, `colca_log_withheld_total{reason="rate_limited"}`); got != 15 {
		t.Fatalf("colca_log_withheld_total{rate_limited} = %v, want 15", got)
	}
}

func TestABatchCollapsesRepeatsToo(t *testing.T) {
	e, _, _ := newLogEngine(t, config.Logs{})
	batch := make([]BatchRecord, 5)
	for i := range batch {
		batch[i] = BatchRecord{Topic: m1Errors, Payload: logLine("same")}
	}
	results := e.IngestClientBatch("m1", batch)
	if results[0].Err != nil || !results[0].Persisted {
		t.Fatalf("first: %+v", results[0])
	}
	for i, r := range results[1:] {
		if r.Err != nil || r.Withheld != "collapsed" {
			t.Fatalf("record %d: %+v", i+1, r)
		}
	}
	if got := len(logRecords(t, e)); got != 1 {
		t.Fatalf("%d stored, want 1", got)
	}
}

func TestReplicatedLogsAreNotGatedAgain(t *testing.T) {
	e, _, _ := newLogEngine(t, config.Logs{MaxPerService: intPtr(1)})
	var recs []store.ReplRecord
	for i := range 5 {
		recs = append(recs, store.ReplRecord{
			ChildOffset: uint64(i + 1), Topic: "colca/v1/_Log/n-child/press/ERROR",
			Payload: logLine("same"), TS: int64(i + 1),
		})
	}
	if _, _, err := e.IngestReplicated("n-child", "logs", recs); err != nil {
		t.Fatal(err)
	}
	if got := len(logRecords(t, e)); got != 5 {
		t.Fatalf("%d replicated records stored, want all 5: the child gated them", got)
	}
}

func TestFlushLogsWritesPendingWindowsAndOpensTheGate(t *testing.T) {
	e, _, _ := newLogEngine(t, config.Logs{})
	for range 3 {
		if _, err := e.IngestClient("m1", m1Errors, logLine("x")); err != nil {
			t.Fatal(err)
		}
	}
	e.FlushLogs()
	if got := len(logRecords(t, e)); got != 2 {
		t.Fatalf("%d records after FlushLogs, want the first and the summary", got)
	}
	if res, _ := e.IngestClient("m1", m1Errors, logLine("x")); !res.Persisted {
		t.Fatalf("after FlushLogs every record is stored, got %+v", res)
	}
}

func TestRunLogGateWritesTheSummaryAtTheDeadline(t *testing.T) {
	s, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	window := config.Duration(100 * time.Millisecond)
	e := New(s, &config.Config{ULID: "n-edge1", Logs: config.Logs{Window: &window}}, testIDs(), nil, nil, nil)
	placeTestElements(t, e)
	e.SetContracts(logBundle(t))
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		e.RunLogGate(stop)
	}()
	defer func() { close(stop); <-done }()
	for range 4 {
		if _, err := e.IngestClient("m1", m1Errors, logLine("x")); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(5 * time.Second)
	for len(logRecords(t, e)) < 2 {
		if time.Now().After(deadline) {
			t.Fatal("no summary written after the window ended")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestAZeroWindowStoresEveryLogRecord(t *testing.T) {
	zero := config.Duration(0)
	e, _, _ := newLogEngine(t, config.Logs{Window: &zero, MaxPerService: intPtr(1)})
	for range 5 {
		if res, err := e.IngestClient("m1", m1Errors, logLine("x")); err != nil || !res.Persisted {
			t.Fatalf("%+v %v", res, err)
		}
	}
}

// The summary and the drop notice must pass the schema the doors run, or the
// gate would write records its own node refuses everywhere else.
func TestGateWritesAreValidAgainstTheGeneratedBundle(t *testing.T) {
	tbl, err := contracts.Load(contractstest.GeneratedBundlePath(t), "")
	if err != nil {
		t.Fatal(err)
	}
	e, clk, _ := newLogEngine(t, config.Logs{MaxPerService: intPtr(2)})
	e.SetContracts(tbl)
	for range 3 {
		if _, err := e.IngestClient("m1", m1Errors, logLine("same")); err != nil {
			t.Fatal(err)
		}
	}
	for i := range 3 {
		if _, err := e.IngestClient("m1", m1Errors, logLine(fmt.Sprintf("line %d", i))); err != nil {
			t.Fatal(err)
		}
	}
	clk.advance(time.Minute)
	e.writeLogGate(e.logs.Due(clk.now()))
	recs := logRecords(t, e)
	if len(recs) != 4 {
		t.Fatalf("%d records, want the two the budget allows, a summary and a notice", len(recs))
	}
	for _, r := range recs[2:] {
		if err := e.validateContract("_Log", r.Payload); err != nil {
			t.Fatalf("%s: %v", r.Payload, err)
		}
	}
}
