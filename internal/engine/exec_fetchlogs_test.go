package engine

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/alpamayo-solutions/colca/internal/store"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

func logStore(t *testing.T) *store.Store {
	t.Helper()
	s, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// logRec is a _Log record of service svc at level lvl, written at ts.
func logRec(ts int64, svc, lvl string) store.Record {
	return store.Record{
		Topic:   fmt.Sprintf("colca/v1/_Log/n-edge1/line1/%s/%s", svc, lvl),
		Payload: []byte(fmt.Sprintf(`{"message":"m%d","level":%q}`, ts, lvl)),
		TS:      ts,
	}
}

func appendRecs(t *testing.T, s *store.Store, recs ...store.Record) {
	t.Helper()
	if _, _, err := s.Append("logs", recs); err != nil {
		t.Fatal(err)
	}
}

var bigBudget = fetchLogsBudget{bytes: FetchLogsMaxResultBytes, scan: FetchLogsMaxScan}

// past is a "now" more than FetchLogsSkew after every window in these tests.
var past = time.UnixMilli(1 << 40)

func mustFetch(t *testing.T, s *store.Store, payload string, budget fetchLogsBudget) FetchLogsResult {
	t.Helper()
	q, err := parseFetchLogs([]byte(payload))
	if err != nil {
		t.Fatal(err)
	}
	page, err := fetchLogs(s, q, past, budget)
	if err != nil {
		t.Fatal(err)
	}
	return page
}

func offsets(page FetchLogsResult) string {
	var out []string
	for _, r := range page.Records {
		out = append(out, fmt.Sprint(r.Offset))
	}
	return "[" + strings.Join(out, " ") + "]"
}

func nextOf(page FetchLogsResult) string {
	if page.Next == nil {
		return "nil"
	}
	return fmt.Sprint(*page.Next)
}

func TestParseFetchLogsValidation(t *testing.T) {
	for _, tc := range []struct{ payload, want string }{
		{`{"to":2}`, "from and to"},
		{`{"from":1}`, "from and to"},
		{`{"from":2,"to":2}`, "must be before"},
		{`{"from":3,"to":2}`, "must be before"},
		{`{"from":"1","to":2}`, "unreadable"},
		{`{"from":1.5,"to":2}`, "unreadable"},
		{`{"from":1,"to":2,"after":-1}`, "after must be"},
		{`{"from":1,"to":2,"limit":0}`, "limit must be 1..1000"},
		{`{"from":1,"to":2,"limit":1001}`, "limit must be 1..1000"},
		{`{"from":1,"to":2,"min_level":"NOTICE"}`, "min_level must be one of"},
		{`{"from":1,"to":2,"service":""}`, "service must be"},
		{`{"from":1,"to":2,"service":"a/b"}`, "service must be"},
	} {
		if _, err := parseFetchLogs([]byte(tc.payload)); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("parseFetchLogs(%s) = %v, want error containing %q", tc.payload, err, tc.want)
		}
	}
	q, err := parseFetchLogs([]byte(`{"from":1,"to":2,"min_level":"warning","limit":1000,"after":0}`))
	if err != nil {
		t.Fatal(err)
	}
	if q.limit != 1000 || q.minLevel != logLevels["WARNING"] || q.after != 0 {
		t.Fatalf("parsed %+v", q)
	}
	q, _ = parseFetchLogs([]byte(`{"from":1,"to":2}`))
	if q.limit != FetchLogsDefaultLimit || q.minLevel != logLevels["DEBUG"] {
		t.Fatalf("defaults %+v", q)
	}
}

func TestFetchLogsEmptyStream(t *testing.T) {
	s := logStore(t)
	page := mustFetch(t, s, `{"from":0,"to":100}`, bigBudget)
	if len(page.Records) != 0 || !page.Complete || page.Next != nil {
		t.Fatalf("empty stream page = %+v, want complete with no records", page)
	}
	b, _ := json.Marshal(page)
	if string(b) != `{"records":[],"complete":true,"lwm":1,"gap":false}` {
		t.Fatalf("empty page encodes as %s", b)
	}
}

// The window is [from, to): records before from and at or after to are out, and
// a record at or after to completes the page.
func TestFetchLogsWindow(t *testing.T) {
	s := logStore(t)
	appendRecs(t, s,
		logRec(10, "svc", "INFO"), logRec(20, "svc", "INFO"), logRec(30, "svc", "INFO"),
		logRec(40, "svc", "INFO"), logRec(50, "svc", "INFO"))
	page := mustFetch(t, s, `{"from":20,"to":40}`, bigBudget)
	if offsets(page) != "[2 3]" || !page.Complete || page.Next != nil {
		t.Fatalf("window [20,40) = %s complete=%v next=%s", offsets(page), page.Complete, nextOf(page))
	}
	r := page.Records[0]
	if r.TS != 20 || r.Topic != "colca/v1/_Log/n-edge1/line1/svc/INFO" || string(r.Payload) != `{"message":"m20","level":"INFO"}` {
		t.Fatalf("record = %+v", r)
	}
	// From before everything, to after the head, with "now" past to.
	page = mustFetch(t, s, `{"from":0,"to":1000}`, bigBudget)
	if offsets(page) != "[1 2 3 4 5]" || !page.Complete {
		t.Fatalf("whole stream = %s complete=%v", offsets(page), page.Complete)
	}
}

// A window that ended less than FetchLogsSkew ago is not complete at the head:
// records can still arrive. next lets the caller continue later.
func TestFetchLogsRecentWindowAtHead(t *testing.T) {
	s := logStore(t)
	appendRecs(t, s, logRec(10, "svc", "INFO"), logRec(20, "svc", "INFO"))
	q, _ := parseFetchLogs([]byte(`{"from":0,"to":100}`))
	now := time.UnixMilli(100 + FetchLogsSkew.Milliseconds() - 1)
	page, err := fetchLogs(s, q, now, bigBudget)
	if err != nil {
		t.Fatal(err)
	}
	if offsets(page) != "[1 2]" || page.Complete || nextOf(page) != "2" {
		t.Fatalf("recent window = %s complete=%v next=%s", offsets(page), page.Complete, nextOf(page))
	}
	// Nothing new: the resumed page is empty and keeps its position.
	q.after, q.hasAfter = 2, true
	page, _ = fetchLogs(s, q, now, bigBudget)
	if len(page.Records) != 0 || page.Complete || nextOf(page) != "2" {
		t.Fatalf("resumed empty page = %+v next=%s", page, nextOf(page))
	}
	// A record appended late with a timestamp inside the window is found.
	appendRecs(t, s, logRec(50, "svc", "INFO"))
	page, _ = fetchLogs(s, q, now.Add(time.Millisecond), bigBudget)
	if offsets(page) != "[3]" || !page.Complete {
		t.Fatalf("late record page = %s complete=%v", offsets(page), page.Complete)
	}
}

// Timestamps out of append order within FetchLogsSkew are found: the
// binary search alone would start past them.
func TestFetchLogsOutOfOrderTimestamps(t *testing.T) {
	s := logStore(t)
	appendRecs(t, s, logRec(100, "svc", "INFO"), logRec(200, "svc", "INFO"),
		logRec(50, "svc", "INFO"), logRec(300, "svc", "INFO"))
	for _, tc := range []struct {
		window, want string
	}{
		{`"from":150,"to":250`, "[2]"},
		{`"from":40,"to":60`, "[3]"},
		{`"from":0,"to":1000`, "[1 2 3 4]"},
	} {
		page := mustFetch(t, s, `{`+tc.window+`}`, bigBudget)
		if offsets(page) != tc.want || !page.Complete {
			t.Errorf("window %s = %s complete=%v, want %s", tc.window, offsets(page), page.Complete, tc.want)
		}
	}
}

const minute = int64(60_000)

// The node's clock stepped back 20 minutes and later forward 30: every record
// of a window is found on both sides of each step.
func TestFetchLogsClockSteps(t *testing.T) {
	s := logStore(t)
	t0 := int64(10) * 60 * minute
	appendRecs(t, s,
		logRec(t0, "svc", "INFO"), logRec(t0+1*minute, "svc", "INFO"), // 1, 2
		logRec(t0-20*minute, "svc", "INFO"), logRec(t0-19*minute, "svc", "INFO"), // 3, 4: stepped back
		logRec(t0+11*minute, "svc", "INFO"), logRec(t0+12*minute, "svc", "INFO"), // 5, 6: stepped forward
	)
	for _, tc := range []struct {
		from, to int64
		want     string
	}{
		{t0 - 21*minute, t0 - 18*minute, "[3 4]"},
		{t0, t0 + 2*minute, "[1 2]"},
		{t0 - 20*minute, t0 + 2*minute, "[1 2 3 4]"},
		{t0 + 10*minute, t0 + 13*minute, "[5 6]"},
	} {
		page := mustFetch(t, s, fmt.Sprintf(`{"from":%d,"to":%d}`, tc.from, tc.to), bigBudget)
		if offsets(page) != tc.want || !page.Complete {
			t.Errorf("window [%d,%d) = %s complete=%v, want %s", tc.from, tc.to, offsets(page), page.Complete, tc.want)
		}
	}
}

// A child that was offline replicates its backlog after the parent's own newer
// records; the backlog keeps the child's timestamps and is found.
func TestFetchLogsReplicatedChildBacklog(t *testing.T) {
	s := logStore(t)
	t0 := int64(10) * 60 * minute
	var own []store.Record
	for i := int64(0); i < 30; i++ {
		own = append(own, logRec(t0+i*minute, "colca", "INFO"))
	}
	appendRecs(t, s, own...)
	if _, _, err := s.ApplyReplicated("n-child", "logs", []store.ReplRecord{
		{ChildOffset: 1, Topic: "colca/v1/_Log/n-child/site1/child/plc/WARNING", Payload: []byte(`{"message":"backlog 1"}`), TS: t0 + 2*minute},
		{ChildOffset: 2, Topic: "colca/v1/_Log/n-child/site1/child/plc/ERROR", Payload: []byte(`{"message":"backlog 2"}`), TS: t0 + 3*minute},
	}); err != nil {
		t.Fatal(err)
	}
	page := mustFetch(t, s, fmt.Sprintf(`{"from":%d,"to":%d,"service":"plc"}`, t0+2*minute, t0+4*minute), bigBudget)
	if offsets(page) != "[31 32]" || !page.Complete {
		t.Fatalf("backlog page = %s complete=%v", offsets(page), page.Complete)
	}
}

// The window ends at the first record FetchLogsSkew past to, not at the first
// record past to.
func TestFetchLogsStopsSkewPastTo(t *testing.T) {
	s := logStore(t)
	skew := FetchLogsSkew.Milliseconds()
	appendRecs(t, s, logRec(10, "svc", "INFO"), logRec(20+skew/2, "svc", "INFO"),
		logRec(15, "svc", "INFO"), logRec(20+skew, "svc", "INFO"), logRec(16, "svc", "INFO"))
	page := mustFetch(t, s, `{"from":0,"to":20}`, bigBudget)
	if offsets(page) != "[1 3]" || !page.Complete || page.Next != nil {
		t.Fatalf("page = %s complete=%v next=%s", offsets(page), page.Complete, nextOf(page))
	}
}

func TestFetchLogsBeforeLWM(t *testing.T) {
	s := logStore(t)
	appendRecs(t, s, logRec(10, "svc", "INFO"), logRec(20, "svc", "INFO"), logRec(30, "svc", "INFO"))
	if _, err := s.Prune("logs", 3, nil, nil); err != nil {
		t.Fatal(err)
	}
	page := mustFetch(t, s, `{"from":0,"to":100}`, bigBudget)
	if offsets(page) != "[3]" || !page.Complete || page.LWM != 3 || !page.Gap {
		t.Fatalf("after prune = %+v", page)
	}
	// A stale after below the LWM resumes at what is retained, and says so.
	page = mustFetch(t, s, `{"from":0,"to":100,"after":1}`, bigBudget)
	if offsets(page) != "[3]" || !page.Gap {
		t.Fatalf("after below LWM = %+v", page)
	}
	// Resuming at the LWM lost nothing.
	page = mustFetch(t, s, `{"from":0,"to":100,"after":2}`, bigBudget)
	if offsets(page) != "[3]" || page.Gap {
		t.Fatalf("after at LWM = %+v", page)
	}
}

func TestFetchLogsPagingWithAfter(t *testing.T) {
	s := logStore(t)
	for i := int64(1); i <= 7; i++ {
		appendRecs(t, s, logRec(i*10, "svc", "INFO"))
	}
	var got []string
	after := ""
	for pages := 0; ; pages++ {
		if pages > 5 {
			t.Fatal("paging does not terminate")
		}
		page := mustFetch(t, s, `{"from":0,"to":1000,"limit":3`+after+`}`, bigBudget)
		got = append(got, offsets(page))
		if page.Complete {
			if page.Next != nil {
				t.Fatal("complete page carries next")
			}
			break
		}
		after = `,"after":` + nextOf(page)
	}
	if strings.Join(got, "") != "[1 2 3][4 5 6][7]" {
		t.Fatalf("pages = %v", got)
	}
}

func TestFetchLogsFilters(t *testing.T) {
	s := logStore(t)
	appendRecs(t, s,
		logRec(10, "svc", "DEBUG"), logRec(20, "svc", "INFO"), logRec(30, "svc", "WARNING"),
		logRec(40, "other", "ERROR"), logRec(50, "svc", "CRITICAL"), logRec(60, "svc", "TRACE"),
		store.Record{Topic: "colca/v1/_StreamGap/n-edge1/logs", Payload: []byte(`{}`), TS: 70})
	for _, tc := range []struct{ filter, want string }{
		{``, "[1 2 3 4 5 6]"}, // an unknown level and non-_Log records: only unknown levels pass DEBUG
		{`,"min_level":"DEBUG"`, "[1 2 3 4 5 6]"},
		{`,"min_level":"INFO"`, "[2 3 4 5]"},
		{`,"min_level":"WARNING"`, "[3 4 5]"},
		{`,"min_level":"CRITICAL"`, "[5]"},
		{`,"service":"other"`, "[4]"},
		{`,"service":"svc","min_level":"ERROR"`, "[5]"},
	} {
		page := mustFetch(t, s, `{"from":0,"to":1000`+tc.filter+`}`, bigBudget)
		if offsets(page) != tc.want || !page.Complete {
			t.Errorf("filter %s = %s complete=%v, want %s", tc.filter, offsets(page), page.Complete, tc.want)
		}
	}
}

func TestFetchLogsByteBudget(t *testing.T) {
	s := logStore(t)
	for i := int64(1); i <= 10; i++ {
		appendRecs(t, s, logRec(i, "svc", "INFO"))
	}
	one := fetchLogsEntrySize(FetchLogsRecord{Offset: 1, TS: 1, Topic: logRec(1, "svc", "INFO").Topic,
		Payload: logRec(1, "svc", "INFO").Payload})
	page := mustFetch(t, s, `{"from":0,"to":1000}`, fetchLogsBudget{bytes: 3*one + one/2, scan: 100})
	if offsets(page) != "[1 2 3]" || page.Complete || nextOf(page) != "3" {
		t.Fatalf("byte-bounded page = %s complete=%v next=%s", offsets(page), page.Complete, nextOf(page))
	}
	// A record larger than the whole budget is listed truncated, so the page
	// still makes progress.
	page = mustFetch(t, s, `{"from":0,"to":1000}`, fetchLogsBudget{bytes: one / 2, scan: 100})
	if offsets(page) != "[1]" || !page.Records[0].Truncated || page.Records[0].Payload != nil || nextOf(page) != "1" {
		t.Fatalf("oversized record page = %+v next=%s", page, nextOf(page))
	}
}

// A payload nested so deep the ack would pass MaxPayloadDepth is truncated.
func TestFetchLogsDeepPayloadTruncated(t *testing.T) {
	s := logStore(t)
	deep := strings.Repeat(`{"a":`, MaxPayloadDepth-2) + "1" + strings.Repeat("}", MaxPayloadDepth-2)
	appendRecs(t, s, store.Record{Topic: "colca/v1/_Log/n-edge1/svc/INFO", Payload: []byte(deep), TS: 1})
	page := mustFetch(t, s, `{"from":0,"to":1000}`, bigBudget)
	if len(page.Records) != 1 || !page.Records[0].Truncated {
		t.Fatalf("deep payload page = %+v", page)
	}
	ack, _ := json.Marshal(CommandOutcome{CorrelationID: "c", ResultCode: 200, Result: mustMarshal(t, page)})
	if nestedDeeperThan(ack, MaxPayloadDepth) {
		t.Fatal("ack nests deeper than MaxPayloadDepth")
	}
}

// The scan budget ends a page that matches little, with a resume point past
// what it examined.
func TestFetchLogsScanBudget(t *testing.T) {
	s := logStore(t)
	for i := int64(1); i <= 10; i++ {
		appendRecs(t, s, logRec(i, "svc", "DEBUG"))
	}
	appendRecs(t, s, logRec(11, "svc", "ERROR"))
	page := mustFetch(t, s, `{"from":0,"to":1000,"min_level":"ERROR"}`, fetchLogsBudget{bytes: 1 << 20, scan: 4})
	if len(page.Records) != 0 || page.Complete || nextOf(page) != "4" {
		t.Fatalf("scan-bounded page = %+v next=%s", page, nextOf(page))
	}
	page = mustFetch(t, s, `{"from":0,"to":1000,"min_level":"ERROR","after":8}`, fetchLogsBudget{bytes: 1 << 20, scan: 4})
	if offsets(page) != "[11]" || !page.Complete {
		t.Fatalf("resumed page = %s complete=%v", offsets(page), page.Complete)
	}
}

func TestFetchLogsBudgetFitsRecordLimit(t *testing.T) {
	if b := fetchLogsBudgetFor(0); b.bytes != FetchLogsMaxResultBytes || b.scan != FetchLogsMaxScan {
		t.Fatalf("uncapped budget = %+v", b)
	}
	if b := fetchLogsBudgetFor(4 << 20); b.bytes != FetchLogsMaxResultBytes {
		t.Fatalf("default-limit budget = %+v", b)
	}
	if b := fetchLogsBudgetFor(128 << 10); b.bytes != 64<<10 {
		t.Fatalf("small-limit budget = %+v, want half the limit", b)
	}
}

// Through the engine: the page rides in the ack and in the synchronous outcome,
// and a bad payload acks 422.
func TestExecAdminFetchLogsAck(t *testing.T) {
	e, _ := adminEngine(t)
	appendRecs(t, e.Store(), logRec(100, "svc", "INFO"), logRec(200, "svc", "WARNING"))

	payload := fmt.Sprintf(`{"correlation_id":"f-1","expires_at":%d,"from":0,"to":1000,"min_level":"WARNING"}`, futureMS())
	res, err := e.IngestAdmin("colca/v1/_CmdAdmin/n-edge1/fetchLogs", []byte(payload))
	if err != nil {
		t.Fatal(err)
	}
	if res.Command == nil || res.Command.ResultCode != 200 {
		t.Fatalf("synchronous outcome = %+v", res.Command)
	}
	var sync FetchLogsResult
	if err := json.Unmarshal(res.Command.Result, &sync); err != nil || offsets(sync) != "[2]" || !sync.Complete {
		t.Fatalf("synchronous result = %s (%v)", res.Command.Result, err)
	}
	ack := ackFor(t, e, "fetchLogs", "f-1")
	if ack == nil || ack["result_code"].(float64) != 200 {
		t.Fatalf("ack = %v", ack)
	}
	result := ack["result"].(map[string]any)
	recs := result["records"].([]any)
	if len(recs) != 1 || result["complete"] != true {
		t.Fatalf("ack result = %v", result)
	}
	if rec := recs[0].(map[string]any); rec["topic"] != "colca/v1/_Log/n-edge1/line1/svc/WARNING" || rec["ts"].(float64) != 200 {
		t.Fatalf("ack record = %v", rec)
	}

	// A repeat is answered from the ledger, which does not keep the page.
	again, err := e.IngestAdmin("colca/v1/_CmdAdmin/n-edge1/fetchLogs", []byte(payload))
	if err != nil {
		t.Fatal(err)
	}
	if !again.Duplicate || again.Command == nil || again.Command.ResultCode != 200 || again.Command.Result != nil {
		t.Fatalf("repeat = %+v command=%+v", again, again.Command)
	}

	bad := fmt.Sprintf(`{"correlation_id":"f-2","expires_at":%d,"from":5,"to":5}`, futureMS())
	if _, err := e.IngestDownlink("colca/v1/_CmdAdmin/n-edge1/fetchLogs", []byte(bad), 1); err != nil {
		t.Fatal(err)
	}
	ack = ackFor(t, e, "fetchLogs", "f-2")
	if ack == nil || ack["result_code"].(float64) != 422 || !strings.Contains(ack["message"].(string), "must be before") {
		t.Fatalf("bad-input ack = %v", ack)
	}
	if _, ok := ack["result"]; ok {
		t.Fatal("a refused fetchLogs carries no result")
	}
}

func mustMarshal(t *testing.T, v any) json.RawMessage {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestWithoutResult(t *testing.T) {
	for in, want := range map[string]string{
		`{"correlation_id":"c","result_code":200}`:                         `{"correlation_id":"c","result_code":200}`,
		`{"correlation_id":"c","result":{"records":[]},"result_code":200}`: `{"correlation_id":"c","result_code":200}`,
		`{"correlation_id":"c","message":"no \"result\" here"}`:            `{"correlation_id":"c","message":"no \"result\" here"}`,
		`not json "result"`: `not json "result"`,
	} {
		if got := string(withoutResult([]byte(in))); got != want {
			t.Errorf("withoutResult(%s) = %s, want %s", in, got, want)
		}
	}
}

// The concurrency bound: a third scan waits until one of two running scans ends.
func TestFetchLogsConcurrencyBound(t *testing.T) {
	x := NewAdminExecutor(nil, logStore(t))
	for range FetchLogsMaxConcurrent {
		x.scans <- struct{}{}
	}
	done := make(chan int)
	go func() {
		code, _, _, _ := x.ExecuteWithResult(uns.CommandContext{}, "_CmdAdmin", "fetchLogs", []byte(`{"from":0,"to":1}`))
		done <- code
	}()
	select {
	case <-done:
		t.Fatal("a scan ran past the concurrency bound")
	case <-time.After(100 * time.Millisecond):
	}
	<-x.scans
	if code := <-done; code != 200 {
		t.Fatalf("waiting scan answered %d", code)
	}
}

func TestFetchLogsRateLimit(t *testing.T) {
	x := NewAdminExecutor(nil, logStore(t))
	now := time.UnixMilli(1 << 40)
	x.now = func() time.Time { return now }
	run := func() (int, string) {
		code, msg, _, _ := x.ExecuteWithResult(uns.CommandContext{}, "_CmdAdmin", "fetchLogs", []byte(`{"from":0,"to":1}`))
		return code, msg
	}
	for i := range FetchLogsBurst {
		if code, msg := run(); code != 200 {
			t.Fatalf("page %d of the burst: %d %s", i, code, msg)
		}
	}
	if code, msg := run(); code != 429 || !strings.Contains(msg, "retry in 6s") {
		t.Fatalf("past the burst: %d %s", code, msg)
	}
	now = now.Add(6 * time.Second) // one token at 600 an hour
	if code, _ := run(); code != 200 {
		t.Fatalf("after refill: %d", code)
	}
	// A refused command is not counted twice: still empty, not negative.
	if code, _ := run(); code != 429 {
		t.Fatalf("second past the burst: %d", code)
	}
}
