package engine

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/alpamayo-solutions/colca/internal/store"
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

// past is a "now" after every window in these tests.
var past = time.UnixMilli(1_000_000)

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
	if string(b) != `{"records":[],"complete":true}` {
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

// A window that reaches past "now" is not complete at the head: records can
// still arrive. next lets the caller continue later.
func TestFetchLogsOpenWindowAtHead(t *testing.T) {
	s := logStore(t)
	appendRecs(t, s, logRec(10, "svc", "INFO"), logRec(20, "svc", "INFO"))
	q, _ := parseFetchLogs([]byte(`{"from":0,"to":5000000}`))
	page, err := fetchLogs(s, q, past, bigBudget)
	if err != nil {
		t.Fatal(err)
	}
	if offsets(page) != "[1 2]" || page.Complete || nextOf(page) != "2" {
		t.Fatalf("open window = %s complete=%v next=%s", offsets(page), page.Complete, nextOf(page))
	}
	// Nothing new: the resumed page is empty and keeps its position.
	q.after = 2
	page, _ = fetchLogs(s, q, past, bigBudget)
	if len(page.Records) != 0 || page.Complete || nextOf(page) != "2" {
		t.Fatalf("resumed empty page = %+v next=%s", page, nextOf(page))
	}
}

func TestFetchLogsBeforeLWM(t *testing.T) {
	s := logStore(t)
	appendRecs(t, s, logRec(10, "svc", "INFO"), logRec(20, "svc", "INFO"), logRec(30, "svc", "INFO"))
	if _, err := s.Prune("logs", 3, nil, nil); err != nil {
		t.Fatal(err)
	}
	page := mustFetch(t, s, `{"from":0,"to":100}`, bigBudget)
	if offsets(page) != "[3]" || !page.Complete {
		t.Fatalf("after prune = %s complete=%v", offsets(page), page.Complete)
	}
	// A stale after below the LWM resumes at what is retained.
	page = mustFetch(t, s, `{"from":0,"to":100,"after":1}`, bigBudget)
	if offsets(page) != "[3]" {
		t.Fatalf("after below LWM = %s", offsets(page))
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
