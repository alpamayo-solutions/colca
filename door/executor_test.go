package door

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"
)

const execNode = "01NODE"

// execNodeFake is the part of a node's HTTP API an Executor uses: the commands
// stream, cursors, publish and healthz.
type execNodeFake struct {
	mu          sync.Mutex
	records     []Record
	cursors     map[string]int64 // last acked offset
	failPublish int              // publishes still to refuse with 503
	gap         *Gap
	srv         *httptest.Server
}

func newExecNode(t *testing.T) *execNodeFake {
	t.Helper()
	f := &execNodeFake{cursors: map[string]int64{}}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"ulid":"` + execNode + `"}`))
	})
	mux.HandleFunc("GET /fetch", f.fetch)
	mux.HandleFunc("POST /ack", f.ack)
	mux.HandleFunc("POST /publish", f.publish)
	f.srv = httptest.NewServer(mux)
	t.Cleanup(f.srv.Close)
	return f
}

func (f *execNodeFake) fetch(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	q := r.URL.Query()
	limit, _ := strconv.Atoi(q.Get("max"))
	if limit == 0 {
		limit = 100
	}
	head := int64(len(f.records))
	from := f.cursors[q.Get("cursor")] + 1
	if v := q.Get("from"); v != "" {
		ahead, _ := strconv.ParseInt(v, 10, 64)
		from = max(from, ahead)
	}
	if q.Get("tail") != "" {
		from = max(1, head-int64(limit)+1)
	}
	contracts := map[string]bool{}
	for _, c := range q["contract"] {
		contracts[c] = true
	}
	page := Page{Records: []Record{}, From: from, Store: "store-1", Gap: f.gap}
	next := from
	for ; next <= head && len(page.Records) < limit; next++ {
		rec := f.records[next-1]
		if c, _, _, ok := splitTopic(rec.Topic); ok && (len(contracts) == 0 || contracts[c]) {
			page.Records = append(page.Records, rec)
		}
	}
	page.Next = next
	_ = json.NewEncoder(w).Encode(page)
}

func (f *execNodeFake) ack(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Cursor string `json:"cursor"`
		Offset int64  `json:"offset"`
	}
	_ = json.NewDecoder(r.Body).Decode(&in)
	f.mu.Lock()
	defer f.mu.Unlock()
	moved := in.Offset > f.cursors[in.Cursor]
	if moved {
		f.cursors[in.Cursor] = in.Offset
	}
	_ = json.NewEncoder(w).Encode(map[string]bool{"moved": moved})
}

func (f *execNodeFake) publish(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Topic   string          `json:"topic"`
		Payload json.RawMessage `json:"payload"`
	}
	_ = json.NewDecoder(r.Body).Decode(&in)
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failPublish > 0 {
		f.failPublish--
		http.Error(w, `{"error":"unavailable"}`, http.StatusServiceUnavailable)
		return
	}
	f.append(Record{Topic: in.Topic, Payload: in.Payload, ActorID: "01SVC", ActorKind: "service"})
	_, _ = w.Write([]byte(`{}`))
}

func (f *execNodeFake) append(rec Record) {
	rec.Offset = int64(len(f.records)) + 1
	f.records = append(f.records, rec)
}

// command appends a command from sender at path with the given envelope.
func (f *execNodeFake) command(contract, path string, sender Actor, env map[string]any) {
	raw, _ := json.Marshal(env)
	f.mu.Lock()
	defer f.mu.Unlock()
	f.append(Record{Topic: "colca/v1/" + contract + "/" + execNode + "/" + path, Payload: raw, TS: 1,
		ActorID: sender.ID, ActorLabel: sender.Label, ActorKind: sender.Kind})
}

// answers are the _Ack payloads on the stream, in order.
func (f *execNodeFake) answers() []map[string]any {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []map[string]any
	for _, rec := range f.records {
		if c, _, _, _ := splitTopic(rec.Topic); c == "_Ack" {
			var body map[string]any
			_ = json.Unmarshal(rec.Payload, &body)
			body["topic"] = rec.Topic
			out = append(out, body)
		}
	}
	return out
}

func (f *execNodeFake) cursor(name string) int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.cursors[name]
}

// recorder is a Handler that answers what answer says and records each call.
type recorder struct {
	mu     sync.Mutex
	paths  map[string]bool
	calls  []Command
	answer func(context.Context, Command) (Answer, error)
}

func newRecorder(paths ...string) *recorder {
	r := &recorder{paths: map[string]bool{}}
	for _, p := range paths {
		r.paths[p] = true
	}
	return r
}

func (r *recorder) Handles(contract, path string) bool {
	return contract == "_CmdMaintain" && r.paths[path]
}

func (r *recorder) Execute(ctx context.Context, cmd Command) (Answer, error) {
	r.mu.Lock()
	r.calls = append(r.calls, cmd)
	answer := r.answer
	r.mu.Unlock()
	if answer != nil {
		return answer(ctx, cmd)
	}
	return Answer{Message: "done", Result: map[string]any{"ok": true}}, nil
}

func (r *recorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.calls)
}

var (
	operator = Actor{ID: "kc-anna", Label: "anna", Kind: "human"}
	hub      = Actor{ID: "01HUB", Label: "hub-ui", Kind: "service"}
)

func newExecutor(f *execNodeFake, h Handler) *Executor {
	return &Executor{
		Client:    &Client{BaseURL: f.srv.URL, Service: "orchestrator"},
		Handler:   h,
		Contracts: []string{"_CmdMaintain"},
		Log:       slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

func drain(t *testing.T, x *Executor) {
	t.Helper()
	if err := x.Drain(context.Background()); err != nil {
		t.Fatalf("Drain: %v", err)
	}
}

func TestACommandIsRunOnceAndAnsweredBesideItWithItsResult(t *testing.T) {
	f := newExecNode(t)
	h := newRecorder("svc/plan")
	f.command("_CmdMaintain", "svc/plan", operator, map[string]any{
		"correlation_id": "c1", "command": map[string]any{"name": "m3"},
	})
	x := newExecutor(f, h)
	drain(t, x)
	drain(t, x)

	if h.count() != 1 {
		t.Fatalf("handler ran %d times, want 1", h.count())
	}
	cmd := h.calls[0]
	if cmd.Sender != operator || cmd.Verb != "plan" || string(cmd.Params) != `{"name":"m3"}` {
		t.Fatalf("command = %+v", cmd)
	}
	answers := f.answers()
	if len(answers) != 1 {
		t.Fatalf("answers = %v", answers)
	}
	a := answers[0]
	if a["topic"] != "colca/v1/_Ack/"+execNode+"/svc/plan" || a["correlation_id"] != "c1" ||
		a["result_code"] != 200.0 || a["message"] != "done" || a["result"].(map[string]any)["ok"] != true {
		t.Fatalf("answer = %v", a)
	}
	if got := f.cursor("c/orchestrator/commands"); got != 2 {
		t.Fatalf("cursor = %d, want 2 (the command and its answer read)", got)
	}
}

func TestAnExpiredCommandIsAnswered498AndNotRun(t *testing.T) {
	f := newExecNode(t)
	h := newRecorder("svc/plan")
	now := time.UnixMilli(1_800_000_000_000)
	f.command("_CmdMaintain", "svc/plan", operator, map[string]any{"correlation_id": "late", "expires_at": now.UnixMilli()})
	f.command("_CmdMaintain", "svc/plan", operator, map[string]any{"correlation_id": "never"})
	x := newExecutor(f, h)
	x.Now = func() time.Time { return now }
	drain(t, x)

	if h.count() != 1 || h.calls[0].CorrelationID != "never" {
		t.Fatalf("handler calls = %+v, want only the command without expiry", h.calls)
	}
	if a := f.answers()[0]; a["correlation_id"] != "late" || a["result_code"] != 498.0 {
		t.Fatalf("answer to the expired command = %v", a)
	}
}

// expires_at gates the start only: a handler that started runs on past it, so
// a long change is not cancelled half way.
func TestTheHandlersContextIsNotCutAtTheCommandsDeadline(t *testing.T) {
	f := newExecNode(t)
	h := newRecorder("svc/plan")
	now := time.Now()
	clock := now
	x := newExecutor(f, h)
	x.Now = func() time.Time { return clock }
	var hasDeadline bool
	var ctxErr error
	h.answer = func(ctx context.Context, _ Command) (Answer, error) {
		_, hasDeadline = ctx.Deadline()
		clock = now.Add(time.Hour) // the command expires while it runs
		ctxErr = ctx.Err()
		return Answer{Message: "applied"}, nil
	}
	f.command("_CmdMaintain", "svc/plan", operator, map[string]any{"correlation_id": "c1",
		"expires_at": now.Add(time.Minute).UnixMilli()})
	drain(t, x)
	if hasDeadline || ctxErr != nil {
		t.Fatalf("handler context: deadline %v err %v, want neither", hasDeadline, ctxErr)
	}
	if a := f.answers()[0]; a["result_code"] != 200.0 || a["message"] != "applied" {
		t.Fatalf("answer = %v", a)
	}
}

// budgeted is a recorder with a Budget per verb.
type budgeted struct {
	*recorder
	budgets map[string]time.Duration
}

func (b budgeted) Budget(cmd Command) time.Duration { return b.budgets[cmd.Verb] }

func TestAHandlersBudgetBoundsItsContext(t *testing.T) {
	f := newExecNode(t)
	h := budgeted{newRecorder("svc/apply", "svc/plan"), map[string]time.Duration{"apply": 2 * time.Minute}}
	deadlines := map[string]time.Duration{}
	h.answer = func(ctx context.Context, cmd Command) (Answer, error) {
		if d, ok := ctx.Deadline(); ok {
			deadlines[cmd.Verb] = time.Until(d)
		}
		return Answer{}, nil
	}
	f.command("_CmdMaintain", "svc/apply", operator, map[string]any{"correlation_id": "c1",
		"expires_at": time.Now().Add(time.Second).UnixMilli()})
	f.command("_CmdMaintain", "svc/plan", operator, map[string]any{"correlation_id": "c2"})
	drain(t, newExecutor(f, h))
	if d, ok := deadlines["apply"]; !ok || d < time.Minute || d > 2*time.Minute {
		t.Fatalf("apply deadline in %v, want its two-minute budget", d)
	}
	if _, ok := deadlines["plan"]; ok {
		t.Fatalf("plan has a deadline without a budget")
	}
}

func TestFailuresAreAnsweredWithTheirCodes(t *testing.T) {
	f := newExecNode(t)
	h := newRecorder("svc/a", "svc/b", "svc/c")
	h.answer = func(_ context.Context, cmd Command) (Answer, error) {
		switch cmd.Verb {
		case "a":
			return Answer{Code: 409, Message: "plan changed"}, nil
		case "b":
			return Answer{}, errors.New("docker unreachable")
		}
		panic("boom")
	}
	for _, v := range []string{"a", "b", "c"} {
		f.command("_CmdMaintain", "svc/"+v, operator, map[string]any{"correlation_id": v})
	}
	drain(t, newExecutor(f, h))
	want := map[string]float64{"a": 409, "b": 500, "c": 500}
	for _, a := range f.answers() {
		if a["result_code"] != want[a["correlation_id"].(string)] {
			t.Fatalf("answer = %v", a)
		}
	}
	if n := len(f.answers()); n != 3 {
		t.Fatalf("%d answers, want 3", n)
	}
}

// Commands the handler does not take, and commands for another node, are
// left alone, and the cursor still moves past them.
func TestOtherCommandsAreSkippedAndTheCursorMovesPastThem(t *testing.T) {
	f := newExecNode(t)
	h := newRecorder("svc/plan")
	f.command("_CmdMaintain", "other/plan", operator, map[string]any{"correlation_id": "c1"})
	f.command("_CmdOperate", "svc/plan", operator, map[string]any{"correlation_id": "c2"})
	f.mu.Lock()
	f.append(Record{Topic: "colca/v1/_CmdMaintain/01CHILD/svc/plan", Payload: []byte(`{"correlation_id":"c3"}`)})
	f.mu.Unlock()
	x := newExecutor(f, h)
	drain(t, x)
	if h.count() != 0 || len(f.answers()) != 0 {
		t.Fatalf("calls %d answers %v, want none", h.count(), f.answers())
	}
	if got := f.cursor(x.Cursor); got != 3 {
		t.Fatalf("cursor = %d, want 3", got)
	}
}

func TestACommandWithoutCorrelationIDRunsButIsNotAnswered(t *testing.T) {
	f := newExecNode(t)
	h := newRecorder("svc/plan")
	f.command("_CmdMaintain", "svc/plan", operator, map[string]any{})
	drain(t, newExecutor(f, h))
	if h.count() != 1 || len(f.answers()) != 0 {
		t.Fatalf("calls %d answers %v", h.count(), f.answers())
	}
}

func TestACommandWithoutCorrelationIDIsRefusedWhenOneIsRequired(t *testing.T) {
	f := newExecNode(t)
	h := newRecorder("svc/plan")
	f.command("_CmdMaintain", "svc/plan", operator, map[string]any{})
	f.command("_CmdMaintain", "svc/plan", operator, map[string]any{"correlation_id": "c1"})
	x := newExecutor(f, h)
	x.RequireCorrelationID = true
	drain(t, x)
	if h.count() != 1 || h.calls[0].CorrelationID != "c1" {
		t.Fatalf("handler calls = %+v, want only c1", h.calls)
	}
	if n := len(f.answers()); n != 1 {
		t.Fatalf("%d answers, want only c1's", n)
	}
	if got := x.Uncorrelated(); got != 1 {
		t.Fatalf("Uncorrelated = %d, want 1", got)
	}
	if got := f.cursor(x.Cursor); got != 2 {
		t.Fatalf("cursor = %d, want 2 (past both commands)", got)
	}
}

// A command whose answer is already on the stream was answered before a
// restart: it is neither run nor answered again, even when the answer is
// pages further on.
func TestACommandAnsweredOnTheStreamIsNeitherRunNorAnswered(t *testing.T) {
	f := newExecNode(t)
	h := newRecorder("svc/plan")
	f.command("_CmdMaintain", "svc/plan", operator, map[string]any{"correlation_id": "c1"})
	for i := range 5 {
		f.command("_CmdMaintain", "other/x", operator, map[string]any{"correlation_id": "o" + strconv.Itoa(i)})
	}
	f.mu.Lock()
	f.append(Record{Topic: "colca/v1/_Ack/" + execNode + "/svc/plan",
		Payload: []byte(`{"correlation_id":"c1","result_code":200}`)})
	f.mu.Unlock()
	x := newExecutor(f, h)
	x.PageSize = 1
	drain(t, x)
	if h.count() != 0 || len(f.answers()) != 1 {
		t.Fatalf("calls %d answers %v, want the one already there", h.count(), f.answers())
	}
}

// A fresh cursor starts at the oldest retained command. Its answer must count
// however many answers to other commands lie ahead of it: more than the
// executor would ever keep in memory.
func TestAnAnswerIsFoundBehindMoreAnswersThanTheExecutorRemembers(t *testing.T) {
	f := newExecNode(t)
	h := newRecorder("svc/plan")
	now := time.UnixMilli(1_800_000_000_000)
	f.command("_CmdMaintain", "svc/plan", operator, map[string]any{"correlation_id": "old", "expires_at": now.UnixMilli() - 1})
	f.mu.Lock()
	for i := range 150 {
		f.append(Record{Topic: "colca/v1/_CmdMaintain/" + execNode + "/other/x", Payload: []byte(`{"correlation_id":"x` + strconv.Itoa(i) + `"}`)})
	}
	f.append(Record{Topic: "colca/v1/_Ack/" + execNode + "/svc/plan", Payload: []byte(`{"correlation_id":"old","result_code":498}`)})
	for i := range ledgerLimit + 500 {
		f.append(Record{Topic: "colca/v1/_Ack/" + execNode + "/other/x",
			Payload: []byte(`{"correlation_id":"a` + strconv.Itoa(i) + `","result_code":200}`)})
	}
	f.mu.Unlock()
	x := newExecutor(f, h)
	x.Now = func() time.Time { return now }
	drain(t, x)
	if h.count() != 0 {
		t.Fatalf("handler ran %d times, want 0", h.count())
	}
	var mine int
	for _, a := range f.answers() {
		if a["correlation_id"] == "old" {
			mine++
		}
	}
	if mine != 1 {
		t.Fatalf("%d answers to the old command, want the one already there", mine)
	}
	if len(x.answered) != 0 {
		t.Fatalf("executor remembers %d answers, want none: it published none", len(x.answered))
	}
}

// A 202 says a node queued or forwarded the command, not that it was answered.
func TestAProgressAckDoesNotCountAsTheAnswer(t *testing.T) {
	f := newExecNode(t)
	h := newRecorder("svc/plan")
	f.command("_CmdMaintain", "svc/plan", operator, map[string]any{"correlation_id": "c1"})
	f.mu.Lock()
	f.append(Record{Topic: "colca/v1/_Ack/" + execNode + "/svc/plan",
		Payload: []byte(`{"correlation_id":"c1","result_code":202}`)})
	f.mu.Unlock()
	drain(t, newExecutor(f, h))
	if h.count() != 1 {
		t.Fatalf("handler ran %d times, want 1", h.count())
	}
}

func TestACommandStartedAndNeverAnsweredIsAnswered504AndNotRun(t *testing.T) {
	f := newExecNode(t)
	h := newRecorder("svc/plan")
	ledger := NewMemoryLedger()
	_ = ledger.Start("c1")
	f.command("_CmdMaintain", "svc/plan", operator, map[string]any{"correlation_id": "c1"})
	x := newExecutor(f, h)
	x.Ledger = ledger
	drain(t, x)
	if h.count() != 0 || f.answers()[0]["result_code"] != 504.0 {
		t.Fatalf("calls %d answers %v", h.count(), f.answers())
	}
}

// An answer the node did not take is sent again before the cursor moves; a
// stop while it is pending leaves the cursor, and a restart republishes the
// recorded answer without running the command again.
func TestAPendingAnswerHoldsTheCursorAndIsRepublishedAfterARestart(t *testing.T) {
	f := newExecNode(t)
	h := newRecorder("svc/plan")
	ledger, err := OpenFileLedger(filepath.Join(t.TempDir(), "ledger.json"), 0)
	if err != nil {
		t.Fatal(err)
	}
	f.command("_CmdMaintain", "svc/plan", operator, map[string]any{"correlation_id": "c1"})
	f.failPublish = 1000
	x := newExecutor(f, h)
	x.Ledger = ledger
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if err := x.Drain(ctx); err == nil {
		t.Fatal("Drain returned nil with its answer unpublished")
	}
	if f.cursor(x.Cursor) != 0 {
		t.Fatal("the cursor moved past a command whose answer was not published")
	}

	reopened, err := OpenFileLedger(ledger.path, 0)
	if err != nil {
		t.Fatal(err)
	}
	f.failPublish = 1
	restarted := newExecutor(f, h)
	restarted.Ledger = reopened
	drain(t, restarted)
	if h.count() != 1 {
		t.Fatalf("handler ran %d times across the restart, want 1", h.count())
	}
	if a := f.answers(); len(a) != 1 || a[0]["result_code"] != 200.0 {
		t.Fatalf("answers = %v, want the recorded 200 once", a)
	}
	if f.cursor(x.Cursor) != 1 {
		t.Fatalf("cursor = %d, want 1, past the command", f.cursor(x.Cursor))
	}
}

func TestAPrunedCommandIsAGapError(t *testing.T) {
	f := newExecNode(t)
	f.command("_CmdMaintain", "svc/plan", operator, map[string]any{"correlation_id": "c1"})
	f.gap = &Gap{Stream: "commands", FromOffset: 1, ToOffset: 4}
	err := newExecutor(f, newRecorder("svc/plan")).Drain(context.Background())
	var gap *GapError
	if !errors.As(err, &gap) || gap.Gap.ToOffset != 4 {
		t.Fatalf("Drain = %v, want a GapError", err)
	}
}

func TestARepeatedOperationIsAnsweredFromTheRecordAndNotRunAgain(t *testing.T) {
	f := newExecNode(t)
	h := newRecorder("svc/rollout")
	h.answer = func(context.Context, Command) (Answer, error) {
		return Answer{Code: 409, Message: "plan hash changed", Result: map[string]any{"plan": "abc"}}, nil
	}
	for _, c := range []string{"c1", "c2"} {
		f.command("_CmdMaintain", "svc/rollout", hub, map[string]any{
			"correlation_id": c, "operation_id": "op-1", "command": map[string]any{"plan": "abc"},
			"on_behalf_of": map[string]any{"id": "kc-anna", "label": "anna"},
		})
	}
	drain(t, newExecutor(f, h))
	if h.count() != 1 {
		t.Fatalf("handler ran %d times, want 1", h.count())
	}
	if got := h.calls[0].OnBehalfOf; got == nil || *got != (Actor{ID: "kc-anna", Label: "anna", Kind: "human"}) {
		t.Fatalf("on_behalf_of = %+v", got)
	}
	answers := f.answers()
	first, repeat := answers[0], answers[1]
	if first["replayed"] != nil || repeat["replayed"] != true || repeat["result_code"] != 409.0 ||
		repeat["message"] != "plan hash changed" || repeat["result"].(map[string]any)["plan"] != "abc" ||
		repeat["operation_id"] != "op-1" || repeat["on_behalf_of"].(map[string]any)["id"] != "kc-anna" {
		t.Fatalf("answers = %v", answers)
	}
}

func TestAnOperationIDReusedForAnotherCommandIsRefused409(t *testing.T) {
	f := newExecNode(t)
	h := newRecorder("svc/rollout")
	f.command("_CmdMaintain", "svc/rollout", hub, map[string]any{"correlation_id": "c1", "operation_id": "op-1", "command": map[string]any{"plan": "a"}})
	f.command("_CmdMaintain", "svc/rollout", hub, map[string]any{"correlation_id": "c2", "operation_id": "op-1", "command": map[string]any{"plan": "b"}})
	// The same id from another sender is another operation.
	f.command("_CmdMaintain", "svc/rollout", Actor{ID: "01CLI", Kind: "service"}, map[string]any{"correlation_id": "c3", "operation_id": "op-1"})
	drain(t, newExecutor(f, h))
	if h.count() != 2 {
		t.Fatalf("handler ran %d times, want 2", h.count())
	}
	if a := f.answers()[1]; a["result_code"] != 409.0 || a["replayed"] != nil {
		t.Fatalf("answer to the reuse = %v", a)
	}
}

func TestAnOperationStartedAndNeverAnsweredIsAnswered504OnRepeat(t *testing.T) {
	f := newExecNode(t)
	h := newRecorder("svc/rollout")
	ledger := NewMemoryLedger()
	_ = ledger.Start(operationKey(Command{Sender: hub, OperationID: "op-1"}))
	f.command("_CmdMaintain", "svc/rollout", hub, map[string]any{"correlation_id": "c1", "operation_id": "op-1"})
	x := newExecutor(f, h)
	x.Ledger = ledger
	drain(t, x)
	if a := f.answers()[0]; h.count() != 0 || a["result_code"] != 504.0 || a["replayed"] != true {
		t.Fatalf("calls %d answer %v", h.count(), a)
	}
}

// An expired operation was not executed, so it is not recorded and a retry
// with a new deadline runs.
func TestAnExpiredOperationIsNotRecordedSoItsRetryRuns(t *testing.T) {
	f := newExecNode(t)
	h := newRecorder("svc/rollout")
	now := time.UnixMilli(1_800_000_000_000)
	f.command("_CmdMaintain", "svc/rollout", hub, map[string]any{"correlation_id": "c1", "operation_id": "op-1", "expires_at": now.UnixMilli() - 1})
	f.command("_CmdMaintain", "svc/rollout", hub, map[string]any{"correlation_id": "c2", "operation_id": "op-1", "expires_at": now.UnixMilli() + 60_000})
	x := newExecutor(f, h)
	x.Now = func() time.Time { return now }
	drain(t, x)
	if h.count() != 1 || h.calls[0].CorrelationID != "c2" {
		t.Fatalf("handler calls = %+v", h.calls)
	}
}

func TestAMalformedEnvelopeIsRefusedAndNotRun(t *testing.T) {
	cases := map[string]map[string]any{
		"operation_id not a string":   {"operation_id": 7},
		"operation_id with a slash":   {"operation_id": "a/b"},
		"on_behalf_of without an id":  {"on_behalf_of": map[string]any{"label": "anna"}},
		"on_behalf_of unknown kind":   {"on_behalf_of": map[string]any{"id": "x", "kind": "robot"}},
		"on_behalf_of unknown field":  {"on_behalf_of": map[string]any{"id": "x", "role": "admin"}},
		"on_behalf_of not an object":  {"on_behalf_of": "anna"},
		"operation_id with a control": {"operation_id": "a\nb"},
	}
	for name, env := range cases {
		t.Run(name, func(t *testing.T) {
			f := newExecNode(t)
			h := newRecorder("svc/plan")
			env["correlation_id"] = "c1"
			f.command("_CmdMaintain", "svc/plan", hub, env)
			drain(t, newExecutor(f, h))
			a := f.answers()[0]
			if h.count() != 0 || a["result_code"] != 422.0 || a["result"].(map[string]any)["outcome"] != "refused" {
				t.Fatalf("calls %d answer %v", h.count(), a)
			}
		})
	}
}

func TestAPersonCannotSendACommandOnBehalfOfSomeoneElse(t *testing.T) {
	f := newExecNode(t)
	h := newRecorder("svc/plan")
	f.command("_CmdMaintain", "svc/plan", operator, map[string]any{"correlation_id": "c1", "on_behalf_of": map[string]any{"id": "kc-bob"}})
	f.command("_CmdMaintain", "svc/plan", operator, map[string]any{"correlation_id": "c2", "on_behalf_of": map[string]any{"id": operator.ID}})
	drain(t, newExecutor(f, h))
	if a := f.answers()[0]; a["result_code"] != 403.0 {
		t.Fatalf("answer = %v", a)
	}
	if h.count() != 1 || h.calls[0].CorrelationID != "c2" {
		t.Fatalf("handler calls = %+v", h.calls)
	}
}

func TestAFileLedgerSurvivesReopeningAndDropsOldEntries(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sub", "ledger.json")
	l, err := OpenFileLedger(path, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	now := time.UnixMilli(1_800_000_000_000)
	l.now = func() time.Time { return now }
	if err := l.Start("old"); err != nil {
		t.Fatal(err)
	}
	if err := l.Answer("old", []byte(`{"a":1}`)); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenFileLedger(path, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if started, answer, _ := reopened.Entry("old"); !started || string(answer) != `{"a":1}` {
		t.Fatalf("entry after reopening = %v %s", started, answer)
	}
	reopened.now = func() time.Time { return now.Add(2 * time.Hour) }
	if err := reopened.Start("new"); err != nil {
		t.Fatal(err)
	}
	if started, _, _ := reopened.Entry("old"); started {
		t.Fatal("an entry older than keep survived")
	}
	if started, answer, _ := reopened.Entry("new"); !started || answer != nil {
		t.Fatalf("new entry = %v %s, want started and unanswered", started, answer)
	}
}
