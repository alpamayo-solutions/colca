package door

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

// publishedRecord is one /publish body, as the node received it.
type publishedRecord struct {
	Topic   string         `json:"topic"`
	Payload map[string]any `json:"payload"`
}

// fakeNode answers /self and records what is published to it.
type fakeNode struct {
	mu        sync.Mutex
	published []publishedRecord
	got       chan struct{}
}

func newFakeNode() (*fakeNode, *httptest.Server) {
	node := &fakeNode{got: make(chan struct{}, 64)}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/self":
			_ = json.NewEncoder(w).Encode(Self{
				ULID: "01SVC", Name: "colca-historian", Node: "01NODE", Mount: "line1",
			})
		case "/publish":
			var record publishedRecord
			_ = json.NewDecoder(r.Body).Decode(&record)
			node.mu.Lock()
			node.published = append(node.published, record)
			node.mu.Unlock()
			_ = json.NewEncoder(w).Encode(map[string]any{"stream": "logs", "offset": 1, "topic": record.Topic})
			node.got <- struct{}{}
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	return node, server
}

func (n *fakeNode) waitFor(t *testing.T, count int) []publishedRecord {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		n.mu.Lock()
		have := len(n.published)
		n.mu.Unlock()
		if have >= count {
			n.mu.Lock()
			defer n.mu.Unlock()
			return append([]publishedRecord(nil), n.published...)
		}
		select {
		case <-n.got:
		case <-deadline:
			t.Fatalf("waited 5s for %d published records, the node received %d", count, have)
		}
	}
}

// countingHandler stands in for the console handler being wrapped.
type countingHandler struct {
	mu      sync.Mutex
	handled int
	level   slog.Level
}

func (h *countingHandler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= h.level
}

func (h *countingHandler) Handle(_ context.Context, _ slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.handled++
	return nil
}

func (h *countingHandler) WithAttrs(_ []slog.Attr) slog.Handler { return h }

func (h *countingHandler) WithGroup(string) slog.Handler { return h }

func TestPublishedRecordMatchesTheTopicGrammar(t *testing.T) {
	node, server := newFakeNode()
	defer server.Close()

	publisher := NewLogPublisher(&countingHandler{},
		&Client{BaseURL: server.URL, Service: "colca-historian"},
		LogPublisherOptions{MinLevel: slog.LevelInfo})
	publisher.Start(context.Background())

	log := slog.New(publisher).With("service", "colca-historian")
	log.Warn("catch-up stalled", "stream", "metrics")

	records := node.waitFor(t, 1)
	parts := strings.Split(records[0].Topic, "/")
	if len(parts) != 7 {
		t.Fatalf("topic %q must be colca/v1/_Log/{node}/{mount…}/{service}/{LEVEL}",
			records[0].Topic)
	}
	if parts[0] != "colca" || parts[1] != "v1" || parts[2] != "_Log" {
		t.Errorf("topic prefix is wrong: %q", records[0].Topic)
	}
	if parts[3] != "01NODE" {
		t.Errorf("topic level 4 must be the node the door reported, got %q", parts[3])
	}
	// A service may only write its own subtree, so the record sits at its mount
	// and name.
	if parts[4] != "line1" || parts[5] != "colca-historian" {
		t.Errorf("the record must sit at the service's own position "+
			"(mount then name), got %q", records[0].Topic)
	}
	// The API drops records whose last segment is not a known level.
	if parts[6] != "WARNING" {
		t.Errorf("slog.LevelWarn must publish as WARNING, got %q", parts[6])
	}
	if records[0].Payload["message"] != "catch-up stalled" {
		t.Errorf("payload lost the message: %#v", records[0].Payload)
	}
	if records[0].Payload["timestamp"] == "" || records[0].Payload["timestamp"] == nil {
		t.Error("payload must carry a timestamp; the view sorts on it")
	}
}

func TestPublishingNeverSilencesTheConsole(t *testing.T) {
	node, server := newFakeNode()
	defer server.Close()

	inner := &countingHandler{level: slog.LevelDebug}
	publisher := NewLogPublisher(inner, &Client{BaseURL: server.URL},
		// Publish only warnings: the console must still get everything.
		LogPublisherOptions{MinLevel: slog.LevelWarn})
	publisher.Start(context.Background())

	log := slog.New(publisher)
	log.Info("routine")
	log.Warn("not routine")

	node.waitFor(t, 1)
	inner.mu.Lock()
	handled := inner.handled
	inner.mu.Unlock()
	if handled != 2 {
		t.Errorf("the wrapped handler must see every record, saw %d of 2", handled)
	}
	node.mu.Lock()
	defer node.mu.Unlock()
	if len(node.published) != 1 {
		t.Errorf("only the record at or above MinLevel may be published, got %d", len(node.published))
	}
}

func TestTheQueueIsBoundedAndKeepsTheNewest(t *testing.T) {
	publisher := NewLogPublisher(&countingHandler{},
		&Client{BaseURL: "http://127.0.0.1:1"}, // never started: nothing drains
		LogPublisherOptions{MinLevel: slog.LevelInfo, Capacity: 3})

	log := slog.New(publisher)
	for i := range 10 {
		log.Info("line", "n", i)
	}

	if got := len(publisher.records); got != 3 {
		t.Fatalf("queue must stay at its capacity of 3, holds %d", got)
	}
	var kept []string
	for len(publisher.records) > 0 {
		item := <-publisher.records
		extra, _ := item.payload["extra"].(map[string]any)
		value, _ := extra["n"].(string)
		kept = append(kept, value)
	}
	want := []string{"7", "8", "9"}
	for i := range want {
		if kept[i] != want[i] {
			t.Fatalf("the newest records must survive the drop, kept %v want %v", kept, want)
		}
	}
}

func TestHandleDoesNotWaitOnTheNode(t *testing.T) {
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	defer close(release)

	publisher := NewLogPublisher(&countingHandler{}, &Client{BaseURL: server.URL},
		LogPublisherOptions{MinLevel: slog.LevelInfo})
	publisher.Start(context.Background())

	done := make(chan struct{})
	go func() {
		slog.New(publisher).Info("a line")
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Handle blocked on the node; a log statement must never wait on the network")
	}
}

func TestTheBuiltinDefaultHandlerIsRefused(t *testing.T) {
	// Wrapping slog's built-in default and installing the wrapper would hang on
	// the first log call. This also pins the type name the guard matches, which
	// slog does not export.
	builtin := slog.Default().Handler()
	if got := reflect.TypeOf(builtin).String(); got != builtinDefaultHandler {
		t.Fatalf("slog's built-in default handler is now %q, not %q -- the guard "+
			"in usableBase no longer matches it and the hang is back", got, builtinDefaultHandler)
	}

	publisher := NewLogPublisher(builtin, &Client{BaseURL: "http://127.0.0.1:1"},
		LogPublisherOptions{MinLevel: slog.LevelInfo})
	if publisher.inner == builtin {
		t.Fatal("the built-in default handler must be replaced, not wrapped")
	}

	// A hang would otherwise only show as a suite timeout, minutes later.
	done := make(chan struct{})
	go func() {
		defer close(done)
		slog.New(publisher).Info("this must not hang")
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("logging through the publisher never returned -- the default-handler loop is back")
	}
}

// logPayloadVector lists what a _Log payload must carry. It is generated from
// the Python contract and keeps the hand-built Go payload in step.
type logPayloadVector struct {
	Contract string   `json:"contract"`
	Required []string `json:"required"`
	Optional []string `json:"optional"`
}

func TestThePayloadCarriesEveryFieldTheContractRequires(t *testing.T) {
	// The vector is generated from the contract, so a missing field here means the
	// node would refuse the record.
	path := filepath.Join("..", "contracts", "src",
		"colca_data_contracts", "vectors", "log_payload.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("the shared _Log vector is unreadable (%v). It is what keeps this "+
			"payload equal to the contract; without it this test proves nothing.", err)
	}
	var vector logPayloadVector
	if err := json.Unmarshal(raw, &vector); err != nil {
		t.Fatal(err)
	}
	if vector.Contract != "_Log" || len(vector.Required) == 0 {
		t.Fatalf("the vector does not describe _Log's required fields: %+v", vector)
	}

	node, server := newFakeNode()
	defer server.Close()
	publisher := NewLogPublisher(&countingHandler{},
		&Client{BaseURL: server.URL, Service: "colca-historian"},
		LogPublisherOptions{MinLevel: slog.LevelInfo})
	publisher.Start(context.Background())
	slog.New(publisher).With("service", "colca-historian").Warn("catch-up stalled")

	payload := node.waitFor(t, 1)[0].Payload
	for _, field := range vector.Required {
		value, present := payload[field]
		if !present {
			t.Errorf("payload omits %q, which _Log requires — the node refuses the "+
				"whole record for a missing field, and says so only on stderr", field)
			continue
		}
		if value == nil {
			t.Errorf("payload sends %q as null; _Log requires a value", field)
		}
	}
}

func TestTheLevelInThePayloadIsTheOneInTheTopic(t *testing.T) {
	// slog says WARN, the topic grammar says WARNING; payload and topic must agree.
	node, server := newFakeNode()
	defer server.Close()
	publisher := NewLogPublisher(&countingHandler{}, &Client{BaseURL: server.URL},
		LogPublisherOptions{MinLevel: slog.LevelInfo})
	publisher.Start(context.Background())
	slog.New(publisher).Warn("careful")

	record := node.waitFor(t, 1)[0]
	parts := strings.Split(record.Topic, "/")
	if got := record.Payload["level"]; got != parts[len(parts)-1] {
		t.Errorf("payload level %v disagrees with the topic's %q", got, parts[len(parts)-1])
	}
	if record.Payload["level"] != "WARNING" {
		t.Errorf("slog's WARN must be published as WARNING, got %v", record.Payload["level"])
	}
}

// blockingSink lets a test hold one publish in flight and see what the
// publisher does around it.
type blockingSink struct {
	entered  chan struct{}
	release  chan struct{}
	mu       sync.Mutex
	finished int
	after    int // publishes that STARTED after Stop returned
	stopped  bool
}

func (s *blockingSink) LogPosition(context.Context) (string, []string, error) {
	return "01NODE", []string{"colca"}, nil
}

func (s *blockingSink) PublishLog(context.Context, string, map[string]any) error {
	s.mu.Lock()
	if s.stopped {
		s.after++
	}
	s.mu.Unlock()
	select {
	case s.entered <- struct{}{}:
	default:
	}
	<-s.release
	s.mu.Lock()
	s.finished++
	s.mu.Unlock()
	return nil
}

func (s *blockingSink) counts() (finished, after int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.finished, s.after
}

func (s *blockingSink) markStopped() {
	s.mu.Lock()
	s.stopped = true
	s.mu.Unlock()
}

// TestStopWaitsForAnInFlightPublish: Stop returns only once an in-flight publish
// has finished, so the owner can close its store right after.
func TestStopWaitsForAnInFlightPublish(t *testing.T) {
	sink := &blockingSink{entered: make(chan struct{}, 1), release: make(chan struct{})}
	publisher := NewLogPublisher(&countingHandler{level: slog.LevelInfo}, sink,
		LogPublisherOptions{MinLevel: slog.LevelInfo})
	log := slog.New(publisher)
	publisher.Start(context.Background())

	log.Info("in flight")
	select {
	case <-sink.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the drain never reached the sink")
	}

	returned := make(chan struct{})
	go func() {
		defer close(returned)
		publisher.Stop()
	}()

	select {
	case <-returned:
		t.Fatal("Stop returned while a publish was still in flight — the store's owner " +
			"would now close it underneath that publish")
	case <-time.After(200 * time.Millisecond):
	}

	close(sink.release)
	select {
	case <-returned:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop did not return after the in-flight publish finished")
	}
	if finished, _ := sink.counts(); finished == 0 {
		t.Fatal("no publish ever finished, so this test proved nothing about waiting")
	}
}

// TestNothingPublishesAfterStop is the other half: once Stop returns, records
// keep reaching the console but never the sink.
func TestNothingPublishesAfterStop(t *testing.T) {
	sink := &blockingSink{entered: make(chan struct{}, 1), release: make(chan struct{})}
	close(sink.release) // nothing blocks here; we only care what arrives
	console := &countingHandler{level: slog.LevelInfo}
	publisher := NewLogPublisher(console, sink, LogPublisherOptions{MinLevel: slog.LevelInfo})
	log := slog.New(publisher)
	publisher.Start(context.Background())

	log.Info("before")
	select {
	case <-sink.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the drain never reached the sink")
	}

	publisher.Stop()
	sink.markStopped()

	before := console.handled
	for i := 0; i < 200; i++ {
		log.Info("after stop", "i", i)
	}
	time.Sleep(100 * time.Millisecond) // a drain still alive would have published by now

	if _, after := sink.counts(); after != 0 {
		t.Errorf("%d records reached the sink after Stop returned, want 0", after)
	}
	if console.handled <= before {
		t.Errorf("console handled %d records, was %d before Stop — logging must keep working",
			console.handled, before)
	}
}

// TestStopWithoutStartReturns — a service that never started its publisher
// (or failed before Start) must still be able to shut down.
func TestStopWithoutStartReturns(t *testing.T) {
	publisher := NewLogPublisher(&countingHandler{level: slog.LevelInfo},
		&blockingSink{entered: make(chan struct{}, 1), release: make(chan struct{})},
		LogPublisherOptions{MinLevel: slog.LevelInfo})

	done := make(chan struct{})
	go func() { defer close(done); publisher.Stop(); publisher.Stop() }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop hung on a publisher that was never started")
	}
}

func TestARecordWithoutAProgramCounterStillSatisfiesTheContract(t *testing.T) {
	// Records bridged from the standard log package may have no program counter.
	// The contract's string fields must still be non-empty.
	node, server := newFakeNode()
	defer server.Close()
	publisher := NewLogPublisher(&countingHandler{},
		&Client{BaseURL: server.URL, Service: "colca-historian"},
		LogPublisherOptions{MinLevel: slog.LevelInfo})
	publisher.Start(context.Background())
	handler := slog.New(publisher).With("service", "colca-historian").Handler()

	record := slog.NewRecord(time.Now(), slog.LevelInfo, "Found 0 WALs", 0)
	if err := handler.Handle(context.Background(), record); err != nil {
		t.Fatal(err)
	}

	payload := node.waitFor(t, 1)[0].Payload
	for _, field := range []string{"module", "function", "logger_name", "message", "level", "timestamp"} {
		value, _ := payload[field].(string)
		if value == "" {
			t.Errorf("a record without a program counter sends %q empty; _Log requires "+
				"minLength 1 and the node refuses the whole record (payload %v)", field, payload)
		}
	}
}

// quietSink knows its position and accepts every record.
type quietSink struct{}

func (quietSink) LogPosition(context.Context) (string, []string, error) {
	return "n1", []string{"svc"}, nil
}

func (quietSink) PublishLog(context.Context, string, map[string]any) error { return nil }

// Clones that slog makes while the worker resolves the node must not race with it.
func TestWithWhilePublishingDoesNotRace(t *testing.T) {
	p := NewLogPublisher(slog.NewTextHandler(io.Discard, nil), quietSink{}, LogPublisherOptions{})
	p.Start(context.Background())
	defer p.Stop()
	logger := slog.New(p)

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 500; i++ {
			logger.With("request", i).Info("handled")
		}
	}()
	for i := 0; i < 500; i++ {
		logger.Info("tick")
	}
	<-done
}
