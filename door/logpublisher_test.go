package door

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
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
	// A service may write its own subtree and nothing above it, so a record
	// addressed at the node root is refused outright -- `no write scope
	// covers colca/v1/_Log/...` is what silenced every placed service.
	if parts[4] != "line1" || parts[5] != "colca-historian" {
		t.Errorf("the record must sit at the service's own position "+
			"(mount then name), got %q", records[0].Topic)
	}
	// The API drops any record whose last segment is not one of
	// colca_data_contracts.logging.LOG_LEVELS, so an unmapped slog level
	// would vanish from the view rather than show up wrong.
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
	// Wrapping slog's built-in default and then calling slog.SetDefault is an
	// infinite loop: that handler writes through the `log` package, and
	// SetDefault redirects `log` back into slog. It does not crash -- the
	// first log call simply never returns, so the service prints nothing and
	// never reaches its HTTP listener. That is what took the level-4 suite
	// red: a healthcheck that never passed and an empty container log.
	//
	// This also pins the name the guard matches on. slog does not export the
	// type, so if a future Go renames it the guard stops matching and the
	// hang returns -- this test is what says so.
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

	// The claim is that logging through it RETURNS. Nothing else in this file
	// would notice a hang: a deadlocked Handle fails the suite by timeout,
	// minutes later, with no indication of which test it was.
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
