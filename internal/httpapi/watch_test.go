package httpapi

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/alpamayo-solutions/colca/internal/store"
)

func TestLocalWatchWakesAfterCommitAndDoesNotMoveCursor(t *testing.T) {
	h := newLocalHandler(t)
	server := httptest.NewServer(h)
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET", server.URL+"/watch?stream=metrics", nil)
	req.Header.Set("X-Colca-Service", "test-watch")
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		t.Fatal(response.StatusCode)
	}
	scanner := bufio.NewScanner(response.Body)
	read := func() {
		t.Helper()
		if !scanner.Scan() {
			t.Fatalf("no hint: %v", scanner.Err())
		}
		var hint struct {
			Streams []string `json:"streams"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &hint); err != nil {
			t.Fatal(err)
		}
		if len(hint.Streams) != 1 || hint.Streams[0] != "metrics" {
			t.Fatalf("wrong hint: %+v", hint)
		}
	}
	read() // every subscription first requests catch-up
	if _, _, err := h.eng.Store().Append("metrics", []store.Record{{Topic: "sample", Payload: []byte(`1`)}}); err != nil {
		t.Fatal(err)
	}
	read()
	if h.eng.Store().CursorGet("c/test-watch/metrics", "metrics") != 1 {
		t.Fatal("hint acknowledged records")
	}
	cancel()
}

func TestWatchRejectsUnknownStream(t *testing.T) {
	h := newLocalHandler(t)
	req := httptest.NewRequest("GET", "/watch?stream=does-not-exist", nil)
	req.Header.Set("X-Colca-Service", "test-watch")
	response := httptest.NewRecorder()
	h.ServeHTTP(response, req)
	if response.Code != 400 {
		t.Fatal(response.Code)
	}
}

func TestBacklogWatchIncludesConsumerProgressWithoutWakingRecordReaders(t *testing.T) {
	h := newLocalHandler(t)
	server := httptest.NewServer(h)
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET", server.URL+"/watch?backlog=1", nil)
	req.Header.Set("X-Colca-Service", "test-watch")
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	scanner := bufio.NewScanner(response.Body)
	read := func() {
		t.Helper()
		if !scanner.Scan() {
			t.Fatalf("missing hint: %v", scanner.Err())
		}
		var hint map[string]bool
		if err := json.Unmarshal(scanner.Bytes(), &hint); err != nil || !hint["backlog_changed"] {
			t.Fatalf("hint: %s %v", scanner.Bytes(), err)
		}
	}
	read()
	recordWake, _ := h.eng.Store().Changes()
	if !h.eng.Store().CursorAck("c/test-watch/metrics", "metrics", 2) {
		t.Fatal("ack failed")
	}
	read()
	select {
	case <-recordWake:
		t.Fatal("consumer progress woke record readers")
	default:
	}
	if _, _, err := h.eng.Store().Append("metrics", []store.Record{{Topic: "sample", Payload: []byte(`1`)}}); err != nil {
		t.Fatal(err)
	}
	read()
	cancel()
}

func TestContractScopedWatchSuppressesUnrelatedWakeups(t *testing.T) {
	h := newLocalHandler(t)
	server := httptest.NewServer(h)
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET", server.URL+"/watch?stream=entities&contract=_Signal", nil)
	req.Header.Set("X-Colca-Service", "test-watch")
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	scanner := bufio.NewScanner(response.Body)
	if !scanner.Scan() {
		t.Fatal("missing initial catch-up")
	}
	lines := make(chan string, 4)
	go func() {
		for scanner.Scan() {
			lines <- scanner.Text()
		}
	}()
	if _, _, err := h.eng.Store().Append("entities", []store.Record{{Topic: "prekit/v1/_ServiceDetails/node/worker"}}); err != nil {
		t.Fatal(err)
	}
	select {
	case line := <-lines:
		t.Fatalf("unrelated hint: %s", line)
	case <-time.After(100 * time.Millisecond):
	}
	if _, _, err := h.eng.Store().Append("entities", []store.Record{{Topic: "prekit/v1/_Signal/node/signal"}}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-lines:
	case <-ctx.Done():
		t.Fatal("matching commit not announced")
	}
}
