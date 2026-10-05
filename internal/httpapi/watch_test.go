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
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/watch?stream=metrics", nil)
	req.Header.Set("X-Colca-Service", "test-watch")
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
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
	req := httptest.NewRequest(http.MethodGet, "/watch?stream=does-not-exist", nil)
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
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/watch?backlog=1", nil)
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
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/watch?stream=entities&contract=_Signal", nil)
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

// A contract-scoped watch under a stream of matching appends: once they stop,
// the newest hint names the stream's real head. Each hint's next offset comes
// from the same snapshot as the scoped positions; read separately, an append
// between them left the last hint short of it with no further hint to follow.
func TestContractScopedWatchHintEndsAtTheHead(t *testing.T) {
	h := newLocalHandler(t)
	server := httptest.NewServer(h)
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/watch?stream=entities&contract=_Signal&interval_ms=0", nil)
	req.Header.Set("X-Colca-Service", "test-watch")
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	heads := make(chan uint64, 1024)
	go func() {
		scanner := bufio.NewScanner(response.Body)
		for scanner.Scan() {
			var hint struct {
				Next map[string]uint64 `json:"next"`
			}
			if json.Unmarshal(scanner.Bytes(), &hint) == nil && hint.Next["entities"] > 0 {
				heads <- hint.Next["entities"]
			}
		}
	}()
	<-heads // catch-up
	for i := 0; i < 300; i++ {
		if _, _, err := h.eng.Store().Append("entities", []store.Record{{Topic: "prekit/v1/_Signal/node/signal"}}); err != nil {
			t.Fatal(err)
		}
	}
	_, want := h.eng.Store().StreamChanges([]string{"entities"})
	var last uint64
	for last != want["entities"] {
		select {
		case last = <-heads:
		case <-time.After(time.Second):
			t.Fatalf("last hint next=%d, stream next=%d", last, want["entities"])
		}
	}
}
