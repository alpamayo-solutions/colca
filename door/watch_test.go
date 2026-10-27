package door

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"sync/atomic"
	"testing"
	"time"
)

func TestWatchReconnectsAndIgnoresHeartbeats(t *testing.T) {
	var connections atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Colca-Service") != "consumer" || r.URL.Query().Get("stream") != "metrics" {
			t.Error("missing identity or stream")
		}
		connections.Add(1)
		w.Header().Set("Content-Type", "application/x-ndjson")
		w.Write([]byte("{\"streams\":null}\n{\"streams\":[\"metrics\"]}\n"))
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	calls := 0
	(&Client{BaseURL: server.URL, Service: "consumer"}).WatchForever(ctx, []string{"metrics"}, 0, time.Millisecond, func(hint Hint) {
		calls++
		if calls == 2 {
			cancel()
		}
	}, nil)
	if calls != 2 || connections.Load() != 2 {
		t.Fatalf("calls=%d connections=%d", calls, connections.Load())
	}
}

func TestWaitChangeBatchesWithoutTrailingEdgeStarvation(t *testing.T) {
	var signal Signal
	changed := signal.Changes()
	signal.Notify() // notify before waiting must survive
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	start := time.Now()
	if !WaitChange(ctx, changed, time.Hour, start.Add(30*time.Millisecond)) {
		t.Fatal("missed change")
	}
	if time.Since(start) < 30*time.Millisecond {
		t.Fatal("batch interval ignored")
	}
	cancel()
	if WaitChange(ctx, signal.Changes(), time.Hour, time.Time{}) {
		t.Fatal("cancellation ignored")
	}
}

func TestWatchForAsksForItsContractsOnly(t *testing.T) {
	var query atomic.Value
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query.Store(r.URL.Query())
		w.Header().Set("Content-Type", "application/x-ndjson")
		w.Write([]byte("{\"streams\":[\"entities\"]}\n"))
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req := WatchRequest{Streams: []string{"entities", "definitions"}, Contracts: []string{"_SystemElement", "_Group"}, Interval: 250 * time.Millisecond}
	_ = (&Client{BaseURL: server.URL}).WatchFor(ctx, req, func(Hint) { cancel() })
	q, _ := query.Load().(url.Values)
	if !slices.Equal(q["stream"], req.Streams) || !slices.Equal(q["contract"], req.Contracts) || q.Get("interval_ms") != "250" {
		t.Fatalf("query = %v, want the streams, the contracts and the interval", q)
	}
}
