package door

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestWatchPassesHintsOnAndSkipsHeartbeats(t *testing.T) {
	var query, service string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query, service = r.URL.RawQuery, r.Header.Get("X-Colca-Service")
		w.Header().Set("Content-Type", "application/x-ndjson")
		fmt.Fprintln(w, `{"streams":["entities","annotations"],"next":{"entities":4,"annotations":9}}`)
		fmt.Fprintln(w, `{"streams":[]}`)
		fmt.Fprintln(w, `{"streams":["annotations"],"next":{"annotations":10}}`)
	}))
	defer srv.Close()

	c := &Client{BaseURL: srv.URL, Service: "projector"}
	var hints []Hint
	err := c.Watch(context.Background(), []string{"entities", "annotations"}, 0, func(h Hint) { hints = append(hints, h) })

	if err == nil || !strings.Contains(err.Error(), "connection closed") {
		t.Fatalf("err = %v, want the closed connection reported", err)
	}
	if query != "stream=entities&stream=annotations" || service != "projector" {
		t.Fatalf("request query %q service %q", query, service)
	}
	if len(hints) != 2 || hints[1].Streams[0] != "annotations" || hints[1].Next["annotations"] != 10 {
		t.Fatalf("hints = %+v", hints)
	}
}

func TestWatchReportsARefusal(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"error":"request limit exceeded"}`, http.StatusTooManyRequests)
	}))
	defer srv.Close()
	err := (&Client{BaseURL: srv.URL}).Watch(context.Background(), []string{"metrics"}, 0, func(Hint) {})
	if err == nil || !strings.Contains(err.Error(), "429") {
		t.Fatalf("err = %v, want HTTP 429", err)
	}
}
