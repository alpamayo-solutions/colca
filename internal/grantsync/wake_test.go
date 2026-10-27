package grantsync

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// countingNode serves an empty tree and counts the reads of it: one per cycle.
func countingNode(t *testing.T) (*NodeClient, *atomic.Int32) {
	t.Helper()
	var reads atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/kv") {
			reads.Add(1)
		}
		_, _ = w.Write([]byte(`{"entries":[]}`))
	}))
	t.Cleanup(srv.Close)
	return &NodeClient{BaseURL: srv.URL, Service: "grantsync"}, &reads
}

func runSyncer(t *testing.T, s *Syncer, safety time.Duration) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); s.Run(ctx, safety) }()
	t.Cleanup(func() { cancel(); <-done })
}

func waitReads(t *testing.T, reads *atomic.Int32, want int32, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for reads.Load() < want {
		if time.Now().After(deadline) {
			t.Fatalf("%d cycle(s) after %v, want %d", reads.Load(), within, want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// A tree change (or a POST /sync) runs a cycle at once. Before, the only thing
// that ran one was the 30 s ticker.
func TestAWakeRunsACycleWithoutWaitingForTheSafetyInterval(t *testing.T) {
	node, reads := countingNode(t)
	s := syncer(node, grantingRealm(t))
	s.Coalesce = 10 * time.Millisecond
	runSyncer(t, s, time.Hour)
	waitReads(t, reads, 1, 5*time.Second) // the cycle at start
	s.Wake()
	waitReads(t, reads, 2, 5*time.Second)
}

// Without a wake nothing runs before the safety interval: the loop does not
// poll the tree.
func TestNoCycleRunsWithoutAWakeBeforeTheSafetyInterval(t *testing.T) {
	node, reads := countingNode(t)
	s := syncer(node, grantingRealm(t))
	s.Coalesce = time.Millisecond
	runSyncer(t, s, time.Hour)
	waitReads(t, reads, 1, 5*time.Second)
	time.Sleep(200 * time.Millisecond)
	if n := reads.Load(); n != 1 {
		t.Fatalf("%d cycles without a wake, want only the one at start", n)
	}
}

// A burst of wakes costs one cycle, not one each: a wake within Coalesce of
// the last cycle's start waits for the window to close.
func TestABurstOfWakesIsMergedIntoOneCycle(t *testing.T) {
	node, reads := countingNode(t)
	s := syncer(node, grantingRealm(t))
	s.Coalesce = 400 * time.Millisecond
	runSyncer(t, s, time.Hour)
	waitReads(t, reads, 1, 5*time.Second)
	for range 50 {
		s.Wake()
		time.Sleep(2 * time.Millisecond) // spread over the window, as a busy tree does
	}
	waitReads(t, reads, 2, 5*time.Second)
	time.Sleep(500 * time.Millisecond)
	if n := reads.Load(); n != 2 {
		t.Fatalf("%d cycles after a burst of 50 wakes, want 2", n)
	}
}

// WatchTree watches only the tree contracts, on the streams they live on, and
// wakes the syncer on every hint.
func TestWatchTreeWakesOnAHintForTheTreeContracts(t *testing.T) {
	var query atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query.Store(r.URL.Query())
		w.Header().Set("Content-Type", "application/x-ndjson")
		_, _ = w.Write([]byte("{\"streams\":[\"entities\"]}\n"))
	}))
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	node := &NodeClient{BaseURL: srv.URL, Service: "grantsync", HTTP: srv.Client()}
	woken := false
	node.WatchTree(ctx, func() { woken = true; cancel() }, nil)
	if !woken {
		t.Fatal("a hint did not wake the syncer")
	}
	q := query.Load().(url.Values)
	streams := slices.Sorted(slices.Values(q["stream"]))
	if !slices.Equal(streams, []string{"definitions", "entities"}) || !slices.Equal(q["contract"], treeContracts) {
		t.Fatalf("watched %v, want the tree contracts on their streams", q)
	}
}
