package store

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
)

// holdAcks makes the next ack commit wait until release is closed, and counts
// the commits.
func holdAcks(s *Store) (started <-chan struct{}, release chan struct{}, commits *atomic.Int64) {
	first := make(chan struct{})
	release = make(chan struct{})
	commits = &atomic.Int64{}
	var once sync.Once
	s.ackApplyHook = func(b *pebble.Batch, o *pebble.WriteOptions) error {
		if commits.Add(1) == 1 {
			once.Do(func() { close(first) })
			<-release
		}
		return s.db.Apply(b, o)
	}
	return first, release, commits
}

// Acks that arrive while one commits share the next commit instead of syncing
// one after the other, and each still returns only once it is durable.
func TestConcurrentCursorAcksShareOneSyncedCommit(t *testing.T) {
	s := mustOpen(t)
	started, release, commits := holdAcks(s)
	done := make(chan bool, 51)
	go func() { done <- s.CursorAck("first", "metrics", 2) }()
	<-started
	for i := 0; i < 50; i++ {
		go func() { done <- s.CursorAck(fmt.Sprintf("c%d", i), "metrics", uint64(10+i)) }()
	}
	waitQueued(t, &s.ackQueue, 50)
	close(release)
	for i := 0; i < 51; i++ {
		if !<-done {
			t.Fatal("an ack did not move its cursor")
		}
	}
	if n := commits.Load(); n != 2 {
		t.Fatalf("%d commits for 51 acks, want 2: the held one and one for the rest", n)
	}
	for i := 0; i < 50; i++ {
		if got := s.CursorGet(fmt.Sprintf("c%d", i), "metrics"); got != uint64(10+i) {
			t.Fatalf("c%d at %d, want %d", i, got, 10+i)
		}
	}
}

// Two acks of one cursor in one group: the second, lower one does not move it
// back, and reports that it did not move.
func TestAnAckGroupMovesACursorForwardOnly(t *testing.T) {
	s := mustOpen(t)
	started, release, _ := holdAcks(s)
	go s.CursorAck("other", "metrics", 2)
	<-started
	high, low := make(chan bool), make(chan bool)
	go func() { high <- s.CursorAck("c", "metrics", 9) }()
	waitQueued(t, &s.ackQueue, 1)
	go func() { low <- s.CursorAck("c", "metrics", 5) }()
	waitQueued(t, &s.ackQueue, 2)
	close(release)
	if !<-high {
		t.Fatal("the higher ack did not move the cursor")
	}
	if <-low {
		t.Fatal("the lower ack reported a move")
	}
	if got := s.CursorGet("c", "metrics"); got != 9 {
		t.Fatalf("cursor at %d, want 9", got)
	}
}

// waitQueued waits until n requests wait behind the leader of q.
func waitQueued[R groupMember](t *testing.T, q *groupQueue[R], n int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		q.mu.Lock()
		pending := len(q.pending)
		q.mu.Unlock()
		if pending >= n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d requests queued, want %d", pending, n)
		}
		time.Sleep(time.Millisecond)
	}
}

// BenchmarkConcurrentCursorAcks is 16 consumers acking their own cursors at
// once, as many services draining after a burst do. Set COLCA_BENCH_DIR to a
// directory on a real disk: on tmpfs a sync costs nothing.
//
//	COLCA_BENCH_DIR=$HOME/.cache/colca-bench go test ./internal/store -run '^$' -bench ConcurrentCursorAcks
func BenchmarkConcurrentCursorAcks(b *testing.B) {
	dir := b.TempDir()
	if base := os.Getenv("COLCA_BENCH_DIR"); base != "" {
		var err error
		if dir, err = os.MkdirTemp(base, "acks"); err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() { os.RemoveAll(dir) })
	}
	s, err := Open(dir)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { s.Close() })
	var next atomic.Uint64
	var id atomic.Int64
	b.SetParallelism(16)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		name := fmt.Sprintf("c%d", id.Add(1))
		for pb.Next() {
			s.CursorAck(name, "metrics", next.Add(1)+1)
		}
	})
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "acks/s")
}
