package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
)

func replRec(child string, off uint64) ReplRecord {
	return ReplRecord{
		ChildOffset: off,
		Topic:       fmt.Sprintf("colca/v1/_Metric/%s/m/s", child),
		Payload:     []byte(fmt.Sprintf(`{"v":%d,"child":%q}`, off, child)),
		TS:          int64(off),
	}
}

// Many children push at once, each its batches in order, as a parent's door
// sees them. Every record lands exactly once, under gapless local offsets, each
// child's records in its own order, and every HWM is the child's last offset.
func TestConcurrentReplicatedBatchesLandOnceInOrder(t *testing.T) {
	s := mustOpen(t)
	const children, batches, perBatch = 24, 20, 3
	var wg sync.WaitGroup
	for c := 0; c < children; c++ {
		wg.Add(1)
		go func(c int) {
			defer wg.Done()
			child := fmt.Sprintf("n-%02d", c)
			for b := 0; b < batches; b++ {
				var recs []ReplRecord
				for i := 1; i <= perBatch; i++ {
					recs = append(recs, replRec(child, uint64(b*perBatch+i)))
				}
				// Every second batch is sent twice, as a child does when an answer is lost.
				for range 1 + b%2 {
					if _, hwm, err := s.ApplyReplicated(child, "metrics", recs); err != nil || hwm != uint64((b+1)*perBatch) {
						t.Errorf("%s batch %d: hwm %d err %v", child, b, hwm, err)
						return
					}
				}
			}
		}(c)
	}
	wg.Wait()

	const total = children * batches * perBatch
	if got := s.NextOffset("metrics"); got != total+1 {
		t.Fatalf("metrics next = %d, want %d: a record was lost or applied twice", got, total+1)
	}
	recs, _, err := s.Read("metrics", 1, total+10, nil)
	if err != nil {
		t.Fatal(err)
	}
	last := map[string]float64{}
	for i, r := range recs {
		if r.Offset != uint64(i+1) {
			t.Fatalf("record %d has offset %d: offsets are not gapless", i, r.Offset)
		}
		var p struct {
			V     float64 `json:"v"`
			Child string  `json:"child"`
		}
		if err := json.Unmarshal(r.Payload, &p); err != nil {
			t.Fatal(err)
		}
		if p.V != last[p.Child]+1 {
			t.Fatalf("%s: record %v follows %v: a child's order was not kept", p.Child, p.V, last[p.Child])
		}
		last[p.Child] = p.V
	}
	for c := 0; c < children; c++ {
		child := fmt.Sprintf("n-%02d", c)
		if got := s.HWMGet(child, "metrics"); got != batches*perBatch {
			t.Fatalf("%s HWM = %d, want %d", child, got, batches*perBatch)
		}
	}
}

// While one commit syncs, the requests that arrive are committed together in
// the next one: the parent's commit rate no longer bounds its children's. Each
// caller still returns only after the commit holding its records.
func TestReplicatedBatchesArrivingDuringACommitShareTheNext(t *testing.T) {
	s := mustOpen(t)
	release := make(chan struct{})
	var applies atomic.Int32
	inFirst := make(chan struct{})
	apply := s.appendApply
	s.appendApply = func(b *pebble.Batch, o *pebble.WriteOptions) error {
		if applies.Add(1) == 1 {
			close(inFirst)
			<-release
		}
		return apply(b, o)
	}

	first := make(chan error, 1)
	go func() {
		_, _, err := s.ApplyReplicated("n-first", "metrics", []ReplRecord{replRec("n-first", 1)})
		first <- err
	}()
	<-inFirst
	const waiting = 16
	results := make(chan error, waiting)
	for i := 0; i < waiting; i++ {
		go func(i int) {
			child := fmt.Sprintf("n-%02d", i)
			_, _, err := s.ApplyReplicated(child, "metrics", []ReplRecord{replRec(child, 1)})
			results <- err
		}(i)
	}
	// Each waiting request is queued before the first commit is let go.
	deadline := time.Now().Add(5 * time.Second)
	for {
		s.replQueue.mu.Lock()
		n := len(s.replQueue.pending)
		s.replQueue.mu.Unlock()
		if n == waiting {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d of %d requests queued", n, waiting)
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case err := <-results:
		t.Fatalf("a request returned (%v) before any commit holding it finished", err)
	default:
	}
	close(release)
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	for i := 0; i < waiting; i++ {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	if got := applies.Load(); got != 2 {
		t.Fatalf("%d commits for %d requests, want 2: the waiting requests were not grouped", got, waiting+1)
	}
	if got := s.NextOffset("metrics"); got != waiting+2 {
		t.Fatalf("metrics next = %d, want %d", got, waiting+2)
	}
}

// A request that cannot be built fails alone; the others in its commit land.
// A commit that fails fails every request in it and moves nothing.
func TestAFailedReplicatedRequestDoesNotTakeItsGroupDown(t *testing.T) {
	s := mustOpen(t)
	bad := &replRequest{child: "n-bad", stream: "metrics", recs: []ReplRecord{{ChildOffset: 2, SkipFrom: 3}}}
	good := &replRequest{child: "n-good", stream: "metrics", recs: []ReplRecord{replRec("n-good", 1)}}
	unknown := &replRequest{child: "n-good", stream: "nope", recs: []ReplRecord{replRec("n-good", 1)}}
	s.commitReplicated([]*replRequest{bad, good, unknown})
	if bad.err == nil || unknown.err == nil {
		t.Fatalf("bad err %v, unknown-stream err %v: both should fail", bad.err, unknown.err)
	}
	if good.err != nil || len(good.applied) != 1 || good.hwm != 1 {
		t.Fatalf("good request: applied %d hwm %d err %v", len(good.applied), good.hwm, good.err)
	}
	if got := s.NextOffset("metrics"); got != 2 {
		t.Fatalf("metrics next = %d, want 2", got)
	}

	s.appendApply = func(*pebble.Batch, *pebble.WriteOptions) error { return errors.New("disk unavailable") }
	a := &replRequest{child: "n-a", stream: "metrics", recs: []ReplRecord{replRec("n-a", 1)}}
	b := &replRequest{child: "n-good", stream: "metrics", recs: []ReplRecord{replRec("n-good", 2)}}
	s.commitReplicated([]*replRequest{a, b})
	if a.err == nil || b.err == nil || a.applied != nil || b.applied != nil {
		t.Fatalf("a failed commit answered a=%v/%d b=%v/%d", a.err, len(a.applied), b.err, len(b.applied))
	}
	if b.hwm != 1 {
		t.Fatalf("failed request reports hwm %d, want the stored 1", b.hwm)
	}
	if got := s.NextOffset("metrics"); got != 2 {
		t.Fatalf("a failed commit moved metrics next to %d", got)
	}
	if got := s.HWMGet("n-good", "metrics"); got != 1 {
		t.Fatalf("a failed commit moved the HWM to %d", got)
	}
}

// An unchanged incarnation is answered from memory, and a changed one still
// resets the child's marks.
func TestAdoptChildStoreRemembersTheIncarnation(t *testing.T) {
	s := mustOpen(t)
	if reset, err := s.AdoptChildStore("n-c", "inc-1"); err != nil || reset {
		t.Fatalf("first adopt: reset %v err %v", reset, err)
	}
	if _, _, err := s.ApplyReplicated("n-c", "metrics", []ReplRecord{replRec("n-c", 1)}); err != nil {
		t.Fatal(err)
	}
	if reset, err := s.AdoptChildStore("n-c", "inc-1"); err != nil || reset {
		t.Fatalf("same incarnation: reset %v err %v", reset, err)
	}
	if reset, err := s.AdoptChildStore("n-c", "inc-2"); err != nil || !reset {
		t.Fatalf("rebuilt store: reset %v err %v", reset, err)
	}
	if got := s.HWMGet("n-c", "metrics"); got != 0 {
		t.Fatalf("HWM after a rebuilt store = %d, want 0", got)
	}
}
