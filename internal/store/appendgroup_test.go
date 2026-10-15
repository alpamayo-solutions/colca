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

func writerRec(writer string, seq int) Record {
	return Record{
		Topic:   fmt.Sprintf("colca/v1/_Metric/n1/%s/s", writer),
		Payload: []byte(fmt.Sprintf(`{"v":%d,"w":%q}`, seq, writer)),
		TS:      int64(seq),
		KVPath:  writer + "/s", KVNode: "n1",
	}
}

// Many writers append at once, each its records in order, as MQTT clients
// publishing to one node do. Every record lands exactly once, under gapless
// offsets, each writer's records in its own order, and each call's offsets are
// the ones its records were stored under.
func TestConcurrentAppendsLandOnceInOrder(t *testing.T) {
	s := mustOpen(t)
	const writers, appends = 32, 25
	var wg sync.WaitGroup
	for w := range writers {
		wg.Go(func() {
			writer := fmt.Sprintf("w%02d", w)
			seq := 0
			for a := range appends {
				n := 1 + a%3 // one to three records per call
				recs := make([]Record, n)
				for i := range recs {
					seq++
					recs[i] = writerRec(writer, seq)
				}
				first, last, err := s.Append("metrics", recs)
				if err != nil || last-first+1 != uint64(n) {
					t.Errorf("%s append %d: [%d,%d] %v", writer, a, first, last, err)
					return
				}
				stored, _, err := s.Read("metrics", first, n, nil)
				if err != nil || len(stored) != n {
					t.Errorf("%s append %d: read back %d records, %v", writer, a, len(stored), err)
					return
				}
				for i, r := range stored {
					if r.Offset != first+uint64(i) || string(r.Payload) != string(recs[i].Payload) {
						t.Errorf("%s append %d: offset %d holds %s", writer, a, r.Offset, r.Payload)
						return
					}
				}
			}
		})
	}
	wg.Wait()

	total := 0
	for a := range appends {
		total += writers * (1 + a%3)
	}
	if got := s.NextOffset("metrics"); got != uint64(total+1) {
		t.Fatalf("metrics next = %d, want %d: a record was lost or written twice", got, total+1)
	}
	recs, _, err := s.Read("metrics", 1, total+10, nil)
	if err != nil {
		t.Fatal(err)
	}
	last := map[string]int{}
	for i, r := range recs {
		if r.Offset != uint64(i+1) {
			t.Fatalf("record %d has offset %d: offsets are not gapless", i, r.Offset)
		}
		var p struct {
			V int    `json:"v"`
			W string `json:"w"`
		}
		if err := json.Unmarshal(r.Payload, &p); err != nil {
			t.Fatal(err)
		}
		if p.V != last[p.W]+1 {
			t.Fatalf("%s: record %d follows %d: a writer's order was not kept", p.W, p.V, last[p.W])
		}
		last[p.W] = p.V
	}
}

// While one commit syncs, the appends that arrive are committed together in
// the next one. None of them returns, and no reader sees their offsets, before
// that commit is done.
func TestAppendsArrivingDuringACommitShareTheNext(t *testing.T) {
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
		_, _, err := s.Append("metrics", []Record{writerRec("first", 1)})
		first <- err
	}()
	<-inFirst
	const waiting = 16
	results := make(chan error, waiting)
	for i := range waiting {
		go func() {
			_, _, err := s.Append("metrics", []Record{writerRec(fmt.Sprintf("w%02d", i), 1)})
			results <- err
		}()
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		s.appendQueue.mu.Lock()
		n := len(s.appendQueue.pending)
		s.appendQueue.mu.Unlock()
		if n == waiting {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d of %d appends queued", n, waiting)
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case err := <-results:
		t.Fatalf("an append returned (%v) before any commit holding it finished", err)
	case err := <-first:
		t.Fatalf("the first append returned (%v) before its commit finished", err)
	default:
	}
	if got := s.appendQueue.queued.Load(); got != waiting+1 {
		t.Fatalf("%d records queued, want %d", got, waiting+1)
	}
	close(release)
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	if got := s.NextOffset("metrics"); got < 2 {
		t.Fatalf("metrics next = %d after the first commit", got)
	}
	for range waiting {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	if got := applies.Load(); got != 2 {
		t.Fatalf("%d commits for %d appends, want 2: the waiting appends were not grouped", got, waiting+1)
	}
	if got := s.NextOffset("metrics"); got != waiting+2 {
		t.Fatalf("metrics next = %d, want %d", got, waiting+2)
	}
}

// An append that fails its checks fails alone; the others in its commit land.
// A commit that fails fails every append in it and moves nothing.
func TestAFailedAppendDoesNotTakeItsGroupDown(t *testing.T) {
	s := mustOpen(t)
	s.SetMaxRecordBytes(64)
	req := func(stream string, rec Record) *appendRequest {
		return &appendRequest{stream: stream, recs: []Record{rec}}
	}
	tooLarge := writerRec("big", 1)
	tooLarge.Payload = make([]byte, 65)
	bad := req("metrics", tooLarge)
	good := req("metrics", writerRec("good", 1))
	unknown := req("nope", writerRec("good", 2))
	s.mu.Lock()
	s.commitAppendBatch([]*appendRequest{bad, good, unknown})
	s.mu.Unlock()
	if !errors.Is(bad.err, ErrRecordTooLarge) || unknown.err == nil {
		t.Fatalf("too large: %v, unknown stream: %v: both should fail", bad.err, unknown.err)
	}
	if good.err != nil || good.first != 1 || good.last != 1 {
		t.Fatalf("good append: [%d,%d] %v", good.first, good.last, good.err)
	}
	if got := s.NextOffset("metrics"); got != 2 {
		t.Fatalf("metrics next = %d, want 2", got)
	}

	s.appendApply = func(*pebble.Batch, *pebble.WriteOptions) error { return errors.New("disk unavailable") }
	a := req("metrics", writerRec("a", 1))
	b := req("entities", Record{Topic: "colca/v1/_SystemElement/n1/b", Payload: []byte(`{}`), KVPath: "b", KVNode: "n1"})
	s.mu.Lock()
	s.commitAppendBatch([]*appendRequest{a, b})
	s.mu.Unlock()
	if a.err == nil || b.err == nil || a.first != 0 || b.first != 0 {
		t.Fatalf("a failed commit answered a=[%d] %v b=[%d] %v", a.first, a.err, b.first, b.err)
	}
	if got := s.NextOffset("metrics"); got != 2 {
		t.Fatalf("a failed commit moved metrics next to %d", got)
	}
	if got := s.NextOffset("entities"); got != 1 {
		t.Fatalf("a failed commit moved entities next to %d", got)
	}
	if _, ok, _ := s.KVGet("b", "n1", "colca/v1/_SystemElement/n1/b"); ok {
		t.Fatal("a failed commit left a KV entry")
	}
}

// A metric queued behind a signal sees that signal's replication policy, as it
// would if each append were its own commit: the group is split in two batches.
func TestAMetricQueuedBehindItsSignalSeesThePolicy(t *testing.T) {
	s := mustOpen(t)
	var applies atomic.Int32
	apply := s.appendApply
	s.appendApply = func(b *pebble.Batch, o *pebble.WriteOptions) error {
		applies.Add(1)
		return apply(b, o)
	}
	signal := &appendRequest{groupWait: newGroupWait(), stream: "entities", recs: []Record{{
		Topic: "colca/v1/_Signal/n1/temp", Payload: []byte(`{"id":"s1","replication_policy":"source_local_only"}`),
		KVPath: "temp", KVNode: "n1",
	}}}
	metric := &appendRequest{groupWait: newGroupWait(), stream: "metrics", recs: []Record{{
		Topic: "colca/v1/_Metric/n1/temp", Payload: []byte(`{"signal_id":"s1","value":42}`),
		KVPath: "temp", KVNode: "n1",
	}}}
	other := &appendRequest{groupWait: newGroupWait(), stream: "metrics", recs: []Record{writerRec("w", 1)}}
	s.commitAppends([]*appendRequest{signal, metric, other})
	for _, r := range []*appendRequest{signal, metric, other} {
		if r.err != nil {
			t.Fatal(r.err)
		}
	}
	if got := applies.Load(); got != 2 {
		t.Fatalf("%d commits, want 2: the signal and the metric after it", got)
	}
	recs, _, err := s.Read("metrics", 1, 10, nil)
	if err != nil || len(recs) != 2 {
		t.Fatalf("read %d metrics, %v", len(recs), err)
	}
	if !recs[0].SourceLocalOnly || recs[1].SourceLocalOnly {
		t.Fatalf("source_local_only: %v, %v; want true for the signal's metric only", recs[0].SourceLocalOnly, recs[1].SourceLocalOnly)
	}
}
