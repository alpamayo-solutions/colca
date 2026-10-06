package store

import (
	"fmt"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/pebble/v2"
)

// Group commit for replicated batches.
//
// A parent receives one /replicate request per child batch, from many children
// at once. Each used to be its own synced Pebble commit under s.mu, so the
// parent's commit rate was bounded by one fsync per child batch, and every other
// user of s.mu (downlink waiters, cursor acks, reads, /metrics) queued behind
// that chain of fsyncs. With 100 children it saturated: the door waited on the
// disk while the CPU idled.
//
// Concurrent calls now queue up. One caller at a time leads: it takes s.mu,
// applies every queued request, in arrival order, in one batch, syncs it once,
// and publishes the new positions. A request is answered only after the commit
// that holds it is durable, and records keep the order their requests had in
// the queue, so each child's acknowledgement still means "on disk, in order".
// The leader then hands leadership to the oldest request that arrived meanwhile,
// so no caller keeps working for others indefinitely.

// maxGroupRecords bounds one group commit. Without a bound a leader took every
// queued request: when children pushed faster than the store committed, a
// parent with 1000 children built one batch of tens of thousands of records,
// held it with the queue behind it and its memtables, and reached its memory
// ceiling (fleet scale benchmark, 2026-10). Larger groups also gain nothing: a
// few thousand records already amortise the sync. A request is never split,
// and the oldest request is always taken.
const maxGroupRecords = 4096

// takeGroup removes the next group from the front of pending: the oldest
// request and those after it up to maxGroupRecords records.
func takeGroup(pending []*replRequest) (group, rest []*replRequest) {
	n, records := 0, 0
	for n < len(pending) && (n == 0 || records+len(pending[n].recs) <= maxGroupRecords) {
		records += len(pending[n].recs)
		n++
	}
	return pending[:n:n], pending[n:]
}

type replRequest struct {
	child, stream string
	recs          []ReplRecord

	// Written by the leader that commits the request, read after wake.
	finished bool
	applied  []ReplRecord
	hwm      uint64
	err      error
	wake     chan struct{}
}

type replQueue struct {
	mu      sync.Mutex
	pending []*replRequest
	leading bool
	// records counts the records of requests queued or being committed.
	records atomic.Int64
}

// ReplicatedBacklog reports how many replicated records wait for, or are in,
// a group commit. A parent's door refuses new pushes (429) while it is high:
// they would only wait in memory behind the ones already queued.
func (s *Store) ReplicatedBacklog() int64 { return s.replQueue.records.Load() }

// ApplyReplicated appends records with ChildOffset > HWM(child, stream) under
// local offsets and updates KV and the HWM in one atomic batch, so replays are
// harmless. It returns newly applied records and skip ranges, or nil on error.
// The caller mirrors only data records onto the local MQTT bus.
//
// Concurrent calls share one synced commit (see the comment above); each
// returns only once its records are durable.
func (s *Store) ApplyReplicated(child, stream string, recs []ReplRecord) (applied []ReplRecord, hwm uint64, err error) {
	req := &replRequest{child: child, stream: stream, recs: recs, wake: make(chan struct{}, 1)}
	q := &s.replQueue
	q.records.Add(int64(len(recs)))
	defer q.records.Add(-int64(len(recs)))
	q.mu.Lock()
	q.pending = append(q.pending, req)
	lead := !q.leading
	q.leading = true
	q.mu.Unlock()
	if !lead {
		<-req.wake
		if req.finished {
			return req.applied, req.hwm, req.err
		}
		// Handed leadership: req is the oldest pending request.
	}

	q.mu.Lock()
	group, rest := takeGroup(q.pending)
	q.pending = rest
	q.mu.Unlock()

	s.commitReplicated(group)

	q.mu.Lock()
	if len(q.pending) > 0 {
		q.pending[0].wake <- struct{}{} // still leading: the next leader takes over
	} else {
		q.leading = false
	}
	q.mu.Unlock()
	for _, r := range group {
		if r != req {
			r.wake <- struct{}{}
		}
	}
	return req.applied, req.hwm, req.err
}

type replHWMKey struct{ child, stream string }

// commitReplicated applies group in order as one synced batch and fills in every
// request's result. A request that cannot be built fails alone; a commit that
// fails fails every request in it and changes nothing in memory.
func (s *Store) commitReplicated(group []*replRequest) {
	defer func() {
		for _, r := range group {
			r.finished = true
		}
	}()
	s.mu.Lock()
	defer s.mu.Unlock()

	next := map[string]uint64{}
	liveBytes := map[string]uint64{}
	hwms := map[replHWMKey]uint64{}
	var grown []string // streams with a committed request, in first-seen order
	b := s.db.NewBatch()
	defer b.Close()
	var committed []*replRequest
	for _, r := range group {
		if _, ok := next[r.stream]; !ok {
			off := s.next[r.stream]
			if off == 0 {
				r.hwm, r.err = s.HWMGet(r.child, r.stream), fmt.Errorf("unknown stream %q", r.stream)
				continue
			}
			next[r.stream], liveBytes[r.stream] = off, s.bytes[r.stream]
		}
		key := replHWMKey{r.child, r.stream}
		prev, ok := hwms[key]
		if !ok {
			prev = s.HWMGet(r.child, r.stream)
		}
		applied, hwm, off, added, err := s.buildReplicated(b, r, prev, next[r.stream])
		if err != nil {
			r.hwm, r.err = prev, err
			continue
		}
		r.hwm = hwm
		if len(applied) == 0 {
			continue
		}
		r.applied = applied
		if !slices.Contains(grown, r.stream) {
			grown = append(grown, r.stream)
		}
		next[r.stream], liveBytes[r.stream] = off, liveBytes[r.stream]+added
		hwms[key] = hwm
		committed = append(committed, r)
	}
	if len(committed) == 0 {
		return
	}
	// Readers see a commit's records, KV and new head together: the state lock
	// is held from the apply until the head is published. It is not held while
	// the group is built, and cursor moves never take it.
	s.state.Lock()
	defer s.state.Unlock()
	err := func() error {
		for stream, off := range next {
			if off == s.next[stream] {
				continue
			}
			if err := b.Set(metaKey(stream), be64(off), nil); err != nil {
				return err
			}
			if err := b.Set(bytesKey(stream), be64(liveBytes[stream]), nil); err != nil {
				return err
			}
		}
		for key, hwm := range hwms {
			if err := b.Set(hwmKey(key.child, key.stream), be64(hwm), nil); err != nil {
				return err
			}
		}
		return s.appendApply(b, pebble.Sync)
	}()
	if err != nil {
		for _, r := range committed {
			r.applied, r.err = nil, err
			r.hwm = s.HWMGet(r.child, r.stream)
		}
		return
	}
	s.hwmsCommitted(hwms)
	for stream, off := range next {
		s.next[stream] = off
		s.bytes[stream] = liveBytes[stream]
	}
	for _, r := range committed {
		for _, record := range r.applied {
			if record.SkipFrom == 0 {
				s.noteContractLocked(r.stream, record.Topic)
			}
		}
	}
	for _, stream := range grown {
		s.streamGrewLocked(stream)
	}
}

// buildReplicated adds r's records above prev to b, starting at offset off. It
// builds into a batch of its own first, so a request that fails leaves nothing
// in b. It returns the applied records, the new HWM, the next offset and the
// bytes added.
func (s *Store) buildReplicated(b *pebble.Batch, r *replRequest, prev, off uint64) (applied []ReplRecord, hwm, next, added uint64, err error) {
	own := s.db.NewBatch()
	defer own.Close()
	hwm = prev
	for _, rec := range r.recs {
		if rec.SkipFrom != 0 && (r.stream != "metrics" || rec.SkipFrom > rec.ChildOffset || rec.Topic != "" || len(rec.Payload) != 0) {
			return nil, prev, off, 0, fmt.Errorf("invalid metric skip range")
		}
		if rec.ChildOffset <= hwm {
			continue
		}
		if rec.SkipFrom != 0 {
			applied = append(applied, rec)
			hwm = rec.ChildOffset
			continue
		}
		if rec.OriginOffset == 0 {
			// Compatibility with a direct/legacy child: at the first hop its
			// child offset is the owner-authored coordinate.
			rec.OriginOffset = rec.ChildOffset
		}
		n, err := addRecord(own, r.stream, off, Record{
			Topic: rec.Topic, Payload: rec.Payload, TS: rec.TS,
			WrittenBy: rec.WrittenBy, ActorID: rec.ActorID,
			ActorLabel: rec.ActorLabel, ActorKind: rec.ActorKind, ActorGroups: rec.ActorGroups,
			OriginOffset: rec.OriginOffset,
			KVPath:       rec.KVPath, KVNode: rec.KVNode, Delete: rec.Delete,
		})
		if err != nil {
			return nil, prev, off, 0, err
		}
		added += n
		off++
		applied = append(applied, rec)
		hwm = rec.ChildOffset
	}
	if len(applied) == 0 {
		return nil, prev, off, 0, nil
	}
	if err := b.Apply(own, nil); err != nil {
		return nil, prev, off, 0, err
	}
	return applied, hwm, off, added, nil
}
