package store

import (
	"time"

	"github.com/cockroachdb/pebble/v2"
)

// Group commit for cursor acks.
//
// Every consumer acks its cursor after each page, and every ack used to be a
// synced commit of its own under cursorMu. Acks of different consumers queued
// behind each other's fsync: an edge on eMMC took about 20 acks a second in
// all, and a busy one fell behind. Concurrent acks now share one synced commit
// (see groupcommit.go). Each still returns only once its move is on disk, and
// a cursor still only moves forward: the requests of one group are decided in
// queue order against the positions the group itself is about to write.

type ackRequest struct {
	groupWait
	name, stream string
	off          uint64
	// marked also records the parent's mark (CursorAckMarked).
	marked bool
	parent string
	mark   uint64

	// Written by the leader that commits the request, read after wake.
	moved bool
	err   error
}

func (r *ackRequest) records() int { return 1 }

// ackApply commits an ack batch. Tests replace it to hold a commit open.
func (s *Store) ackApply(b *pebble.Batch, o *pebble.WriteOptions) error {
	if s.ackApplyHook != nil {
		return s.ackApplyHook(b, o)
	}
	return s.db.Apply(b, o)
}

// ack runs one request through the ack queue.
func (s *Store) ack(req *ackRequest) {
	req.groupWait = newGroupWait()
	s.ackQueue.run(req, s.commitAcks)
}

// commitAcks writes group as one synced batch and fills in every request's
// result: a failed commit fails every request in it and moves nothing.
func (s *Store) commitAcks(group []*ackRequest) {
	s.cursorMu.Lock()
	defer s.cursorMu.Unlock()
	b := s.db.NewBatch()
	defer b.Close()
	positions := map[string]uint64{}            // cursor key -> position this group writes
	marks := map[string]uint64{}                // acked key -> mark this group writes
	now := be64(uint64(time.Now().UnixMilli())) //nolint:gosec // a positive timestamp
	anyMoved := false
	for _, r := range group {
		key := cursorKey(r.name, r.stream)
		cur, ok := positions[string(key)]
		if !ok {
			cur = s.readU64(key, 1)
		}
		if r.off > cur {
			positions[string(key)] = r.off
			if r.err = b.Set(key, be64(r.off), nil); r.err == nil {
				r.err = b.Set(ctKey(r.name, r.stream), now, nil)
			}
			r.moved = r.err == nil
		}
		if r.marked && r.err == nil {
			ak := ackedKey(r.parent, r.stream)
			prev, ok := marks[string(ak)]
			if !ok {
				prev = s.readU64(ak, 0)
			}
			if r.mark != prev {
				marks[string(ak)] = r.mark
				r.err = b.Set(ak, be64(r.mark), nil)
			}
		}
		if r.err != nil {
			// Part of r may be in b: nothing of the group is committed.
			for _, other := range group {
				other.moved, other.err = false, r.err
			}
			return
		}
		anyMoved = anyMoved || r.moved
	}
	if b.Empty() {
		return
	}
	if err := s.ackApply(b, pebble.Sync); err != nil {
		for _, r := range group {
			r.moved, r.err = false, err
		}
		return
	}
	if anyMoved {
		s.signalBacklogChange()
	}
}
