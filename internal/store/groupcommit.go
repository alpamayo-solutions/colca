package store

import (
	"sync"
	"sync/atomic"
)

// Group commit.
//
// Concurrent writers queue up. One caller at a time leads: it takes s.mu,
// writes every queued request, in arrival order, in one batch, syncs it once,
// and publishes the new positions. A request is answered only after the commit
// that holds it is durable, and records keep the order their requests had in
// the queue. The leader then hands leadership to the oldest request that
// arrived meanwhile, so no caller keeps working for others indefinitely.

// maxGroupRecords bounds one group commit. Without a bound a leader took every
// queued request: when children pushed faster than the store committed, a
// parent with 1000 children built one batch of tens of thousands of records,
// held it with the queue behind it and its memtables, and reached its memory
// ceiling (fleet scale benchmark, 2026-10). Larger groups also gain nothing: a
// few thousand records already amortise the sync. A request is never split,
// and the oldest request is always taken.
const maxGroupRecords = 4096

// groupMember is one request in a groupQueue.
type groupMember interface {
	records() int
	waiter() *groupWait
}

// groupWait is the part of a request its leader writes: finished is set, after
// the commit that holds the request, before wake is signalled.
type groupWait struct {
	finished bool
	wake     chan struct{}
}

func newGroupWait() groupWait { return groupWait{wake: make(chan struct{}, 1)} }

func (w *groupWait) waiter() *groupWait { return w }

type groupQueue[R groupMember] struct {
	mu      sync.Mutex
	pending []R
	leading bool
	// queued counts the records of requests queued or being committed.
	queued atomic.Int64
}

// run queues req and returns once a commit holding it has finished: commit
// writes a group's results into its requests. The caller reads req's results.
func (q *groupQueue[R]) run(req R, commit func(group []R)) {
	n := int64(req.records())
	q.queued.Add(n)
	defer q.queued.Add(-n)
	w := req.waiter()
	q.mu.Lock()
	q.pending = append(q.pending, req)
	lead := !q.leading
	q.leading = true
	q.mu.Unlock()
	if !lead {
		<-w.wake
		if w.finished {
			return
		}
		// Handed leadership: req is the oldest pending request.
	}

	q.mu.Lock()
	group, rest := takeGroup(q.pending)
	q.pending = rest
	q.mu.Unlock()

	commit(group)
	for _, r := range group {
		r.waiter().finished = true
	}

	q.mu.Lock()
	if len(q.pending) > 0 {
		q.pending[0].waiter().wake <- struct{}{} // still leading: the next leader takes over
	} else {
		q.leading = false
	}
	q.mu.Unlock()
	for _, r := range group {
		if rw := r.waiter(); rw != w {
			rw.wake <- struct{}{}
		}
	}
}

// takeGroup removes the next group from the front of pending: the oldest
// request and those after it up to maxGroupRecords records.
func takeGroup[R groupMember](pending []R) (group, rest []R) {
	n, records := 0, 0
	for n < len(pending) && (n == 0 || records+pending[n].records() <= maxGroupRecords) {
		records += pending[n].records()
		n++
	}
	return pending[:n:n], pending[n:]
}
