package store

import (
	"fmt"
	"slices"

	"github.com/cockroachdb/pebble/v2"

	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// Group commit for local appends.
//
// MQTT clients and services publish one record at a time, each from its own
// goroutine, and every publish is answered only after its record is durable.
// Each Append used to be its own synced commit under s.mu, so a node took one
// fsync per publish, one after the other: on a slow disk or a starved CPU every
// client queued behind every other client's sync, long enough for MQTT
// connections to miss their write deadlines. Concurrent Append calls now share
// one synced commit (see groupcommit.go). Records keep the order of their
// requests in the queue, offsets stay gapless, and each call still returns only
// once its records are on disk.

type appendRequest struct {
	groupWait
	stream string
	recs   []Record

	// Written by the leader that commits the request, read after wake.
	first, last uint64
	err         error
}

func (r *appendRequest) records() int { return len(r.recs) }

// commitAppends commits group in order, as one synced batch unless a metric
// follows a signal in it. A metric's replication policy is read from its
// signal's committed KV entry, so a metric request after a request that writes
// a _Signal goes into the next batch: it sees that signal as it would have if
// each request were its own commit.
func (s *Store) commitAppends(group []*appendRequest) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for len(group) > 0 {
		n, signal := 0, false
		for n < len(group) && (!signal || group[n].stream != "metrics") {
			signal = signal || writesSignal(group[n].recs)
			n++
		}
		s.commitAppendBatch(group[:n])
		group = group[n:]
	}
}

func writesSignal(recs []Record) bool {
	for _, rec := range recs {
		if rec.KVPath != "" && uns.DecidesReplication(rec.Topic) {
			return true
		}
	}
	return false
}

// commitAppendBatch writes group in order as one synced batch and fills in
// every request's result. A request that fails its checks (unknown stream,
// record too large, invalid replication policy) fails alone; a commit that
// fails fails every request in it and changes nothing in memory. The caller
// holds s.mu.
func (s *Store) commitAppendBatch(group []*appendRequest) {
	next := map[string]uint64{}
	liveBytes := map[string]uint64{}
	var grown []string // streams with a committed request, in first-seen order
	b := s.db.NewBatch()
	defer b.Close()
	var committed []*appendRequest
	failAll := func(err error) {
		for _, r := range group {
			r.first, r.last, r.err = 0, 0, err
		}
	}
	for _, r := range group {
		if _, ok := next[r.stream]; !ok {
			off := s.next[r.stream]
			if off == 0 {
				r.err = fmt.Errorf("unknown stream %q", r.stream)
				continue
			}
			next[r.stream], liveBytes[r.stream] = off, s.bytes[r.stream]
		}
		localOnly, err := s.checkAppend(r.stream, r.recs)
		if err != nil {
			r.err = err
			continue
		}
		first := next[r.stream]
		off, added, err := writeAppend(b, r.stream, r.recs, localOnly, first)
		if err != nil {
			// Part of r may be in b: nothing of the group is committed.
			failAll(err)
			return
		}
		r.first, r.last = first, off-1
		next[r.stream], liveBytes[r.stream] = off, liveBytes[r.stream]+added
		if !slices.Contains(grown, r.stream) {
			grown = append(grown, r.stream)
		}
		committed = append(committed, r)
	}
	if len(committed) == 0 {
		return
	}
	// Readers see the records once the head is published: see commitReplicated.
	err := func() error {
		for _, stream := range grown {
			if err := setHead(b, stream, next[stream]); err != nil {
				return err
			}
			if err := b.Set(bytesKey(stream), be64(liveBytes[stream]), nil); err != nil {
				return err
			}
		}
		return s.appendApply(b, pebble.Sync)
	}()
	if err != nil {
		for _, r := range committed {
			r.first, r.last, r.err = 0, 0, err
		}
		return
	}
	s.state.Lock()
	defer s.state.Unlock()
	for _, stream := range grown {
		s.next[stream] = next[stream]
		s.bytes[stream] = liveBytes[stream]
	}
	for _, r := range committed {
		s.noteContractsLocked(r.stream, r.recs)
	}
	for _, stream := range grown {
		s.streamGrewLocked(stream)
	}
}
