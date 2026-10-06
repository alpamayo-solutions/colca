package store

import (
	"bytes"
	"container/heap"
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble/v2"
)

// The signal index lets a reader that follows a few signals on a long metrics
// stream skip the records of every other signal without reading them.
//
// Every record that carries a signal id (a _Metric, see uns.MetricSignalID)
// also writes si/{stream}/{signal}/{offset}, empty, in the same batch. A
// filtered read merges the index ranges of the wanted signals in offset order
// and reads exactly those records, so its cost follows the records it returns,
// not the length of the stream it crosses.
//
// The entry lives and dies with its record, always in the same batch:
//   - Prune deletes [LWM, upTo) of every signal the doomed prefix carries, one
//     range per signal. The prefix is decoded for the prune journal anyway, so
//     the signals cost no extra read.
//   - Compaction and eviction delete the entry of each record they delete.
//
// A fetchLogs ack is filed under the reserved key uns.FetchLogsAckKey in the
// same way, so per-signal retention can remove those pages from the commands
// stream after hours instead of the stream's months.
//
// Replication needs nothing of its own: a replicated record is appended through
// the same addRecord on the receiving node, so the index is rebuilt there and
// never travels.
//
// The index covers a stream from sf/{stream} on. Records below it were written
// by a version without the index (or before a downgrade and upgrade), and a
// read that starts there scans them as before. That range only shrinks:
// retention prunes it away.

// sigKey identifies one record of one signal. The signal id is a ULID, so it
// never contains the separator.
func sigKey(stream, signalID string, off uint64) []byte {
	return append([]byte("si\x00"+stream+"\x00"+signalID+"\x00"), be64(off)...)
}

// sigFromKey holds the first offset of a stream the signal index covers.
func sigFromKey(stream string) []byte { return []byte("sf\x00" + stream) }

// sigIndexCleanKey records, at a clean Close, every stream's next offset and
// LWM. If both still match at the next Open, nothing appended or pruned in
// between without maintaining the index, and the coverage in sf/ stands. An
// older version does not know this key, so a run of it leaves either no key
// (Open deletes it) or one that no longer matches; then the coverage restarts
// at the head. A crash also leaves no key, and is treated the same way:
// conservative, since only the read path's speed depends on it.
var sigIndexCleanKey = []byte("sfc\x00")

// sigIndexCleanValue is every stream's next offset and LWM in stream order.
func (s *Store) sigIndexCleanValue() []byte {
	var v []byte
	for _, stream := range streams {
		v = append(v, be64(s.next[stream])...)
		v = append(v, be64(s.lwm[stream])...)
	}
	return v
}

// openSigIndex runs at Open, after the counters are read: it keeps the index's
// coverage if the last Close left it known to be complete, and otherwise
// restarts it at each stream's head. Either way it clears the clean mark for
// this run.
func (s *Store) openSigIndex() error {
	trusted := false
	clean, closer, err := s.db.Get(sigIndexCleanKey)
	switch {
	case err == nil:
		trusted = bytes.Equal(clean, s.sigIndexCleanValue())
		closer.Close()
	case !errors.Is(err, pebble.ErrNotFound):
		return err
	}
	b := s.db.NewBatch()
	defer b.Close()
	for _, stream := range streams {
		from := s.next[stream]
		if trusted {
			if from, err = readCounter(s.db, sigFromKey(stream), s.next[stream], "signal index start", stream); err != nil {
				return err
			}
		}
		if err := b.Set(sigFromKey(stream), be64(from), nil); err != nil {
			return err
		}
		s.sigFrom[stream] = from
	}
	if err := b.Delete(sigIndexCleanKey, nil); err != nil {
		return err
	}
	return b.Commit(pebble.Sync)
}

// ReadSignals is ReadRecordsBounded for a reader that wants only the records of
// the given signals: every record it returns carries one of them, and filter
// decides on the rest of its view (grants, topics). It reads through the signal
// index, so the records of other signals cost nothing; maxScan bounds the index
// entries visited.
//
// next is the offset after the last record visited, or the stream head when
// none of the signals has a record before it, so a reader of rare signals
// reaches the head in one call. A read that starts below the index's coverage
// is ReadRecordsBounded.
func (s *Store) ReadSignals(ctx context.Context, stream string, from uint64, limit, maxScan int, signals []string, filter func(StoredRecord) bool) (out []StoredRecord, next uint64, err error) {
	s.state.RLock()
	covered, head := s.sigFrom[stream], s.next[stream]
	// Appends publish head after they commit, so this snapshot holds every
	// record below head and the index entries written with them.
	snap := s.db.NewSnapshot()
	s.state.RUnlock()
	defer func() { _ = snap.Close() }()
	if from < covered {
		return s.ReadRecordsBounded(ctx, stream, from, limit, maxScan, filter)
	}
	if from >= head {
		return nil, from, nil
	}

	var cursors offsetHeap
	defer func() {
		for _, c := range cursors.all {
			_ = c.iter.Close()
		}
	}()
	seen := make(map[string]bool, len(signals))
	for _, signalID := range signals {
		if seen[signalID] {
			continue
		}
		seen[signalID] = true
		iter, err := snap.NewIter(&pebble.IterOptions{
			LowerBound: sigKey(stream, signalID, from),
			UpperBound: sigKey(stream, signalID, head),
		})
		if err != nil {
			return nil, from, err
		}
		c := &sigCursor{iter: iter}
		cursors.all = append(cursors.all, c)
		if iter.First() {
			c.off, _ = offsetOf(iter.Key())
			cursors.live = append(cursors.live, c)
		} else if err := iter.Error(); err != nil {
			return nil, from, err
		}
	}
	heap.Init(&cursors)

	next = from
	scanned := 0
	for cursors.Len() > 0 && len(out) < limit {
		if maxScan > 0 && scanned >= maxScan {
			return out, next, nil
		}
		scanned++
		if scanned%1024 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, next, err
			}
		}
		c := cursors.live[0]
		off := c.off
		if c.iter.Next() {
			c.off, _ = offsetOf(c.iter.Key())
			heap.Fix(&cursors, 0)
		} else {
			if err := c.iter.Error(); err != nil {
				return nil, next, err
			}
			heap.Pop(&cursors)
		}
		next = off + 1
		val, closer, err := snap.Get(streamKey(stream, off))
		if errors.Is(err, pebble.ErrNotFound) {
			continue // an entry its record outlived; nothing to return
		}
		if err != nil {
			return nil, next, err
		}
		var e recEnc
		err = json.Unmarshal(val, &e)
		closer.Close()
		if err != nil {
			return nil, next, fmt.Errorf("decode record %d of %q: %w", off, stream, err)
		}
		record := storedRecord(off, e)
		if filter != nil && !filter(record) {
			continue
		}
		out = append(out, record)
	}
	if cursors.Len() == 0 {
		// No wanted record remains before the head.
		next = head
	}
	return out, next, nil
}

// sigCursor is one signal's position in a merged index read.
type sigCursor struct {
	iter *pebble.Iterator
	off  uint64
}

// offsetHeap orders the live cursors by their next offset. all keeps every
// iterator opened, so each is closed exactly once however the read ends.
type offsetHeap struct {
	live []*sigCursor
	all  []*sigCursor
}

func (h *offsetHeap) Len() int           { return len(h.live) }
func (h *offsetHeap) Less(i, j int) bool { return h.live[i].off < h.live[j].off }
func (h *offsetHeap) Swap(i, j int)      { h.live[i], h.live[j] = h.live[j], h.live[i] }
func (h *offsetHeap) Push(x any)         { h.live = append(h.live, x.(*sigCursor)) }
func (h *offsetHeap) Pop() any {
	last := h.live[len(h.live)-1]
	h.live = h.live[:len(h.live)-1]
	return last
}
