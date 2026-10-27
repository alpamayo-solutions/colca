package store

import (
	"bytes"
	"container/heap"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"

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
//   - Prune deletes the entry of each record of the doomed prefix. The prefix
//     is decoded for the retention policy and the prune journal anyway, so the
//     entries cost no extra read.
//   - Compaction, eviction and per-signal retention delete the entry of each
//     record they delete.
//
// Prune used to delete [LWM, upTo) of every signal the prefix carried as one
// range per signal. That is one range tombstone per signal and prune, and they
// are slow to leave: Pebble drops one only when it reaches the last level. A
// parent pruning 450 signals in batches of 100k records had 361,000 of them in
// its tables, megabytes of range deletion block per table, which every iterator
// on the index has to load whole. A point tombstone lies in the data blocks
// beside the entry it deletes, below the low-water mark where no read seeks,
// and goes with it in the next compaction. The stream's own records still go as
// one range per prune.
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
// read that starts there scans them, within its budget, before it switches to
// the index. That range only shrinks: retention prunes it away.
//
// Coverage survives every restart, a crash included: each batch that appends
// records writes their index entries, the new head (m/) and ix/{stream}, the
// head the index is complete through, together. At Open ix/ equal to m/ says
// that every record was indexed when it was written. A version that appends
// without the index moves m/ and not ix/; then the coverage of that stream
// restarts at its head. See openIndexes.

// sigKey identifies one record of one signal. The signal id is a ULID, so it
// never contains the separator.
func sigKey(stream, signalID string, off uint64) []byte {
	return append([]byte("si\x00"+stream+"\x00"+signalID+"\x00"), be64(off)...)
}

// sigFromKey holds the first offset of a stream the signal index covers.
func sigFromKey(stream string) []byte { return []byte("sf\x00" + stream) }

// indexedKey holds the head through which every record of the stream was
// written with its index entries: the signal index and the KV contract index.
// It is written with m/{stream} in the same batch (see setHead).
func indexedKey(stream string) []byte { return []byte("ix\x00" + stream) }

// setHead adds a stream's new head to b, together with the mark that the
// records below it were written with their index entries. Every batch that
// appends records sets the head through it.
func setHead(b *pebble.Batch, stream string, next uint64) error {
	if err := b.Set(metaKey(stream), be64(next), nil); err != nil {
		return err
	}
	return b.Set(indexedKey(stream), be64(next), nil)
}

// legacySigCleanKey and legacyKVCleanKey are the marks versions before
// indexedKey wrote at a clean Close: every stream's next offset and LWM, and
// every stream's next offset. Open still honours them for a store such a
// version wrote last, so the upgrade does not cost the coverage, and deletes
// them.
var (
	legacySigCleanKey = []byte("sfc\x00")
	legacyKVCleanKey  = []byte("xc\x00")
)

// legacyClean reports whether key holds the value a clean Close of a version
// before indexedKey would have written for the current counters.
func (s *Store) legacyClean(key []byte, withLWM bool) (bool, error) {
	v, closer, err := s.db.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	defer closer.Close()
	var want []byte
	for _, stream := range streams {
		want = append(want, be64(s.next[stream])...)
		if withLWM {
			want = append(want, be64(s.lwm[stream])...)
		}
	}
	return bytes.Equal(v, want), nil
}

// indexTrust says, per stream, whether every record below the head was written
// with its index entries. ix/{stream} decides when present. A stream without it
// was either never written (nothing to index) or last written by a version
// before indexedKey, whose clean-close mark (legacy) then decides.
func (s *Store) indexTrust(legacy []byte, withLWM bool) (map[string]bool, error) {
	legacyOK, err := s.legacyClean(legacy, withLWM)
	if err != nil {
		return nil, err
	}
	trust := make(map[string]bool, len(streams))
	for _, stream := range streams {
		through, err := readCounter(s.db, indexedKey(stream), 0, "indexed head", stream)
		if err != nil {
			return nil, err
		}
		switch {
		case through != 0:
			trust[stream] = through == s.next[stream]
		case s.next[stream] == 1:
			trust[stream] = true // never written
		default:
			trust[stream] = legacyOK
		}
	}
	return trust, nil
}

// openIndexes runs at Open, after the counters are read. It reconciles the KV
// contract index unless every stream was indexed through its head, keeps each
// stream's signal index coverage where that holds and restarts it at the head
// where it does not, and then marks every stream indexed through its head. A
// crash during the reconcile leaves the marks as they were, so the next Open
// resumes it.
func (s *Store) openIndexes() error {
	kvTrust, err := s.indexTrust(legacyKVCleanKey, false)
	if err != nil {
		return err
	}
	sigTrust, err := s.indexTrust(legacySigCleanKey, true)
	if err != nil {
		return err
	}
	kvTrusted := true
	for _, ok := range kvTrust {
		kvTrusted = kvTrusted && ok
	}
	if !kvTrusted {
		added, removed, err := s.reconcileKVIndex()
		if err != nil {
			return fmt.Errorf("reconcile the KV contract index: %w", err)
		}
		slog.Info("store: KV contract index reconciled", "added", added, "removed", removed)
	}
	b := s.db.NewBatch()
	defer b.Close()
	for _, stream := range streams {
		from := s.next[stream]
		if sigTrust[stream] {
			if from, err = readCounter(s.db, sigFromKey(stream), s.next[stream], "signal index start", stream); err != nil {
				return err
			}
		} else {
			slog.Info("store: signal index coverage restarts at the head: records were written without it",
				"stream", stream, "head", from)
		}
		if err := b.Set(sigFromKey(stream), be64(from), nil); err != nil {
			return err
		}
		if err := b.Set(indexedKey(stream), be64(s.next[stream]), nil); err != nil {
			return err
		}
		s.sigFrom[stream] = from
	}
	if err := b.Delete(legacySigCleanKey, nil); err != nil {
		return err
	}
	if err := b.Delete(legacyKVCleanKey, nil); err != nil {
		return err
	}
	return b.Commit(pebble.Sync)
}

// ReadSignals is ReadRecordsBounded for a reader that wants only the records of
// the given signals: every record it returns carries one of them, and keepHead
// and filter decide on the rest of its view (grants, topics; see HeadFilter).
// It reads through the signal index, so the records of other signals cost
// nothing; maxScan bounds the index entries visited and maxBytes the stored
// bytes returned (see pageBytes).
//
// next is the offset after the last record visited, or the stream head when
// none of the signals has a record before it, so a reader of rare signals
// reaches the head in one call. A read that starts below the index's coverage
// scans the records below it like ReadRecordsBounded, within the same limit and
// budgets, and goes on through the index from the coverage on.
//
// The read walks the signals one after the other with a single iterator and
// keeps only the smallest offsets it may still visit. It used to hold one open
// iterator per signal and merge them. Every open iterator pins the range
// deletion block of each table it stands in, and a block the cache cannot keep
// is allocated once per iterator: a read of some hundred signals on a store
// whose index tables carried megabytes of range tombstones held gigabytes for
// as long as it ran. Now one read pins the blocks of one position, whatever the
// number of signals.
func (s *Store) ReadSignals(ctx context.Context, stream string, from uint64, limit, maxScan int, maxBytes uint64, signals []string, keepHead HeadFilter, filter func(StoredRecord) bool) (out []StoredRecord, next uint64, err error) {
	s.state.RLock()
	covered, head, lwm := s.sigFrom[stream], s.next[stream], s.lwm[stream]
	// Appends publish head after they commit, so this snapshot holds every
	// record below head and the index entries written with them.
	snap := s.db.NewSnapshot()
	s.state.RUnlock()
	defer func() { _ = snap.Close() }()
	page := pageBytes{max: maxBytes}
	scanned := 0
	if from < covered {
		// Below the coverage the records carry no index entry: scan them, then go on
		// through the index.
		var stopped bool
		out, next, scanned, stopped, err = readRange(ctx, snap, stream, from, min(covered, head), limit, maxScan, &page, keepHead, filter, nil)
		if err != nil || stopped || covered >= head {
			return out, next, err
		}
		from = covered
	}
	if from >= head {
		return out, from, nil
	}

	// In key order, so the iterator only ever moves forward within a round.
	wanted := slices.Compact(slices.Sorted(slices.Values(signals)))
	iter, err := snap.NewIter(nil)
	if err != nil {
		return nil, from, err
	}
	defer func() { _ = iter.Close() }()

	next = from
	// A round collects the `want` smallest index entries from next on and visits
	// them in offset order. One round serves a read whose filter accepts what the
	// index yields; each further one looks at more entries at once, so a filter
	// that rejects most of them does not cost a pass over the signals per page.
	want := limit - len(out)
	for len(out) < limit {
		budget := maxSigRound
		if maxScan > 0 {
			if scanned >= maxScan {
				return out, next, nil
			}
			budget = min(budget, maxScan-scanned)
		}
		want = min(max(want, limit-len(out)), budget)
		// One more than this round visits tells whether any entry is left after it.
		// Below the low-water mark lie only the tombstones of pruned entries.
		offs, err := smallestSigOffsets(ctx, iter, stream, wanted, max(next, lwm), head, want+1)
		if err != nil {
			return nil, next, err
		}
		more := len(offs) > want
		if more {
			offs = offs[:want]
		}
		for i, off := range offs {
			scanned++
			if scanned%1024 == 0 {
				if err := ctx.Err(); err != nil {
					return nil, next, err
				}
			}
			next = off + 1
			val, closer, err := snap.Get(streamKey(stream, off))
			if errors.Is(err, pebble.ErrNotFound) {
				continue // an entry its record outlived; nothing to return
			}
			if err != nil {
				return nil, next, err
			}
			if keepHead != nil {
				if topic, signalID, ok := peekHead(val); ok && !keepHead(topic, signalID) {
					closer.Close()
					continue
				}
			}
			var e recEnc
			size := len(val)
			err = json.Unmarshal(val, &e)
			closer.Close()
			if err != nil {
				return nil, next, fmt.Errorf("decode record %d of %q: %w", off, stream, err)
			}
			record := storedRecord(off, e)
			if filter != nil && !filter(record) {
				continue
			}
			if !page.take(size, len(out)) {
				return out, off, nil // the next page starts at this record
			}
			out = append(out, record)
			if len(out) == limit {
				more = more || i < len(offs)-1
				break
			}
		}
		if !more {
			// No wanted record remains before the head.
			return out, head, nil
		}
		want *= 4
	}
	return out, next, nil
}

// maxSigRound bounds the offsets one round of ReadSignals holds (8 bytes each)
// when the caller sets no scan budget.
const maxSigRound = 1 << 16

// smallestSigOffsets returns, ascending, the n smallest offsets in [from, head)
// that the signal index holds for any of the signals, or all of them when there
// are fewer. It moves the one iterator from signal to signal. Once it holds n
// offsets, a later signal is only read below the largest of them, so the entries
// it steps over stay close to n however many records the signals have.
func smallestSigOffsets(ctx context.Context, iter *pebble.Iterator, stream string, signals []string, from, head uint64, n int) ([]uint64, error) {
	offs := make(offsetMaxHeap, 0, min(n, 1024))
	for i, signalID := range signals {
		if i%64 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		upTo := head
		if len(offs) == n {
			upTo = offs[0]
			if upTo-from < uint64(n) { //nolint:gosec // n is positive
				break // the n offsets are from..from+n-1: none smaller is left
			}
		}
		iter.SetBounds(sigKey(stream, signalID, from), sigKey(stream, signalID, upTo))
		for valid := iter.First(); valid; valid = iter.Next() {
			off, _ := offsetOf(iter.Key())
			if len(offs) < n {
				heap.Push(&offs, off)
				continue
			}
			if off >= offs[0] {
				break
			}
			offs[0] = off
			heap.Fix(&offs, 0)
		}
		if err := iter.Error(); err != nil {
			return nil, err
		}
	}
	slices.Sort(offs)
	return offs, nil
}

// offsetMaxHeap keeps the largest of the offsets it holds on top.
type offsetMaxHeap []uint64

func (h offsetMaxHeap) Len() int           { return len(h) }
func (h offsetMaxHeap) Less(i, j int) bool { return h[i] > h[j] }
func (h offsetMaxHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *offsetMaxHeap) Push(x any)        { *h = append(*h, x.(uint64)) }
func (h *offsetMaxHeap) Pop() any {
	old := *h
	last := old[len(old)-1]
	*h = old[:len(old)-1]
	return last
}
