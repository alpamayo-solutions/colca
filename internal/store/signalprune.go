package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/cockroachdb/pebble/v2"
)

// Per-signal retention removes the old records of chosen signals from inside
// the live range of a stream, below the stream's own policy. Prune can only cut
// a prefix, so this works like Compact: it deletes scattered offsets and leaves
// the LWM, the prune journal and every other signal's records alone.
//
// It walks the signal index (sigindex.go) one signal at a time, so its cost
// follows the records it removes plus a couple of reads per signal, never the
// length of the stream. For each signal it keeps:
//   - every record at or after now - max_age;
//   - the newest record before that cutoff, the value in force when the window
//     opens, so a reader of the window knows the value at its start and a
//     signal that stopped changing keeps its last value;
//   - every record at or above the cursor floor, which no consumer has read.
//
// "Newest" is the highest timestamp, so a late sample appended after a newer
// one does not become the value in force. The walk of a signal stops at its
// first record at or after the cutoff, in offset order, as the stream prune
// does: an older sample appended after that point stays until the stream
// policy removes it.
//
// Records written before the signal index covered the stream carry no index
// entry. They are left to the stream's own policy.

// SignalRule returns the retention window for one signal, read from the topic
// of its oldest live record. ok=false leaves the signal to the stream policy.
type SignalRule func(signalID, topic string) (maxAge time.Duration, ok bool)

// SignalPruneResult reports one PruneSignals call.
type SignalPruneResult struct {
	Removed uint64 // records deleted
	Bytes   uint64 // logical bytes reclaimed (the unit b/{stream} counts)
	Signals int    // signals examined
	// Resume is the signal the next call should start at; "" when this call
	// reached the end of the index.
	Resume string
	// Overridden names the stale cursors a committed delete passed. Each batch
	// that passed one carried a _StreamGap marker built by the marker callback.
	Overridden []string
}

// signalPruneBatch bounds the deletes one synced batch carries.
const signalPruneBatch = 10_000

// doomedRec is one record per-signal retention deletes.
type doomedRec struct {
	off      uint64
	signalID string
	ts       int64
	size     uint64
}

// PruneSignals applies per-signal retention to stream, starting at signal from
// ("" for the first). It examines signals until it has visited maxVisit index
// entries (0 means no limit), deletes in synced batches, and never deletes at
// or above clamp, the pruner's floor of protecting cursors.
//
// overridden maps the stale cursors the caller stopped protecting to the
// position each had when the caller decided. Under the mutex, before each
// batch, the floor is taken again over every other cursor, and over an
// overridden cursor whose position has moved since: a consumer that acked
// meanwhile, stale or not, is never passed. When a batch passes an
// overridden cursor, marker builds the records appended in that batch (the
// _StreamGap marker); the span is the lowest and highest offset deleted. A
// marker that returns no records lets the batch commit without one.
func (s *Store) PruneSignals(stream, from string, now time.Time, rule SignalRule, clamp uint64, overridden map[string]uint64, maxVisit int, marker func(span PruneSpan, passed []string) []Record) (SignalPruneResult, error) {
	var res SignalPruneResult
	s.mu.Lock()
	lwm, next := s.lwm[stream], s.next[stream]
	s.mu.Unlock()
	if next == 0 {
		return res, fmt.Errorf("unknown stream %q", stream)
	}
	clamp = min(clamp, next)
	if clamp <= lwm {
		return res, nil
	}
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte("si\x00" + stream + "\x00" + from),
		UpperBound: []byte("si\x00" + stream + "\x01"),
	})
	if err != nil {
		return res, err
	}
	defer iter.Close()

	var doomed []doomedRec
	flush := func() error {
		if len(doomed) == 0 {
			return nil
		}
		removed, shed, passed, err := s.deleteSignalRecords(stream, doomed, overridden, marker)
		doomed = doomed[:0]
		if err != nil {
			return err
		}
		res.Removed += removed
		res.Bytes += shed
		res.Overridden = appendNew(res.Overridden, passed)
		return nil
	}

	cutoffFor := func(maxAge time.Duration) int64 { return now.UnixMilli() - maxAge.Milliseconds() }
	visited := 0
	valid := iter.First()
	for valid {
		signalID, _, ok := splitSigKey(iter.Key(), stream)
		if !ok {
			return res, fmt.Errorf("corrupt signal index key %q", iter.Key())
		}
		if maxVisit > 0 && visited >= maxVisit {
			res.Resume = signalID
			break
		}
		res.Signals++
		// Seek this signal's range to the LWM: entries below it belong to a pruned
		// prefix whose index range Prune already removed, or were never written.
		valid = iter.SeekGE(sigKey(stream, signalID, lwm))
		var (
			cutoff  int64
			decided bool
			keep    *doomedRec // the value in force so far: highest TS before the cutoff
		)
		for ; valid; valid = iter.Next() {
			id, off, ok := splitSigKey(iter.Key(), stream)
			if !ok {
				return res, fmt.Errorf("corrupt signal index key %q", iter.Key())
			}
			if id != signalID || off >= clamp {
				break
			}
			visited++
			rec, found, err := s.readRecordMeta(stream, off)
			if err != nil {
				return res, err
			}
			if !found {
				continue // an index entry its record outlived; ReadSignals skips it too
			}
			if maxVisit > 0 && visited > maxVisit {
				// A long run of one signal: commit what is decided and resume at this
				// signal. The kept candidate (keep) is not in doomed, so the next call
				// sees it first and carries on.
				res.Resume = signalID
				return res, flush()
			}
			if !decided {
				decided = true
				maxAge, ok := rule(signalID, rec.Topic)
				if !ok || maxAge <= 0 {
					break // left to the stream policy
				}
				cutoff = cutoffFor(maxAge)
			}
			if rec.TS >= cutoff {
				break // the window starts here
			}
			cand := &doomedRec{off: off, signalID: signalID, ts: rec.TS, size: rec.size}
			if keep == nil {
				keep = cand
				continue
			}
			// The value in force is the one with the latest timestamp, not the latest
			// offset: a late sample appended after a newer one does not replace it. On
			// equal timestamps the later offset wins.
			if cand.ts >= keep.ts {
				keep, cand = cand, keep
			}
			doomed = append(doomed, *cand)
			if len(doomed) >= signalPruneBatch {
				if err := flush(); err != nil {
					return res, err
				}
			}
		}
		// Next signal: the first key after every entry of this one.
		valid = iter.SeekGE([]byte("si\x00" + stream + "\x00" + signalID + "\x01"))
	}
	if err := iter.Error(); err != nil {
		return res, err
	}
	return res, flush()
}

// appendNew appends the names of add not yet in list.
func appendNew(list, add []string) []string {
outer:
	for _, a := range add {
		for _, l := range list {
			if l == a {
				continue outer
			}
		}
		list = append(list, a)
	}
	return list
}

// splitSigKey reads the signal id and offset back out of a signal index key.
func splitSigKey(key []byte, stream string) (signalID string, off uint64, ok bool) {
	prefix := len("si\x00") + len(stream) + 1
	if len(key) < prefix+1+8 {
		return "", 0, false
	}
	off, _ = offsetOf(key)
	return string(key[prefix : len(key)-9]), off, true
}

// readRecordMeta reads the topic, timestamp and stored size of one record.
func (s *Store) readRecordMeta(stream string, off uint64) (recEnc, bool, error) {
	key := streamKey(stream, off)
	val, closer, err := s.db.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return recEnc{}, false, nil
	}
	if err != nil {
		return recEnc{}, false, err
	}
	defer closer.Close()
	var e recEnc
	if err := json.Unmarshal(val, &e); err != nil {
		return recEnc{}, false, fmt.Errorf("decode record %d of %q: %w", off, stream, err)
	}
	e.size = uint64(len(key) + len(val))
	return e, true, nil
}

// deleteSignalRecords deletes doomed records and their index entries in one
// synced batch, after taking the cursor floor again under the mutex.
func (s *Store) deleteSignalRecords(stream string, doomed []doomedRec, overridden map[string]uint64, marker func(PruneSpan, []string) []Record) (removed, shed uint64, passed []string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	floor := s.next[stream]
	stale := map[string]uint64{}
	if err := s.scanU64Pairs('c', func(name, cstream string, pos uint64) {
		if cstream != stream {
			return
		}
		if at, ok := overridden[name]; ok && at == pos {
			stale[name] = pos
			return
		}
		floor = min(floor, pos)
	}); err != nil {
		return 0, 0, nil, err
	}
	lwm := s.lwm[stream]
	span := PruneSpan{}
	b := s.db.NewBatch()
	defer b.Close()
	for _, d := range doomed {
		if d.off >= floor || d.off < lwm {
			continue // a cursor moved below it, or the prefix prune took it
		}
		if removed == 0 || d.off < span.From {
			span.From = d.off
		}
		if removed == 0 || d.off > span.To {
			span.To = d.off
		}
		if removed == 0 || d.ts < span.FirstTS {
			span.FirstTS = d.ts
		}
		if removed == 0 || d.ts > span.LastTS {
			span.LastTS = d.ts
		}
		removed++
		shed += d.size
		if err := b.Delete(streamKey(stream, d.off), nil); err != nil {
			return 0, 0, nil, err
		}
		if err := b.Delete(sigKey(stream, d.signalID, d.off), nil); err != nil {
			return 0, 0, nil, err
		}
	}
	if removed == 0 {
		return 0, 0, nil, nil
	}
	span.Shed = shed
	for name, pos := range stale {
		if pos <= span.To {
			passed = append(passed, name)
		}
	}
	liveBytes := s.bytes[stream]
	if shed > liveBytes {
		liveBytes = 0 // records from before the byte counter, as in Prune
	} else {
		liveBytes -= shed
	}
	off := s.next[stream]
	if len(passed) > 0 && marker != nil {
		for _, r := range marker(span, passed) {
			n, err := addRecord(b, stream, off, r)
			if err != nil {
				return 0, 0, nil, err
			}
			liveBytes += n
			off++
		}
	}
	if off != s.next[stream] {
		if err := b.Set(metaKey(stream), be64(off), nil); err != nil {
			return 0, 0, nil, err
		}
	}
	if err := b.Set(bytesKey(stream), be64(liveBytes), nil); err != nil {
		return 0, 0, nil, err
	}
	if err := s.db.Apply(b, pebble.Sync); err != nil {
		return 0, 0, nil, err
	}
	grew := off != s.next[stream]
	s.next[stream] = off
	s.bytes[stream] = liveBytes
	if grew {
		s.streamGrewLocked(stream)
	} else {
		s.signalChangeLocked()
	}
	return removed, shed, passed, nil
}
