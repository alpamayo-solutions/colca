package store

import (
	"encoding/json"
	"fmt"

	"github.com/cockroachdb/pebble/v2"
)

// SeekTS returns the first retained offset of stream whose record timestamp is
// at or after ts, found by binary search over [LWM, NextOffset). It assumes the
// stream is appended in time order, which holds for what a node writes itself;
// a record that arrived late with an older timestamp (a child replicating a
// backlog) can sit past the returned offset. A caller that needs every record in
// a time window still filters by timestamp while it scans forward.
//
// It returns NextOffset when no retained record is that recent, and the LWM when
// ts lies before the first retained record. Offsets the search lands on but no
// record holds count as the next record that exists.
func (s *Store) SeekTS(stream string, ts int64) (uint64, error) {
	s.mu.Lock()
	lo, hi := s.lwm[stream], s.next[stream]
	s.mu.Unlock()
	if lo == 0 {
		lo = 1 // unknown stream: offsets start at 1
	}
	if hi <= lo {
		return hi, nil
	}
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: streamKey(stream, lo),
		UpperBound: streamKey(stream, hi),
	})
	if err != nil {
		return 0, err
	}
	defer iter.Close()
	// Invariant: every record below lo is older than ts; the answer is <= hi.
	for lo < hi {
		mid := lo + (hi-lo)/2
		if !iter.SeekGE(streamKey(stream, mid)) {
			if err := iter.Error(); err != nil {
				return 0, err
			}
			hi = mid // nothing retained in [mid, hi)
			continue
		}
		var e recEnc
		if err := json.Unmarshal(iter.Value(), &e); err != nil {
			return 0, fmt.Errorf("decode record %q during seek: %w", iter.Key(), err)
		}
		if e.TS >= ts {
			hi = mid // the first record at or after mid already qualifies
			continue
		}
		off, _ := offsetOf(iter.Key())
		lo = off + 1
	}
	return lo, nil
}

// EachRecord calls fn for each record of stream in [from, upTo), in offset
// order, until fn returns false. Like ScanRecords it reads a consistent Pebble
// snapshot without holding the store mutex.
func (s *Store) EachRecord(stream string, from, upTo uint64, fn func(StoredRecord) bool) error {
	if upTo <= from {
		return nil
	}
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: streamKey(stream, from),
		UpperBound: streamKey(stream, upTo),
	})
	if err != nil {
		return err
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		off, _ := offsetOf(iter.Key())
		var e recEnc
		if err := json.Unmarshal(iter.Value(), &e); err != nil {
			return fmt.Errorf("decode record %q during scan: %w", iter.Key(), err)
		}
		if !fn(storedRecord(off, e)) {
			break
		}
	}
	return iter.Error()
}
