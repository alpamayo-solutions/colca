package store

import (
	"encoding/json"
	"fmt"

	"github.com/cockroachdb/pebble/v2"
)

// Compaction for streams whose value is the latest record per topic rather than
// their history. Unlike Prune it deletes scattered offsets and leaves the LWM
// alone, because the record that matters is whichever came last for its topic.
// The holes are not reported as gaps: a compacted definition still exists in the
// record that superseded it.

// CompactStats reports what one compaction pass removed.
type CompactStats struct {
	Superseded uint64 // records dropped because a later record replaced them
	Tombstones uint64 // retractions dropped because every cursor had passed them
	Bytes      uint64 // storage reclaimed
}

// Compact removes records the stream no longer needs, by two different rules:
//
//   - A superseded record may go at once. A later record for the same topic sits
//     further along, so every consumer still ends with the current value.
//   - A tombstone may only go once every cursor on the stream has passed it. A
//     consumer behind a dropped tombstone would keep the retracted definition
//     forever, and for a group that is an authorization thought revoked.
//
// The floor is the lowest cursor on the stream; for a parent, that is every
// child's downlink-definitions cursor.
func (s *Store) Compact(stream string) (CompactStats, error) {
	var st CompactStats
	s.mu.Lock()
	next, lwm := s.next[stream], s.lwm[stream]
	s.mu.Unlock()
	if next == 0 {
		return st, fmt.Errorf("unknown stream %q", stream)
	}

	// Pass 1, without the mutex (same discipline as scanDoomed): the latest
	// offset per topic, and whether that latest record is a retraction.
	latest := map[string]uint64{}
	tombstone := map[string]bool{}
	if err := s.scanRecords(stream, lwm, next, func(off uint64, e recEnc) {
		latest[e.Topic] = off
		tombstone[e.Topic] = len(e.Payload) == 0
	}); err != nil {
		return st, err
	}
	if len(latest) == 0 {
		return st, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lwm[stream] != lwm || s.next[stream] != next {
		// The stream moved under the scan. Nothing was deleted; the next cycle
		// sees the newer state.
		return st, fmt.Errorf("concurrent write on stream %q during compaction", stream)
	}
	floor, err := s.cursorFloorLocked(stream, next)
	if err != nil {
		return st, err
	}

	b := s.db.NewBatch()
	defer b.Close()
	var shed uint64
	if err := s.scanRecords(stream, lwm, next, func(off uint64, e recEnc) {
		last := latest[e.Topic]
		switch {
		case off < last:
			// Superseded: safe whatever any cursor is doing.
		case tombstone[e.Topic] && off < floor:
			// A retraction every consumer has already seen.
			st.Tombstones++
		default:
			return // the live value, or a retraction somebody has not read yet
		}
		if off < last {
			st.Superseded++
		}
		key := streamKey(stream, off)
		shed += uint64(len(key)) + e.size
		_ = b.Delete(key, nil)
	}); err != nil {
		return st, err
	}
	if st.Superseded == 0 && st.Tombstones == 0 {
		return st, nil
	}

	live := s.bytes[stream]
	if shed > live {
		shed = live // same clamp discipline as Prune's byte accounting
	}
	if err := b.Set(bytesKey(stream), be64(live-shed), nil); err != nil {
		return st, err
	}
	if err := b.Commit(pebble.Sync); err != nil {
		return st, err
	}
	s.bytes[stream] = live - shed
	st.Bytes = shed
	return st, nil
}

// cursorFloorLocked is the lowest position of any cursor on the stream, or next
// when the stream has none. The caller holds the mutex.
func (s *Store) cursorFloorLocked(stream string, next uint64) (uint64, error) {
	floor := next
	err := s.scanU64Pairs('c', func(_, cstream string, pos uint64) {
		if cstream == stream && pos < floor {
			floor = pos
		}
	})
	return floor, err
}

// scanRecords walks [from, upTo) of a stream, decoding each record. fn is called
// with the offset and the decoded record plus its encoded size.
func (s *Store) scanRecords(stream string, from, upTo uint64, fn func(uint64, recEnc)) error {
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: streamKey(stream, from),
		UpperBound: streamKey(stream, upTo),
	})
	if err != nil {
		return err
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		off, ok := offsetOf(iter.Key())
		if !ok {
			return fmt.Errorf("corrupt stream key %q during compaction of %q", iter.Key(), stream)
		}
		var e recEnc
		if err := json.Unmarshal(iter.Value(), &e); err != nil {
			return fmt.Errorf("decode record %q during compaction: %w", iter.Key(), err)
		}
		e.size = uint64(len(iter.Value()))
		fn(off, e)
	}
	return iter.Error()
}
