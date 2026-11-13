package store

import (
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
//
// One scan without the mutex reads each record's head (topic, payload
// emptiness, signal id; see peekRecord), not the record. Records appended
// meanwhile lie past the scan and can only supersede more, so the pass goes on
// with what it read; only a prune or another removal of records since the scan
// makes it stop, as the records it read may be gone.
func (s *Store) Compact(stream string) (CompactStats, error) {
	var st CompactStats
	s.mu.Lock()
	next, lwm, holes := s.next[stream], s.lwm[stream], s.holes[stream]
	s.mu.Unlock()
	if next == 0 {
		return st, fmt.Errorf("unknown stream %q", stream)
	}

	type candidate struct {
		off, size uint64
		topic     string
		signalID  string
		tombstone bool
	}
	// The latest offset per topic, and every record that is not, or is a
	// retraction (which may go once every cursor passed it).
	latest := map[string]uint64{}
	var records []candidate
	if err := s.scanStream(stream, lwm, next, "compaction", func(off uint64, h *recHead, size uint64) bool {
		latest[h.topic] = off
		records = append(records, candidate{off: off, size: size, topic: h.topic, signalID: h.signalID, tombstone: h.emptyPayload})
		return true
	}); err != nil {
		return st, err
	}
	if len(records) == 0 {
		return st, nil
	}

	if s.compactScanned != nil {
		s.compactScanned()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lwm[stream] != lwm || s.holes[stream] != holes {
		// Records the scan read may be gone. Nothing was deleted; the next cycle
		// sees the newer state.
		return st, fmt.Errorf("concurrent removal on stream %q during compaction", stream)
	}
	floor, err := s.cursorFloorLocked(stream, s.next[stream])
	if err != nil {
		return st, err
	}

	b := s.db.NewBatch()
	defer b.Close()
	var shed uint64
	for _, r := range records {
		last := latest[r.topic]
		switch {
		case r.off < last:
			st.Superseded++ // safe whatever any cursor is doing
		case r.tombstone && r.off < floor:
			st.Tombstones++ // a retraction every consumer has already seen
		default:
			continue // the live value, or a retraction somebody has not read yet
		}
		shed += r.size
		if err := b.Delete(streamKey(stream, r.off), nil); err != nil {
			return st, err
		}
		if r.signalID != "" {
			if err := b.Delete(sigKey(stream, r.signalID, r.off), nil); err != nil {
				return st, err
			}
		}
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
	s.holes[stream]++
	s.state.Lock()
	s.bytes[stream] = live - shed
	s.state.Unlock()
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
