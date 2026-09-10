package store

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/cockroachdb/pebble/v2"
)

// Eviction of records nothing at this node will ever read: node-private state
// (uns.IsNodePrivate) authored by another node, which no later record will
// retire. Like compaction it deletes scattered offsets and leaves the LWM alone;
// the holes are not gaps, since the stream is already a filtered uplink. The
// caller supplies the predicate; the store only does the bounded scan, the
// atomic batch and the byte accounting.

// EvictStats reports what one EvictRecords pass did.
type EvictStats struct {
	Removed uint64 // stream records deleted
	Bytes   uint64 // storage reclaimed from the stream's byte accounting
	// Resume is where the next pass continues. It equals the stream's next
	// offset at scan time when this pass reached the head (Done).
	Resume uint64
	Done   bool
}

// EvictRecords deletes every record in [from, next) whose topic the caller
// dooms, examining at most maxScan records per call. The caller continues from
// Resume until Done and then starts over at the LWM; a pass over clean records
// removes nothing. KV entries are left to EvictKV.
func (s *Store) EvictRecords(stream string, from, maxScan uint64, doomed func(topic string) bool) (EvictStats, error) {
	var st EvictStats
	s.mu.Lock()
	next, lwm := s.next[stream], s.lwm[stream]
	s.mu.Unlock()
	if next == 0 {
		return st, fmt.Errorf("unknown stream %q", stream)
	}
	if maxScan == 0 {
		return st, fmt.Errorf("evict %q: maxScan must be positive", stream)
	}
	if from < lwm {
		from = lwm
	}
	st.Resume, st.Done = next, true
	if from >= next {
		return st, nil
	}

	// Pass 1 without the mutex, as in scanDoomed and Compact: find the doomed
	// offsets. Appends only write at offsets >= next, and the prefix only shrinks
	// through Prune, which the LWM recheck below serializes.
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: streamKey(stream, from),
		UpperBound: streamKey(stream, next),
	})
	if err != nil {
		return st, err
	}
	var (
		keys    [][]byte
		shed    uint64
		scanned uint64
	)
	for iter.First(); iter.Valid(); iter.Next() {
		off, ok := offsetOf(iter.Key())
		if !ok {
			iter.Close()
			return st, fmt.Errorf("corrupt stream key %q during eviction from %q", iter.Key(), stream)
		}
		if scanned == maxScan {
			st.Resume, st.Done = off, false
			break
		}
		scanned++
		var e recEnc
		if err := json.Unmarshal(iter.Value(), &e); err != nil {
			iter.Close()
			return st, fmt.Errorf("decode record %q during eviction: %w", iter.Key(), err)
		}
		if !doomed(e.Topic) {
			continue
		}
		keys = append(keys, append([]byte(nil), iter.Key()...))
		shed += uint64(len(iter.Key())) + uint64(len(iter.Value()))
	}
	if err := iter.Error(); err != nil {
		iter.Close()
		return st, err
	}
	iter.Close()
	if len(keys) == 0 {
		return st, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lwm[stream] != lwm {
		// A prefix prune ran during the scan and may have removed some of these keys;
		// deleting them again would count their bytes twice. Delete nothing and let the
		// caller restart.
		return EvictStats{}, fmt.Errorf("concurrent prune on stream %q during eviction", stream)
	}
	b := s.db.NewBatch()
	defer b.Close()
	for _, key := range keys {
		if err := b.Delete(key, nil); err != nil {
			return st, err
		}
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
	st.Removed = uint64(len(keys))
	st.Bytes = shed
	return st, nil
}

// EvictKV deletes every current-state entry whose topic the caller dooms and
// returns how many. The topic comes from the key, so kept entries are never
// decoded. The scan runs without the mutex and the delete under it; an entry
// rewritten in between is still doomed, since the predicate only looks at the
// topic.
func (s *Store) EvictKV(doomed func(topic string) bool) (uint64, error) {
	lb := kvPrefix("")
	ub := append(append([]byte{}, lb...), 0xFF)
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: lb, UpperBound: ub})
	if err != nil {
		return 0, fmt.Errorf("store: kv eviction: open iterator: %w", err)
	}
	var keys [][]byte
	for iter.First(); iter.Valid(); iter.Next() {
		key := string(iter.Key()[2:]) // strip "k\x00"
		pathSep := strings.IndexByte(key, 0)
		if pathSep < 0 {
			continue
		}
		rest := key[pathSep+1:]
		nodeSep := strings.IndexByte(rest, 0)
		if nodeSep < 0 {
			continue
		}
		if doomed(rest[nodeSep+1:]) {
			keys = append(keys, append([]byte(nil), iter.Key()...))
		}
	}
	if err := iter.Error(); err != nil {
		iter.Close()
		return 0, fmt.Errorf("store: kv eviction: %w", err)
	}
	iter.Close()
	if len(keys) == 0 {
		return 0, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	b := s.db.NewBatch()
	defer b.Close()
	for _, key := range keys {
		if err := b.Delete(key, nil); err != nil {
			return 0, err
		}
	}
	if err := b.Commit(pebble.Sync); err != nil {
		return 0, err
	}
	return uint64(len(keys)), nil
}
