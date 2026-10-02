package store

import (
	"fmt"

	"github.com/cockroachdb/pebble/v2"
)

// Registry keys: r/{ulid} holds the entry of a locally enrolled identity and is
// the authority for authentication. The _EnrolledIdentity record written in the
// same batch is the replicated view of the same fact; one replicated from a
// child updates KV and history only, never this family.

func regKey(ulid string) []byte { return []byte("r\x00" + ulid) }

// revokedKey marks an identity revoked at this node. Commands it sent that are
// still queued for a child are refused rather than forwarded: this node admitted
// them on grants the identity no longer holds. Enrolling the identity again
// clears the mark.
func revokedKey(ulid string) []byte { return []byte("rv\x00" + ulid) }

// Revoked reports whether ulid was revoked at this node and not enrolled again.
func (s *Store) Revoked(ulid string) bool {
	v, closer, err := s.db.Get(revokedKey(ulid))
	if err != nil {
		return false
	}
	defer closer.Close()
	return len(v) > 0
}

// regBounds covers every registry key: \x00 bumped to \x01 sorts strictly
// after all ulid suffixes.
func regBounds() (lb, ub []byte) { return []byte("r\x00"), []byte("r\x01") }

// RegistryPut writes the registry entry and appends its _EnrolledIdentity record
// with KV projection in one synced batch, so an identity never authenticates
// without its record or the reverse. It returns the record's offset.
func (s *Store) RegistryPut(ulid string, entry []byte, stream string, rec Record) (uint64, error) {
	return s.registryBatch(stream, []Record{rec}, func(b *pebble.Batch) error {
		if err := b.Delete(revokedKey(ulid), nil); err != nil {
			return err
		}
		return b.Set(regKey(ulid), entry, nil)
	})
}

// RegistryDelete removes the registry entry, appends the retirement tombstone
// and deletes the identity's KV entry in one synced batch, so a revoked identity
// does not come back when retained messages are reseeded after a restart. rec's
// KV fields name what to delete; the record is appended without a KV set.
//
// also carries the records the identity itself authored, retired in the same
// batch. They belong in this batch rather than in a second append because only
// their author may write them: a crash between two appends would leave a record
// standing with no identity left that could retire it. The returned offset is
// the identity's own tombstone; the authored ones follow it.
func (s *Store) RegistryDelete(ulid string, stream string, rec Record, also ...Record) (uint64, error) {
	kvPath, kvNode, kvTopic := rec.KVPath, rec.KVNode, rec.Topic
	rec.KVPath, rec.KVNode = "", ""
	return s.registryBatch(stream, append([]Record{rec}, also...), func(b *pebble.Batch) error {
		if kvPath != "" {
			if err := deleteKV(b, kvPath, kvNode, kvTopic); err != nil {
				return err
			}
		}
		if err := b.Set(revokedKey(ulid), []byte{1}, nil); err != nil {
			return err
		}
		return b.Delete(regKey(ulid), nil)
	})
}

// ChildRetirement is what RegistryRetire takes from this node together with a
// child node's identity: the current state the child and its subtree replicated
// up, and the replication bookkeeping kept for it.
type ChildRetirement struct {
	// Child names the replication child whose high-water marks and recorded store
	// incarnation are cleared.
	Child string
	// StreamOf selects the KV entries to retire. It returns the stream an entry's
	// tombstone is appended to, the one its contract rises on, or "" to keep it.
	StreamOf func(KVEntry) string
	// TS stamps the tombstones.
	TS int64
}

// RegistryRetire is RegistryDelete for a child node taken out of service for
// good. In the same synced batch it appends a tombstone for every KV entry
// ret.StreamOf selects, on the stream it names, and clears the child's
// high-water marks and recorded store incarnation. The selection runs under the
// store lock, so no write lands between the scan and the batch.
//
// The tombstones are ordinary retained deletes: this node's uplink carries them
// up like any record, so every ancestor retires its copies too. Clearing the
// marks means the same identity enrolled again is applied from its first
// offset instead of being deduplicated against a store that was retired.
//
// It returns the identity's tombstone offset and the tombstones appended for
// the retired state, for the caller to clear from the local bus.
func (s *Store) RegistryRetire(ulid, stream string, rec Record, also []Record, ret ChildRetirement) (uint64, []Record, error) {
	kvPath, kvNode, kvTopic := rec.KVPath, rec.KVNode, rec.Topic
	rec.KVPath, rec.KVNode = "", ""
	s.mu.Lock()
	defer s.mu.Unlock()
	entries, err := s.KVScan("")
	if err != nil {
		return 0, nil, fmt.Errorf("scan the state %s replicated: %w", ret.Child, err)
	}
	batches := []streamRecords{{stream: stream, recs: append([]Record{rec}, also...)}}
	var retired []Record
	for _, e := range entries {
		target := ret.StreamOf(e)
		if target == "" {
			continue
		}
		if kvPath != "" && e.Path == kvPath && e.NodeID == kvNode && e.Topic == kvTopic {
			continue // the identity's own record, retired below
		}
		tomb := Record{Topic: e.Topic, TS: ret.TS, KVPath: e.Path, KVNode: e.NodeID, Delete: true}
		if target == "metrics" {
			// Decided like any metrics append, so a tombstone never rises past
			// where its samples stopped.
			if tomb.SourceLocalOnly, err = s.metricSourceLocalOnly(tomb); err != nil {
				return 0, nil, err
			}
		}
		retired = append(retired, tomb)
		placed := false
		for i := range batches {
			if batches[i].stream == target {
				batches[i].recs = append(batches[i].recs, tomb)
				placed = true
				break
			}
		}
		if !placed {
			batches = append(batches, streamRecords{stream: target, recs: []Record{tomb}})
		}
	}
	off, err := s.registryBatchLocked(batches, func(b *pebble.Batch) error {
		if kvPath != "" {
			if err := deleteKV(b, kvPath, kvNode, kvTopic); err != nil {
				return err
			}
		}
		if err := b.DeleteRange(hwmKey(ret.Child, ""), hwmChildEnd(ret.Child), nil); err != nil {
			return err
		}
		if err := b.Delete(childStoreKey(ret.Child), nil); err != nil {
			return err
		}
		if err := b.Set(revokedKey(ulid), []byte{1}, nil); err != nil {
			return err
		}
		return b.Delete(regKey(ulid), nil)
	})
	if err != nil {
		return 0, nil, err
	}
	return off, retired, nil
}

// streamRecords is one stream's share of a registry batch.
type streamRecords struct {
	stream string
	recs   []Record
}

// registryBatch appends recs to stream and applies mut in the same batch,
// maintaining meta and byte accounting exactly like Append. It returns the first
// record's offset.
func (s *Store) registryBatch(stream string, recs []Record, mut func(*pebble.Batch) error) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.registryBatchLocked([]streamRecords{{stream: stream, recs: recs}}, mut)
}

// registryBatchLocked is registryBatch over several streams, each named once. It
// returns the offset of the first stream's first record. The caller holds s.mu.
func (s *Store) registryBatchLocked(batches []streamRecords, mut func(*pebble.Batch) error) (uint64, error) {
	b := s.db.NewBatch()
	defer b.Close()
	next := make(map[string]uint64, len(batches))
	live := make(map[string]uint64, len(batches))
	var first uint64
	for i, sr := range batches {
		off := s.next[sr.stream]
		if off == 0 {
			return 0, fmt.Errorf("unknown stream %q", sr.stream)
		}
		if i == 0 {
			first = off
		}
		liveBytes := s.bytes[sr.stream]
		for _, rec := range sr.recs {
			n, err := addRecord(b, sr.stream, off, rec)
			if err != nil {
				return 0, err
			}
			liveBytes += n
			off++
		}
		if err := b.Set(metaKey(sr.stream), be64(off), nil); err != nil {
			return 0, err
		}
		if err := b.Set(bytesKey(sr.stream), be64(liveBytes), nil); err != nil {
			return 0, err
		}
		next[sr.stream], live[sr.stream] = off, liveBytes
	}
	if err := mut(b); err != nil {
		return 0, err
	}
	if err := s.db.Apply(b, pebble.Sync); err != nil {
		return 0, err
	}
	for _, sr := range batches {
		s.next[sr.stream] = next[sr.stream]
		s.bytes[sr.stream] = live[sr.stream]
		s.noteContractsLocked(sr.stream, sr.recs)
		s.streamGrewLocked(sr.stream)
	}
	return first, nil
}

// RegistryScan returns every locally enrolled entry (ulid to entry JSON), which
// the in-memory registry loads at startup.
func (s *Store) RegistryScan() (map[string][]byte, error) {
	lb, ub := regBounds()
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: lb, UpperBound: ub})
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	out := map[string][]byte{}
	for iter.First(); iter.Valid(); iter.Next() {
		ulid := string(iter.Key()[len(lb):])
		out[ulid] = append([]byte(nil), iter.Value()...)
	}
	return out, iter.Error()
}
