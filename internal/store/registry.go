package store

import (
	"fmt"

	"github.com/cockroachdb/pebble/v2"
)

// Registry key family (auth design §2.2): r/{ulid} → entry JSON marks a
// LOCALLY enrolled identity — the authoritative auth source. The _EdgeNode
// entity record written in the same batch is the same fact as visible,
// replicating state; a replicated _EdgeNode from a child updates KV/history
// only and never this family (authority is local, delegation like DNS).

func regKey(ulid string) []byte { return []byte("r\x00" + ulid) }

// regBounds covers every registry key: \x00 bumped to \x01 sorts strictly
// after all ulid suffixes.
func regBounds() (lb, ub []byte) { return []byte("r\x00"), []byte("r\x01") }

// RegistryPut writes the registry entry and appends its _EdgeNode entity
// record (with KV projection) in ONE synced batch — an enrollment is atomic:
// there is no state where the identity authenticates without its entity
// record, or vice versa. Returns the entity record's offset.
func (s *Store) RegistryPut(ulid string, entry []byte, stream string, rec Record) (uint64, error) {
	return s.registryBatch(stream, rec, func(b *pebble.Batch) error {
		return b.Set(regKey(ulid), entry, nil)
	})
}

// RegistryDelete removes the registry entry, appends the retirement record
// (empty-payload _EdgeNode tombstone) and deletes the identity's KV
// projection (rec.KVPath/KVNode/Topic) in ONE synced batch — a revoked identity
// must not survive a restart's retained-set reseed. rec's KV fields are used
// for DELETION here; the record itself is appended without a KV set.
func (s *Store) RegistryDelete(ulid string, stream string, rec Record) (uint64, error) {
	kvPath, kvNode, kvTopic := rec.KVPath, rec.KVNode, rec.Topic
	rec.KVPath, rec.KVNode = "", ""
	return s.registryBatch(stream, rec, func(b *pebble.Batch) error {
		if kvPath != "" {
			if err := b.Delete(kvKey(kvPath, kvNode, kvTopic), nil); err != nil {
				return err
			}
		}
		return b.Delete(regKey(ulid), nil)
	})
}

// registryBatch appends rec to stream and applies mut in the same batch,
// maintaining meta and byte accounting exactly like Append.
func (s *Store) registryBatch(stream string, rec Record, mut func(*pebble.Batch) error) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	off := s.next[stream]
	if off == 0 {
		return 0, fmt.Errorf("unknown stream %q", stream)
	}
	b := s.db.NewBatch()
	defer b.Close()
	n, err := addRecord(b, stream, off, rec)
	if err != nil {
		return 0, err
	}
	liveBytes := s.bytes[stream] + n
	if err := b.Set(metaKey(stream), be64(off+1), nil); err != nil {
		return 0, err
	}
	if err := b.Set(bytesKey(stream), be64(liveBytes), nil); err != nil {
		return 0, err
	}
	if err := mut(b); err != nil {
		return 0, err
	}
	if err := s.db.Apply(b, pebble.Sync); err != nil {
		return 0, err
	}
	s.next[stream] = off + 1
	s.bytes[stream] = liveBytes
	return off, nil
}

// RegistryScan returns every locally enrolled entry (ulid → entry JSON) — the
// startup load for the in-memory registry map.
func (s *Store) RegistryScan() map[string][]byte {
	lb, ub := regBounds()
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: lb, UpperBound: ub})
	if err != nil {
		return nil
	}
	defer iter.Close()
	out := map[string][]byte{}
	for iter.First(); iter.Valid(); iter.Next() {
		ulid := string(iter.Key()[len(lb):])
		out[ulid] = append([]byte(nil), iter.Value()...)
	}
	return out
}
