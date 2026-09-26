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

// regBounds covers every registry key: \x00 bumped to \x01 sorts strictly
// after all ulid suffixes.
func regBounds() (lb, ub []byte) { return []byte("r\x00"), []byte("r\x01") }

// RegistryPut writes the registry entry and appends its _EnrolledIdentity record
// with KV projection in one synced batch, so an identity never authenticates
// without its record or the reverse. It returns the record's offset.
func (s *Store) RegistryPut(ulid string, entry []byte, stream string, rec Record) (uint64, error) {
	return s.registryBatch(stream, []Record{rec}, func(b *pebble.Batch) error {
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
		return b.Delete(regKey(ulid), nil)
	})
}

// registryBatch appends recs to stream and applies mut in the same batch,
// maintaining meta and byte accounting exactly like Append. It returns the first
// record's offset.
func (s *Store) registryBatch(stream string, recs []Record, mut func(*pebble.Batch) error) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	off := s.next[stream]
	if off == 0 {
		return 0, fmt.Errorf("unknown stream %q", stream)
	}
	first := off
	b := s.db.NewBatch()
	defer b.Close()
	liveBytes := s.bytes[stream]
	for _, rec := range recs {
		n, err := addRecord(b, stream, off, rec)
		if err != nil {
			return 0, err
		}
		liveBytes += n
		off++
	}
	if err := b.Set(metaKey(stream), be64(off), nil); err != nil {
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
	s.next[stream] = off
	s.bytes[stream] = liveBytes
	s.streamGrewLocked(stream)
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
