package store

import (
	"github.com/cockroachdb/pebble/v2"
)

// Enrollment families: p/{fingerprint} holds a pending, rejected or blocked
// enrollment request, a/{id} a pre-approval. Neither is a registry entry, so a
// key that is only pending can never authenticate. Each value is written in the
// same batch as its retained mirror record, like the registry and its
// _EnrolledIdentity.
const (
	EnrollmentRequests     = "p"
	EnrollmentPreapprovals = "a"
)

func enrollmentKey(family, id string) []byte { return []byte(family + "\x00" + id) }

// EnrollmentPut stores value under family/id and appends its mirror record
// (with its KV projection) in one synced batch.
func (s *Store) EnrollmentPut(family, id string, value []byte, stream string, rec Record) (uint64, error) {
	return s.registryBatch(stream, []Record{rec}, func(b *pebble.Batch) error {
		return b.Set(enrollmentKey(family, id), value, nil)
	})
}

// EnrollmentDelete removes family/id, appends the mirror's tombstone and deletes
// its KV entry in one synced batch. rec's KV fields name what to delete.
func (s *Store) EnrollmentDelete(family, id string, stream string, rec Record) (uint64, error) {
	kvPath, kvNode, kvTopic := rec.KVPath, rec.KVNode, rec.Topic
	rec.KVPath, rec.KVNode = "", ""
	return s.registryBatch(stream, []Record{rec}, func(b *pebble.Batch) error {
		if kvPath != "" {
			if err := deleteKV(b, kvPath, kvNode, kvTopic); err != nil {
				return err
			}
		}
		return b.Delete(enrollmentKey(family, id), nil)
	})
}

// EnrollmentScan returns every value of a family, keyed by id.
func (s *Store) EnrollmentScan(family string) (map[string][]byte, error) {
	lb := []byte(family + "\x00")
	ub := []byte(family + "\x01")
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: lb, UpperBound: ub})
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	out := map[string][]byte{}
	for iter.First(); iter.Valid(); iter.Next() {
		out[string(iter.Key()[len(lb):])] = append([]byte(nil), iter.Value()...)
	}
	return out, iter.Error()
}
