// Package store implements Colca's durable storage: append-only streams with
// gapless offsets on top of Pebble, plus a KV projection written atomically
// with the records that produce it.
package store

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/cockroachdb/pebble/v2"
)

// streams is the fixed set of streams a store maintains offsets for.
var streams = []string{"metrics", "entities", "commands"}

type Record struct {
	Topic   string `json:"t"`
	Payload []byte `json:"p"`
	TS      int64  `json:"ts"`
	// optional KV projection written in the same atomic batch:
	KVPath string `json:"-"` // hierarchy path (segments after contract, post-mount)
	KVNode string `json:"-"` // node-id (level 4)
}

type StoredRecord struct {
	Offset  uint64
	Topic   string
	Payload []byte
	TS      int64
}

type KVEntry struct {
	Path, NodeID, Topic string
	Payload             []byte
	TS                  int64
	Offset              uint64
}

type Store struct {
	db   *pebble.DB
	mu   sync.Mutex
	next map[string]uint64 // next offset per stream
}

// Open opens (or creates) the store at dir and restores the next offset of
// every stream from persisted meta, so offsets stay gapless across restarts.
func Open(dir string) (*Store, error) {
	db, err := pebble.Open(dir, &pebble.Options{})
	if err != nil {
		return nil, err
	}
	s := &Store{db: db, next: map[string]uint64{}}
	for _, stream := range streams {
		next, err := readMeta(db, stream)
		if err != nil {
			db.Close()
			return nil, err
		}
		s.next[stream] = next
	}
	return s, nil
}

// readMeta returns the persisted next offset of a stream, or 1 if the stream
// has never been written. Any other error is fatal: silently restarting at 1
// would overwrite existing records and break offset continuity.
func readMeta(db *pebble.DB, stream string) (uint64, error) {
	v, closer, err := db.Get(metaKey(stream))
	if errors.Is(err, pebble.ErrNotFound) {
		return 1, nil
	}
	if err != nil {
		return 0, fmt.Errorf("read meta for stream %q: %w", stream, err)
	}
	defer closer.Close()
	if len(v) != 8 {
		return 0, fmt.Errorf("corrupt meta for stream %q: %d bytes", stream, len(v))
	}
	return binary.BigEndian.Uint64(v), nil
}

func (s *Store) Close() error { return s.db.Close() }

type recEnc struct {
	Topic   string `json:"t"`
	Payload []byte `json:"p"`
	TS      int64  `json:"ts"`
}
type kvEnc struct {
	Topic   string `json:"t"`
	Payload []byte `json:"p"`
	TS      int64  `json:"ts"`
	Offset  uint64 `json:"o"`
}

// Append writes records + meta + KV projections in ONE atomic, synced batch.
func (s *Store) Append(stream string, recs []Record) (first, last uint64, err error) {
	if len(recs) == 0 {
		return 0, 0, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	off := s.next[stream]
	if off == 0 {
		return 0, 0, fmt.Errorf("unknown stream %q", stream)
	}
	first = off
	b := s.db.NewBatch()
	defer b.Close()
	for _, r := range recs {
		val, err := json.Marshal(recEnc{r.Topic, r.Payload, r.TS})
		if err != nil {
			return 0, 0, err
		}
		if err := b.Set(streamKey(stream, off), val, nil); err != nil {
			return 0, 0, err
		}
		if r.KVPath != "" {
			kval, err := json.Marshal(kvEnc{r.Topic, r.Payload, r.TS, off})
			if err != nil {
				return 0, 0, err
			}
			if err := b.Set(kvKey(r.KVPath, r.KVNode), kval, nil); err != nil {
				return 0, 0, err
			}
		}
		off++
	}
	last = off - 1
	if err := b.Set(metaKey(stream), be64(off), nil); err != nil {
		return 0, 0, err
	}
	if err := s.db.Apply(b, pebble.Sync); err != nil {
		return 0, 0, err
	}
	s.next[stream] = off
	return first, last, nil
}

func (s *Store) NextOffset(stream string) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.next[stream]
}

// Read returns up to max records with Offset >= from, optionally topic-filtered.
// next is the offset to continue from (scanned position + 1, including filtered-out records).
func (s *Store) Read(stream string, from uint64, max int, filter func(string) bool) (out []StoredRecord, next uint64, err error) {
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: streamKey(stream, from),
		UpperBound: streamKey(stream, ^uint64(0)),
	})
	if err != nil {
		return nil, from, err
	}
	defer iter.Close()
	next = from
	for iter.First(); iter.Valid() && len(out) < max; iter.Next() {
		key := iter.Key()
		off := binary.BigEndian.Uint64(key[len(key)-8:])
		var e recEnc
		if err := json.Unmarshal(iter.Value(), &e); err != nil {
			return nil, next, err
		}
		next = off + 1
		if filter != nil && !filter(e.Topic) {
			continue
		}
		out = append(out, StoredRecord{Offset: off, Topic: e.Topic, Payload: e.Payload, TS: e.TS})
	}
	return out, next, nil
}
