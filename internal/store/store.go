// Package store implements Colca's durable storage: append-only streams with
// gapless offsets on top of Pebble, plus a KV projection written atomically
// with the records that produce it.
package store

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
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

// readU64 returns the big-endian counter stored at key, or dflt when the key is
// absent or does not hold exactly 8 bytes.
func (s *Store) readU64(key []byte, dflt uint64) uint64 {
	v, closer, err := s.db.Get(key)
	if err != nil {
		return dflt
	}
	defer closer.Close()
	if len(v) != 8 {
		return dflt
	}
	return binary.BigEndian.Uint64(v)
}

// CursorGet returns the next offset a named consumer should read from a stream.
// A cursor that was never acked starts at 1, the first offset Append hands out.
func (s *Store) CursorGet(name, stream string) uint64 {
	return s.readU64(cursorKey(name, stream), 1)
}

// CursorAck moves the cursor forward only (monotonic); returns whether it moved.
func (s *Store) CursorAck(name, stream string, off uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if off <= s.readU64(cursorKey(name, stream), 1) {
		return false
	}
	if err := s.db.Set(cursorKey(name, stream), be64(off), pebble.Sync); err != nil {
		return false
	}
	return true
}

// HWMGet returns the highest child offset already applied for (child, stream),
// or 0 when nothing from that child has been applied yet.
func (s *Store) HWMGet(child, stream string) uint64 {
	return s.readU64(hwmKey(child, stream), 0)
}

// ReplRecord is a record as it travels from a child node to its parent. The
// json tags are the wire format — do not rename them.
type ReplRecord struct {
	ChildOffset uint64 `json:"o"`
	Topic       string `json:"t"`
	Payload     []byte `json:"p"`
	TS          int64  `json:"ts"`
	KVPath      string `json:"kp,omitempty"`
	KVNode      string `json:"kn,omitempty"`
}

// ApplyReplicated appends records with ChildOffset > HWM(child,stream), assigns LOCAL offsets,
// updates KV and the HWM — all in one atomic batch. Idempotent by construction.
//
// It returns the records it ACTUALLY wrote, in write order. That is what makes
// the caller able to mirror exactly the new records onto the local MQTT bus:
// deduplicated records are not in the returned slice, and on any error the
// slice is nil, so nothing that is not durable can ever reach the bus.
func (s *Store) ApplyReplicated(child, stream string, recs []ReplRecord) (applied []ReplRecord, hwm uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	prev := s.HWMGet(child, stream)
	hwm = prev
	off := s.next[stream]
	if off == 0 {
		return nil, prev, fmt.Errorf("unknown stream %q", stream)
	}
	b := s.db.NewBatch()
	defer b.Close()
	for _, r := range recs {
		if r.ChildOffset <= hwm {
			continue
		}
		val, err := json.Marshal(recEnc{r.Topic, r.Payload, r.TS})
		if err != nil {
			return nil, prev, err
		}
		if err := b.Set(streamKey(stream, off), val, nil); err != nil {
			return nil, prev, err
		}
		if r.KVPath != "" {
			kval, err := json.Marshal(kvEnc{r.Topic, r.Payload, r.TS, off})
			if err != nil {
				return nil, prev, err
			}
			if err := b.Set(kvKey(r.KVPath, r.KVNode), kval, nil); err != nil {
				return nil, prev, err
			}
		}
		off++
		applied = append(applied, r)
		hwm = r.ChildOffset
	}
	if len(applied) == 0 {
		return nil, prev, nil
	}
	if err := b.Set(metaKey(stream), be64(off), nil); err != nil {
		return nil, prev, err
	}
	if err := b.Set(hwmKey(child, stream), be64(hwm), nil); err != nil {
		return nil, prev, err
	}
	if err := s.db.Apply(b, pebble.Sync); err != nil {
		return nil, prev, err
	}
	s.next[stream] = off
	return applied, hwm, nil
}

// KVScan returns the current KV projection for every path starting with prefix.
// An empty prefix scans the whole projection.
func (s *Store) KVScan(prefix string) []KVEntry {
	lb := kvPrefix(prefix)
	ub := append(append([]byte{}, lb...), 0xFF)
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: lb, UpperBound: ub})
	if err != nil {
		return nil
	}
	defer iter.Close()
	var out []KVEntry
	for iter.First(); iter.Valid(); iter.Next() {
		key := string(iter.Key()[2:]) // strip "k\x00"
		// key = path \x00 nodeID
		sep := strings.IndexByte(key, 0)
		if sep < 0 {
			continue
		}
		var e kvEnc
		if json.Unmarshal(iter.Value(), &e) != nil {
			continue
		}
		out = append(out, KVEntry{
			Path:    key[:sep],
			NodeID:  key[sep+1:],
			Topic:   e.Topic,
			Payload: e.Payload,
			TS:      e.TS,
			Offset:  e.Offset,
		})
	}
	return out
}

// DiskMetrics is a point-in-time snapshot of the bytes Pebble has pushed to
// disk. Deltas of it are the basis for flash-endurance estimates: every
// ingested record costs WAL bytes now and level (flush+compaction) bytes later.
type DiskMetrics struct {
	WALBytesWritten   uint64 // bytes written to the write-ahead log
	LevelBytesWritten uint64 // bytes flushed + compacted across all LSM levels
	DiskUsageBytes    uint64 // current on-disk footprint of the store
}

func (s *Store) DiskMetrics() DiskMetrics {
	m := s.db.Metrics()
	var level uint64
	for i := range m.Levels {
		level += uint64(m.Levels[i].TableBytesFlushed) + uint64(m.Levels[i].TableBytesCompacted)
	}
	return DiskMetrics{
		WALBytesWritten:   uint64(m.WAL.BytesWritten),
		LevelBytesWritten: level,
		DiskUsageBytes:    m.DiskSpaceUsage(),
	}
}
