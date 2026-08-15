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
	db    *pebble.DB
	mu    sync.Mutex
	next  map[string]uint64 // next offset per stream
	lwm   map[string]uint64 // low-water mark per stream: lowest retained offset
	bytes map[string]uint64 // live logical bytes per stream (stream key + encoded value)
}

// Open opens (or creates) the store at dir and restores the next offset,
// low-water mark and byte counter of every stream from persisted meta, so
// offsets stay gapless and the pruned-prefix contract holds across restarts.
func Open(dir string) (*Store, error) {
	db, err := pebble.Open(dir, &pebble.Options{})
	if err != nil {
		return nil, err
	}
	s := &Store{db: db, next: map[string]uint64{}, lwm: map[string]uint64{}, bytes: map[string]uint64{}}
	for _, stream := range streams {
		next, err := readCounter(db, metaKey(stream), 1, "meta", stream)
		if err != nil {
			db.Close()
			return nil, err
		}
		lwm, err := readCounter(db, lwmKey(stream), 1, "low-water mark", stream)
		if err != nil {
			db.Close()
			return nil, err
		}
		liveBytes, err := readCounter(db, bytesKey(stream), 0, "byte counter", stream)
		if err != nil {
			db.Close()
			return nil, err
		}
		s.next[stream] = next
		s.lwm[stream] = lwm
		s.bytes[stream] = liveBytes
		// Validate the prune journal with the same fail-loud discipline: a
		// corrupt entry would misreport gap spans for as long as it lived.
		if _, err := s.readJournal(stream); err != nil {
			db.Close()
			return nil, err
		}
	}
	return s, nil
}

// readCounter returns the 8-byte big-endian counter at key, or dflt when the
// key was never written. Any other outcome is fatal: silently substituting the
// default would break the invariant the counter protects — offset continuity
// for m/, the pruned-prefix contract for l/ (a corrupt LWM read as 1 would
// resurrect the pruned range as a phantom gap), honest size accounting for b/.
func readCounter(db *pebble.DB, key []byte, dflt uint64, what, stream string) (uint64, error) {
	v, closer, err := db.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return dflt, nil
	}
	if err != nil {
		return 0, fmt.Errorf("read %s for stream %q: %w", what, stream, err)
	}
	defer closer.Close()
	if len(v) != 8 {
		return 0, fmt.Errorf("corrupt %s for stream %q: %d bytes", what, stream, len(v))
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

// addRecord writes one stream record (and its optional KV projection) into the
// batch and returns the record's logical byte cost — len(stream key) +
// len(encoded value), the unit the b/{stream} accounting tracks (spec §4).
func addRecord(b *pebble.Batch, stream string, off uint64, topic string, payload []byte, ts int64, kvPath, kvNode string) (uint64, error) {
	val, err := json.Marshal(recEnc{topic, payload, ts})
	if err != nil {
		return 0, err
	}
	key := streamKey(stream, off)
	if err := b.Set(key, val, nil); err != nil {
		return 0, err
	}
	if kvPath != "" {
		kval, err := json.Marshal(kvEnc{topic, payload, ts, off})
		if err != nil {
			return 0, err
		}
		if err := b.Set(kvKey(kvPath, kvNode), kval, nil); err != nil {
			return 0, err
		}
	}
	return uint64(len(key) + len(val)), nil
}

// Append writes records + meta + byte counter + KV projections in ONE atomic,
// synced batch.
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
	liveBytes := s.bytes[stream]
	for _, r := range recs {
		n, err := addRecord(b, stream, off, r.Topic, r.Payload, r.TS, r.KVPath, r.KVNode)
		if err != nil {
			return 0, 0, err
		}
		liveBytes += n
		off++
	}
	last = off - 1
	if err := b.Set(metaKey(stream), be64(off), nil); err != nil {
		return 0, 0, err
	}
	if err := b.Set(bytesKey(stream), be64(liveBytes), nil); err != nil {
		return 0, 0, err
	}
	if err := s.db.Apply(b, pebble.Sync); err != nil {
		return 0, 0, err
	}
	s.next[stream] = off
	s.bytes[stream] = liveBytes
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

// CursorInfo is one persisted consumer cursor: the next offset the named
// consumer will read from a stream.
type CursorInfo struct {
	Name, Stream string
	Position     uint64
}

// HWMInfo is one replication high-water mark: the highest child offset already
// applied locally for (child, stream).
type HWMInfo struct {
	Child, Stream string
	HWM           uint64
}

// scanU64Pairs iterates every key of the form {prefix}\x00{first}\x00{second}
// holding an 8-byte big-endian counter. Malformed keys and values are skipped,
// the same tolerance KVScan applies.
func (s *Store) scanU64Pairs(prefix byte, fn func(first, second string, v uint64)) {
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte{prefix, 0x00},
		UpperBound: []byte{prefix, 0xFF},
	})
	if err != nil {
		return
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		key := string(iter.Key()[2:]) // strip "{prefix}\x00"
		sep := strings.IndexByte(key, 0)
		if sep < 0 {
			continue
		}
		v := iter.Value()
		if len(v) != 8 {
			continue
		}
		fn(key[:sep], key[sep+1:], binary.BigEndian.Uint64(v))
	}
}

// Cursors returns every persisted cursor. Read-only.
func (s *Store) Cursors() []CursorInfo {
	var out []CursorInfo
	s.scanU64Pairs('c', func(name, stream string, v uint64) {
		out = append(out, CursorInfo{Name: name, Stream: stream, Position: v})
	})
	return out
}

// HWMs returns every persisted replication high-water mark. Read-only.
func (s *Store) HWMs() []HWMInfo {
	var out []HWMInfo
	s.scanU64Pairs('h', func(child, stream string, v uint64) {
		out = append(out, HWMInfo{Child: child, Stream: stream, HWM: v})
	})
	return out
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
	liveBytes := s.bytes[stream]
	for _, r := range recs {
		if r.ChildOffset <= hwm {
			continue
		}
		n, err := addRecord(b, stream, off, r.Topic, r.Payload, r.TS, r.KVPath, r.KVNode)
		if err != nil {
			return nil, prev, err
		}
		liveBytes += n
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
	if err := b.Set(bytesKey(stream), be64(liveBytes), nil); err != nil {
		return nil, prev, err
	}
	if err := b.Set(hwmKey(child, stream), be64(hwm), nil); err != nil {
		return nil, prev, err
	}
	if err := s.db.Apply(b, pebble.Sync); err != nil {
		return nil, prev, err
	}
	s.next[stream] = off
	s.bytes[stream] = liveBytes
	return applied, hwm, nil
}

// LWM returns the low-water mark of a stream: the lowest offset still
// retained. Streams start at 1; pruning advances it — the pruned region is
// exactly the contiguous prefix [1..LWM).
func (s *Store) LWM(stream string) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lwm[stream]
}

// StreamBytes returns the live logical bytes of a stream: the sum of
// len(stream key)+len(encoded value) over every retained record, maintained
// by Append/ApplyReplicated (+) and Prune (−) inside their atomic batches.
func (s *Store) StreamBytes(stream string) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.bytes[stream]
}

// PruneSpan is one prune-journal entry: the inclusive offset range [From..To]
// a prune run removed and the time span of the removed records. The journal
// is a contiguous ordered partition of [1..LWM) — it answers "what span is
// missing", never "what were the values". Coalesced marks an entry merged
// from older entries: its time span is a union, so a gap answered from it is
// approximate (the API layer maps this to the gap object's approx field,
// spec §6.1/§6.2).
type PruneSpan struct {
	From, To        uint64
	FirstTS, LastTS int64
	Coalesced       bool
}

// journalCap bounds the prune journal per stream (spec §6.2). When a new
// entry would exceed it, the two oldest are coalesced (union range, min/max
// ts) in the same batch — coverage of [1..LWM) stays complete forever,
// granularity degrades only for the oldest history.
const journalCap = 64

type journalEnc struct {
	To        uint64 `json:"to"`
	FirstTS   int64  `json:"ft"`
	LastTS    int64  `json:"lt"`
	Coalesced bool   `json:"c,omitempty"`
}

// PruneJournal returns the prune journal of a stream, oldest first.
func (s *Store) PruneJournal(stream string) []PruneSpan {
	spans, err := s.readJournal(stream)
	if err != nil {
		// Journals are validated at Open and every write goes through
		// writeJournal; corruption here means the disk changed under a
		// running process. Nothing sane to return — the next Open fails
		// loudly on it.
		return nil
	}
	return spans
}

// readJournal scans the journal entries of a stream in key order (ascending
// From). A malformed key or value is an error, the same fail-loud discipline
// as the l/ and b/ counters: a silently dropped entry would punch a hole in
// the [1..LWM) partition and misreport a gap's time span.
func (s *Store) readJournal(stream string) ([]PruneSpan, error) {
	lb, ub := journalBounds(stream)
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: lb, UpperBound: ub})
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	var out []PruneSpan
	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		if len(key) < 8 {
			return nil, fmt.Errorf("corrupt prune-journal key %q for stream %q", key, stream)
		}
		var e journalEnc
		if err := json.Unmarshal(iter.Value(), &e); err != nil {
			return nil, fmt.Errorf("corrupt prune-journal entry %q for stream %q: %w", key, stream, err)
		}
		out = append(out, PruneSpan{
			From:      binary.BigEndian.Uint64(key[len(key)-8:]),
			To:        e.To,
			FirstTS:   e.FirstTS,
			LastTS:    e.LastTS,
			Coalesced: e.Coalesced,
		})
	}
	return out, nil
}

func setJournal(b *pebble.Batch, stream string, span PruneSpan) error {
	val, err := json.Marshal(journalEnc{To: span.To, FirstTS: span.FirstTS, LastTS: span.LastTS, Coalesced: span.Coalesced})
	if err != nil {
		return err
	}
	return b.Set(journalKey(stream, span.From), val, nil)
}

// writeJournal adds one entry to the prune batch, coalescing the two oldest
// entries (union range, min/max ts, marked Coalesced) while the journal would
// exceed journalCap. Called under s.mu.
func (s *Store) writeJournal(b *pebble.Batch, stream string, span PruneSpan) error {
	existing, err := s.readJournal(stream)
	if err != nil {
		return err
	}
	entries := append(existing, span)
	merged := false
	for len(entries) > journalCap {
		next := entries[1]
		if err := b.Delete(journalKey(stream, next.From), nil); err != nil {
			return err
		}
		entries[1] = PruneSpan{
			From:      entries[0].From,
			To:        next.To,
			FirstTS:   min(entries[0].FirstTS, next.FirstTS),
			LastTS:    max(entries[0].LastTS, next.LastTS),
			Coalesced: true,
		}
		entries = entries[1:]
		merged = true
	}
	if merged {
		if err := setJournal(b, stream, entries[0]); err != nil {
			return err
		}
	}
	return setJournal(b, stream, span)
}

// Prune removes the contiguous prefix [LWM..upTo) of a stream in ONE atomic,
// synced batch: a single range tombstone, the advanced l/{stream}, the
// decremented b/{stream}, the journal entry, and any gapRecords appended at
// the head (ordinary records with KV semantics — the durable gap markers of
// spec §6.4 ride here so they survive their own prune run). Because it is one
// batch there is no "crash between delete and LWM" state: after a crash the
// stream is either fully pre-prune or fully post-prune (spec §4.2).
//
// upTo <= LWM is a no-op (0, nil). Unknown streams and upTo beyond the next
// offset (pruning the future) are errors. KV projections, cursors and HWMs
// are never touched. Returns the number of records removed.
func (s *Store) Prune(stream string, upTo uint64, gapRecords []Record) (uint64, error) {
	s.mu.Lock()
	next, lwm := s.next[stream], s.lwm[stream]
	s.mu.Unlock()
	if next == 0 {
		return 0, fmt.Errorf("unknown stream %q", stream)
	}
	if upTo <= lwm {
		return 0, nil
	}
	if upTo > next {
		return 0, fmt.Errorf("prune %q up to %d: beyond next offset %d", stream, upTo, next)
	}

	// Accounting scan over the doomed prefix [lwm..upTo), without the mutex
	// (spec §4.1: the mutex is for the commit, never the scan): Append writes
	// only at offsets >= next >= upTo, and the prefix can only shrink through
	// Prune itself, which the LWM recheck below serializes.
	var pruned, shed uint64
	var firstTS, lastTS int64
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: streamKey(stream, lwm),
		UpperBound: streamKey(stream, upTo),
	})
	if err != nil {
		return 0, err
	}
	for iter.First(); iter.Valid(); iter.Next() {
		var e recEnc
		if err := json.Unmarshal(iter.Value(), &e); err != nil {
			iter.Close()
			return 0, fmt.Errorf("decode record %q during prune: %w", iter.Key(), err)
		}
		if pruned == 0 || e.TS < firstTS {
			firstTS = e.TS
		}
		if pruned == 0 || e.TS > lastTS {
			lastTS = e.TS
		}
		shed += uint64(len(iter.Key()) + len(iter.Value()))
		pruned++
	}
	if err := iter.Close(); err != nil {
		return 0, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lwm[stream] != lwm {
		return 0, fmt.Errorf("concurrent prune on stream %q", stream)
	}
	b := s.db.NewBatch()
	defer b.Close()
	if err := b.DeleteRange(streamKey(stream, lwm), streamKey(stream, upTo), nil); err != nil {
		return 0, err
	}
	if err := b.Set(lwmKey(stream), be64(upTo), nil); err != nil {
		return 0, err
	}
	// A store upgraded from a pre-accounting version has records b/ never
	// counted; clamp at zero instead of underflowing — sizes are honest for
	// everything written since the counter existed. Note the clamp can also
	// absorb legitimately-counted bytes while pre-counter records are being
	// pruned out (counted and uncounted records share one counter); on this
	// greenfield branch no pre-counter store exists, so that state is
	// unreachable in practice.
	liveBytes := s.bytes[stream]
	if shed > liveBytes {
		liveBytes = 0
	} else {
		liveBytes -= shed
	}
	off := s.next[stream]
	for _, r := range gapRecords {
		n, err := addRecord(b, stream, off, r.Topic, r.Payload, r.TS, r.KVPath, r.KVNode)
		if err != nil {
			return 0, err
		}
		liveBytes += n
		off++
	}
	if off != s.next[stream] {
		if err := b.Set(metaKey(stream), be64(off), nil); err != nil {
			return 0, err
		}
	}
	if err := b.Set(bytesKey(stream), be64(liveBytes), nil); err != nil {
		return 0, err
	}
	if err := s.writeJournal(b, stream, PruneSpan{From: lwm, To: upTo - 1, FirstTS: firstTS, LastTS: lastTS}); err != nil {
		return 0, err
	}
	if err := s.db.Apply(b, pebble.Sync); err != nil {
		return 0, err
	}
	s.next[stream] = off
	s.lwm[stream] = upTo
	s.bytes[stream] = liveBytes
	return pruned, nil
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
