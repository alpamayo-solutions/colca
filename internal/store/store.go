// Package store is Colca's durable storage: append-only streams with gapless
// offsets on Pebble, and a KV projection written in the same batch as the
// records that produce it.
package store

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/pebble/v2"

	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// streams is the fixed set of streams a store maintains offsets for.
var streams = []string{"metrics", "entities", "commands", "definitions", "audit", "alarms", "annotations", "logs"}

// Streams returns the stream set. Other packages should ask for it instead of
// keeping a copy that silently misses a new stream. The result is a copy.
func Streams() []string {
	out := make([]string, len(streams))
	copy(out, streams)
	return out
}

// ErrRecordTooLarge is returned by Append when a payload exceeds the configured
// cap. Only ingress checks it: refusing a record a child already stored would
// block that child's uplink forever.
var ErrRecordTooLarge = errors.New("record payload exceeds the configured limit")

// ErrInvalidPageToken marks a malformed KV page token, or one issued for another
// prefix. Callers pass tokens back unchanged.
var ErrInvalidPageToken = errors.New("invalid KV page token")

type Record struct {
	// SourceLocalOnly is decided under the append lock from the current signal.
	SourceLocalOnly bool   `json:"slo,omitempty"`
	Topic           string `json:"t"`
	Payload         []byte `json:"p"`
	TS              int64  `json:"ts"`
	WrittenBy       string `json:"wb,omitempty"`
	OriginOffset    uint64 `json:"oo,omitempty"`
	ActorID         string `json:"aid,omitempty"`
	ActorLabel      string `json:"al,omitempty"`
	ActorKind       string `json:"ak,omitempty"`
	// ActorGroups are the groups a person's grants came from, kept so a replicated
	// command can be authorized again where it executes.
	ActorGroups []string `json:"ag,omitempty"`
	// optional KV projection written in the same atomic batch:
	KVPath string `json:"-"` // hierarchy path (segments after contract, post-mount)
	KVNode string `json:"-"` // node-id (level 4)
	// Delete makes the record a tombstone: the KV key (path, node, topic) is deleted
	// in the same batch instead of set, so retiring one contract leaves the others
	// at that path alone. The stream record is still appended. The engine sets it
	// for an empty payload on a KV-projecting class.
	Delete bool `json:"-"`
}

type StoredRecord struct {
	SourceLocalOnly bool
	Offset          uint64
	OriginOffset    uint64
	Topic           string
	Payload         []byte
	TS              int64
	WrittenBy       string
	ActorID         string
	ActorLabel      string
	ActorKind       string
	ActorGroups     []string
}

type KVEntry struct {
	Path, NodeID, Topic string
	Payload             []byte
	TS                  int64
	Offset              uint64
	OriginOffset        uint64
}

type Store struct {
	db    *pebble.DB
	mu    sync.Mutex
	next  map[string]uint64 // next offset per stream
	lwm   map[string]uint64 // low-water mark per stream: lowest retained offset
	bytes map[string]uint64 // live logical bytes per stream (stream key + encoded value)
	// appendApply is Pebble's atomic apply boundary. Keeping the bound method
	// injectable lets tests prove an apply failure changes neither stream nor KV.
	appendApply func(*pebble.Batch, *pebble.WriteOptions) error
	// maxRecordBytes is 0 (no cap) until SetMaxRecordBytes. It is set once before
	// the first append, so s.mu does not guard it.
	maxRecordBytes uint64
}

// SetMaxRecordBytes installs the ingress record cap. Call once at startup,
// before the doors are listening; 0 leaves the store uncapped.
func (s *Store) SetMaxRecordBytes(limit uint64) { s.maxRecordBytes = limit }

// Open opens or creates the store at dir and restores each stream's next offset,
// low-water mark and byte counter, so offsets stay gapless across restarts.
func Open(dir string) (*Store, error) {
	db, err := pebble.Open(dir, &pebble.Options{})
	if err != nil {
		return nil, err
	}
	s := &Store{
		db: db, next: map[string]uint64{}, lwm: map[string]uint64{}, bytes: map[string]uint64{},
		appendApply: db.Apply,
	}
	for _, stream := range streams {
		next, err := readCounter(db, metaKey(stream), 1, "meta", stream)
		if err != nil {
			_ = db.Close()
			return nil, err
		}
		lwm, err := readCounter(db, lwmKey(stream), 1, "low-water mark", stream)
		if err != nil {
			_ = db.Close()
			return nil, err
		}
		liveBytes, err := readCounter(db, bytesKey(stream), 0, "byte counter", stream)
		if err != nil {
			_ = db.Close()
			return nil, err
		}
		s.next[stream] = next
		s.lwm[stream] = lwm
		s.bytes[stream] = liveBytes
		// Validate the prune journal with the same fail-loud discipline: a
		// corrupt entry would misreport gap spans for as long as it lived.
		if _, err := s.readJournal(stream); err != nil {
			_ = db.Close()
			return nil, err
		}
		// A corrupt rp/ key read as "nothing pending" would silently drop a refresh a
		// crash left owed.
		if err := validateRefreshPending(db, stream); err != nil {
			_ = db.Close()
			return nil, err
		}
	}
	return s, nil
}

// validateRefreshPending checks that rp/{stream}, if present, holds a sane
// [From, To) range. Absent is fine (no refresh pending).
func validateRefreshPending(db *pebble.DB, stream string) error {
	v, closer, err := db.Get(rpKey(stream))
	if errors.Is(err, pebble.ErrNotFound) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read pending refresh for stream %q: %w", stream, err)
	}
	defer closer.Close()
	if len(v) != 16 {
		return fmt.Errorf("corrupt pending refresh for stream %q: %d bytes", stream, len(v))
	}
	if from, to := binary.BigEndian.Uint64(v[:8]), binary.BigEndian.Uint64(v[8:]); from >= to {
		return fmt.Errorf("corrupt pending refresh for stream %q: empty range [%d, %d)", stream, from, to)
	}
	return nil
}

// readCounter returns the 8-byte big-endian counter at key, or dflt if the key
// was never written. Anything else is an error: a made-up default would break
// offset continuity (m/), the pruned prefix (l/) or the byte accounting (b/).
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
	SourceLocalOnly bool     `json:"slo,omitempty"`
	Topic           string   `json:"t"`
	Payload         []byte   `json:"p"`
	TS              int64    `json:"ts"`
	WrittenBy       string   `json:"wb,omitempty"`
	OriginOffset    uint64   `json:"oo,omitempty"`
	ActorID         string   `json:"aid,omitempty"`
	ActorLabel      string   `json:"al,omitempty"`
	ActorKind       string   `json:"ak,omitempty"`
	ActorGroups     []string `json:"ag,omitempty"`
	// size is the encoded length of this record as stored, set by scanRecords for
	// the byte accounting. Not serialized.
	size uint64 `json:"-"`
}
type kvEnc struct {
	Topic        string `json:"t"`
	Payload      []byte `json:"p"`
	TS           int64  `json:"ts"`
	Offset       uint64 `json:"o"`
	OriginOffset uint64 `json:"oo,omitempty"`
}

// addRecord writes one stream record and its optional KV projection into the
// batch and returns its byte cost, len(key) + len(value), the unit b/{stream}
// counts. For a tombstone it deletes the KV key instead; deleting an absent key
// is a no-op, so a replayed tombstone is harmless.
func addRecord(b *pebble.Batch, stream string, off uint64, rec Record) (uint64, error) {
	originOffset := rec.OriginOffset
	if originOffset == 0 {
		originOffset = off
	}
	val, err := json.Marshal(recEnc{
		SourceLocalOnly: rec.SourceLocalOnly,
		Topic:           rec.Topic, Payload: rec.Payload, TS: rec.TS,
		WrittenBy: rec.WrittenBy, ActorID: rec.ActorID,
		ActorLabel: rec.ActorLabel, ActorKind: rec.ActorKind, ActorGroups: rec.ActorGroups, OriginOffset: originOffset,
	})
	if err != nil {
		return 0, err
	}
	key := streamKey(stream, off)
	if err := b.Set(key, val, nil); err != nil {
		return 0, err
	}
	if rec.KVPath != "" {
		if rec.Delete {
			if err := b.Delete(kvKey(rec.KVPath, rec.KVNode, rec.Topic), nil); err != nil {
				return 0, err
			}
		} else {
			kval, err := json.Marshal(kvEnc{
				Topic: rec.Topic, Payload: rec.Payload, TS: rec.TS,
				Offset: off, OriginOffset: originOffset,
			})
			if err != nil {
				return 0, err
			}
			if err := b.Set(kvKey(rec.KVPath, rec.KVNode, rec.Topic), kval, nil); err != nil {
				return 0, err
			}
		}
	}
	return uint64(len(key) + len(val)), nil
}

// Append writes records, meta, the byte counter and KV projections in one
// atomic, synced batch.
func (s *Store) Append(stream string, recs []Record) (first, last uint64, err error) {
	if len(recs) == 0 {
		return 0, 0, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.appendLocked(stream, recs)
}

// appendLocked is Append's body; the caller holds s.mu.
func (s *Store) appendLocked(stream string, recs []Record) (first, last uint64, err error) {
	off := s.next[stream]
	if off == 0 {
		return 0, 0, fmt.Errorf("unknown stream %q", stream)
	}
	if s.maxRecordBytes > 0 {
		for i, rec := range recs {
			if uint64(len(rec.Payload)) > s.maxRecordBytes {
				return 0, 0, fmt.Errorf("stream %q record %d (%s): %d bytes > %d: %w",
					stream, i, rec.Topic, len(rec.Payload), s.maxRecordBytes, ErrRecordTooLarge)
			}
		}
	}
	first = off
	b := s.db.NewBatch()
	defer b.Close()
	liveBytes := s.bytes[stream]
	for _, r := range recs {
		if stream == "metrics" {
			var err error
			r.SourceLocalOnly, err = s.metricSourceLocalOnly(r)
			if err != nil {
				return 0, 0, err
			}
		}
		n, err := addRecord(b, stream, off, r)
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
	if err := s.appendApply(b, pebble.Sync); err != nil {
		return 0, 0, err
	}
	s.next[stream] = off
	s.bytes[stream] = liveBytes
	return first, last, nil
}

// kvOffsets returns the local and origin offsets of the current KV entry for
// (path, node, topic); ok is false when the key is absent or undecodable. The
// caller holds s.mu.
func (s *Store) kvOffsets(path, node, topic string) (local, origin uint64, ok bool) {
	v, closer, err := s.db.Get(kvKey(path, node, topic))
	if err != nil {
		return 0, 0, false
	}
	defer closer.Close()
	var e kvEnc
	if json.Unmarshal(v, &e) != nil {
		return 0, 0, false
	}
	origin = e.OriginOffset
	if origin == 0 {
		origin = e.Offset
	}
	return e.Offset, origin, true
}

// AppendIfKVUnchanged appends rec (stream record and KV projection) only if the
// KV entry for (rec.KVPath, rec.KVNode, rec.Topic) still has Offset ==
// ifKVOffset. The check runs under s.mu, so it is a real compare-and-swap.
// Retention's state refresh relies on it: a refresh of a snapshot that a
// tombstone or newer write has superseded is skipped entirely (applied=false).
// rec must carry a KV projection.
func (s *Store) AppendIfKVUnchanged(stream string, rec Record, ifKVOffset uint64) (off uint64, applied bool, err error) {
	if rec.KVPath == "" {
		return 0, false, fmt.Errorf("guarded append requires a KV projection")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, origin, ok := s.kvOffsets(rec.KVPath, rec.KVNode, rec.Topic)
	if !ok || cur != ifKVOffset {
		return 0, false, nil // retired or superseded since the snapshot — skip entirely
	}
	// A refresh restates the owner's version. Keeping the origin offset stops an
	// ancestor's local refresh from posing as a new Edit version there.
	rec.OriginOffset = origin
	first, _, err := s.appendLocked(stream, []Record{rec})
	if err != nil {
		return 0, false, err
	}
	return first, true, nil
}

func (s *Store) NextOffset(stream string) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.next[stream]
}

// Read returns up to limit records with Offset >= from, optionally filtered by
// topic. Views that need the payload use ReadRecords.
func (s *Store) Read(stream string, from uint64, limit int, filter func(string) bool) (out []StoredRecord, next uint64, err error) {
	var recordFilter func(StoredRecord) bool
	if filter != nil {
		recordFilter = func(record StoredRecord) bool { return filter(record.Topic) }
	}
	return s.ReadRecords(stream, from, limit, recordFilter)
}

// ReadRecords returns up to limit matching records with Offset >= from. next is
// the last scanned position + 1, filtered records included, so a consumer of a
// sparse view keeps moving forward.
func (s *Store) ReadRecords(stream string, from uint64, limit int, filter func(StoredRecord) bool) (out []StoredRecord, next uint64, err error) {
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: streamKey(stream, from),
		UpperBound: streamKey(stream, ^uint64(0)),
	})
	if err != nil {
		return nil, from, err
	}
	defer iter.Close()
	next = from
	for iter.First(); iter.Valid() && len(out) < limit; iter.Next() {
		key := iter.Key()
		off := binary.BigEndian.Uint64(key[len(key)-8:])
		var e recEnc
		if err := json.Unmarshal(iter.Value(), &e); err != nil {
			return nil, next, err
		}
		next = off + 1
		record := StoredRecord{
			SourceLocalOnly: e.SourceLocalOnly,
			Offset:          off, OriginOffset: originOffset(e.OriginOffset, off),
			Topic: e.Topic, Payload: e.Payload, TS: e.TS,
			WrittenBy: e.WrittenBy, ActorID: e.ActorID,
			ActorLabel: e.ActorLabel, ActorKind: e.ActorKind, ActorGroups: e.ActorGroups,
		}
		if filter != nil && !filter(record) {
			continue
		}
		out = append(out, record)
	}
	if err := iter.Error(); err != nil {
		return nil, next, err
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

// CursorAck moves the cursor forward only and reports whether it moved. The
// cursor and its last-advance timestamp (ct/) are written in one synced batch.
func (s *Store) CursorAck(name, stream string, off uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if off <= s.readU64(cursorKey(name, stream), 1) {
		return false
	}
	b := s.db.NewBatch()
	defer b.Close()
	if err := b.Set(cursorKey(name, stream), be64(off), nil); err != nil {
		return false
	}
	if err := b.Set(ctKey(name, stream), be64(uint64(time.Now().UnixMilli())), nil); err != nil {
		return false
	}
	if err := s.db.Apply(b, pebble.Sync); err != nil {
		return false
	}
	return true
}

// CursorSetIfAbsent creates a cursor at off unless one exists and reports
// whether it did. It can record position 1, which CursorAck refuses: CursorGet
// returns 1 for a missing cursor too, and replication must tell "never met this
// parent" from "still at its first offset". The cursor and its ct/ timestamp go
// in one synced batch; a failed write is returned as an error, distinct from
// "already existed".
func (s *Store) CursorSetIfAbsent(name, stream string, off uint64) (created bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, closer, err := s.db.Get(cursorKey(name, stream)); err == nil {
		closer.Close()
		return false, nil
	}
	b := s.db.NewBatch()
	defer b.Close()
	if err := b.Set(cursorKey(name, stream), be64(off), nil); err != nil {
		return false, err
	}
	if err := b.Set(ctKey(name, stream), be64(uint64(time.Now().UnixMilli())), nil); err != nil {
		return false, err
	}
	if err := s.db.Apply(b, pebble.Sync); err != nil {
		return false, err
	}
	return true, nil
}

// CursorDelete removes a cursor and its last-advance timestamp in one synced
// batch. Deleting an absent cursor is a no-op, so a repeated revoke is fine.
func (s *Store) CursorDelete(name, stream string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	b := s.db.NewBatch()
	defer b.Close()
	if err := b.Delete(cursorKey(name, stream), nil); err != nil {
		return err
	}
	if err := b.Delete(ctKey(name, stream), nil); err != nil {
		return err
	}
	return s.db.Apply(b, pebble.Sync)
}

// CursorMarkSeen sets ts (unix ms) as the last-advance time of a cursor that has
// none yet, so a cursor from before ct/ timestamps gets a full staleness window
// from its first sighting. Synced, so a restart does not reset that clock.
func (s *Store) CursorMarkSeen(name, stream string, ts int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.readU64(ctKey(name, stream), 0) != 0 {
		return
	}
	if err := s.db.Set(ctKey(name, stream), be64(uint64(ts)), pebble.Sync); err != nil { //nolint:gosec // an int64 timestamp stored bit for bit
		// Not fatal, since the caller treats the cursor as fresh either way, but
		// logged: an unpersisted sighting resets the staleness clock on restart.
		slog.Warn("store: persisting cursor first-sighting timestamp failed",
			"cursor", name, "stream", stream, "err", err)
	}
}

// HWMGet returns the highest child offset already applied for (child, stream),
// or 0 when nothing from that child has been applied yet.
func (s *Store) HWMGet(child, stream string) uint64 {
	return s.readU64(hwmKey(child, stream), 0)
}

// CursorInfo is a persisted consumer cursor: the next offset the consumer reads.
// LastAdvanceMS is the unix-ms time of its last advance, or 0 if none was ever
// recorded (see CursorMarkSeen).
type CursorInfo struct {
	Name, Stream  string
	Position      uint64
	LastAdvanceMS int64
}

// HWMInfo is one replication high-water mark: the highest child offset already
// applied locally for (child, stream).
type HWMInfo struct {
	Child, Stream string
	HWM           uint64
}

// scanU64Pairs calls fn for every {prefix}\x00{first}\x00{second} key holding
// an 8-byte counter, skipping malformed entries. The upper bound is
// {prefix, 0x01} so key families sharing the first byte (ct/ inside c/) stay out.
func (s *Store) scanU64Pairs(prefix byte, fn func(first, second string, v uint64)) error {
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte{prefix, 0x00},
		UpperBound: []byte{prefix, 0x01},
	})
	if err != nil {
		return err
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
	return iter.Error()
}

// Cursors returns every persisted cursor. Read-only.
func (s *Store) Cursors() []CursorInfo {
	var out []CursorInfo
	err := s.scanU64Pairs('c', func(name, stream string, v uint64) {
		out = append(out, CursorInfo{
			Name: name, Stream: stream, Position: v,
			LastAdvanceMS: int64(s.readU64(ctKey(name, stream), 0)), //nolint:gosec // stored bit for bit
		})
	})
	if err != nil {
		slog.Warn("store: cursor scan stopped early", "err", err)
	}
	return out
}

// ProtectedCursors splits stream's cursors at now into those still protecting
// the stream (inside the staleness window, or window <= 0 meaning never
// override) and stale ones. The pruner and the metrics collector share it so
// they classify alike. A cursor without a timestamp counts as advancing now, so
// it protects; its CursorInfo comes back unchanged and the pruner persists the
// stamp with CursorMarkSeen. This method never writes.
func (s *Store) ProtectedCursors(stream string, now time.Time, window time.Duration) (protecting, stale []CursorInfo) {
	for _, c := range s.Cursors() {
		if c.Stream != stream {
			continue
		}
		last := c.LastAdvanceMS
		if last == 0 {
			last = now.UnixMilli()
		}
		if window > 0 && now.Sub(time.UnixMilli(last)) > window {
			stale = append(stale, c)
			continue
		}
		protecting = append(protecting, c)
	}
	return protecting, stale
}

// HWMs returns every persisted replication high-water mark. Read-only.
func (s *Store) HWMs() []HWMInfo {
	var out []HWMInfo
	err := s.scanU64Pairs('h', func(child, stream string, v uint64) {
		out = append(out, HWMInfo{Child: child, Stream: stream, HWM: v})
	})
	if err != nil {
		slog.Warn("store: high-water mark scan stopped early", "err", err)
	}
	return out
}

// ReplRecord is a record as it travels from a child node to its parent. The
// json tags are the wire format — do not rename them.
type ReplRecord struct {
	// SkipFrom marks an inclusive intentionally omitted metric range ending at ChildOffset.
	// Such entries advance the child HWM but never create a local stream record.
	SkipFrom     uint64   `json:"skip_from,omitempty"`
	ChildOffset  uint64   `json:"o"`
	OriginOffset uint64   `json:"oo,omitempty"`
	Topic        string   `json:"t"`
	Payload      []byte   `json:"p"`
	TS           int64    `json:"ts"`
	WrittenBy    string   `json:"wb,omitempty"`
	ActorID      string   `json:"aid,omitempty"`
	ActorLabel   string   `json:"al,omitempty"`
	ActorKind    string   `json:"ak,omitempty"`
	ActorGroups  []string `json:"ag,omitempty"`
	KVPath       string   `json:"kp,omitempty"`
	KVNode       string   `json:"kn,omitempty"`
	// Delete mirrors Record.Delete. The parent derives it after decoding, as the
	// engine does, from an empty payload on a KV-projecting class; it is not a wire
	// field.
	Delete bool `json:"-"`
}

// ApplyReplicated appends records with ChildOffset > HWM(child, stream) under
// local offsets and updates KV and the HWM in one atomic batch, so replays are
// harmless. It returns newly applied records and skip ranges, or nil on error.
// The caller mirrors only data records onto the local MQTT bus.
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
		if r.SkipFrom != 0 && (stream != "metrics" || r.SkipFrom > r.ChildOffset || r.Topic != "" || len(r.Payload) != 0) {
			return nil, prev, fmt.Errorf("invalid metric skip range")
		}
		if r.ChildOffset <= hwm {
			continue
		}
		if r.SkipFrom != 0 {
			applied = append(applied, r)
			hwm = r.ChildOffset
			continue
		}
		if r.OriginOffset == 0 {
			// Compatibility with a direct/legacy child: at the first hop its
			// child offset is the owner-authored coordinate.
			r.OriginOffset = r.ChildOffset
		}
		n, err := addRecord(b, stream, off, Record{
			Topic: r.Topic, Payload: r.Payload, TS: r.TS,
			WrittenBy: r.WrittenBy, ActorID: r.ActorID,
			ActorLabel: r.ActorLabel, ActorKind: r.ActorKind, ActorGroups: r.ActorGroups,
			OriginOffset: r.OriginOffset,
			KVPath:       r.KVPath, KVNode: r.KVNode, Delete: r.Delete,
		})
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

// LWM returns the low-water mark of a stream, the lowest retained offset.
// Streams start at 1, and pruning removes the prefix [1..LWM).
func (s *Store) LWM(stream string) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lwm[stream]
}

// StreamBytes returns the live logical bytes of a stream, len(key) + len(value)
// over retained records, kept current inside the append and prune batches.
func (s *Store) StreamBytes(stream string) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.bytes[stream]
}

// PruneSpan is one prune-journal entry: an inclusive offset range [From..To] a
// prune removed, with the time span of those records. The entries partition
// [1..LWM). A Coalesced entry was merged from older ones, so its time span is
// approximate.
type PruneSpan struct {
	From, To        uint64
	FirstTS, LastTS int64
	Coalesced       bool
	// Shed is the bytes this commit removed after the in-batch recheck. It is only
	// set on the span Prune passes to its plan callback; spans read back from the
	// journal leave it zero.
	Shed uint64
}

// GapSpan is the gap object on the wire (/fetch and GET /downlink): the pruned
// range [FromOffset..ToOffset] below a consumer's position and the time span of
// the removed records. Approx is set when a coalesced journal entry supplied
// FirstTS. The json tags are the wire format.
type GapSpan struct {
	Stream     string `json:"stream"`
	FromOffset uint64 `json:"from_offset"`
	ToOffset   uint64 `json:"to_offset"`
	FirstTS    int64  `json:"first_ts"`
	LastTS     int64  `json:"last_ts"`
	Approx     bool   `json:"approx"`
}

// Gap reports the pruned range a consumer at position (the next offset it would
// read) has missed; ok is false when position >= LWM. Pruning removes only the
// prefix, so the range is [position..LWM-1], timed from the journal. An unknown
// stream has LWM 0 and no gap, and position 0 counts as 1.
func (s *Store) Gap(stream string, position uint64) (GapSpan, bool) {
	lwm := s.LWM(stream)
	if position < 1 {
		position = 1 // cursor positions start at 1; the journal starts there too
	}
	if lwm == 0 || position >= lwm {
		return GapSpan{}, false // lwm == 0: unknown stream (a real stream's LWM is >= 1)
	}
	g := GapSpan{Stream: stream, FromOffset: position, ToOffset: lwm - 1}
	for _, sp := range s.PruneJournal(stream) {
		if g.FromOffset >= sp.From && g.FromOffset <= sp.To {
			g.FirstTS = sp.FirstTS
			g.Approx = sp.Coalesced // a coalesced entry blurs first_ts
		}
		if g.ToOffset >= sp.From && g.ToOffset <= sp.To {
			g.LastTS = sp.LastTS
		}
	}
	return g, true
}

// journalCap bounds the prune journal per stream. Past it the two oldest entries
// are merged, so the journal still covers [1..LWM) and only old history loses
// detail.
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
		// Journals are validated at Open and only written through writeJournal, so
		// corruption here means the disk changed underneath us; the next Open reports
		// it.
		return nil
	}
	return spans
}

// readJournal returns the journal entries of a stream in key order. A malformed
// entry is an error: dropping it would leave a hole in [1..LWM) and misreport a
// gap's time span.
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
	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("read prune journal for stream %q: %w", stream, err)
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

// writeJournal adds an entry to the prune batch, merging the two oldest entries
// while the journal would exceed journalCap. The caller holds s.mu.
func (s *Store) writeJournal(b *pebble.Batch, stream string, span PruneSpan) error {
	entries, err := s.readJournal(stream)
	if err != nil {
		return err
	}
	entries = append(entries, span)
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

// pruneStats is the accounting of a doomed prefix: record count, shed logical
// bytes and the time span, gathered by scanDoomed.
type pruneStats struct {
	pruned, shed    uint64
	firstTS, lastTS int64
}

// scanDoomed accumulates the accounting stats of the prefix [from, upTo).
func (s *Store) scanDoomed(stream string, from, upTo uint64) (pruneStats, error) {
	var st pruneStats
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: streamKey(stream, from),
		UpperBound: streamKey(stream, upTo),
	})
	if err != nil {
		return st, err
	}
	for iter.First(); iter.Valid(); iter.Next() {
		var e recEnc
		if err := json.Unmarshal(iter.Value(), &e); err != nil {
			iter.Close()
			return st, fmt.Errorf("decode record %q during prune: %w", iter.Key(), err)
		}
		if st.pruned == 0 || e.TS < st.firstTS {
			st.firstTS = e.TS
		}
		if st.pruned == 0 || e.TS > st.lastTS {
			st.lastTS = e.TS
		}
		st.shed += uint64(len(iter.Key()) + len(iter.Value()))
		st.pruned++
	}
	if err := iter.Close(); err != nil {
		return st, err
	}
	return st, nil
}

// RefreshRange is an owed state refresh: KV entries with Offset in [From, To)
// must be appended again. It is stored as rp/{stream} in the prune batch and
// cleared only after every refresh append succeeded, so a crash cannot lose it.
type RefreshRange struct {
	From, To uint64
}

// PruneOutcome is what a plan callback adds to the prune batch: gap markers
// appended at the head, and optionally a refresh range stored as rp/{stream}.
type PruneOutcome struct {
	GapRecords []Record
	Refresh    *RefreshRange
}

// Prune removes the prefix [LWM..upTo) of a stream in one atomic, synced batch
// (range tombstone, l/ and b/ counters, journal entry, and whatever plan adds),
// so a crash leaves the stream either fully pruned or untouched.
//
// Under the mutex, which CursorAck also takes, upTo shrinks to any cursor not in
// overridden: a consumer that acks after the caller's policy decision is never
// pruned past. plan runs once per committing prune with the effective span and
// must not call back into the store. Unknown streams and upTo beyond the next
// offset are errors; KV entries, cursors and HWMs are untouched. Prune returns
// the number of records removed.
func (s *Store) Prune(stream string, upTo uint64, overridden []string, plan func(span PruneSpan) PruneOutcome) (uint64, error) {
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

	// Scan the doomed prefix without the mutex: appends only write at offsets >=
	// upTo, and the prefix only shrinks through Prune, which the LWM recheck below
	// serializes.
	stats, err := s.scanDoomed(stream, lwm, upTo)
	if err != nil {
		return 0, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lwm[stream] != lwm {
		return 0, fmt.Errorf("concurrent prune on stream %q", stream)
	}
	// Recheck under the mutex: shrink upTo to the floor of every cursor the caller
	// did not override.
	ov := make(map[string]bool, len(overridden))
	for _, name := range overridden {
		ov[name] = true
	}
	if err := s.scanU64Pairs('c', func(name, cstream string, pos uint64) {
		if cstream != stream || ov[name] {
			return
		}
		if pos < upTo {
			upTo = pos
		}
	}); err != nil {
		return 0, err
	}
	if upTo <= lwm {
		return 0, nil // a live cursor moved into the doomed range: nothing may go
	}
	if stats.pruned != upTo-lwm {
		// A cursor moved into the range after the scan. Rescan the smaller range so the
		// journal and plan see exactly what gets deleted.
		stats, err = s.scanDoomed(stream, lwm, upTo)
		if err != nil {
			return 0, err
		}
	}
	span := PruneSpan{From: lwm, To: upTo - 1, FirstTS: stats.firstTS, LastTS: stats.lastTS, Shed: stats.shed}
	var out PruneOutcome
	if plan != nil {
		out = plan(span)
	}
	b := s.db.NewBatch()
	defer b.Close()
	if err := b.DeleteRange(streamKey(stream, lwm), streamKey(stream, upTo), nil); err != nil {
		return 0, err
	}
	if err := b.Set(lwmKey(stream), be64(upTo), nil); err != nil {
		return 0, err
	}
	// Records written before the byte counter existed were never counted, so clamp
	// at zero instead of underflowing.
	liveBytes := s.bytes[stream]
	if stats.shed > liveBytes {
		liveBytes = 0
	} else {
		liveBytes -= stats.shed
	}
	off := s.next[stream]
	for _, r := range out.GapRecords {
		n, err := addRecord(b, stream, off, r)
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
	if out.Refresh != nil {
		// Store the refresh obligation in the prune batch so it survives a crash, merged
		// with any range still pending.
		r := *out.Refresh
		if cur, ok := s.refreshPending(stream); ok {
			r.From = min(r.From, cur.From)
			r.To = max(r.To, cur.To)
		}
		val := append(be64(r.From), be64(r.To)...)
		if err := b.Set(rpKey(stream), val, nil); err != nil {
			return 0, err
		}
	}
	if err := b.Set(bytesKey(stream), be64(liveBytes), nil); err != nil {
		return 0, err
	}
	if err := s.writeJournal(b, stream, span); err != nil {
		return 0, err
	}
	if err := s.db.Apply(b, pebble.Sync); err != nil {
		return 0, err
	}
	s.next[stream] = off
	s.lwm[stream] = upTo
	s.bytes[stream] = liveBytes
	return stats.pruned, nil
}

// refreshPending reads rp/{stream} without locking (callers hold s.mu or are
// the single pruner goroutine). ok=false when no refresh is pending.
func (s *Store) refreshPending(stream string) (RefreshRange, bool) {
	v, closer, err := s.db.Get(rpKey(stream))
	if err != nil {
		return RefreshRange{}, false
	}
	defer closer.Close()
	if len(v) != 16 {
		return RefreshRange{}, false
	}
	return RefreshRange{
		From: binary.BigEndian.Uint64(v[:8]),
		To:   binary.BigEndian.Uint64(v[8:]),
	}, true
}

// RefreshPending returns the stream's pending state-refresh range, if any.
func (s *Store) RefreshPending(stream string) (RefreshRange, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.refreshPending(stream)
}

// ClearRefreshPending removes the stream's refresh obligation. Call it only
// after every refresh append of the range has succeeded.
func (s *Store) ClearRefreshPending(stream string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.Delete(rpKey(stream), pebble.Sync)
}

// ScanRecords calls fn for each record of stream in [from, upTo), in offset
// order, with its offset, timestamp and byte cost (the unit b/{stream} counts),
// until fn returns false. It takes no mutex; Pebble iterators are consistent
// snapshots. A record that does not decode is an error.
func (s *Store) ScanRecords(stream string, from, upTo uint64, fn func(off uint64, ts int64, size uint64) bool) error {
	if upTo <= from {
		return nil
	}
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: streamKey(stream, from),
		UpperBound: streamKey(stream, upTo),
	})
	if err != nil {
		return err
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		off := binary.BigEndian.Uint64(key[len(key)-8:])
		var e recEnc
		if err := json.Unmarshal(iter.Value(), &e); err != nil {
			return fmt.Errorf("decode record %q during scan: %w", key, err)
		}
		if !fn(off, e.TS, uint64(len(key)+len(iter.Value()))) {
			break
		}
	}
	return iter.Error()
}

// DefaultPolicyScanCap bounds how many records one PolicyPruneTarget call
// decodes. With no cursor to stop it the walk would be unbounded, which is the
// stuck-consumer case the blocked-by-cursor metric reports. 100k records take on
// the order of 100 ms, well inside a scrape timeout.
const DefaultPolicyScanCap = 100_000

// PolicyPruneTarget walks stream forward from lwm and returns the offset the
// retention policy alone would prune up to: records older than maxAge, and
// enough bytes to bring the stream under maxBytes (maxAge <= 0 or maxBytes == 0
// disables either). It never passes clamp or examines more than maxScan records
// (0 means no limit). The pruner passes the cursor floor as clamp; the metrics
// collector passes next to see the policy without cursors.
//
// clampedAtCap and hitScanCap report which limit stopped a policy that wanted
// more. Either way target is a floor: safe to prune to, possibly short of the
// policy's real target.
func (s *Store) PolicyPruneTarget(stream string, lwm, next uint64, now time.Time, maxAge time.Duration, maxBytes, liveBytes, clamp, maxScan uint64) (target uint64, clampedAtCap, hitScanCap bool, err error) {
	cutoff := now.UnixMilli() - maxAge.Milliseconds()
	target = lwm
	var shed, scanned uint64
	err = s.ScanRecords(stream, lwm, next, func(off uint64, ts int64, size uint64) bool {
		if maxScan > 0 && scanned >= maxScan {
			hitScanCap = true // policy wants more, the scan budget forbids it
			return false
		}
		scanned++
		ageWants := maxAge > 0 && ts < cutoff
		sizeWants := maxBytes > 0 && liveBytes > shed+maxBytes // liveBytes−shed > maxBytes, underflow-safe
		if !ageWants && !sizeWants {
			return false // first record the policy keeps
		}
		if off >= clamp {
			clampedAtCap = true // policy wants more, the cursor cap forbids it
			return false
		}
		shed += size
		target = off + 1
		return true
	})
	return target, clampedAtCap, hitScanCap, err
}

// KVScan returns the KV projection for every path starting with prefix (all of
// it for ""), one entry per contract at a node and path. An error means the
// result may be incomplete, whether the iterator failed to open or stopped on a
// storage fault. Callers must not treat that as empty: the blob sweeper would
// delete files that are still referenced.
func (s *Store) KVScan(prefix string) ([]KVEntry, error) {
	lb := kvPrefix(prefix)
	ub := append(append([]byte{}, lb...), 0xFF)
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: lb, UpperBound: ub})
	if err != nil {
		return nil, fmt.Errorf("store: kv scan %q: open iterator: %w", prefix, err)
	}
	defer iter.Close()
	var out []KVEntry
	for iter.First(); iter.Valid(); iter.Next() {
		key := string(iter.Key()[2:]) // strip "k\x00"
		// The key is path \x00 node \x00 topic. Keeping the topic in the key makes the
		// contract part of replacement and deletion without changing path-first scans.
		pathSep := strings.IndexByte(key, 0)
		if pathSep < 0 {
			continue
		}
		rest := key[pathSep+1:]
		nodeSep := strings.IndexByte(rest, 0)
		if nodeSep < 0 {
			continue
		}
		var e kvEnc
		if json.Unmarshal(iter.Value(), &e) != nil {
			continue
		}
		out = append(out, KVEntry{
			Path:         key[:pathSep],
			NodeID:       rest[:nodeSep],
			Topic:        e.Topic,
			Payload:      e.Payload,
			TS:           e.TS,
			Offset:       e.Offset,
			OriginOffset: originOffset(e.OriginOffset, e.Offset),
		})
	}
	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("store: kv scan %q: %w", prefix, err)
	}
	return out, nil
}

// KVScanPage returns at most limit KV entries and an opaque continuation token.
// The limit applies before authorization filtering, so a sparse grant cannot
// turn one request into a full walk. A non-empty contracts keeps only those
// contracts; the check reads the topic from the key, so other entries are
// skipped without decoding their payload.
func (s *Store) KVScanPage(prefix, after string, limit int, contracts []string) ([]KVEntry, string, error) {
	if limit <= 0 {
		return nil, "", fmt.Errorf("store: KV page size must be positive")
	}
	var want map[string]bool
	if len(contracts) > 0 {
		want = make(map[string]bool, len(contracts))
		for _, c := range contracts {
			want[c] = true
		}
	}
	lb := kvPrefix(prefix)
	ub := append(append([]byte{}, lb...), 0xFF)
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: lb, UpperBound: ub})
	if err != nil {
		return nil, "", fmt.Errorf("store: kv page %q: open iterator: %w", prefix, err)
	}
	defer iter.Close()

	valid := iter.First()
	if after != "" {
		raw, decodeErr := base64.RawURLEncoding.DecodeString(after)
		if decodeErr != nil || bytes.Compare(raw, lb) < 0 || bytes.Compare(raw, ub) >= 0 {
			return nil, "", ErrInvalidPageToken
		}
		valid = iter.SeekGE(raw)
		if valid && bytes.Equal(iter.Key(), raw) {
			valid = iter.Next()
		}
	}

	// A large limit must not preallocate a large slice; append grows it as needed.
	const maxPrealloc = 1000
	capacity := limit
	if capacity > maxPrealloc {
		capacity = maxPrealloc
	}
	out := make([]KVEntry, 0, capacity)
	matched := 0
	var lastKey []byte
	for valid && matched < limit {
		lastKey = append(lastKey[:0], iter.Key()...)
		key := string(iter.Key()[2:]) // strip "k\x00"
		if pathSep := strings.IndexByte(key, 0); pathSep >= 0 {
			rest := key[pathSep+1:]
			if nodeSep := strings.IndexByte(rest, 0); nodeSep >= 0 {
				topic := rest[nodeSep+1:]
				if kvContractMatches(topic, want) {
					var e kvEnc
					if json.Unmarshal(iter.Value(), &e) == nil {
						out = append(out, KVEntry{
							Path: key[:pathSep], NodeID: rest[:nodeSep], Topic: e.Topic,
							Payload: e.Payload, TS: e.TS, Offset: e.Offset,
							OriginOffset: originOffset(e.OriginOffset, e.Offset),
						})
						matched++
					}
				}
			}
		}
		valid = iter.Next()
	}
	if err := iter.Error(); err != nil {
		return nil, "", fmt.Errorf("store: kv page %q: %w", prefix, err)
	}
	if valid && len(lastKey) > 0 {
		return out, base64.RawURLEncoding.EncodeToString(lastKey), nil
	}
	return out, "", nil
}

// kvContractMatches reports whether topic's contract is in want; a nil want
// matches everything. uns.Parse owns the topic grammar, so it does the parsing.
func kvContractMatches(topic string, want map[string]bool) bool {
	if want == nil {
		return true
	}
	parsed, err := uns.Parse(topic)
	if err != nil {
		return false
	}
	return want[parsed.Contract]
}

func originOffset(origin, local uint64) uint64 {
	if origin != 0 {
		return origin
	}
	return local
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
		level += m.Levels[i].TableBytesFlushed + m.Levels[i].TableBytesCompacted
	}
	return DiskMetrics{
		WALBytesWritten:   m.WAL.BytesWritten,
		LevelBytesWritten: level,
		DiskUsageBytes:    m.DiskSpaceUsage(),
	}
}
