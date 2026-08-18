// Package store implements Colca's durable storage: append-only streams with
// gapless offsets on top of Pebble, plus a KV projection written atomically
// with the records that produce it.
package store

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/pebble/v2"
)

// streams is the fixed set of streams a store maintains offsets for.
var streams = []string{"metrics", "entities", "commands", "definitions"}

type Record struct {
	Topic   string `json:"t"`
	Payload []byte `json:"p"`
	TS      int64  `json:"ts"`
	// optional KV projection written in the same atomic batch:
	KVPath string `json:"-"` // hierarchy path (segments after contract, post-mount)
	KVNode string `json:"-"` // node-id (level 4)
	// Delete marks the record as a tombstone (retention design §7.1): the KV
	// key k/{KVPath}\x00{KVNode} is DELETED in the same atomic batch instead of
	// set. The stream record itself is appended as usual — the retirement is
	// history. Set by the engine on an empty payload for a KV-projecting class.
	Delete bool `json:"-"`
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
		// Same for the pending-refresh range: a corrupt rp/ silently read as
		// "nothing pending" would drop a crash-persisted refresh obligation —
		// exactly the loss the key exists to prevent.
		if err := validateRefreshPending(db, stream); err != nil {
			db.Close()
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
	// size is the encoded length of THIS record as stored, filled in by
	// scanRecords. Never serialized — it is what the byte accounting needs and
	// only the reader can know it.
	size uint64 `json:"-"`
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
//
// del is the tombstone flag (retention design §7.1): the KV key is deleted in
// the batch instead of set. Deleting an absent key is a no-op in Pebble, so a
// replayed tombstone is idempotent by construction.
func addRecord(b *pebble.Batch, stream string, off uint64, topic string, payload []byte, ts int64, kvPath, kvNode string, del bool) (uint64, error) {
	val, err := json.Marshal(recEnc{Topic: topic, Payload: payload, TS: ts})
	if err != nil {
		return 0, err
	}
	key := streamKey(stream, off)
	if err := b.Set(key, val, nil); err != nil {
		return 0, err
	}
	if kvPath != "" {
		if del {
			if err := b.Delete(kvKey(kvPath, kvNode), nil); err != nil {
				return 0, err
			}
		} else {
			kval, err := json.Marshal(kvEnc{topic, payload, ts, off})
			if err != nil {
				return 0, err
			}
			if err := b.Set(kvKey(kvPath, kvNode), kval, nil); err != nil {
				return 0, err
			}
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
	return s.appendLocked(stream, recs)
}

// appendLocked is Append's body; the caller holds s.mu.
func (s *Store) appendLocked(stream string, recs []Record) (first, last uint64, err error) {
	off := s.next[stream]
	if off == 0 {
		return 0, 0, fmt.Errorf("unknown stream %q", stream)
	}
	first = off
	b := s.db.NewBatch()
	defer b.Close()
	liveBytes := s.bytes[stream]
	for _, r := range recs {
		n, err := addRecord(b, stream, off, r.Topic, r.Payload, r.TS, r.KVPath, r.KVNode, r.Delete)
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

// kvOffset returns the Offset field of the current KV entry for (path, node),
// ok=false when the key is absent or undecodable. Callers hold s.mu.
func (s *Store) kvOffset(path, node string) (uint64, bool) {
	v, closer, err := s.db.Get(kvKey(path, node))
	if err != nil {
		return 0, false
	}
	defer closer.Close()
	var e kvEnc
	if json.Unmarshal(v, &e) != nil {
		return 0, false
	}
	return e.Offset, true
}

// AppendIfKVUnchanged appends rec — stream record AND KV projection — only if
// the current KV entry for (rec.KVPath, rec.KVNode) still exists with Offset
// == ifKVOffset. The guard is evaluated under s.mu, the same mutex every KV
// write serializes on, so it is a true compare-and-swap: nothing can retire or
// supersede the entry between the check and the batch application.
//
// This is the §6.5 state refresh's append path (spec §6.5 [delta]) and its
// only intended caller: a refresh re-states a KV snapshot, and a tombstone
// (§7) or newer write landing after that snapshot makes the re-statement
// stale — applying it would resurrect a retired path or clobber the newer
// value. When the guard fails the WHOLE record is skipped (applied=false, no
// stream append either): a refresh of a superseded snapshot is not history
// worth writing. rec must carry a KV projection.
func (s *Store) AppendIfKVUnchanged(stream string, rec Record, ifKVOffset uint64) (off uint64, applied bool, err error) {
	if rec.KVPath == "" {
		return 0, false, fmt.Errorf("guarded append requires a KV projection")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.kvOffset(rec.KVPath, rec.KVNode)
	if !ok || cur != ifKVOffset {
		return 0, false, nil // retired or superseded since the snapshot — skip entirely
	}
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
// The cursor and its last-advance timestamp (ct/, the staleness input of spec
// §5.2) are ONE synced batch: a crash can never persist an advance without its
// timestamp or vice versa.
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

// CursorMarkSeen records ts (unix ms) as the last-advance time of a cursor
// that has no recorded timestamp yet; a no-op when one exists. This is the
// spec §5.2 upgrade case: a cursor key that predates the ct/ timestamps is
// treated as advancing NOW at first sighting, so it gets a full staleness
// window before it can ever be overridden. Synced: the sighting must survive
// restart, otherwise every restart would rewind the staleness clock.
func (s *Store) CursorMarkSeen(name, stream string, ts int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.readU64(ctKey(name, stream), 0) != 0 {
		return
	}
	if err := s.db.Set(ctKey(name, stream), be64(uint64(ts)), pebble.Sync); err != nil {
		// Non-fatal by design (the caller treats the cursor as fresh either
		// way), but never silent: an unpersisted sighting rewinds the
		// staleness clock on the next restart.
		slog.Warn("store: persisting cursor first-sighting timestamp failed",
			"cursor", name, "stream", stream, "err", err)
	}
}

// HWMGet returns the highest child offset already applied for (child, stream),
// or 0 when nothing from that child has been applied yet.
func (s *Store) HWMGet(child, stream string) uint64 {
	return s.readU64(hwmKey(child, stream), 0)
}

// CursorInfo is one persisted consumer cursor: the next offset the named
// consumer will read from a stream. LastAdvanceMS is the unix-ms timestamp of
// the cursor's last advance (the ct/ key, spec §5.2's staleness input); 0
// means no timestamp was ever recorded — a cursor key predating the ct/
// mechanism, which the pruner treats as advancing now at first sighting
// (CursorMarkSeen).
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

// scanU64Pairs iterates every key of the form {prefix}\x00{first}\x00{second}
// holding an 8-byte big-endian counter. Malformed keys and values are skipped,
// the same tolerance KVScan applies. The upper bound is {prefix, 0x01}, not
// {prefix, 0xFF}: only keys whose SECOND byte is the \x00 separator belong to
// the family — a wider bound would sweep up multi-byte prefixes sharing the
// first byte (concretely: ct/ cursor timestamps inside the c/ cursor scan).
func (s *Store) scanU64Pairs(prefix byte, fn func(first, second string, v uint64)) {
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte{prefix, 0x00},
		UpperBound: []byte{prefix, 0x01},
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
		out = append(out, CursorInfo{
			Name: name, Stream: stream, Position: v,
			LastAdvanceMS: int64(s.readU64(ctKey(name, stream), 0)),
		})
	})
	return out
}

// ProtectedCursors classifies stream's persisted cursors against the §5.2
// staleness window at now: cursors still protecting the stream (silence
// window not passed, or window<=0 meaning "never override") and cursors the
// staleness override has stopped protecting ("stale"). Shared by
// retention.Pruner (the actual clamp/override decision) and the metrics
// collector (colca_retention_pressure/_blocked_by_cursor, design §8) so both
// always see the identical classification — one scan, not two hand-kept
// copies.
//
// A cursor with no persisted last-advance timestamp (LastAdvanceMS==0, the
// upgrade case) is treated as advancing NOW for this classification only —
// conservatively giving it a full staleness window before it can ever be
// overridden — which always resolves it into `protecting` (staleFor≈0 can
// never exceed a positive window). Its CursorInfo is returned UNCHANGED
// (LastAdvanceMS still 0): a caller that is actually about to prune this
// cycle (only the pruner) detects that and persists the stamp itself via
// CursorMarkSeen, so the staleness clock does not restart on every process
// restart. A read-only caller (the collector) does no such thing and simply
// re-derives the same classification on every scrape — this method itself
// never mutates the store.
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
	// Delete mirrors Record.Delete (retention design §7.1): ApplyReplicated
	// deletes the KV key in its batch instead of setting it. The parent's
	// replication server derives it the same way the engine does — empty
	// payload on a KV-projecting class — because the empty payload IS the wire
	// truth of the tombstone (§7.1 rejected-alternative argument). NOT a wire
	// field (the repl wire type is wireRec; this flag is derived server-side
	// after decoding), hence excluded from marshaling.
	Delete bool `json:"-"`
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
		n, err := addRecord(b, stream, off, r.Topic, r.Payload, r.TS, r.KVPath, r.KVNode, r.Delete)
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
	// Shed is the logical bytes actually removed by THIS commit — the
	// post-in-batch-recheck accounting (design §8,
	// colca_retention_pruned_bytes_total). Only ever populated on the
	// ephemeral span Prune hands to its plan callback; PruneJournal's
	// reconstructed spans (read back from the on-disk journal, which never
	// stored bytes) always carry the zero value.
	Shed uint64
}

// GapSpan is the wire gap object of spec §6.1/§6.2: the contiguous pruned
// hole [FromOffset..ToOffset] below a consumer's position, with the time span
// of the removed records answered from the prune journal. The json tags are
// the wire contract (/fetch and GET /downlink responses) — do not rename
// them. Approx is true when a coalesced journal entry answered FirstTS.
type GapSpan struct {
	Stream     string `json:"stream"`
	FromOffset uint64 `json:"from_offset"`
	ToOffset   uint64 `json:"to_offset"`
	FirstTS    int64  `json:"first_ts"`
	LastTS     int64  `json:"last_ts"`
	Approx     bool   `json:"approx"`
}

// Gap reports the pruned hole a consumer positioned at position (the next
// offset it would read) faces on a stream: ok is false when position >= LWM
// (nothing it wants is gone — including the whole untouched-stream case,
// LWM 1). Because pruning removes only the contiguous prefix [1..LWM),
// the hole is exactly [position..LWM-1]; FirstTS/LastTS come from the
// journal entries containing the two boundary offsets (the journal is a
// complete ordered partition of [1..LWM), so both lookups always hit).
//
// An unknown stream has no LWM (0) and therefore no gap — without the guard
// the LWM-1 arithmetic would underflow into a fabricated max-uint64 span on
// the wire. Position 0 is clamped to 1 BEFORE the comparison: cursor
// positions start at 1, and comparing the raw 0 would invert the span.
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
			g.Approx = sp.Coalesced // spec §6.2: approx marks a blurred first_ts
		}
		if g.ToOffset >= sp.From && g.ToOffset <= sp.To {
			g.LastTS = sp.LastTS
		}
	}
	return g, true
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

// RefreshRange is a pending entities state-refresh obligation (spec §6.5
// [delta]): the KV-projection Offsets [From, To) whose current entries must be
// re-appended. Persisted as rp/{stream} inside the prune batch and cleared
// only after every refresh append succeeded, so a crash between the batch and
// the refresh leaves the obligation on disk instead of losing it.
type RefreshRange struct {
	From, To uint64
}

// PruneOutcome is what a Prune plan callback contributes to the prune batch:
// gap-marker records appended at the head (spec §6.4) and an optional pending
// refresh range persisted as rp/{stream} (spec §6.5 [delta]).
type PruneOutcome struct {
	GapRecords []Record
	Refresh    *RefreshRange
}

// Prune removes the contiguous prefix [LWM..upTo) of a stream in ONE atomic,
// synced batch: a single range tombstone, the advanced l/{stream}, the
// decremented b/{stream}, the journal entry, plus whatever the plan callback
// contributes — gap markers appended at the head (spec §6.4, so they survive
// their own prune run) and the pending refresh range (spec §6.5). Because it
// is one batch there is no "crash between delete and LWM" state: after a
// crash the stream is either fully pre-prune or fully post-prune (spec §4.2).
//
// In-batch cursor recheck (spec §5.2 [delta]): under the mutex — which
// CursorAck also takes, so no ack can interleave before the commit — the
// protected-cursor floor is recomputed and upTo shrinks to the position of
// any cursor on the stream NOT named in overridden. This closes the
// caller-snapshot race: a consumer whose first ack lands between the caller's
// policy evaluation and this commit is structurally impossible to prune past.
// If the shrink makes upTo <= LWM the prune is a no-op — no batch, no
// journal, no marker.
//
// plan, if non-nil, is invoked exactly once per committing prune with the
// EFFECTIVE span (post-shrink offsets and time span — the same values the
// journal entry records), making it the single source of truth for marker
// spans. It runs under the store mutex and must not call back into the store.
//
// upTo <= LWM is a no-op (0, nil). Unknown streams and upTo beyond the next
// offset (pruning the future) are errors. KV projections, cursors and HWMs
// are never touched. Returns the number of records removed.
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

	// Accounting scan over the doomed prefix [lwm..upTo), without the mutex
	// (spec §4.1: the mutex is for the commit, never the scan): Append writes
	// only at offsets >= next >= upTo, and the prefix can only shrink through
	// Prune itself, which the LWM recheck below serializes.
	stats, err := s.scanDoomed(stream, lwm, upTo)
	if err != nil {
		return 0, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lwm[stream] != lwm {
		return 0, fmt.Errorf("concurrent prune on stream %q", stream)
	}
	// The §5.2 in-batch recheck: shrink upTo to the floor of every cursor on
	// this stream the caller did not explicitly override.
	ov := make(map[string]bool, len(overridden))
	for _, name := range overridden {
		ov[name] = true
	}
	s.scanU64Pairs('c', func(name, cstream string, pos uint64) {
		if cstream != stream || ov[name] {
			return
		}
		if pos < upTo {
			upTo = pos
		}
	})
	if upTo <= lwm {
		return 0, nil // a live cursor moved into the doomed range: nothing may go
	}
	if stats.pruned != upTo-lwm {
		// The floor shrank after the mutex-free scan — rescan the smaller
		// range under the mutex so the journal and the plan see the stats of
		// exactly what is being deleted. Rare (only when a cursor advanced
		// into the doomed range mid-cycle) and bounded by the original scan.
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
	// A store upgraded from a pre-accounting version has records b/ never
	// counted; clamp at zero instead of underflowing — sizes are honest for
	// everything written since the counter existed. Note the clamp can also
	// absorb legitimately-counted bytes while pre-counter records are being
	// pruned out (counted and uncounted records share one counter); on this
	// greenfield branch no pre-counter store exists, so that state is
	// unreachable in practice.
	liveBytes := s.bytes[stream]
	if stats.shed > liveBytes {
		liveBytes = 0
	} else {
		liveBytes -= stats.shed
	}
	off := s.next[stream]
	for _, r := range out.GapRecords {
		n, err := addRecord(b, stream, off, r.Topic, r.Payload, r.TS, r.KVPath, r.KVNode, r.Delete)
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
		// Persist the refresh obligation IN the prune batch (spec §6.5
		// [delta]): if the process dies before the refresh runs, the range is
		// still owed after restart. An already-pending range is unioned — the
		// obligation only ever grows until a completed refresh clears it.
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

// RefreshPending returns the stream's persisted pending state-refresh range,
// if any (spec §6.5 [delta]).
func (s *Store) RefreshPending(stream string) (RefreshRange, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.refreshPending(stream)
}

// ClearRefreshPending removes the stream's pending refresh obligation. Called
// only after EVERY refresh append of the range succeeded; its own small
// synced write, deliberately separate from (and after) the refresh appends.
func (s *Store) ClearRefreshPending(stream string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.Delete(rpKey(stream), pebble.Sync)
}

// ScanRecords iterates the records of a stream in [from, upTo) in offset
// order, calling fn with each record's offset, timestamp and logical byte
// cost — len(stream key) + len(encoded value), the exact unit the b/{stream}
// accounting and Prune's shed computation use. Iteration stops early when fn
// returns false. Mutex-free by design: Pebble iterators are
// snapshot-consistent, and spec §4.1 places the pruner's policy scan outside
// the store mutex. A record that fails to decode is a fail-loud error, same
// discipline as Prune's accounting scan.
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
	return nil
}

// DefaultPolicyScanCap bounds how many records a single PolicyPruneTarget
// call will examine (JSON-decode) before giving up and reporting a floor
// instead of the true target. Without a cap, the walk from lwm is bounded
// only by clamp — and clamp is uncapped (=next) whenever no live cursor is
// protecting the stream, which is exactly the alert state
// colca_retention_blocked_by_cursor exists to surface (dead consumer,
// default ignore_cursors_after=0 never overriding, backlog growing without
// bound). At the measured store throughput (bench/RESULTS.md:
// BenchmarkReadSequential 924,235 rec/s decoding+paging, BenchmarkKVScan
// 100,000 paths in ~124ms) 100k records costs on the order of 100ms on
// development hardware — generous enough that no real backlog ever needs a
// second pass to answer one scrape, bounded enough to stay well inside any
// Prometheus scrape timeout even on slower edge storage.
const DefaultPolicyScanCap = 100_000

// PolicyPruneTarget scans stream's records forward from lwm (via ScanRecords)
// applying the age (record TS < now−maxAge) and size (running shed total
// keeps live_bytes−shed > maxBytes) criteria of design §4.1 step 2, and
// returns the offset the policy alone wants to prune up to — capped at
// clamp, which it never advances past, AND capped at maxScan records
// examined (0 disables the scan cap; callers in this codebase always pass
// DefaultPolicyScanCap). Either policy limit may be disabled (maxAge<=0
// skips the age check, maxBytes==0 skips the size check).
//
// Shared by retention.Pruner (called with clamp = the protected-cursor
// floor, the real prune bound) and the metrics collector (called with
// clamp = next, i.e. uncapped — colca_retention_pressure/_blocked_by_cursor,
// design §8, want to know how far the policy would go with NO cursor floor
// at all). Passing next as clamp is always a no-op cap: ScanRecords never
// yields an offset >= next in the first place.
//
// clampedAtCap is true when the scan stopped only because it hit clamp with
// the policy still wanting more (the §5.2 WARN log / pressure signal).
// hitScanCap is true when the scan stopped only because it examined maxScan
// records with the policy still wanting more — independent of clampedAtCap,
// since the two have different causes and different callers care about them
// differently (the pruner's WARN log names a blocking cursor; a scan-cap
// stop has no cursor to name). Either flag means target is a FLOOR: safe to
// prune up to (never advances past a record the policy did not examine and
// accept), but possibly short of where the policy would truly stop — never
// past it, so a caller building an undercount-safe signal from target (like
// blocked-cursor counting) stays correct; one that needs the exact target
// does not get it in one call.
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
			return false // first record the policy keeps — early exit (§4.1)
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
