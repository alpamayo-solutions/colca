package store

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/cockroachdb/pebble/v2"

	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// The contract index lists every KV entry again under its contract:
//
//	x\x00{contract}\x00{path}\x00{node}\x00{topic}  ->  (empty)
//
// Everything after the contract is the KV key without its "k\x00", so walking one
// contract's index in key order visits its entries in the order a KV scan does,
// and a merge over several contracts keeps that order and its page tokens. It is
// written in the same batch as the KV entry it lists.

// kvIndexKey is the index key of the KV entry (path, node, topic), or nil for a
// topic without a contract, which no contract filter matches.
func kvIndexKey(path, nodeID, topic string) []byte {
	contract, ok := contractOf(topic)
	if !ok {
		return nil
	}
	return []byte("x\x00" + contract + "\x00" + path + "\x00" + nodeID + "\x00" + topic)
}

func kvIndexPrefix(contract string) []byte { return []byte("x\x00" + contract + "\x00") }

// contractOf reads the contract with the same grammar the contract filter uses.
func contractOf(topic string) (string, bool) {
	parsed, err := uns.Parse(topic)
	if err != nil || parsed.Contract == "" {
		return "", false
	}
	return parsed.Contract, true
}

// setKV writes one KV entry and its index key.
func setKV(b *pebble.Batch, path, nodeID, topic string, value []byte) error {
	if err := b.Set(kvKey(path, nodeID, topic), value, nil); err != nil {
		return err
	}
	if ik := kvIndexKey(path, nodeID, topic); ik != nil {
		return b.Set(ik, nil, nil)
	}
	return nil
}

// deleteKV deletes one KV entry and its index key.
func deleteKV(b *pebble.Batch, path, nodeID, topic string) error {
	if err := b.Delete(kvKey(path, nodeID, topic), nil); err != nil {
		return err
	}
	if ik := kvIndexKey(path, nodeID, topic); ik != nil {
		return b.Delete(ik, nil)
	}
	return nil
}

// splitKVKey splits the part of a KV key after "k\x00" into path, node and topic.
func splitKVKey(key string) (path, nodeID, topic string, ok bool) {
	pathSep := strings.IndexByte(key, 0)
	if pathSep < 0 {
		return "", "", "", false
	}
	rest := key[pathSep+1:]
	nodeSep := strings.IndexByte(rest, 0)
	if nodeSep < 0 {
		return "", "", "", false
	}
	return key[:pathSep], rest[:nodeSep], rest[nodeSep+1:], true
}

// kvIndexCleanKey records, at a clean Close, every stream's next offset. KV
// entries are only ever set together with an append, so if the offsets still
// match at the next Open, nothing wrote KV in between without the index (an older
// version, or a crash mid-write), and the reconcile can be skipped. Open deletes
// it, so a crash leaves it absent.
var kvIndexCleanKey = []byte("xc\x00")

// kvIndexCleanValue is every stream's next offset in stream order.
func (s *Store) kvIndexCleanValue() []byte {
	var v []byte
	for _, stream := range streams {
		v = append(v, be64(s.next[stream])...)
	}
	return v
}

// openKVIndex runs at Open: it reconciles the contract index unless the last
// Close left it known to be complete, then clears that mark for this run.
func (s *Store) openKVIndex() (added, removed int, err error) {
	clean, closer, err := s.db.Get(kvIndexCleanKey)
	switch {
	case err == nil:
		matches := bytes.Equal(clean, s.kvIndexCleanValue())
		closer.Close()
		if matches {
			return 0, 0, s.db.Delete(kvIndexCleanKey, pebble.Sync)
		}
	case !errors.Is(err, pebble.ErrNotFound):
		return 0, 0, err
	}
	added, removed, err = s.reconcileKVIndex()
	if err != nil {
		return added, removed, err
	}
	return added, removed, s.db.Delete(kvIndexCleanKey, pebble.Sync)
}

// Recovery checkpoints are versioned and tied to the stream heads. A version
// without this index may have written between opens; changed heads invalidate
// its checkpoint just as they invalidate the clean marker.
var kvIndexRecoveryKey = []byte("xr\x00")

const (
	kvIndexRecoveryVersion = 1
	kvIndexRecoveryRows    = 4096
	// The byte target may be exceeded by one indivisible key and its checkpoint.
	kvIndexRecoveryBytes = 1 << 20
)

type kvIndexRecovery struct {
	Version int    `json:"version"`
	Heads   []byte `json:"heads"`
	Phase   int    `json:"phase"`
	After   []byte `json:"after"`
	Scanned uint64 `json:"scanned"`
}

// reconcileKVIndex uses point lookups rather than an inventory of all keys.
// Each bounded page commits its repairs and resume position together, including
// pages needing no repairs, so an interrupted open makes durable progress.
func (s *Store) reconcileKVIndex() (added, removed int, err error) {
	return s.reconcileKVIndexBounded(kvIndexRecoveryRows, kvIndexRecoveryBytes, nil)
}

func (s *Store) reconcileKVIndexBounded(rows, batchBytes int, committed func(kvIndexRecovery, int) error) (added, removed int, err error) {
	progress := kvIndexRecovery{Version: kvIndexRecoveryVersion, Heads: s.kvIndexCleanValue()}
	raw, closer, err := s.db.Get(kvIndexRecoveryKey)
	if err == nil {
		var saved kvIndexRecovery
		decodeErr := json.Unmarshal(raw, &saved)
		closer.Close()
		if decodeErr == nil && saved.Version == progress.Version && bytes.Equal(saved.Heads, progress.Heads) && saved.Phase >= 0 && saved.Phase < 2 {
			lb, ub := kvIndexRecoveryBounds(saved.Phase)
			if len(saved.After) == 0 || (bytes.Compare(saved.After, lb) >= 0 && bytes.Compare(saved.After, ub) < 0) {
				progress = saved
			}
		}
	} else if !errors.Is(err, pebble.ErrNotFound) {
		return 0, 0, err
	}
	pages := 0
	for progress.Phase < 2 {
		phase := progress.Phase
		lb, ub := kvIndexRecoveryBounds(progress.Phase)
		iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: lb, UpperBound: ub})
		if err != nil {
			return added, removed, err
		}
		valid := iter.First()
		if len(progress.After) > 0 {
			valid = iter.SeekGE(progress.After)
			if valid && bytes.Equal(iter.Key(), progress.After) {
				valid = iter.Next()
			}
		}
		b := s.db.NewBatch()
		scanned := 0
		for valid && scanned < rows && b.Len() < batchBytes {
			key := iter.Key()
			// Reserve space for the repair and its key-bearing checkpoint before
			// admitting another row. An oversized first row still makes progress.
			if scanned > 0 && b.Len()+3*len(key)+512 > batchBytes {
				break
			}
			if progress.Phase == 0 {
				path, node, topic, ok := splitKVKey(string(key[2:]))
				if ok {
					ik := kvIndexKey(path, node, topic)
					if ik != nil {
						_, c, getErr := s.db.Get(ik)
						switch {
						case getErr == nil:
							c.Close()
						case errors.Is(getErr, pebble.ErrNotFound):
							err = b.Set(ik, nil, nil)
							added++
						default:
							err = getErr
						}
					}
				}
			} else {
				rest := key[2:]
				sep := bytes.IndexByte(rest, 0)
				stale := sep < 0
				if !stale {
					suffix := rest[sep+1:]
					path, node, topic, ok := splitKVKey(string(suffix))
					stale = !ok || !bytes.Equal(key, kvIndexKey(path, node, topic))
					if !stale {
						kvk := append([]byte("k\x00"), suffix...)
						_, c, getErr := s.db.Get(kvk)
						switch {
						case getErr == nil:
							c.Close()
						case errors.Is(getErr, pebble.ErrNotFound):
							stale = true
						default:
							err = getErr
						}
					}
				}
				if stale {
					err = b.Delete(key, nil)
					removed++
				}
			}
			if err != nil {
				break
			}
			progress.After = append(progress.After[:0], key...)
			scanned++
			progress.Scanned++
			valid = iter.Next()
		}
		if err == nil {
			err = iter.Error()
		}
		if err == nil && !valid {
			progress.Phase++
			progress.After = nil
		}
		iter.Close()
		if err == nil {
			if progress.Phase == 2 {
				err = b.Delete(kvIndexRecoveryKey, nil)
			} else {
				raw, marshalErr := json.Marshal(progress)
				err = marshalErr
				if err == nil {
					err = b.Set(kvIndexRecoveryKey, raw, nil)
				}
			}
		}
		if err == nil {
			err = b.Commit(pebble.Sync)
		}
		size := b.Len()
		b.Close()
		if err != nil {
			return added, removed, err
		}
		pages++
		if pages == 1 || pages%64 == 0 || progress.Phase != phase {
			slog.Info("store: KV contract index recovery checkpoint", "phase", progress.Phase, "scanned", progress.Scanned, "added", added, "removed", removed)
		}
		if committed != nil {
			if err := committed(progress, size); err != nil {
				return added, removed, err
			}
		}
	}
	return added, removed, nil
}

func kvIndexRecoveryBounds(phase int) ([]byte, []byte) {
	if phase == 0 {
		return []byte("k\x00"), []byte("k\x01")
	}
	return []byte("x\x00"), []byte("x\x01")
}

// kvPageToken decodes a page token and checks that it lies within [lb, ub).
func kvPageToken(after string, lb, ub []byte) ([]byte, error) {
	raw, err := base64.RawURLEncoding.DecodeString(after)
	if err != nil || bytes.Compare(raw, lb) < 0 || bytes.Compare(raw, ub) >= 0 {
		return nil, ErrInvalidPageToken
	}
	return raw, nil
}

// kvScanPageIndexed is KVScanPage for a contract filter: it walks the contract
// index instead of every entry under the prefix, so the work is proportional to
// the entries returned. Entries come in KV key order and tokens are KV keys, as
// from the unindexed walk.
func (s *Store) kvScanPageIndexed(prefix, after string, limit int, maxBytes uint64, contracts []string) ([]KVEntry, string, error) {
	lb := kvPrefix(prefix)
	ub := append(append([]byte{}, lb...), 0xFF)
	var resume []byte // the token's KV key without "k\x00"
	if after != "" {
		raw, err := kvPageToken(after, lb, ub)
		if err != nil {
			return nil, "", err
		}
		resume = raw[2:]
	}

	snap := s.db.NewSnapshot()
	defer func() { _ = snap.Close() }()

	type cursor struct {
		iter *pebble.Iterator
		skip int // length of the index prefix before the KV key
	}
	seen := make(map[string]bool, len(contracts))
	var cursors []*cursor
	defer func() {
		for _, c := range cursors {
			c.iter.Close()
		}
	}()
	for _, contract := range contracts {
		if seen[contract] {
			continue
		}
		seen[contract] = true
		ip := kvIndexPrefix(contract)
		ilb := append(append([]byte{}, ip...), prefix...)
		iub := append(append([]byte{}, ilb...), 0xFF)
		iter, err := snap.NewIter(&pebble.IterOptions{LowerBound: ilb, UpperBound: iub})
		if err != nil {
			return nil, "", fmt.Errorf("store: kv index %q: open iterator: %w", contract, err)
		}
		c := &cursor{iter: iter, skip: len(ip)}
		cursors = append(cursors, c)
		if resume == nil {
			iter.First()
			continue
		}
		from := append(append([]byte{}, ip...), resume...)
		if iter.SeekGE(from) && bytes.Equal(iter.Key(), from) {
			iter.Next()
		}
	}

	// A filtered page is usually small; append grows it past this.
	out := make([]KVEntry, 0, 64)
	page := pageBytes{max: maxBytes}
	var lastKey []byte
	for len(out) < limit {
		var next *cursor
		for _, c := range cursors {
			if !c.iter.Valid() {
				continue
			}
			if next == nil || bytes.Compare(c.iter.Key()[c.skip:], next.iter.Key()[next.skip:]) < 0 {
				next = c
			}
		}
		if next == nil {
			break
		}
		kvk := append([]byte("k\x00"), next.iter.Key()[next.skip:]...)
		entry, size, ok, err := getKVEntry(snap, kvk)
		if err != nil {
			return nil, "", fmt.Errorf("store: kv page %q: %w", prefix, err)
		}
		if ok && !page.take(size, len(out)) {
			break // the cursor stays on this entry, so the token resumes at it
		}
		next.iter.Next()
		lastKey = kvk
		if ok {
			out = append(out, entry)
		}
	}
	for _, c := range cursors {
		if err := c.iter.Error(); err != nil {
			return nil, "", fmt.Errorf("store: kv index page %q: %w", prefix, err)
		}
	}
	for _, c := range cursors {
		if c.iter.Valid() && len(lastKey) > 0 {
			return out, base64.RawURLEncoding.EncodeToString(lastKey), nil
		}
	}
	return out, "", nil
}

// getKVEntry reads one KV entry and its stored size; ok is false when it is
// absent or undecodable.
func getKVEntry(r pebble.Reader, kvk []byte) (KVEntry, int, bool, error) {
	value, closer, err := r.Get(kvk)
	if errors.Is(err, pebble.ErrNotFound) {
		return KVEntry{}, 0, false, nil
	}
	if err != nil {
		return KVEntry{}, 0, false, err
	}
	defer closer.Close()
	path, node, _, ok := splitKVKey(string(kvk[2:]))
	if !ok {
		return KVEntry{}, 0, false, nil
	}
	entry, ok := decodeKVEntry(path, node, value)
	return entry, len(value), ok, nil
}

// kvRelativeDepth is how many path segments path has below prefix: 0 for the
// prefix itself. A leading "/" after the prefix does not count as a segment.
func kvRelativeDepth(prefix, path string) (depth int, lead int) {
	rel := path[len(prefix):]
	if strings.HasPrefix(rel, "/") {
		rel, lead = rel[1:], 1
	}
	if rel == "" {
		return 0, lead
	}
	return strings.Count(rel, "/") + 1, lead
}

// kvSkipBelow is the KV key to seek to past every entry deeper than depth
// segments under prefix on the branch of path, a path deeper than depth. The
// subtree is everything whose path starts with its ancestor at depth plus "/", so
// the seek target is that ancestor followed by the byte after "/".
func kvSkipBelow(prefix, path string, depth, lead int) []byte {
	rel := path[len(prefix)+lead:]
	cut := len(prefix) + lead
	for i := 0; i < depth; i++ {
		slash := strings.IndexByte(rel, '/')
		cut += slash + 1
		rel = rel[slash+1:]
	}
	// cut now points at the first byte of segment depth+1; the "/" before it ends
	// the ancestor at depth.
	return kvPrefix(path[:cut-1] + "0")
}
