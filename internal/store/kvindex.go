package store

import (
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
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

// reconcileKVIndex makes the contract index list exactly the KV entries: it adds
// the index key of every entry that lacks one and deletes index keys whose entry
// is gone, so a store written by a version without the index (a first start
// after the upgrade, or after a downgrade and upgrade) is indexed before the
// first read. It holds the indexed KV keys in memory for one pass over each
// range. It returns how many keys it added and removed.
func (s *Store) reconcileKVIndex() (added, removed int, err error) {
	indexed := map[string][]byte{} // KV key suffix -> its index key
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: []byte("x\x00"), UpperBound: []byte("x\x01")})
	if err != nil {
		return 0, 0, err
	}
	var stale [][]byte
	for iter.First(); iter.Valid(); iter.Next() {
		key := append([]byte(nil), iter.Key()...)
		rest := key[2:]
		sep := bytes.IndexByte(rest, 0)
		if sep < 0 {
			stale = append(stale, key)
			continue
		}
		indexed[string(rest[sep+1:])] = key
	}
	err = iter.Error()
	iter.Close()
	if err != nil {
		return 0, 0, err
	}

	b := s.db.NewBatch()
	defer func() { b.Close() }()
	flush := func() error {
		if b.Len() < 4<<20 {
			return nil
		}
		if err := b.Commit(pebble.NoSync); err != nil {
			return err
		}
		b.Close()
		b = s.db.NewBatch()
		return nil
	}

	kvLB := kvPrefix("")
	kvUB := append(append([]byte{}, kvLB...), 0xFF)
	iter, err = s.db.NewIter(&pebble.IterOptions{LowerBound: kvLB, UpperBound: kvUB})
	if err != nil {
		return 0, 0, err
	}
	for iter.First(); iter.Valid(); iter.Next() {
		suffix := string(iter.Key()[2:])
		if _, ok := indexed[suffix]; ok {
			delete(indexed, suffix)
			continue
		}
		path, node, topic, ok := splitKVKey(suffix)
		if !ok {
			continue
		}
		ik := kvIndexKey(path, node, topic)
		if ik == nil {
			continue
		}
		if err := b.Set(ik, nil, nil); err != nil {
			iter.Close()
			return added, removed, err
		}
		added++
		if err := flush(); err != nil {
			iter.Close()
			return added, removed, err
		}
	}
	err = iter.Error()
	iter.Close()
	if err != nil {
		return added, removed, err
	}
	// What is left names entries that are gone.
	for _, key := range indexed {
		stale = append(stale, key)
	}
	for _, key := range stale {
		if err := b.Delete(key, nil); err != nil {
			return added, removed, err
		}
		removed++
		if err := flush(); err != nil {
			return added, removed, err
		}
	}
	return added, removed, b.Commit(pebble.Sync)
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
func (s *Store) kvScanPageIndexed(prefix, after string, limit int, contracts []string) ([]KVEntry, string, error) {
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
		next.iter.Next()
		lastKey = kvk
		entry, ok, err := getKVEntry(snap, kvk)
		if err != nil {
			return nil, "", fmt.Errorf("store: kv page %q: %w", prefix, err)
		}
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

// getKVEntry reads one KV entry; ok is false when it is absent or undecodable.
func getKVEntry(r pebble.Reader, kvk []byte) (KVEntry, bool, error) {
	value, closer, err := r.Get(kvk)
	if errors.Is(err, pebble.ErrNotFound) {
		return KVEntry{}, false, nil
	}
	if err != nil {
		return KVEntry{}, false, err
	}
	defer closer.Close()
	path, node, _, ok := splitKVKey(string(kvk[2:]))
	if !ok {
		return KVEntry{}, false, nil
	}
	entry, ok := decodeKVEntry(path, node, value)
	return entry, ok, nil
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
