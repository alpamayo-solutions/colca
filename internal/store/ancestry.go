package store

import "github.com/cockroachdb/pebble/v2"

// Node ancestry (key "j/ancestry"): the position the parent handed down survives
// restarts, so a node that comes back while its parent is unreachable keeps
// evaluating grants. A stored key means known, even the root's empty ancestry;
// no key means never learned, and scoped grants fail closed. The value is opaque
// to the store.

var ancestryKey = []byte("j\x00ancestry")

// AncestryPut persists the node's position (synced).
func (s *Store) AncestryPut(b []byte) error {
	return s.db.Set(ancestryKey, b, pebble.Sync)
}

// AncestryGet returns the persisted position; ok=false when never stored.
func (s *Store) AncestryGet() ([]byte, bool) {
	v, closer, err := s.db.Get(ancestryKey)
	if err != nil {
		return nil, false
	}
	defer closer.Close()
	return append([]byte(nil), v...), true
}
