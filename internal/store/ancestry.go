package store

import "github.com/cockroachdb/pebble/v2"

// Node-ancestry persistence (id-grants design §4, key family "j/"): the
// position the parent handed down survives restarts, so a node that comes back
// while its parent is unreachable keeps answering grants. Key PRESENCE is the
// "known" bit — a root's empty ancestry is a stored value, absence means the
// position was never learned (scoped grants fail closed).
//
// The value is opaque here: the store keeps bytes, and what they mean belongs
// to the domain plugin that defines the shape.

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
