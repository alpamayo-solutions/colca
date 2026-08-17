package store

import "github.com/cockroachdb/pebble/v2"

// Node-prefix persistence (cmdadmin design §3, key family "j/"): the
// root-frame prefix the parent handed down survives restarts, so a node that
// comes back while its parent is unreachable keeps translating human grants.
// Key PRESENCE is the "known" bit — a root's empty prefix is a stored value,
// absence means the prefix was never learned (scoped grants fail closed).

var prefixKey = []byte("j\x00prefix")

// PrefixPut persists the root-frame prefix (synced).
func (s *Store) PrefixPut(p string) error {
	return s.db.Set(prefixKey, []byte(p), pebble.Sync)
}

// PrefixGet returns the persisted prefix; ok=false when never stored.
func (s *Store) PrefixGet() (string, bool) {
	v, closer, err := s.db.Get(prefixKey)
	if err != nil {
		return "", false
	}
	defer closer.Close()
	return string(v), true
}
