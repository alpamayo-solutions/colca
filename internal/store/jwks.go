package store

import "github.com/cockroachdb/pebble/v2"

// JWKS persistence (key "j/jwks"): the last fetched JWKS document survives
// restarts, so a node that comes back while the issuer is unreachable keeps
// validating tokens until they expire. Losing it is not corruption, since the
// next fetch restores it, so reads are best effort.

var jwksKey = []byte("j\x00jwks")

// JWKSPut persists the raw JWKS document (synced).
func (s *Store) JWKSPut(raw []byte) error {
	return s.db.Set(jwksKey, raw, pebble.Sync)
}

// JWKSGet returns the persisted JWKS document, or nil when never stored.
func (s *Store) JWKSGet() []byte {
	v, closer, err := s.db.Get(jwksKey)
	if err != nil {
		return nil
	}
	defer closer.Close()
	return append([]byte(nil), v...)
}
