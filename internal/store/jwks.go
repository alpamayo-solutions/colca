package store

import "github.com/cockroachdb/pebble/v2"

// JWKS persistence (key "j/jwks/<url>"): the last document fetched from each
// JWKS URL survives restarts, so a node that comes back while the issuer is
// unreachable keeps validating tokens until they expire. Losing it is not
// corruption, since the next fetch restores it, so reads are best effort.

func jwksKey(url string) []byte { return []byte("j\x00jwks\x00" + url) }

// JWKSPut persists the raw JWKS document fetched from url (synced).
func (s *Store) JWKSPut(url string, raw []byte) error {
	return s.db.Set(jwksKey(url), raw, pebble.Sync)
}

// JWKSGet returns the document persisted for url, or nil when never stored.
func (s *Store) JWKSGet(url string) []byte {
	v, closer, err := s.db.Get(jwksKey(url))
	if err != nil {
		return nil
	}
	defer closer.Close()
	return append([]byte(nil), v...)
}
