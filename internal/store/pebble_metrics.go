package store

import "github.com/cockroachdb/pebble/v2"

// PebbleMetrics returns Pebble's in-memory statistics of the store. It reads
// counters only: no table is opened or scanned, nothing is compacted.
func (s *Store) PebbleMetrics() *pebble.Metrics { return s.db.Metrics() }
