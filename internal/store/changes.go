package store

// A watcher learns that a stream grew without reading it: StreamChanges hands out,
// per stream, a channel that closes on the stream's next append, together with
// the stream's next offset at that moment. Both are taken under the store mutex
// that appends hold, so an append between reading the offset and waiting on the
// channel still closes it. Pruning does not close it: consumers wait for new
// records, not for old ones to go.

// StreamChanges returns, for each named stream, a channel that closes on its next
// append and its next offset now. An unknown stream is left out of both maps.
func (s *Store) StreamChanges(streams []string) (map[string]<-chan struct{}, map[string]uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.changed == nil {
		s.changed = make(map[string]chan struct{})
	}
	waits := make(map[string]<-chan struct{}, len(streams))
	next := make(map[string]uint64, len(streams))
	for _, stream := range streams {
		off, ok := s.next[stream]
		if !ok {
			continue
		}
		ch := s.changed[stream]
		if ch == nil {
			ch = make(chan struct{})
			s.changed[stream] = ch
		}
		waits[stream] = ch
		next[stream] = off
	}
	return waits, next
}

// streamGrewLocked wakes the watchers of stream after an append. The caller holds
// s.mu. With no watcher it costs a map lookup.
func (s *Store) streamGrewLocked(stream string) {
	if ch := s.changed[stream]; ch != nil {
		close(ch)
		delete(s.changed, stream)
	}
}
