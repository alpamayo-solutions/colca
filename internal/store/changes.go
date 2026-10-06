package store

import "strings"

// A watcher learns that a stream grew without reading it: StreamChanges hands out,
// per stream, a channel that closes on the stream's next append, together with
// the stream's next offset at that moment. Both are taken under the state lock
// an append publishes under, so an append between reading the offset and
// waiting on the channel still closes it. Pruning does not close it: consumers wait for new
// records, not for old ones to go.

// StreamChanges returns, for each named stream, a channel that closes on its next
// append and its next offset now. An unknown stream is left out of both maps.
func (s *Store) StreamChanges(streams []string) (map[string]<-chan struct{}, map[string]uint64) {
	s.state.Lock()
	defer s.state.Unlock()
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
// s.state. With no watcher it costs a map lookup.
func (s *Store) streamGrewLocked(stream string) {
	s.signalChangeLocked()
	if ch := s.changed[stream]; ch != nil {
		close(ch)
		delete(s.changed, stream)
	}
}

// StreamPosition changes on append or pruning. A reconnect always takes a fresh
// snapshot; positions are hints, never consumer cursors.
type StreamPosition struct{ Next, Low uint64 }

// Changes captures the wake channel and positions under the same lock. A commit
// between this snapshot and waiting closes the captured channel, so cannot be lost.
func (s *Store) Changes(contracts ...string) (<-chan struct{}, map[string]StreamPosition) {
	s.state.Lock()
	defer s.state.Unlock()
	return s.changesLocked(contracts)
}

// ScopedStreamChanges is Changes for the named streams together with each
// stream's next offset, all taken under one lock. A watch scoped to contracts
// reports the next offset in its hints; read separately, an append between the
// two reads could pair positions that include it with a next offset that does
// not, and the captured channel, taken after the append, would never announce
// it. An unknown stream is left out of next.
func (s *Store) ScopedStreamChanges(streams []string, contracts []string) (<-chan struct{}, map[string]uint64, map[string]StreamPosition) {
	s.state.Lock()
	defer s.state.Unlock()
	wake, positions := s.changesLocked(contracts)
	next := make(map[string]uint64, len(streams))
	for _, stream := range streams {
		if off, ok := s.next[stream]; ok {
			next[stream] = off
		}
	}
	return wake, next, positions
}

func (s *Store) changesLocked(contracts []string) (<-chan struct{}, map[string]StreamPosition) {
	if s.allChanged == nil {
		s.allChanged = make(chan struct{})
	}
	positions := make(map[string]StreamPosition, len(s.next))
	for stream, next := range s.next {
		if len(contracts) > 0 {
			next = 0
			for _, contract := range contracts {
				next = max(next, s.contractHeads[stream][contract])
			}
		}
		positions[stream] = StreamPosition{next, s.lwm[stream]}
	}
	return s.allChanged, positions
}

func (s *Store) signalChangeLocked() {
	s.signalBacklogChangeLocked()
	if s.allChanged != nil {
		close(s.allChanged)
	}
	s.allChanged = make(chan struct{})
}

// BacklogChanges includes consumer progress without waking record consumers.
// Its channel has a lock of its own, so a cursor move signals it without
// waiting for a stream commit that holds the state lock.
func (s *Store) BacklogChanges() <-chan struct{} {
	s.backlogMu.Lock()
	defer s.backlogMu.Unlock()
	if s.backlogChanged == nil {
		s.backlogChanged = make(chan struct{})
	}
	return s.backlogChanged
}

// signalBacklogChange wakes backlog watchers after a cursor moved.
func (s *Store) signalBacklogChange() { s.signalBacklogChangeLocked() }

// signalBacklogChangeLocked wakes backlog watchers. The name is historical: it
// takes backlogMu itself and may be called with or without the state lock.
func (s *Store) signalBacklogChangeLocked() {
	s.backlogMu.Lock()
	defer s.backlogMu.Unlock()
	if s.backlogChanged != nil {
		close(s.backlogChanged)
	}
	s.backlogChanged = make(chan struct{})
}

// Contract heads are hints scoped to a view's interests. They need not survive
// restart: every new watch starts with catch-up. Pruning still changes Low so
// filtered readers detect retention gaps even without a matching append.
func (s *Store) noteContractsLocked(stream string, records []Record) {
	for _, record := range records {
		s.noteContractLocked(stream, record.Topic)
	}
}

func (s *Store) noteContractLocked(stream, topic string) {
	parts := strings.SplitN(topic, "/", 4)
	if len(parts) < 4 {
		return
	}
	if s.contractHeads == nil {
		s.contractHeads = make(map[string]map[string]uint64)
	}
	if s.contractHeads[stream] == nil {
		s.contractHeads[stream] = make(map[string]uint64)
	}
	s.contractHeads[stream][parts[2]] = s.next[stream]
}
