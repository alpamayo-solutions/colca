package store

import "encoding/json"

// RetainedMove names one retained record and the position it moves to. Both
// positions belong to the same node.
type RetainedMove struct {
	Node      string
	FromPath  string
	FromTopic string
	ToPath    string
	ToTopic   string
}

// MoveRetained moves retained records to new positions in one atomic, synced
// batch on stream. For each move that has a record at its source:
//
//   - the record is restated at the destination with its payload, timestamp
//     and attribution unchanged, so a reader of the destination sees the same
//     value, and a consumer keyed by the payload and timestamp (the historian
//     upserts by signal and timestamp) sees the row it already has;
//   - the source is retired with a tombstone attributed to by.
//
// A destination that already holds a record keeps it: whatever stands there
// was written after the source and is newer. Its source is still retired. A
// move with no record at its source writes nothing.
//
// The checks and the append run under s.mu, so no write can land between
// them. written lists the appended records in order, starting at offset first;
// it is empty when nothing was appended.
func (s *Store) MoveRetained(stream string, moves []RetainedMove, by Record) (written []Record, first uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, move := range moves {
		held, ok := s.kvEntryLocked(move.FromPath, move.Node, move.FromTopic)
		if !ok {
			continue
		}
		if _, taken := s.kvEntryLocked(move.ToPath, move.Node, move.ToTopic); !taken {
			written = append(written, Record{
				Topic: move.ToTopic, Payload: held.Payload, TS: held.TS,
				WrittenBy: held.WrittenBy, ActorID: held.ActorID,
				ActorLabel: held.ActorLabel, ActorKind: held.ActorKind,
				KVPath: move.ToPath, KVNode: move.Node,
			})
		}
		written = append(written, Record{
			Topic: move.FromTopic, TS: by.TS,
			WrittenBy: by.WrittenBy, ActorID: by.ActorID,
			ActorLabel: by.ActorLabel, ActorKind: by.ActorKind,
			KVPath: move.FromPath, KVNode: move.Node, Delete: true,
		})
	}
	if len(written) == 0 {
		return nil, 0, nil
	}
	first, _, err = s.appendLocked(stream, written)
	if err != nil {
		return nil, 0, err
	}
	return written, first, nil
}

// kvEntryLocked reads the KV entry for (path, node, topic). The caller holds
// s.mu.
func (s *Store) kvEntryLocked(path, node, topic string) (kvEnc, bool) {
	v, closer, err := s.db.Get(kvKey(path, node, topic))
	if err != nil {
		return kvEnc{}, false
	}
	defer closer.Close()
	var e kvEnc
	if json.Unmarshal(v, &e) != nil {
		return kvEnc{}, false
	}
	return e, true
}
