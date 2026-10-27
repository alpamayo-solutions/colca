package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/cockroachdb/pebble/v2"
)

// Set-aside uplink records.
//
// A record the parent refuses for good would hold its uplink lane forever:
// order within a stream never changes, so nothing behind it could rise. After a
// grace period the uplink sets such a record aside instead: it copies the
// record here, under ua/{parent}/{stream}/{offset}, and moves the uplink cursor
// past it in the same synced batch. The copy is outside the stream, so the
// stream's retention never removes it; only an operator clears it.
//
// The record never reaches the parent: that is data loss there, which the
// uplink reports as a finding. It cannot be sent again later on its own. The
// parent drops every child offset at or below its high-water mark, and
// appending it again locally under a new offset would replay an old state
// value over a newer one at the parent.

// SetAsideRecord is one record the uplink set aside, with why.
type SetAsideRecord struct {
	Parent string       `json:"parent"`
	Stream string       `json:"stream"`
	Offset uint64       `json:"offset"`
	Record StoredRecord `json:"record"`
	// Status and Reason are the parent's last refusal of the record.
	Status int    `json:"status"`
	Reason string `json:"reason"`
	// HeldSinceMS is when the parent first refused the record alone, AtMS when
	// it was set aside.
	HeldSinceMS int64 `json:"held_since_ms"`
	AtMS        int64 `json:"at_ms"`
}

func setAsideKey(parent, stream string, off uint64) []byte {
	return append([]byte("ua\x00"+parent+"\x00"+stream+"\x00"), be64(off)...)
}

func setAsideBounds(parent string) (lb, ub []byte) {
	return []byte("ua\x00" + parent + "\x00"), []byte("ua\x00" + parent + "\x01")
}

// SetAsideUplink copies r and moves cursor on r.Stream past r.Offset, in one
// synced batch, so a crash can neither skip the record without its copy nor
// keep a copy of a record still being sent. moved is false, and nothing is
// written, when the cursor already stands past the record.
func (s *Store) SetAsideUplink(cursor string, r SetAsideRecord) (moved bool, err error) {
	val, err := json.Marshal(r)
	if err != nil {
		return false, err
	}
	s.cursorMu.Lock()
	defer s.cursorMu.Unlock()
	if s.readU64(cursorKey(cursor, r.Stream), 1) > r.Offset {
		return false, nil
	}
	b := s.db.NewBatch()
	defer b.Close()
	if err := b.Set(setAsideKey(r.Parent, r.Stream, r.Offset), val, nil); err != nil {
		return false, err
	}
	if err := b.Set(cursorKey(cursor, r.Stream), be64(r.Offset+1), nil); err != nil {
		return false, err
	}
	if err := b.Set(ctKey(cursor, r.Stream), be64(uint64(time.Now().UnixMilli())), nil); err != nil { //nolint:gosec // a positive timestamp
		return false, err
	}
	if err := s.db.Apply(b, pebble.Sync); err != nil {
		return false, err
	}
	s.signalBacklogChange()
	return true, nil
}

// SetAsideRecords lists the records set aside for parent, by stream and offset.
// A copy that does not decode is an error: it would hide a lost record.
func (s *Store) SetAsideRecords(parent string) ([]SetAsideRecord, error) {
	lb, ub := setAsideBounds(parent)
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: lb, UpperBound: ub})
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	var out []SetAsideRecord
	for iter.First(); iter.Valid(); iter.Next() {
		var r SetAsideRecord
		if err := json.Unmarshal(iter.Value(), &r); err != nil {
			return nil, fmt.Errorf("corrupt set-aside record %q: %w", iter.Key(), err)
		}
		out = append(out, r)
	}
	return out, iter.Error()
}

// ClearSetAside removes one set-aside copy and reports whether it existed.
func (s *Store) ClearSetAside(parent, stream string, off uint64) (bool, error) {
	key := setAsideKey(parent, stream, off)
	_, closer, err := s.db.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	closer.Close()
	return true, s.db.Delete(key, pebble.Sync)
}
