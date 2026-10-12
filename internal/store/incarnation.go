package store

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/cockroachdb/pebble/v2"
)

// A store's incarnation id says which history its offsets belong to. It is
// minted with the store and again whenever that history may have regressed: a
// volume restored from a backup hands out offsets it handed out before, for
// other records. Everything that remembers an offset of this store outside it
// (a child's downlink position, a parent's high-water mark, a consumer's
// acknowledgement) is only valid for the incarnation it was read from.
//
// A restored volume carries the id it was copied with, so the id alone does not
// reveal a restore. The replication handshake does: a peer that recorded a
// position this store has not reached proves the regression, and the store then
// takes a new id (see repl.Server). An operator can do the same by hand.

// loadOrMintStoreID reads the store's id, or mints one for a store that has
// none: a new store, or one written before stores had ids.
func loadOrMintStoreID(db *pebble.DB) (id string, minted bool, err error) {
	v, closer, err := db.Get(storeIDKey())
	if err == nil {
		id := string(v)
		_ = closer.Close()
		return id, false, nil
	}
	if !errors.Is(err, pebble.ErrNotFound) {
		return "", false, fmt.Errorf("read store id: %w", err)
	}
	id, err = mintStoreID()
	if err != nil {
		return "", false, err
	}
	if err := db.Set(storeIDKey(), []byte(id), pebble.Sync); err != nil {
		return "", false, fmt.Errorf("persist store id: %w", err)
	}
	return id, true, nil
}

func mintStoreID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("mint store id: %w", err)
	}
	return hex.EncodeToString(b), nil
}

// StoreID is this store's incarnation: stable across restarts, new whenever the
// store is created from nothing or its history may have regressed. A child
// sends it with every replication batch so its parent can tell a rebuilt store
// from a resumed one, and a parent answers its children with it so they can
// tell a restored parent from the one they knew.
func (s *Store) StoreID() string { return *s.id.Load() }

// MayHaveRegressed reports whether the history under the current incarnation
// id can be behind what a peer recorded of it. That takes a store opened with
// an id it already had: a directory can be a restored copy. An id this process
// minted names a history that only grew since, so a peer that claims to be
// ahead of it is wrong, and the id is not changed on its word.
func (s *Store) MayHaveRegressed() bool { return !s.idMinted.Load() }

// IncarnationChange records the last time a store took a new incarnation id.
type IncarnationChange struct {
	Previous string `json:"previous"`
	Reason   string `json:"reason"`
	AtMS     int64  `json:"at_ms"`
}

// Reincarnate gives the store a new incarnation id and records why. The marks
// this node kept of what its own parents confirmed go in the same batch: they
// name offsets of the incarnation that ends here.
func (s *Store) Reincarnate(reason string) (id string, err error) {
	id, _, err = s.reincarnate("", reason)
	return id, err
}

// ReincarnateFrom is Reincarnate only while the store still has the id current.
// Several children can prove the same restore at once; the first one changes
// the id and the others find it changed. id is the store's id afterwards.
func (s *Store) ReincarnateFrom(current, reason string) (id string, changed bool, err error) {
	return s.reincarnate(current, reason)
}

func (s *Store) reincarnate(onlyFrom, reason string) (string, bool, error) {
	s.incMu.Lock()
	defer s.incMu.Unlock()
	previous := s.StoreID()
	if onlyFrom != "" && onlyFrom != previous {
		return previous, false, nil
	}
	id, err := mintStoreID()
	if err != nil {
		return previous, false, err
	}
	change, err := json.Marshal(IncarnationChange{Previous: previous, Reason: reason, AtMS: time.Now().UnixMilli()})
	if err != nil {
		return previous, false, err
	}
	// Under cursorMu, so a mark written with an uplink cursor cannot land
	// between the range delete and the new id.
	s.cursorMu.Lock()
	defer s.cursorMu.Unlock()
	b := s.db.NewBatch()
	defer b.Close()
	if err := b.Set(storeIDKey(), []byte(id), nil); err != nil {
		return previous, false, err
	}
	if err := b.Set(incarnationChangeKey(), change, nil); err != nil {
		return previous, false, err
	}
	lb, ub := ackedBounds()
	if err := b.DeleteRange(lb, ub, nil); err != nil {
		return previous, false, err
	}
	if err := s.db.Apply(b, pebble.Sync); err != nil {
		return previous, false, fmt.Errorf("persist the new store incarnation: %w", err)
	}
	s.id.Store(&id)
	s.idMinted.Store(true)
	if s.incChanged != nil {
		close(s.incChanged)
		s.incChanged = nil
	}
	return id, true, nil
}

// IncarnationChanges returns a channel closed when the store next takes a new
// incarnation id. Capture it before reading StoreID, so a change between the
// read and the wait is not missed.
func (s *Store) IncarnationChanges() <-chan struct{} {
	s.incMu.Lock()
	defer s.incMu.Unlock()
	if s.incChanged == nil {
		s.incChanged = make(chan struct{})
	}
	return s.incChanged
}

// LastIncarnationChange reports why and when the store took its current id; ok
// is false for a store that still has the id it was created with.
func (s *Store) LastIncarnationChange() (change IncarnationChange, ok bool) {
	v, closer, err := s.db.Get(incarnationChangeKey())
	if err != nil {
		return IncarnationChange{}, false
	}
	defer closer.Close()
	if json.Unmarshal(v, &change) != nil {
		return IncarnationChange{}, false
	}
	return change, true
}

// ChildStore is the incarnation id a child last replicated from, "" when none
// is recorded (see AdoptChildStore).
func (s *Store) ChildStore(child string) string {
	if known, ok := s.adopted.Load(child); ok {
		return known.(string)
	}
	v, closer, err := s.db.Get(childStoreKey(child))
	if err != nil {
		return ""
	}
	defer closer.Close()
	return string(v)
}

// ParentState returns the document this node keeps about one parent, nil when
// it has none. The store does not read it; the replication client owns it.
func (s *Store) ParentState(parent string) ([]byte, error) {
	v, closer, err := s.db.Get(parentStateKey(parent))
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read the state kept for parent %s: %w", parent, err)
	}
	defer closer.Close()
	return append([]byte(nil), v...), nil
}

// SetParentState replaces the document kept about one parent, synced.
func (s *Store) SetParentState(parent string, state []byte) error {
	return s.db.Set(parentStateKey(parent), state, pebble.Sync)
}

// AckedMark is the highest offset of stream the parent confirmed as stored, 0
// when it confirmed none since this store took its incarnation.
func (s *Store) AckedMark(parent, stream string) uint64 {
	return s.readU64(ackedKey(parent, stream), 0)
}

// CursorAckMarked is CursorAck that also records, in the same synced batch,
// the highest offset the parent confirmed. The cursor moves forward only; the
// mark is set to what the parent said. It reports whether the cursor moved.
func (s *Store) CursorAckMarked(name, stream string, off uint64, parent string, mark uint64) (moved bool, err error) {
	s.cursorMu.Lock()
	defer s.cursorMu.Unlock()
	b := s.db.NewBatch()
	defer b.Close()
	if off > s.readU64(cursorKey(name, stream), 1) {
		if err := b.Set(cursorKey(name, stream), be64(off), nil); err != nil {
			return false, err
		}
		if err := b.Set(ctKey(name, stream), be64(uint64(time.Now().UnixMilli())), nil); err != nil { //nolint:gosec // a positive timestamp
			return false, err
		}
		moved = true
	}
	if mark != s.readU64(ackedKey(parent, stream), 0) {
		if err := b.Set(ackedKey(parent, stream), be64(mark), nil); err != nil {
			return false, err
		}
	}
	if b.Empty() {
		return false, nil
	}
	if err := s.db.Apply(b, pebble.Sync); err != nil {
		return false, err
	}
	if moved {
		s.signalBacklogChange()
	}
	return moved, nil
}

// CursorPlacement puts a cursor at an exact position, which may be behind
// where it stands.
type CursorPlacement struct {
	Name   string
	Stream string
	To     uint64
}

// ResyncParent applies what a re-sync with a parent decided, in one synced
// batch: every cursor placed where the parent's store now stands, the marks of
// what it confirmed, and the document recording the re-sync. A crash leaves
// either the old positions with the old document or all of the new ones, so a
// re-sync is never recorded as done with a cursor still ahead of the parent.
func (s *Store) ResyncParent(parent string, state []byte, cursors []CursorPlacement, marks map[string]uint64) error {
	s.cursorMu.Lock()
	defer s.cursorMu.Unlock()
	b := s.db.NewBatch()
	defer b.Close()
	now := be64(uint64(time.Now().UnixMilli())) //nolint:gosec // a positive timestamp
	for _, c := range cursors {
		if err := b.Set(cursorKey(c.Name, c.Stream), be64(c.To), nil); err != nil {
			return err
		}
		if err := b.Set(ctKey(c.Name, c.Stream), now, nil); err != nil {
			return err
		}
	}
	for stream, mark := range marks {
		if err := b.Set(ackedKey(parent, stream), be64(mark), nil); err != nil {
			return err
		}
	}
	if err := b.Set(parentStateKey(parent), state, nil); err != nil {
		return err
	}
	if err := s.db.Apply(b, pebble.Sync); err != nil {
		return fmt.Errorf("persist the re-sync with parent %s: %w", parent, err)
	}
	s.signalBacklogChange()
	return nil
}
