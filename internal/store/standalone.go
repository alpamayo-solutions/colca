package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/cockroachdb/pebble/v2"
	"time"
)

// StandaloneState is a one-way trust transition. Pending makes retirement
// resumable if startup stops after journaling but before revoking every entry.
type StandaloneState struct {
	CommandsBefore  uint64          `json:"commands_before,omitempty"`
	FormerAncestors []string        `json:"former_ancestors,omitempty"`
	Since           int64           `json:"since"`
	PATs            map[string]bool `json:"pats"`
	Identities      []string        `json:"identities"`
	Ready           bool            `json:"ready"`
	Pending         bool            `json:"pending"`
}

var standaloneKey = []byte("j\x00standalone")

func (s *Store) StandaloneGet() (*StandaloneState, error) {
	raw, closer, err := s.db.Get(standaloneKey)
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	var state StandaloneState
	if err := json.Unmarshal(raw, &state); err != nil {
		return nil, err
	}
	if state.Since <= 0 || state.PATs == nil {
		return nil, fmt.Errorf("invalid standalone journal")
	}
	return &state, nil
}
func (s *Store) StandalonePut(state *StandaloneState) error {
	raw, err := json.Marshal(state)
	if err != nil {
		return err
	}
	return s.db.Set(standaloneKey, raw, pebble.Sync)
}

// CompleteStandalone opens human authentication only after the local identity
// handover succeeded. Retries return the same cutoff, including after restart.
func (s *Store) CompleteStandalone() (*StandaloneState, error) {
	s.standaloneMu.Lock()
	defer s.standaloneMu.Unlock()
	state, err := s.StandaloneGet()
	if err != nil {
		return nil, err
	}
	if state == nil || state.Pending {
		return nil, fmt.Errorf("standalone retirement is not prepared")
	}
	if !state.Ready {
		state.Since = time.Now().Unix() + 1
		state.Ready = true
		if err := s.StandalonePut(state); err != nil {
			return nil, err
		}
	}
	return state, nil
}
