package store

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/alpamayo-solutions/colca/plugins/uns"
	"github.com/cockroachdb/pebble/v2"
)

// metricSourceLocalOnly runs under s.mu, the same lock as signal state writes.
// The source topic identifies its binding at acceptance; subsequent renames,
// deletion or policy changes cannot alter the stored sample's upload decision.
// Replicated records bypass this path: the source has already decided.
func (s *Store) metricSourceLocalOnly(rec Record) (bool, error) {
	p, err := uns.Parse(rec.Topic)
	if err != nil || !uns.IsMetric(p.Contract) {
		return false, nil
	}
	value, closer, err := s.db.Get(kvKey(p.Path, p.NodeID, uns.SignalTopicForMetric(p)))
	if errors.Is(err, pebble.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	defer closer.Close()
	var entry kvEnc
	if err := json.Unmarshal(value, &entry); err != nil {
		return false, err
	}
	var signal map[string]json.RawMessage
	if err := json.Unmarshal(entry.Payload, &signal); err != nil {
		return false, err
	}
	raw, exists := signal["replication_policy"]
	if !exists {
		return false, nil
	}
	var policy string
	if err := json.Unmarshal(raw, &policy); err != nil {
		return false, err
	}
	switch policy {
	case "replicate_to_parents":
		return false, nil
	case "source_local_only":
		return true, nil
	default:
		return false, fmt.Errorf("invalid signal replication_policy %q", policy)
	}
}
