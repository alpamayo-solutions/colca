package store

import (
	"bytes"
	"encoding/json"
	"errors"

	"github.com/cockroachdb/pebble/v2"

	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// policyCacheMax bounds the cached replication decisions: one per signal a
// node holds metrics for. Past it the cache starts over.
const policyCacheMax = 1 << 16

// cachedPolicy is a signal's replication decision with the stored KV value it
// was read from.
type cachedPolicy struct {
	value     []byte
	localOnly bool
}

// metricSourceLocalOnly runs under s.mu, the same lock as signal state writes.
// The source topic identifies its binding at acceptance; subsequent renames,
// deletion or policy changes cannot alter the stored sample's upload decision.
// Replicated records bypass this path: the source has already decided.
//
// What a signal's policy says is the plugin's to read (uns.SignalSourceLocalOnly).
// The answer is cached with the KV value it came from: a sample costs a point
// read and a byte comparison, not two JSON decodes, and a changed signal is
// read again because its stored value differs.
func (s *Store) metricSourceLocalOnly(rec Record) (bool, error) {
	path, node, signalTopic, ok := uns.MetricSignalKey(rec.Topic)
	if !ok {
		return false, nil
	}
	key := kvKey(path, node, signalTopic)
	value, closer, err := s.db.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	defer closer.Close()
	if cached, ok := s.policies[string(key)]; ok && bytes.Equal(cached.value, value) {
		return cached.localOnly, nil
	}
	var entry kvEnc
	if err := json.Unmarshal(value, &entry); err != nil {
		return false, err
	}
	localOnly, err := uns.SignalSourceLocalOnly(entry.Payload)
	if err != nil {
		return false, err
	}
	if s.policies == nil || len(s.policies) >= policyCacheMax {
		s.policies = map[string]cachedPolicy{}
	}
	s.policies[string(key)] = cachedPolicy{value: bytes.Clone(value), localOnly: localOnly}
	return localOnly, nil
}
