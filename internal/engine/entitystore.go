package engine

import (
	"github.com/alpamayo-solutions/colca/internal/store"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// entityStore adapts the engine and its store to the record surface the domain
// plugin declares (uns.EntityStore). The plugin never sees a core type: this
// satisfies its interface structurally, which is what lets domain logic touch
// storage without a single import pointing from the plugin at the core.
type entityStore struct{ e *Engine }

// EntityStore returns the plugin-facing record surface of this node. Wiring
// hands it to the executors that need it.
func (e *Engine) EntityStore() *entityStore { return &entityStore{e: e} }

func (s *entityStore) NodeID() string { return s.e.cfg.ULID }

func (s *entityStore) KVGet(topic string) ([]byte, bool) {
	p, err := uns.Parse(topic)
	if err != nil {
		return nil, false
	}
	for _, kv := range s.e.store.KVScan(p.Path) {
		if kv.Topic == topic {
			return kv.Payload, true
		}
	}
	return nil, false
}

// KVScan filters the projection by contract and publishing identity. Both come
// from the topic, so this needs no extra index; the scan is over the node's
// entity set, which is the set a human curates — hundreds to thousands, not the
// metric stream.
func (s *entityStore) KVScan(contract, nodeID string) []uns.KVRecord {
	return s.scan(contract, func(kv store.KVEntry) bool { return kv.NodeID == nodeID })
}

// KVScanAll is KVScan without the identity filter: every record of the contract
// this node holds, at the path it holds it under.
func (s *entityStore) KVScanAll(contract string) []uns.KVRecord {
	return s.scan(contract, func(store.KVEntry) bool { return true })
}

func (s *entityStore) scan(contract string, keep func(store.KVEntry) bool) []uns.KVRecord {
	var out []uns.KVRecord
	for _, kv := range s.e.store.KVScan("") {
		if !keep(kv) {
			continue
		}
		p, err := uns.Parse(kv.Topic)
		if err != nil || p.Contract != contract {
			continue
		}
		out = append(out, uns.KVRecord{
			Topic:        kv.Topic,
			Path:         p.Path,
			NodeID:       kv.NodeID,
			Payload:      kv.Payload,
			Offset:       kv.Offset,
			OriginOffset: kv.OriginOffset,
		})
	}
	return out
}

// Publish writes as the node through the local admin path: node-local
// coordinates, no mount rewrite, still validated against the loaded bundle. A
// record the node itself authors goes through the same door as everything else
// — there is no privileged write that skips validation.
func (s *entityStore) Publish(topic string, payload []byte) (uns.StateWrite, error) {
	result, err := s.e.IngestAdmin(topic, payload)
	return uns.StateWrite{
		Stream: result.Stream,
		Offset: result.Offset,
		Topic:  result.Topic,
	}, err
}

// PublishBatch is the domain command commit boundary. The engine validates the
// complete result set before it opens one Pebble batch; the adapter merely
// translates the engine's durable coordinates back into plugin-owned types.
func (s *entityStore) PublishBatch(records []uns.StateRecord) ([]uns.StateWrite, error) {
	results, err := s.e.ingestAdminStateBatch(records, Attribution{WrittenBy: "admin"})
	if err != nil {
		return nil, err
	}
	writes := make([]uns.StateWrite, len(results))
	for i, result := range results {
		writes[i] = uns.StateWrite{
			Stream: result.Stream,
			Offset: result.Offset,
			Topic:  result.Topic,
		}
	}
	return writes, nil
}
