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
	kvs, err := s.e.store.KVScan(p.Path)
	if err != nil {
		// uns.EntityStore's KVGet signature carries no error (a narrow, stable
		// port the domain package depends on) — a caller through this port
		// sees a false "not found", which is wrong but not destructive
		// (resources design §8). Logged at ERROR
		// so the failure is never silent, only unpropagated.
		s.e.log.Error("kv scan failed — entity read answered not-found rather than the true state",
			"path", p.Path, "err", err)
		return nil, false
	}
	for _, kv := range kvs {
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

// scan is the uns.EntityStore port's swallow-and-log adapter over
// Engine.scanContract: KVScan/KVScanAll carry no error in their signature (a
// narrow, stable port the domain package depends on), so a storage failure
// here is logged at ERROR and answered as "nothing found" — wrong, but not
// destructive, for every consumer this port currently has. A consumer whose
// decision on an empty answer WOULD be destructive (the blob sweeper marking
// every blob unreferenced) must not go through this port; it uses
// Engine.ScanContractAll instead, which surfaces the error (resources design §8).
func (s *entityStore) scan(contract string, keep func(store.KVEntry) bool) []uns.KVRecord {
	out, err := s.e.scanContract(contract, keep)
	if err != nil {
		s.e.log.Error("kv scan failed — entity records answered as absent rather than the true state",
			"contract", contract, "err", err)
		return nil
	}
	return out
}

// scanContract walks the whole KV projection once and returns the records of
// one contract that keep accepts, converted to the plugin-facing
// uns.KVRecord. Shared by entityStore.scan's swallow-and-log adapter and
// ScanContractAll's error-surfacing one below — same walk, two different
// answers to "what do I do when the store itself failed".
func (e *Engine) scanContract(contract string, keep func(store.KVEntry) bool) ([]uns.KVRecord, error) {
	kvs, err := e.store.KVScan("")
	if err != nil {
		return nil, err
	}
	var out []uns.KVRecord
	for _, kv := range kvs {
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
	return out, nil
}

// ScanContractAll is EntityStore().KVScanAll(contract) with a storage failure
// surfaced instead of swallowed to an empty result (critical-finding fix
// round, resources design §8). A caller whose decision on an empty answer is
// destructive — the blob sweeper marking every blob unreferenced because it
// read zero live resources — must be able to tell "nothing referenced this"
// apart from "the scan itself failed": the two demand opposite actions, and
// uns.EntityStore's own KVScanAll cannot make that distinction (its signature
// is a stable, narrow port other, non-destructive consumers already depend
// on).
func (e *Engine) ScanContractAll(contract string) ([]uns.KVRecord, error) {
	return e.scanContract(contract, func(store.KVEntry) bool { return true })
}

// PublishBatch is the domain command commit boundary, and the plugin's only
// write door. It writes as the node in node-local coordinates, no mount
// rewrite, still validated against the loaded bundle — a record the node itself
// authors passes the same checks as everything else. The engine validates the
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
