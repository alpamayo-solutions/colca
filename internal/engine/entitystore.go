package engine

import (
	"github.com/alpamayo-solutions/colca/internal/store"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// entityStore adapts the engine's store to uns.EntityStore. The plugin never sees
// a core type, so no import points from the plugin at the core.
type entityStore struct{ e *Engine }

// EntityStore returns the plugin-facing record surface of this node. Wiring
// hands it to the executors that need it.
func (e *Engine) EntityStore() uns.EntityStore { return &entityStore{e: e} }

func (s *entityStore) NodeID() string { return s.e.cfg.ULID }

func (s *entityStore) KVGet(topic string) ([]byte, bool) {
	p, err := uns.Parse(topic)
	if err != nil {
		return nil, false
	}
	kvs, err := s.e.store.KVScan(p.Path)
	if err != nil {
		// The port's KVGet cannot return an error, so a failed scan reads as not found.
		// Logged at error level so it is never silent.
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

// KVScan filters the projection by contract and publishing identity, both taken
// from the topic. It scans the entity set, which is small next to metrics.
func (s *entityStore) KVScan(contract, nodeID string) []uns.KVRecord {
	return s.scan(contract, func(kv store.KVEntry) bool { return kv.NodeID == nodeID })
}

// KVScanAll is KVScan without the identity filter: every record of the contract
// this node holds, at the path it holds it under.
func (s *entityStore) KVScanAll(contract string) []uns.KVRecord {
	return s.scan(contract, func(store.KVEntry) bool { return true })
}

// scan wraps scanContract for the uns.EntityStore port, which has no error
// return: a storage failure is logged and answered as nothing found. A caller for
// whom an empty answer is destructive, like the blob sweeper, uses
// ScanContractAll instead.
func (s *entityStore) scan(contract string, keep func(store.KVEntry) bool) []uns.KVRecord {
	out, err := s.e.scanContract(contract, keep)
	if err != nil {
		s.e.log.Error("kv scan failed — entity records answered as absent rather than the true state",
			"contract", contract, "err", err)
		return nil
	}
	return out
}

// scanContract walks the KV projection once and returns the records of one
// contract that keep accepts. Shared by scan and ScanContractAll, which differ
// only in how they treat a store failure.
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

// ScanContractAll is EntityStore().KVScanAll with storage failures returned
// instead of swallowed. The blob sweeper needs to tell "nothing references this"
// from "the scan failed".
func (e *Engine) ScanContractAll(contract string) ([]uns.KVRecord, error) {
	return e.scanContract(contract, func(store.KVEntry) bool { return true })
}

// PublishBatch is the plugin's only write door and the commit boundary of a
// domain command. It writes as the node in node-local coordinates and validates
// like any other write. The engine validates the whole set before opening one
// Pebble batch.
//
// The node is always written_by, but ctx carries the executing command's own
// attribution onward as actor_id/actor_label/actor_kind (see attribution),
// so a write a _CmdEdit or _CmdConfigure makes on a caller's behalf — a
// person setting an operator-input constant, a service upserting its
// signals — reads as written by the node, on behalf of that actor, exactly
// as this node's own _Ack records already do (see (*Engine).ack).
func (s *entityStore) PublishBatch(ctx uns.CommandContext, records []uns.StateRecord) ([]uns.StateWrite, error) {
	results, err := s.e.ingestAdminStateBatch(records, s.attribution(ctx))
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

// PublishEvent is PublishBatch for a class an executor may append but never
// author as state, such as an annotation: one record, no KV projection.
func (s *entityStore) PublishEvent(ctx uns.CommandContext, record uns.StateRecord) (uns.StateWrite, error) {
	result, err := s.e.ingestAdminEvent(record, s.attribution(ctx))
	if err != nil {
		return uns.StateWrite{}, err
	}
	return uns.StateWrite{Stream: result.Stream, Offset: result.Offset, Topic: result.Topic}, nil
}

// attribution builds the Attribution an executor's write carries. With no
// commanding actor (ctx.ActorID == "", the lifecycle trigger's autobind being
// the only such caller) it is today's plain administrative attribution,
// unchanged. With one, WrittenBy switches from that literal "admin" to this
// node's own ULID — the node is what actually appended the record, the same
// fact ack() already states for the outcome of the same command — and the
// commanding actor's own door-verified identity travels as actor_id,
// actor_label and actor_kind.
func (s *entityStore) attribution(ctx uns.CommandContext) Attribution {
	if ctx.ActorID == "" {
		return Attribution{WrittenBy: "admin"}
	}
	return Attribution{
		WrittenBy:  s.e.cfg.ULID,
		ActorID:    ctx.ActorID,
		ActorLabel: ctx.ActorLabel,
		ActorKind:  ctx.ActorKind,
	}
}
