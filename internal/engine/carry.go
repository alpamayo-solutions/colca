package engine

import (
	"sort"
	"strings"
	"time"

	"github.com/alpamayo-solutions/colca/internal/store"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// A command moves a signal or an element by retiring its record at one path
// and writing it, with the same id, at another. Other records stand at those
// positions too, written by other identities, and they would stay behind:
//
//   - a signal's current value, the _Metric at the signal's path. Left at the
//     old path, a reader of the new path sees no value until the producer
//     publishes again, which for a state that rarely changes can be days;
//   - the catalogue of a service placed at a moved element, the _DataTags at
//     <element path>/<service>. The metric door finds a bound signal's
//     producer by the catalogue topic its element now resolves to, so a
//     catalogue left at the old path refuses the producer's every value.
//
// carryRetained moves them after the command's own batch commits. This is the
// node moving state it already holds to follow an authorized configure; no
// value is written that a producer did not write, and the producer rule is
// not involved: a carried record keeps its payload, timestamp and attribution.

// positionMove is one signal or element a batch moves, in this node's frame.
type positionMove struct {
	contract string
	from, to string
}

// movedPositions returns the signals and elements of this node that records
// move: retired at one path and written with the same id at another. It reads
// the retired records' ids from KV, so it runs before the batch commits.
func (e *Engine) movedPositions(records []uns.StateRecord) []positionMove {
	type key struct{ contract, id string }
	retired := map[key]string{}
	written := map[key]string{}
	for _, record := range records {
		p, err := uns.Parse(record.Topic)
		if err != nil || p.NodeID != e.cfg.ULID || (p.Contract != "_Signal" && p.Contract != "_SystemElement") {
			continue
		}
		payload := record.Payload
		into := written
		if len(payload) == 0 {
			held, ok := e.EntityStore().KVGet(record.Topic)
			if !ok {
				continue
			}
			payload, into = held, retired
		}
		if id, _, err := stateIdentity(payload); err == nil && id != "" {
			into[key{p.Contract, id}] = p.Path
		}
	}
	var moves []positionMove
	for k, from := range retired {
		if to, ok := written[k]; ok && to != from {
			moves = append(moves, positionMove{contract: k.contract, from: from, to: to})
		}
	}
	sort.Slice(moves, func(i, j int) bool {
		if moves[i].contract != moves[j].contract {
			return moves[i].contract < moves[j].contract
		}
		return moves[i].from < moves[j].from
	})
	return moves
}

// carryRetained moves the values of moved signals and the catalogues of
// services placed at moved elements. A failure is logged: the command's own
// batch has committed and its outcome stands, and the records it did not
// carry stay where they were, as they did before this existed.
func (e *Engine) carryRetained(moves []positionMove, attribution Attribution) {
	if len(moves) == 0 {
		return
	}
	node := e.cfg.ULID
	var values, catalogues []store.RetainedMove
	for _, move := range moves {
		switch move.contract {
		case "_Signal":
			values = append(values, store.RetainedMove{
				Node: node, FromPath: move.from, ToPath: move.to,
				FromTopic: uns.Prefix() + "_Metric/" + node + "/" + move.from,
				ToTopic:   uns.Prefix() + "_Metric/" + node + "/" + move.to,
			})
		case "_SystemElement":
			catalogues = append(catalogues, e.placedCatalogues(move)...)
		}
	}
	e.moveRetained("_Metric", values, attribution)
	e.moveRetained("_DataTags", catalogues, attribution)
}

// placedCatalogues are the catalogues this node holds directly under a moved
// element: <element path>/<catalogue name>, the topic uns.CatalogueTopic gives
// a service placed there. Catalogues deeper down belong to services placed at
// descendant elements, and move when those do.
func (e *Engine) placedCatalogues(move positionMove) []store.RetainedMove {
	node := e.cfg.ULID
	var out []store.RetainedMove
	for _, record := range e.EntityStore().KVScan("_DataTags", node) {
		name, ok := strings.CutPrefix(record.Path, move.from+"/")
		if !ok || name == "" || strings.Contains(name, "/") {
			continue
		}
		out = append(out, store.RetainedMove{
			Node: node, FromPath: record.Path, FromTopic: record.Topic,
			ToPath:  move.to + "/" + name,
			ToTopic: uns.CatalogueTopic(node, move.to, name),
		})
	}
	return out
}

// moveRetained commits one contract's moves as one batch and applies what was
// written to the indexes and the bus, as every other door does.
func (e *Engine) moveRetained(contract string, moves []store.RetainedMove, attribution Attribution) {
	if len(moves) == 0 {
		return
	}
	class := e.ClassOf(contract)
	stream := uns.StreamFor(class)
	written, first, err := e.store.MoveRetained(stream, moves, store.Record{
		TS:        time.Now().UnixMilli(),
		WrittenBy: attribution.WrittenBy, ActorID: attribution.ActorID,
		ActorLabel: attribution.ActorLabel, ActorKind: attribution.ActorKind,
	})
	if err != nil {
		e.log.Error("a moved position's retained records were not carried; they stay at the old path",
			"contract", contract, "moves", len(moves), "err", err)
		return
	}
	for i, record := range written {
		e.metrics.IngestRecord(stream)
		e.log.Info("carried a retained record with its position", "stream", stream,
			"offset", first+uint64(i), "topic", record.Topic, "tombstone", record.Delete)
		e.observeIndexes(contract, record.Topic, record.Payload)
		if e.deliver != nil {
			e.deliver(record.Topic, record.Payload, retainFor(class))
		}
	}
}
