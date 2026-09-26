package engine

import (
	"errors"
	"time"

	"github.com/alpamayo-solutions/colca/internal/metrics"
	"github.com/alpamayo-solutions/colca/internal/store"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// BatchRecord is one record of a client batch publish.
type BatchRecord struct {
	Topic   string
	Payload []byte
}

// BatchResult is the outcome of one BatchRecord: the stored record, or why it
// was refused. A refused record does not stop the others.
type BatchResult struct {
	Result
	Err error
}

// IngestClientBatch ingests many data, state or ack records from one client in
// one request: each record is checked exactly as IngestClient checks it, and
// the admitted ones are written with one Store.Append per stream, then
// delivered on the bus in order. Commands and audit records are refused here:
// a command is answered one at a time through IngestClient.
//
// It exists for high-rate publishers (a bridge relaying a plant): one publish
// per sample costs a request, an append batch and a sync each.
func (e *Engine) IngestClientBatch(identity string, records []BatchRecord) []BatchResult {
	results := make([]BatchResult, len(records))
	type admitted struct {
		index  int
		parsed uns.Parsed
		class  uns.Class
		record store.Record
	}
	byStream := map[string][]admitted{}
	var streams []string
	ts := time.Now().UnixMilli()
	actorFor := attributionForEntry
	for i, in := range records {
		if !uns.IsUns(in.Topic) {
			results[i].Err = e.batchReject(metrics.ReasonGrammar, "topic outside %s/# is not persisted", uns.Root())
			continue
		}
		p, err := uns.Parse(in.Topic)
		if err != nil {
			results[i].Err = e.batchReject(metrics.ReasonGrammar, "%w", err)
			continue
		}
		if p.Contract == "_EnrolledIdentity" {
			results[i].Err = e.batchReject(metrics.ReasonRegistryContract, "_EnrolledIdentity is enrollment-door only")
			continue
		}
		class := e.ClassOf(p.Contract)
		switch {
		case uns.IsNodeLocal(class):
			results[i].Err = e.batchReject(metrics.ReasonTimeSync, "%s is node-local", p.Contract)
			continue
		case uns.IsCommand(class):
			results[i].Err = e.batchReject(metrics.ReasonValidation, "a command is published on its own, not in a batch")
			continue
		case uns.IsAudit(class):
			results[i].Err = e.batchReject(metrics.ReasonWriteDenied, "_AuditEvent is not published in a batch")
			continue
		}
		attribution, err := e.admitClientData(identity, p, class, in.Topic, in.Payload, actorFor)
		if err != nil {
			results[i].Err = err
			continue
		}
		rec := store.Record{
			Topic: in.Topic, Payload: in.Payload, TS: ts,
			WrittenBy: attribution.WrittenBy, ActorID: attribution.ActorID,
			ActorLabel: attribution.ActorLabel, ActorKind: attribution.ActorKind,
			ActorGroups: attribution.ActorGroups,
		}
		if uns.IsState(class) {
			rec.KVPath, rec.KVNode = p.Path, p.NodeID
			rec.Delete = len(in.Payload) == 0
		}
		stream := uns.StreamFor(class)
		if _, seen := byStream[stream]; !seen {
			streams = append(streams, stream)
		}
		byStream[stream] = append(byStream[stream], admitted{index: i, parsed: p, class: class, record: rec})
	}
	for _, stream := range streams {
		batch := byStream[stream]
		storeRecords := make([]store.Record, len(batch))
		for j := range batch {
			storeRecords[j] = batch[j].record
		}
		first, _, err := e.store.Append(stream, storeRecords)
		if err != nil {
			if errors.Is(err, store.ErrRecordTooLarge) {
				e.metrics.RecordRejected("too_large")
			}
			for _, item := range batch {
				results[item.index].Err = err
			}
			continue
		}
		for j, item := range batch {
			offset := first + uint64(j) //nolint:gosec // j indexes a slice
			topic, payload := item.record.Topic, item.record.Payload
			e.metrics.IngestRecord(stream)
			if uns.IsAck(item.class) {
				if id := correlationID(payload); id != "" {
					e.ledger.acked(id, topic, payload)
				}
			}
			e.elements.Observe(item.parsed.Contract, topic, payload)
			if e.deliver != nil {
				e.deliver(topic, payload, retainFor(item.class))
			}
			e.observe(item.parsed, topic, payload)
			e.checkMetricBinding(item.parsed)
			results[item.index].Result = Result{Persisted: true, Stream: stream, Offset: offset, Topic: topic}
		}
	}
	return results
}

func (e *Engine) batchReject(reason, format string, args ...any) error {
	_, err := e.reject(reason, format, args...)
	return err
}
