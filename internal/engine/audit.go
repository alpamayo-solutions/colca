package engine

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"time"

	"github.com/alpamayo-solutions/colca/internal/store"
	"github.com/alpamayo-solutions/colca/plugins/uns"
	"github.com/oklog/ulid/v2"
)

// AuditDenial is the input to the internal audit writer. It only has fields for
// identifiers and allow-listed context, so credentials and request bodies cannot
// get in.
type AuditDenial struct {
	Operation  string
	ReasonCode string
	ActorID    string
	ActorLabel string
	ActorKind  string
	EntityType string
	EntityID   string
	Metadata   map[string]any
}

// SetAuditIDSource replaces event-id generation. It exists for deterministic
// tests and must be set before serving any door.
func (e *Engine) SetAuditIDSource(next func(time.Time) string) { e.auditID = next }

func newAuditID(now time.Time) string {
	return ulid.MustNew(ulid.Timestamp(now), rand.Reader).String()
}

// RecordDenial appends straight to the audit stream, bypassing the Ingest*
// methods, so an audit failure cannot recurse through the path that denied.
func (e *Engine) RecordDenial(d AuditDenial) error {
	if d.ActorKind == "" {
		d.ActorKind = "anonymous"
	}
	nowTime := e.clk.AuthoritativeNow()
	eventID := e.auditID(nowTime)
	now := nowTime.UnixMilli()
	for key, value := range d.Metadata {
		if !auditMetadataKey[key] || !flatAuditMetadata(value) {
			return e.auditFailure(d, fmt.Errorf("unsafe audit metadata %q", key))
		}
	}
	payload := map[string]any{
		"event_id": eventID, "source": "colca", "action": "authorize",
		"outcome": "denied", "actor_kind": d.ActorKind,
		"occurred_at": now, "operation": d.Operation, "reason_code": d.ReasonCode,
	}
	if d.ActorID != "" {
		payload["actor_id"] = d.ActorID
	}
	if d.ActorLabel != "" {
		payload["actor_label"] = d.ActorLabel
	}
	if d.EntityType != "" {
		payload["entity_type"] = d.EntityType
	}
	if d.EntityID != "" {
		payload["entity_id"] = d.EntityID
	}
	if len(d.Metadata) > 0 {
		payload["metadata"] = d.Metadata
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return e.auditFailure(d, fmt.Errorf("encode audit event: %w", err))
	}
	topic := uns.Prefix() + "_AuditEvent/" + e.cfg.ULID + "/_colca/audit/" + eventID
	// An oversize audit record is not counted as RecordRejected, which is for
	// ingress refusals; auditFailure counts it below.
	first, _, err := e.store.Append("audit", []store.Record{{
		Topic: topic, Payload: raw, TS: now, WrittenBy: e.cfg.ULID,
		ActorID: d.ActorID, ActorLabel: d.ActorLabel, ActorKind: d.ActorKind,
	}})
	if err != nil {
		return e.auditFailure(d, err)
	}
	e.metrics.IngestRecord("audit")
	e.log.Debug("audit denial", "offset", first, "operation", d.Operation, "reason_code", d.ReasonCode)
	if e.deliver != nil {
		e.deliver(topic, raw, false)
	}
	return nil
}

var auditMetadataKey = map[string]bool{
	"door": true, "route": true, "method": true, "stream": true,
	"contract": true, "filter": true, "cursor": true,
}

func flatAuditMetadata(value any) bool {
	switch value.(type) {
	case nil, string, bool, float64, int, int64, uint64:
		return true
	}
	switch v := value.(type) {
	case []string:
		return true
	case []any:
		for _, item := range v {
			switch item.(type) {
			case nil, string, bool, float64, int, int64, uint64:
			default:
				return false
			}
		}
		return true
	default:
		return false
	}
}

func (e *Engine) auditFailure(d AuditDenial, err error) error {
	e.metrics.AuditWriteFailure()
	e.log.Error("SECURITY AUDIT APPEND FAILED; protected operation remains denied",
		"operation", d.Operation, "reason_code", d.ReasonCode, "err", err)
	return err
}

func (e *Engine) recordDenial(d AuditDenial) {
	_ = e.RecordDenial(d)
}
