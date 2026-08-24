package engine

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"time"

	"github.com/alpamayo-solutions/colca/internal/store"
	"github.com/oklog/ulid/v2"
)

// AuditDenial is the safe, deliberately small input to Colca's internal audit
// writer. Callers pass identifiers and allow-listed context only; credentials,
// request bodies and arbitrary headers have no field through which to enter.
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

// RecordDenial appends directly to the audit stream. It intentionally does not
// call a public Ingest* method: an audit failure must never recurse through the
// same authorization path whose denial is being recorded.
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
	topic := "colca/v1/_AuditEvent/" + e.cfg.ULID + "/_colca/audit/" + eventID
	// store.ErrRecordTooLarge here is deliberately NOT counted as
	// RecordRejected: that metric means an INGRESS refusal, one with an
	// external author for the door to answer 4xx to. An audit record is
	// engine-authored, not ingress, and its write failure is already
	// counted below by auditFailure's AuditWriteFailure.
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
