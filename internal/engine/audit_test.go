package engine

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestRecordDenialAppendsExactlyOneSafeEventWithoutPublicIngest(t *testing.T) {
	e, delivered := newRecordingEngine(t)
	e.SetAuditIDSource(func(time.Time) string { return "01KTESTAUDIT00000000000000" })

	err := e.RecordDenial(AuditDenial{
		Operation: "publish", ReasonCode: "write_denied",
		ActorID: "m1", ActorLabel: "Mixer 1", ActorKind: "service",
		EntityType: "_Metric", EntityID: "m1/temp",
		Metadata: map[string]any{"door": "mqtt", "contract": "_Metric"},
	})
	if err != nil {
		t.Fatal(err)
	}
	recs, _, err := e.Store().Read("audit", 1, 10, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 1 {
		t.Fatalf("audit records = %d, want exactly 1", len(recs))
	}
	if recs[0].Topic != "colca/v1/_AuditEvent/n-edge1/_colca/audit/01KTESTAUDIT00000000000000" {
		t.Fatalf("topic = %q", recs[0].Topic)
	}
	var payload map[string]any
	if err := json.Unmarshal(recs[0].Payload, &payload); err != nil {
		t.Fatal(err)
	}
	for key, want := range map[string]any{
		"source": "colca", "action": "authorize", "outcome": "denied",
		"operation": "publish", "reason_code": "write_denied", "actor_id": "m1",
	} {
		if payload[key] != want {
			t.Fatalf("payload[%q] = %#v, want %#v", key, payload[key], want)
		}
	}
	if strings.Contains(string(recs[0].Payload), "token") || strings.Contains(string(recs[0].Payload), "password") {
		t.Fatalf("sensitive material entered payload: %s", recs[0].Payload)
	}
	got := delivered.got()
	if len(got) != 1 || got[0].Retain {
		t.Fatalf("delivery = %#v, want one unretained event", got)
	}
}

func TestRecordDenialRejectsUnsafeMetadataWithoutRecursiveAppend(t *testing.T) {
	e := newEngine(t)
	e.SetAuditIDSource(func(time.Time) string { return "01KTESTAUDIT00000000000000" })
	err := e.RecordDenial(AuditDenial{
		Operation: "authenticate", ReasonCode: "bad_token",
		Metadata: map[string]any{"authorization": "Bearer secret"},
	})
	if err == nil {
		t.Fatal("unsafe metadata accepted")
	}
	recs, _, readErr := e.Store().Read("audit", 1, 10, nil)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if len(recs) != 0 {
		t.Fatalf("audit records = %d, want 0", len(recs))
	}
}
