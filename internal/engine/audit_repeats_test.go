package engine

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/alpamayo-solutions/colca/internal/clock"
	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/store"
)

func newClockedEngine(t *testing.T, now *time.Time) *Engine {
	t.Helper()
	s, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return New(s, &config.Config{ULID: "n-hub"}, testIDs(), nil, nil, clock.New(true, func() time.Time { return *now }))
}

func auditPayloads(t *testing.T, e *Engine) []map[string]any {
	t.Helper()
	recs, _, err := e.Store().Read("audit", 1, 10000, nil)
	if err != nil {
		t.Fatal(err)
	}
	out := make([]map[string]any, 0, len(recs))
	for _, rec := range recs {
		var payload map[string]any
		if err := json.Unmarshal(rec.Payload, &payload); err != nil {
			t.Fatal(err)
		}
		out = append(out, payload)
	}
	return out
}

func unknownKey(peer, route string) AuditDenial {
	return AuditDenial{
		Operation: "authenticate", ReasonCode: "unknown_key", Peer: peer,
		Metadata: map[string]any{"door": "repl", "route": route},
	}
}

// An edge that runs before its enrolment is refused on every route it tries,
// several times a second. The parent writes the first refusal and then one
// record per window with the count, under the key that knocked.
func TestRepeatedDenialIsWrittenOnceAndThenCounted(t *testing.T) {
	now := time.Date(2026, 10, 13, 5, 20, 0, 0, time.UTC)
	e := newClockedEngine(t, &now)

	const perWindow = 600
	for i := range 2 * perWindow {
		route := "/replicate"
		if i%2 == 1 {
			route = "/downlink"
		}
		if err := e.RecordDenial(unknownKey("ab12cd34ef567890", route)); err != nil {
			t.Fatal(err)
		}
		now = now.Add(auditRepeatWindow / perWindow)
	}
	got := auditPayloads(t, e)
	if len(got) != 2 {
		t.Fatalf("audit records = %d, want the first denial and one count for the window that ended", len(got))
	}
	first, second := got[0], got[1]
	if first["actor_label"] != "unknown key ab12cd34ef567890" || first["actor_kind"] != "anonymous" {
		t.Fatalf("first record names %v (%v), want the key that knocked", first["actor_label"], first["actor_kind"])
	}
	if meta := first["metadata"].(map[string]any); meta["peer"] != "ab12cd34ef567890" || meta["repeated"] != nil {
		t.Fatalf("first record metadata = %v", meta)
	}
	meta := second["metadata"].(map[string]any)
	if meta["repeated"] != float64(perWindow-1) {
		t.Fatalf("second record counts %v repeats, want %d", meta["repeated"], perWindow-1)
	}
	if meta["repeated_since"] != float64(time.Date(2026, 10, 13, 5, 20, 0, 0, time.UTC).UnixMilli()) {
		t.Fatalf("repeated_since = %v", meta["repeated_since"])
	}

	// The caller stops. What was counted since the last record is written once
	// its window ended, and nothing after that.
	now = now.Add(-time.Second)
	e.writeAuditRepeats(false)
	if n := len(auditPayloads(t, e)); n != 2 {
		t.Fatalf("a window that has not ended wrote a record: %d", n)
	}
	now = now.Add(auditRepeatWindow)
	e.writeAuditRepeats(false)
	got = auditPayloads(t, e)
	if len(got) != 3 || got[2]["metadata"].(map[string]any)["repeated"] != float64(perWindow-1) {
		t.Fatalf("after the caller stopped: %d records, last %v", len(got), got[len(got)-1])
	}
	e.writeAuditRepeats(true)
	if n := len(auditPayloads(t, e)); n != 3 {
		t.Fatalf("an account without repeats wrote a record: %d", n)
	}
}

// Another key, another reason or another subject is another denial.
func TestDistinctDenialsAreEachWritten(t *testing.T) {
	now := time.Date(2026, 10, 13, 5, 20, 0, 0, time.UTC)
	e := newClockedEngine(t, &now)
	denials := []AuditDenial{
		unknownKey("aaaaaaaaaaaaaaaa", "/replicate"),
		unknownKey("bbbbbbbbbbbbbbbb", "/replicate"),
		{Operation: "authenticate", ReasonCode: "kind", ActorID: "m1", Metadata: map[string]any{"door": "repl"}},
		{Operation: "read", ReasonCode: "subscribe_denied", ActorID: "m1", Metadata: map[string]any{"door": "mqtt", "filter": "a/#"}},
		{Operation: "read", ReasonCode: "subscribe_denied", ActorID: "m1", Metadata: map[string]any{"door": "mqtt", "filter": "b/#"}},
		{Operation: "read", ReasonCode: "subscribe_denied", ActorID: "hmi", Metadata: map[string]any{"door": "mqtt", "filter": "a/#"}},
	}
	for range 3 {
		for _, d := range denials {
			if err := e.RecordDenial(d); err != nil {
				t.Fatal(err)
			}
		}
	}
	if n := len(auditPayloads(t, e)); n != len(denials) {
		t.Fatalf("audit records = %d, want one per distinct denial (%d)", n, len(denials))
	}
	// On stop every count still held is written.
	e.FlushAuditRepeats()
	got := auditPayloads(t, e)
	if len(got) != 2*len(denials) {
		t.Fatalf("after the flush: %d records, want %d", len(got), 2*len(denials))
	}
	for _, payload := range got[len(denials):] {
		if payload["metadata"].(map[string]any)["repeated"] != float64(2) {
			t.Fatalf("flushed record = %v, want 2 repeats", payload)
		}
	}
}

// A flood of keys nobody enrolled must not grow the account without bound, and
// must not become a record per key either.
func TestDenialsOfMoreKeysThanAreTrackedShareOneAccount(t *testing.T) {
	now := time.Date(2026, 10, 13, 5, 20, 0, 0, time.UTC)
	e := newClockedEngine(t, &now)
	const extra = 500
	for i := range maxAuditRepeatKeys + extra {
		if err := e.RecordDenial(unknownKey(fmt.Sprintf("%016x", i), "/replicate")); err != nil {
			t.Fatal(err)
		}
	}
	if n := len(auditPayloads(t, e)); n != maxAuditRepeatKeys+1 {
		t.Fatalf("audit records = %d, want %d: one per tracked key and one for the shared account", n, maxAuditRepeatKeys+1)
	}
	if n := len(e.auditRepeats.m); n != maxAuditRepeatKeys+1 {
		t.Fatalf("tracked denials = %d, want %d", n, maxAuditRepeatKeys+1)
	}
}
