package engine

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/alpamayo-solutions/colca/internal/contracts"
	"github.com/alpamayo-solutions/colca/internal/contracts/contractstest"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// bundleAuditLimits reads the maxLength of every `_AuditEvent` field from the
// generated bundle: the limits the contract declares, as the doors enforce them.
func bundleAuditLimits(t *testing.T) map[string]int {
	t.Helper()
	raw, err := os.ReadFile(contractstest.GeneratedBundlePath(t))
	if err != nil {
		t.Fatal(err)
	}
	var bundle struct {
		Contracts map[string]struct {
			Schema struct {
				Properties map[string]struct {
					MaxLength *int `json:"maxLength"`
				} `json:"properties"`
			} `json:"schema"`
		} `json:"contracts"`
	}
	if err := json.Unmarshal(raw, &bundle); err != nil {
		t.Fatal(err)
	}
	limits := map[string]int{}
	for field, schema := range bundle.Contracts["_AuditEvent"].Schema.Properties {
		if schema.MaxLength != nil {
			limits[field] = *schema.MaxLength
		}
	}
	return limits
}

// The limits the node's own audit writer cuts to are the contract's: every
// field the contract bounds is in the Go table with the same number, and the
// table bounds nothing the contract does not. A limit changed in payload.py
// without the table following turns this red.
func TestTheAuditWriterLimitsAreTheContracts(t *testing.T) {
	declared := bundleAuditLimits(t)
	if len(declared) == 0 {
		t.Fatal("the bundle declares no _AuditEvent limit")
	}
	table := uns.AuditFieldLimits()
	for field, limit := range declared {
		if table[field] != limit {
			t.Errorf("%s: the contract allows %d characters, the writer's table says %d", field, limit, table[field])
		}
	}
	for field, limit := range table {
		if _, ok := declared[field]; !ok {
			t.Errorf("%s: the writer's table says %d, the contract declares no limit", field, limit)
		}
	}
}

// A denial repeats what the refused caller sent, so its fields can be any
// length. The record the node writes for it stays inside the contract: the
// bundle's schema accepts it, which is what makes it storable by a read side
// that sizes its columns from the same contract. Before, a refused topic's
// path went into entity_id whole and the projection set the record aside.
func TestADenialOfAnyLengthIsWrittenInsideTheContract(t *testing.T) {
	e := newEngine(t)
	tbl, err := contracts.Load(contractstest.GeneratedBundlePath(t), "")
	if err != nil {
		t.Fatal(err)
	}
	rule, ok := tbl.Lookup("_AuditEvent")
	if !ok {
		t.Fatal("_AuditEvent is not in the bundle")
	}
	// Two bytes per character, so cutting by bytes would leave half as many.
	long := strings.Repeat("ä", 5000)
	if err := e.RecordDenial(AuditDenial{
		Operation: long, ReasonCode: long, ActorID: long, ActorLabel: long, ActorKind: "service",
		EntityType: long, EntityID: long, Metadata: map[string]any{"door": "mqtt"},
	}); err != nil {
		t.Fatal(err)
	}
	recs, _, err := e.Store().Read("audit", 1, 10, nil)
	if err != nil || len(recs) != 1 {
		t.Fatalf("audit records = %d (%v), want 1", len(recs), err)
	}
	if err := rule.Validate(recs[0].Payload); err != nil {
		t.Fatalf("the node wrote an audit record its own contract refuses: %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal(recs[0].Payload, &payload); err != nil {
		t.Fatal(err)
	}
	for field, limit := range bundleAuditLimits(t) {
		if field == "event_id" || field == "correlation_id" {
			continue // the writer makes the id itself and writes no correlation
		}
		value, _ := payload[field].(string)
		if got := utf8.RuneCountInString(value); got != limit {
			t.Errorf("%s: %d characters written, want the contract's %d", field, got, limit)
		}
	}
	// The record's own attribution is the payload's, cut the same way.
	if recs[0].ActorID != payload["actor_id"] || recs[0].ActorLabel != payload["actor_label"] {
		t.Fatalf("envelope actor %q / %q differs from the payload's", recs[0].ActorID, recs[0].ActorLabel)
	}
}

// An unknown key's record names the key by its fingerprint; the label stays
// whole, because the limit leaves room for it.
func TestAnUnknownKeysLabelFitsTheContract(t *testing.T) {
	fingerprint := strings.Repeat("f", 64) // a SHA-256 in hex, the longest a fingerprint is
	label := "unknown key " + fingerprint
	if got := uns.BoundAuditField("actor_label", label); got != label {
		t.Fatalf("the label was cut to %q", got)
	}
}
