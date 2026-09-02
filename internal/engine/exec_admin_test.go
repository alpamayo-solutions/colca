package engine

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/registry"
	"github.com/alpamayo-solutions/colca/internal/store"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// fakeAdmin records ExecAdmin's registry calls and simulates outcomes.
type fakeAdmin struct {
	enrolls []string // raw entry JSON per call
	revokes []string
	err     error // returned by both verbs when set
}

func (f *fakeAdmin) Enroll(entryJSON []byte) (string, uint64, error) {
	f.enrolls = append(f.enrolls, string(entryJSON))
	if f.err != nil {
		return "", 0, f.err
	}
	return "m9", 1, nil
}

func (f *fakeAdmin) Revoke(ulid string) (uint64, bool, error) {
	f.revokes = append(f.revokes, ulid)
	if f.err != nil {
		return 0, false, f.err
	}
	return 2, false, nil
}

func adminEngine(t *testing.T) (*Engine, *fakeAdmin) {
	t.Helper()
	s, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	cfg := &config.Config{ULID: "n-edge1"}
	ids := testIDs()
	ids.entries["n-child"] = &uns.Entry{ULID: "n-child", Kind: uns.KindNode, Element: "el-child1"}
	ids.entries["provisioner"] = &uns.Entry{ULID: "provisioner", Kind: uns.KindExternal, Element: "el-provisioner", Grants: []string{"cmd:#:admin"}}
	e := New(s, cfg, ids, nil, nil, nil)
	fa := &fakeAdmin{}
	e.SetExecutor(Executors(NewAdminExecutor(fa)))
	return e, fa
}

func futureMS() int64 { return time.Now().Add(time.Hour).UnixMilli() }

func enrollPayload(corr string, exp int64) []byte {
	entry := map[string]any{"ulid": "m9", "pubkey": strings.Repeat("ab", 32), "kind": "external", "mount": "m9"}
	b, _ := json.Marshal(map[string]any{"correlation_id": corr, "expires_at": exp, "entry": entry})
	return b
}

// ackFor scans the commands stream for the verb's ack and returns its payload.
func ackFor(t *testing.T, e *Engine, verb, corr string) map[string]any {
	t.Helper()
	recs, _, err := e.Store().Read("commands", 1, 100, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range recs {
		if r.Topic != "colca/v1/_Ack/n-edge1/"+verb {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal(r.Payload, &m); err != nil {
			t.Fatalf("ack payload: %v", err)
		}
		if m["correlation_id"] == corr {
			return m
		}
	}
	return nil
}

// A downlinked _CmdAdmin addressed to this node executes and acks 200.
func TestExecAdminEnrollOnDownlink(t *testing.T) {
	e, fa := adminEngine(t)
	if _, err := e.IngestDownlink("colca/v1/_CmdAdmin/n-edge1/enroll", enrollPayload("c-1", futureMS()), 1); err != nil {
		t.Fatal(err)
	}
	if len(fa.enrolls) != 1 || !strings.Contains(fa.enrolls[0], `"m9"`) {
		t.Fatalf("enroll calls: %v", fa.enrolls)
	}
	ack := ackFor(t, e, "enroll", "c-1")
	if ack == nil || ack["result_code"].(float64) != 200 {
		t.Fatalf("ack = %v, want result_code 200", ack)
	}
}

// Expired on arrival: no execution, ack 498.
func TestExecAdminExpired(t *testing.T) {
	e, fa := adminEngine(t)
	if _, err := e.IngestDownlink("colca/v1/_CmdAdmin/n-edge1/enroll", enrollPayload("c-2", 1000), 1); err != nil {
		t.Fatal(err)
	}
	if len(fa.enrolls) != 0 {
		t.Fatalf("expired command must not execute: %v", fa.enrolls)
	}
	ack := ackFor(t, e, "enroll", "c-2")
	if ack == nil || ack["result_code"].(float64) != 498 {
		t.Fatalf("ack = %v, want 498", ack)
	}
}

// Registry conflict → 409; other registry errors → 422; revoke of absent → 200.
func TestExecAdminResultCodes(t *testing.T) {
	e, fa := adminEngine(t)
	fa.err = fmt.Errorf("enroll m9: pubkey already enrolled: %w", registry.ErrConflict)
	if _, err := e.IngestDownlink("colca/v1/_CmdAdmin/n-edge1/enroll", enrollPayload("c-3", futureMS()), 1); err != nil {
		t.Fatal(err)
	}
	if ack := ackFor(t, e, "enroll", "c-3"); ack == nil || ack["result_code"].(float64) != 409 {
		t.Fatalf("conflict ack = %v, want 409", ack)
	}

	fa.err = fmt.Errorf("entry m9: pubkey must be 64 hex chars")
	if _, err := e.IngestDownlink("colca/v1/_CmdAdmin/n-edge1/enroll", enrollPayload("c-4", futureMS()), 2); err != nil {
		t.Fatal(err)
	}
	if ack := ackFor(t, e, "enroll", "c-4"); ack == nil || ack["result_code"].(float64) != 422 {
		t.Fatalf("invalid ack = %v, want 422", ack)
	}

	fa.err = fmt.Errorf("revoke m9: %w", registry.ErrNotEnrolled)
	rp, _ := json.Marshal(map[string]any{"correlation_id": "c-5", "expires_at": futureMS(), "ulid": "m9"})
	if _, err := e.IngestDownlink("colca/v1/_CmdAdmin/n-edge1/revoke", rp, 3); err != nil {
		t.Fatal(err)
	}
	if ack := ackFor(t, e, "revoke", "c-5"); ack == nil || ack["result_code"].(float64) != 200 {
		t.Fatalf("absent-revoke ack = %v, want 200 (idempotent)", ack)
	}
}

// Unknown verb acks 422 without touching the registry.
func TestExecAdminUnknownVerb(t *testing.T) {
	e, fa := adminEngine(t)
	if _, err := e.IngestDownlink("colca/v1/_CmdAdmin/n-edge1/reboot", enrollPayload("c-6", futureMS()), 1); err != nil {
		t.Fatal(err)
	}
	if len(fa.enrolls)+len(fa.revokes) != 0 {
		t.Fatal("unknown verb must not execute")
	}
	if ack := ackFor(t, e, "reboot", "c-6"); ack == nil || ack["result_code"].(float64) != 422 {
		t.Fatalf("unknown-verb ack = %v, want 422", ack)
	}
}

// A _CmdAdmin addressed to ANOTHER node is stored and forwarded, never run.
func TestExecAdminIgnoresOtherTargets(t *testing.T) {
	e, fa := adminEngine(t)
	if _, err := e.IngestDownlink("colca/v1/_CmdAdmin/n-other/child1/x/enroll", enrollPayload("c-7", futureMS()), 1); err != nil {
		t.Fatal(err)
	}
	if len(fa.enrolls) != 0 {
		t.Fatal("foreign target must not execute here")
	}
	if ack := ackFor(t, e, "enroll", "c-7"); ack != nil {
		t.Fatalf("foreign target must not be acked here: %v", ack)
	}
}

// Records pushed UP by a child never execute (commands flow down; a child
// must not administer its ancestors) — even when they name this node.
func TestExecAdminNeverOnReplicated(t *testing.T) {
	e, fa := adminEngine(t)
	recs := []store.ReplRecord{{ChildOffset: 1, Topic: "colca/v1/_CmdAdmin/n-edge1/enroll", Payload: enrollPayload("c-8", futureMS()), TS: 1}}
	if _, _, err := e.IngestReplicated("n-child", "commands", recs); err != nil {
		t.Fatal(err)
	}
	if len(fa.enrolls) != 0 {
		t.Fatal("replicated (uplink) _CmdAdmin must never execute")
	}
}

// Self-target through the local doors: admin token and a machine holding
// cmd:#:admin both reach execution; re-execution is idempotent (two calls).
func TestExecAdminSelfTargetLocalDoors(t *testing.T) {
	e, fa := adminEngine(t)
	if _, err := e.IngestAdmin("colca/v1/_CmdAdmin/n-edge1/enroll", enrollPayload("c-9", futureMS())); err != nil {
		t.Fatal(err)
	}
	if _, err := e.IngestClient("provisioner", "colca/v1/_CmdAdmin/n-edge1/enroll", enrollPayload("c-10", futureMS())); err != nil {
		t.Fatal(err)
	}
	if len(fa.enrolls) != 2 {
		t.Fatalf("local-door executions = %d, want 2", len(fa.enrolls))
	}
	if ackFor(t, e, "enroll", "c-9") == nil || ackFor(t, e, "enroll", "c-10") == nil {
		t.Fatal("both local-door executions must ack")
	}
	// A machine WITHOUT the admin class is refused at the door.
	if _, err := e.IngestClient("hmi", "colca/v1/_CmdAdmin/n-edge1/enroll", enrollPayload("c-11", futureMS())); err == nil {
		t.Fatal("cmd:m1/#:param must not authorize _CmdAdmin")
	}
}

// A node whose registry never got wired still answers — loudly, with 500.
// Silence is reserved for commands no executor claims at all, which is how a
// machine's command rides through untouched.
func TestExecAdminWithoutARegistry(t *testing.T) {
	s, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	e := New(s, &config.Config{ULID: "n-edge1"}, testIDs(), nil, nil, nil)
	e.SetExecutor(Executors(NewAdminExecutor(nil)))
	if _, err := e.IngestDownlink("colca/v1/_CmdAdmin/n-edge1/enroll", enrollPayload("c-12", futureMS()), 1); err != nil {
		t.Fatal(err)
	}
	if ack := ackFor(t, e, "enroll", "c-12"); ack == nil || ack["result_code"].(float64) != 500 {
		t.Fatalf("no-executor ack = %v, want 500", ack)
	}
}
