package engine

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/store"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

type stateWritingExec struct{ recordingExec }

func (r *stateWritingExec) ExecuteWithWrites(
	ctx uns.CommandContext,
	contract, verb string,
	payload []byte,
) (int, string, string, []uns.StateWrite) {
	code, message, result := r.Execute(ctx, contract, verb, payload)
	return code, message, result, []uns.StateWrite{{
		Stream: "entities", Offset: 7, Topic: "colca/v1/_Node/n-edge1/_colca/nodes/n-edge1",
	}}
}

// recordingExec claims one contract and records what it was asked to do.
type recordingExec struct {
	contract string
	calls    []string     // "contract verb"
	actors   []*uns.Entry // the acting entry each call carried, nil for the admin door
	code     int
}

func (r *recordingExec) Handles(contract string) bool { return contract == r.contract }

func (r *recordingExec) Execute(ctx uns.CommandContext, contract, verb string, _ []byte) (int, string, string) {
	r.calls = append(r.calls, contract+" "+verb)
	r.actors = append(r.actors, ctx.Actor)
	code := r.code
	if code == 0 {
		code = 200
	}
	return code, "", "ok"
}

func execEngine(t *testing.T, x CommandExecutor) *Engine {
	t.Helper()
	s, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	e := New(s, &config.Config{ULID: "n-edge1"}, testIDs(), nil, nil, nil)
	if x != nil {
		e.SetExecutor(x)
	}
	return e
}

func cmdPayload(corr string) []byte {
	b, _ := json.Marshal(map[string]any{"correlation_id": corr, "expires_at": futureMS()})
	return b
}

// The engine dispatches by contract and knows nothing about verbs: whatever an
// executor claims reaches it, verb and payload intact.
func TestExecutorReceivesClaimedContracts(t *testing.T) {
	rec := &recordingExec{contract: "_CmdConfigure"}
	e := execEngine(t, Executors(rec))

	if _, err := e.IngestDownlink("colca/v1/_CmdConfigure/n-edge1/signal/autobind", cmdPayload("c-1"), 1); err != nil {
		t.Fatal(err)
	}
	if len(rec.calls) != 1 || rec.calls[0] != "_CmdConfigure signal/autobind" {
		t.Fatalf("executor calls = %v", rec.calls)
	}
	if ack := ackFor(t, e, "signal/autobind", "c-1"); ack == nil || ack["result_code"].(float64) != 200 {
		t.Fatalf("ack = %v, want 200", ack)
	}
}

func TestLocalCommandResultAndAckNameTheProducedStateOffset(t *testing.T) {
	exec := &stateWritingExec{recordingExec: recordingExec{contract: "_CmdConfigure"}}
	e := execEngine(t, Executors(exec))

	res, err := e.IngestAdmin(
		"colca/v1/_CmdConfigure/n-edge1/entity/upsert",
		cmdPayload("c-state"),
	)
	if err != nil {
		t.Fatal(err)
	}
	if res.Command == nil || len(res.Command.StateWrites) != 1 || res.Command.StateWrites[0].Offset != 7 {
		t.Fatalf("command outcome = %+v", res.Command)
	}
	ack := ackFor(t, e, "entity/upsert", "c-state")
	writes, ok := ack["state_writes"].([]any)
	if !ok || len(writes) != 1 || writes[0].(map[string]any)["offset"].(float64) != 7 {
		t.Fatalf("ack state_writes = %#v", ack["state_writes"])
	}
}

func TestEditCommandCommitsStateAndDurableReplayReceiptTogether(t *testing.T) {
	e := execEngine(t, nil)
	parent, err := e.IngestAdmin(
		"colca/v1/_SystemElement/n-edge1/line1",
		[]byte(`{"id":"el-line1","name":"Line 1"}`),
	)
	if err != nil {
		t.Fatal(err)
	}
	e.SetExecutor(uns.NewEditExec(e.EntityStore(), nil))
	payload, err := json.Marshal(map[string]any{
		"operation_id":   "operation-1",
		"correlation_id": "correlation-1",
		"expires_at":     futureMS(),
		"expected_versions": map[string]string{
			"system-element:el-line1": fmt.Sprint(parent.Offset),
		},
		"intent": map[string]any{
			"type":      "create",
			"entity":    map[string]any{"kind": "constant", "id": "const-target"},
			"parent_id": "el-line1",
			"attributes": map[string]any{
				"name": "Target", "data_type": "int64", "value": 7,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	result, err := e.IngestHuman(humanEntry(t, "cmd:#:configure"), "colca/v1/_CmdEdit/n-edge1/apply", payload)
	if err != nil {
		t.Fatal(err)
	}
	if result.Command == nil || result.Command.ResultCode != 200 || len(result.Command.StateWrites) != 1 {
		t.Fatalf("edit result = %+v", result.Command)
	}
	if got := result.Command.StateWrites[0]; got.Offset != parent.Offset+1 || got.Topic != "colca/v1/_Constant/n-edge1/line1/Target" {
		t.Fatalf("edit state write = %+v", got)
	}
	if got := mustKVScan(t, e.Store(), "_colca/edit/operations/"); len(got) != 1 {
		t.Fatalf("durable replay receipts = %+v, want 1", got)
	}
	entitiesNext := e.Store().NextOffset("entities")

	// Replace the executor to show replay comes from retained state, not an
	// in-process map.
	e.SetExecutor(uns.NewEditExec(e.EntityStore(), nil))
	replayed, err := e.IngestHuman(humanEntry(t, "cmd:#:configure"), "colca/v1/_CmdEdit/n-edge1/apply", payload)
	if err != nil {
		t.Fatal(err)
	}
	if replayed.Command == nil || len(replayed.Command.StateWrites) != 1 ||
		replayed.Command.StateWrites[0] != result.Command.StateWrites[0] {
		t.Fatalf("durable replay = %+v, want original %+v", replayed.Command, result.Command)
	}
	if got := e.Store().NextOffset("entities"); got != entitiesNext {
		t.Fatalf("durable replay appended entity state: next=%d want=%d", got, entitiesNext)
	}
}

// A _CmdEdit an operator sends through their param grant writes the constant
// as the node in node-local coordinates (EntityStore.PublishBatch is the
// only way an executor writes), but the commanding operator must still be
// recoverable from the result: /kv projects this same record, and Unity
// needs to show "set by <operator> at <time>" for an operator-input
// constant. written_by names the node — it is still what physically
// appended the record, exactly as an _Ack already does for the same
// command — while actor_id/actor_label/actor_kind name the operator.
func TestCmdEditByAHumanAttributesTheResultingWriteToThatHuman(t *testing.T) {
	e := execEngine(t, nil)
	if _, err := e.IngestAdmin(
		"colca/v1/_SystemElement/n-edge1/line1",
		[]byte(`{"id":"el-line1","name":"Line 1"}`),
	); err != nil {
		t.Fatal(err)
	}
	constant, err := e.IngestAdmin(
		"colca/v1/_Constant/n-edge1/line1/sta1/operator/sandoffMm",
		[]byte(`{"id":"const-sandoff","name":"Sandoff","data_type":"float64","value":5.0,"system_element_id":"el-line1"}`),
	)
	if err != nil {
		t.Fatal(err)
	}
	edit := uns.NewEditExec(e.EntityStore(), nil)
	edit.SetScope(e.Scope())
	e.SetExecutor(edit)

	payload, err := json.Marshal(map[string]any{
		"operation_id":   "operation-param",
		"correlation_id": "correlation-param",
		"expires_at":     futureMS(),
		"expected_versions": map[string]string{
			"constant:const-sandoff": fmt.Sprint(constant.Offset),
		},
		"intent": map[string]any{
			"type":       "update",
			"entity":     map[string]any{"kind": "constant", "id": "const-sandoff"},
			"attributes": map[string]any{"value": 6.5},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	operator, err := uns.TokenEntry("kc-sub-anna", []string{"cmd:el-line1/#:param"})
	if err != nil {
		t.Fatal(err)
	}
	result, err := e.IngestHumanAttributed(operator, "anna@example.com", "colca/v1/_CmdEdit/n-edge1/apply", payload)
	if err != nil {
		t.Fatal(err)
	}
	if result.Command == nil || result.Command.ResultCode != 200 || len(result.Command.StateWrites) != 1 {
		t.Fatalf("operator param edit = %+v", result.Command)
	}

	got := mustKVScan(t, e.Store(), "line1/sta1/operator/")
	if len(got) != 1 {
		t.Fatalf("kv entries = %+v, want 1", got)
	}
	if entry := got[0]; entry.WrittenBy != "n-edge1" {
		t.Fatalf("written_by = %q, want the node itself", entry.WrittenBy)
	} else if entry.ActorID != "kc-sub-anna" || entry.ActorLabel != "anna@example.com" || entry.ActorKind != "human" {
		t.Fatalf("actor = %+v, want the operator who set it", entry)
	}
}

// A service's own _CmdConfigure write is attributed to that service the same
// way: node-written, actor-attributed to the caller that commanded it.
func TestCmdConfigureByAServiceAttributesTheResultingWriteToThatService(t *testing.T) {
	s, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	ids := fakeIDs{entries: map[string]*uns.Entry{
		"svc-plc": {ULID: "svc-plc", Kind: uns.KindExternal, Grants: []string{"cmd:#:configure"}},
	}}
	e := New(s, &config.Config{ULID: "n-edge1"}, ids, nil, nil, nil)
	cfg := uns.NewConfigExec(e.EntityStore(), nil, e.Elements(), nil, func() string { return "sig-new" }, nil)
	e.SetExecutor(cfg)

	payload, err := json.Marshal(map[string]any{
		"correlation_id": "correlation-svc",
		"expires_at":     futureMS(),
		"constants": []map[string]any{{
			"path":     "panel/target",
			"constant": map[string]any{"id": "const-panel", "name": "Panel", "data_type": "float64", "value": 1.0},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := e.IngestClient("svc-plc", "colca/v1/_CmdConfigure/n-edge1/constant/upsert", payload)
	if err != nil {
		t.Fatal(err)
	}
	if result.Command == nil || result.Command.ResultCode != 200 || len(result.Command.StateWrites) != 1 {
		t.Fatalf("service configure = %+v", result.Command)
	}

	got := mustKVScan(t, e.Store(), "panel/")
	if len(got) != 1 {
		t.Fatalf("kv entries = %+v, want 1", got)
	}
	if entry := got[0]; entry.WrittenBy != "n-edge1" {
		t.Fatalf("written_by = %q, want the node itself", entry.WrittenBy)
	} else if entry.ActorID != "svc-plc" || entry.ActorKind != "service" {
		t.Fatalf("actor = %+v, want the commanding service", entry)
	}
}

// A command for a machine passes through untouched: no execution and no ack,
// which would answer on the machine's behalf.
func TestUnclaimedContractsAreLeftAlone(t *testing.T) {
	rec := &recordingExec{contract: "_CmdConfigure"}
	e := execEngine(t, Executors(rec))

	if _, err := e.IngestDownlink("colca/v1/_CmdParam/n-edge1/set-speed", cmdPayload("c-2"), 1); err != nil {
		t.Fatal(err)
	}
	if len(rec.calls) != 0 {
		t.Fatalf("unclaimed contract reached an executor: %v", rec.calls)
	}
	if ack := ackFor(t, e, "set-speed", "c-2"); ack != nil {
		t.Fatalf("unclaimed contract was acked by the node: %v", ack)
	}
}

// Several executors compose without knowing about each other; each answers only
// what it claims.
func TestExecutorsRouteByContract(t *testing.T) {
	cfg := &recordingExec{contract: "_CmdConfigure"}
	adm := &recordingExec{contract: "_CmdAdmin"}
	e := execEngine(t, Executors(cfg, adm))

	if _, err := e.IngestDownlink("colca/v1/_CmdAdmin/n-edge1/enroll", cmdPayload("c-3"), 1); err != nil {
		t.Fatal(err)
	}
	if _, err := e.IngestDownlink("colca/v1/_CmdConfigure/n-edge1/signal/upsert", cmdPayload("c-4"), 2); err != nil {
		t.Fatal(err)
	}
	if len(adm.calls) != 1 || adm.calls[0] != "_CmdAdmin enroll" {
		t.Fatalf("admin executor calls = %v", adm.calls)
	}
	if len(cfg.calls) != 1 || cfg.calls[0] != "_CmdConfigure signal/upsert" {
		t.Fatalf("configure executor calls = %v", cfg.calls)
	}
}

// Expiry is the engine's business, checked before the executor is reached: a
// command whose window closed must not take effect, whatever it would have done.
func TestExpiryIsCheckedBeforeExecution(t *testing.T) {
	rec := &recordingExec{contract: "_CmdConfigure"}
	e := execEngine(t, Executors(rec))

	payload, _ := json.Marshal(map[string]any{"correlation_id": "c-5", "expires_at": 1000})
	if _, err := e.IngestDownlink("colca/v1/_CmdConfigure/n-edge1/signal/upsert", payload, 1); err != nil {
		t.Fatal(err)
	}
	if len(rec.calls) != 0 {
		t.Fatalf("expired command executed: %v", rec.calls)
	}
	if ack := ackFor(t, e, "signal/upsert", "c-5"); ack == nil || ack["result_code"].(float64) != 498 {
		t.Fatalf("ack = %v, want 498", ack)
	}
}

// A command addressed to another node is stored and forwarded, never executed
// here, for every contract.
func TestForeignTargetsNeverExecute(t *testing.T) {
	rec := &recordingExec{contract: "_CmdConfigure"}
	e := execEngine(t, Executors(rec))

	if _, err := e.IngestDownlink("colca/v1/_CmdConfigure/n-other/child1/signal/upsert", cmdPayload("c-6"), 1); err != nil {
		t.Fatal(err)
	}
	if len(rec.calls) != 0 {
		t.Fatalf("foreign target executed here: %v", rec.calls)
	}
}

// A command addressed to a descendant is accepted and persisted with no outcome,
// which is how a client learns it is queued. The node must not ack it either:
// that would report success before the target ran it.
func TestACommandForAnotherNodeIsAcceptedWithNoOutcomeAndNoAck(t *testing.T) {
	rec := &recordingExec{contract: "_CmdConfigure"}
	e := execEngine(t, Executors(rec))

	queued, err := e.IngestAdmin("colca/v1/_CmdConfigure/n-child/site1/edge1/resource/upsert", cmdPayload("c-queued"))
	if err != nil {
		t.Fatal(err)
	}
	if !queued.Persisted {
		t.Fatal("a command for a descendant must be persisted — it reaches its target by riding this stream")
	}
	if queued.Command != nil {
		t.Fatalf("a descendant's command must carry no synchronous outcome (that absence is 'queued'), got %+v",
			queued.Command)
	}
	if len(rec.calls) != 0 {
		t.Fatalf("a descendant's command must not execute here: %v", rec.calls)
	}
	if acks := acksOnCommands(t, e); len(acks) != 0 {
		t.Fatalf("nothing may ack a command it did not execute; found %v", acks)
	}

	// Denominator: the same verb addressed to this node does return an outcome and
	// ack.
	applied, err := e.IngestAdmin("colca/v1/_CmdConfigure/n-edge1/resource/upsert", cmdPayload("c-applied"))
	if err != nil {
		t.Fatal(err)
	}
	if applied.Command == nil || applied.Command.CorrelationID != "c-applied" {
		t.Fatalf("a command for this node must answer with its outcome, got %+v", applied.Command)
	}
	if len(rec.calls) != 1 {
		t.Fatalf("executor calls = %v, want exactly the self-addressed one", rec.calls)
	}
	if acks := acksOnCommands(t, e); len(acks) != 1 || acks[0] != "c-applied" {
		t.Fatalf("acks = %v, want exactly [c-applied]", acks)
	}
}

// acksOnCommands lists the correlation ids of every _Ack on the commands stream,
// in any frame.
func acksOnCommands(t *testing.T, e *Engine) []string {
	t.Helper()
	recs, _, err := e.Store().Read("commands", 1, 100, nil)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, r := range recs {
		p, err := uns.Parse(r.Topic)
		if err != nil || p.Contract != "_Ack" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal(r.Payload, &m); err != nil {
			t.Fatalf("ack payload: %v", err)
		}
		corr, _ := m["correlation_id"].(string)
		out = append(out, corr)
	}
	return out
}
