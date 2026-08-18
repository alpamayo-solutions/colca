package engine

import (
	"encoding/json"
	"testing"

	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/store"
)

// recordingExec claims one contract and records what it was asked to do.
type recordingExec struct {
	contract string
	calls    []string // "contract verb"
	code     int
}

func (r *recordingExec) Handles(contract string) bool { return contract == r.contract }

func (r *recordingExec) Execute(contract, verb string, _ []byte) (int, string, string) {
	r.calls = append(r.calls, contract+" "+verb)
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

// A command for a machine passes through the node untouched: no execution, and
// crucially no ack — an ack here would answer on the machine's behalf.
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
// here — that holds for every contract, not just the ones core used to know.
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
