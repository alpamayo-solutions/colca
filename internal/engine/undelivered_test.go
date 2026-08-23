package engine

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/metrics"
	"github.com/alpamayo-solutions/colca/internal/metrics/metricstest"
	"github.com/alpamayo-solutions/colca/internal/store"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

const undeliveredCounter = "colca_command_undelivered_total"

// newUndeliveredTestEngine builds an engine wired with real metrics (so
// metricstest can read the counter back through the served exposition
// format, same as production) and a HasLocalSubscriber stub the test fully
// controls, plus a captured log buffer (must start before New — *Engine
// binds e.log to slog.Default() at construction, per engine_test.go's
// captureLogs/newCapturedEngine convention). subscribed is what every call
// to HasLocalSubscriber answers — the one variable the delivery-outcome
// tests below flip.
func newUndeliveredTestEngine(t *testing.T, subscribed bool) (*Engine, *metrics.Metrics, *bytes.Buffer) {
	t.Helper()
	return newUndeliveredTestEngineWithIDs(t, subscribed, testIDs())
}

// newUndeliveredTestEngineWithIDs is the same engine with the registry the
// caller chooses — what the target-identity tests below vary, since the guard
// they exercise is a registry lookup.
func newUndeliveredTestEngineWithIDs(
	t *testing.T, subscribed bool, ids fakeIDs,
) (*Engine, *metrics.Metrics, *bytes.Buffer) {
	t.Helper()
	logBuf := captureLogs(t)
	s, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	cfg := &config.Config{ULID: "n-edge1"}
	m := metrics.New(s, config.Retention{}, nil)
	e := New(s, cfg, ids, func(string, []byte, bool) {}, m, nil)
	e.SetSubscriberCheck(func(string) bool { return subscribed })
	placeTestElements(t, e)
	return e, m, logBuf
}

// expiredCmdPayload matches cmdPayload's shape (exec_test.go) but with an
// expires_at that already passed — same convention TestExpiryIsCheckedBeforeExecution
// uses (a literal past epoch millisecond, not "now minus something").
func expiredCmdPayload(t *testing.T, corr string) []byte {
	t.Helper()
	b, err := json.Marshal(map[string]any{"correlation_id": corr, "expires_at": 1000})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// TestCommandUndeliveredWhenNoSubscriber is the audit finding itself,
// reproduced as a test: a live command addressed to an ENROLLED MACHINE
// ("m1", per testIDs — not this node's own ULID "n-edge1") that reaches
// zero local-bus subscribers must count colca_command_undelivered_total.
// Nothing before this file made this drop observable at all.
func TestCommandUndeliveredWhenNoSubscriber(t *testing.T) {
	e, m, _ := newUndeliveredTestEngine(t, false /* no subscriber connected */)

	topic := "colca/v1/_CmdParam/m1/temp/set"
	if _, err := e.IngestAdmin(topic, cmdPayload("corr-1")); err != nil {
		t.Fatalf("IngestAdmin: %v", err)
	}
	if got := metricstest.Value(t, m, undeliveredCounter); got != 1 {
		t.Fatalf("%s = %v, want 1 (live command to an external target with no subscriber)", undeliveredCounter, got)
	}
}

// TestCommandDeliveredNotCountedWhenSubscribed is the denominator half: the
// exact same live, externally-addressed command, but HasLocalSubscriber now
// answers true (a subscriber IS connected and would receive it) — the
// counter must stay at 0. Without this half, TestCommandUndeliveredWhenNoSubscriber
// alone cannot prove the counter is CONDITIONAL on the absence of a
// subscriber — an implementation that increments unconditionally would pass
// it too (see the mutation check recorded for this pair).
func TestCommandDeliveredNotCountedWhenSubscribed(t *testing.T) {
	e, m, _ := newUndeliveredTestEngine(t, true /* subscriber connected */)

	topic := "colca/v1/_CmdParam/m1/temp/set"
	if _, err := e.IngestAdmin(topic, cmdPayload("corr-2")); err != nil {
		t.Fatalf("IngestAdmin: %v", err)
	}
	if got := metricstest.Value(t, m, undeliveredCounter); got != 0 {
		t.Fatalf("%s = %v, want 0 (a live subscriber received it)", undeliveredCounter, got)
	}
}

// TestExpiredCommandNotCountedAsUndelivered: a command that arrives already
// past its expires_at is not a delivery failure — the issuer waited too
// long before the record was even persisted, which is a different problem
// with a different owner (the issuer's own ack-timeout handling, cmdadmin
// design §5/§11). No subscriber is wired here on purpose: without the
// expiry guard, this alone would false-positive on every expired command
// replayed or relayed after its window closed.
func TestExpiredCommandNotCountedAsUndelivered(t *testing.T) {
	e, m, _ := newUndeliveredTestEngine(t, false /* no subscriber */)

	topic := "colca/v1/_CmdParam/m1/temp/set"
	if _, err := e.IngestAdmin(topic, expiredCmdPayload(t, "corr-3")); err != nil {
		t.Fatalf("IngestAdmin: %v", err)
	}
	if got := metricstest.Value(t, m, undeliveredCounter); got != 0 {
		t.Fatalf("%s = %v, want 0 (already expired on arrival is not a delivery failure)", undeliveredCounter, got)
	}
}

// TestTransitingCommandNotCountedAsUndelivered is the tree case, and the one
// that decides whether this metric tells the truth in the only topology that
// matters. A command addressed to a machine enrolled DEEPER in the tree is
// persisted and mirrored at every ancestor on its way down
// (IngestDownlink → persistTSAttributed), and reaches zero subscribers at
// each of them — the target is fed over the replication door, not this bus.
// That is the healthy path, not a delivery failure, so nothing may be
// counted here. Reproduces tests/system/test_tree_contract.py's root publish
// (colca/v1/_CmdParam/m1/site1/edge1/m1/...) at a node where m1 is not
// enrolled.
func TestTransitingCommandNotCountedAsUndelivered(t *testing.T) {
	// Registry deliberately without the target: exactly what an ancestor's
	// registry looks like for a machine enrolled below it.
	e, m, logBuf := newUndeliveredTestEngineWithIDs(t, false /* no subscriber */, testIDs())

	topic := "colca/v1/_CmdParam/m-deeper/site1/edge1/m-deeper/set"
	if _, err := e.IngestAdmin(topic, cmdPayload("corr-transit")); err != nil {
		t.Fatalf("IngestAdmin: %v", err)
	}
	if got := metricstest.Value(t, m, undeliveredCounter); got != 0 {
		t.Fatalf("%s = %v, want 0 (relayed toward a descendant — delivered over replication, not this bus)",
			undeliveredCounter, got)
	}
	if strings.Contains(logBuf.String(), "no live subscription") {
		t.Fatalf("warned about a healthy relay hop:\n%s", logBuf.String())
	}
}

// TestChildNodeTargetedCommandNotCountedAsUndelivered is the last hop of the
// same journey: the target IS enrolled here, but as a kind=node child, which
// is fed over the replication door (9443) and never subscribes to this bus.
// Presence in the registry is therefore not the question — this is what makes
// MayUseDoor(DoorMQTT) load-bearing rather than a plain `ok` check.
func TestChildNodeTargetedCommandNotCountedAsUndelivered(t *testing.T) {
	ids := testIDs()
	ids.entries["n-child"] = &uns.Entry{ULID: "n-child", Kind: uns.KindNode, Element: "el-m1", Pubkey: strings.Repeat("ab", 32)}
	e, m, _ := newUndeliveredTestEngineWithIDs(t, false /* no subscriber */, ids)

	topic := "colca/v1/_CmdParam/n-child/m1/set"
	if _, err := e.IngestAdmin(topic, cmdPayload("corr-child")); err != nil {
		t.Fatalf("IngestAdmin: %v", err)
	}
	if got := metricstest.Value(t, m, undeliveredCounter); got != 0 {
		t.Fatalf("%s = %v, want 0 (a child node is reached over replication, not the local bus)",
			undeliveredCounter, got)
	}
}

// TestNodeTargetedCommandNotCountedAsUndelivered: _CmdConfigure/_CmdEdit/
// _CmdAdmin addressed to THIS node (level 4 == e.cfg.ULID) execute in-process
// via maybeExec right after this same persist call returns — no MQTT
// subscriber is ever expected for them, so reaching zero must not trip the
// counter. It needs no guard of its own: a node holds no kind=machine entry
// for itself, so the same registry lookup answers it. No subscriber is wired
// here on purpose: without that lookup, every command a node executes on
// itself would register as "undelivered" from the very first one.
func TestNodeTargetedCommandNotCountedAsUndelivered(t *testing.T) {
	e, m, _ := newUndeliveredTestEngine(t, false /* no subscriber */)

	topic := "colca/v1/_CmdConfigure/n-edge1/element/upsert" // addressed to THIS node
	if _, err := e.IngestAdmin(topic, cmdPayload("corr-4")); err != nil {
		t.Fatalf("IngestAdmin: %v", err)
	}
	if got := metricstest.Value(t, m, undeliveredCounter); got != 0 {
		t.Fatalf("%s = %v, want 0 (node-targeted commands execute in-process, no subscriber expected)", undeliveredCounter, got)
	}
}

// TestCommandUndeliveredLogsTopicAndCorrelationID pins the warning's
// content, not just the counter: an operator diagnosing a stuck command
// needs the topic and correlation id in the log line, not merely a metric
// that something somewhere went unheard.
func TestCommandUndeliveredLogsTopicAndCorrelationID(t *testing.T) {
	e, _, logBuf := newUndeliveredTestEngine(t, false /* no subscriber */)

	topic := "colca/v1/_CmdParam/m1/temp/set"
	if _, err := e.IngestAdmin(topic, cmdPayload("corr-log-1")); err != nil {
		t.Fatalf("IngestAdmin: %v", err)
	}

	out := logBuf.String()
	if !strings.Contains(out, topic) {
		t.Fatalf("log output missing topic %q:\n%s", topic, out)
	}
	if !strings.Contains(out, "corr-log-1") {
		t.Fatalf("log output missing correlation_id %q:\n%s", "corr-log-1", out)
	}
	if !strings.Contains(strings.ToUpper(out), "WARN") {
		t.Fatalf("expected a WARN-level log line:\n%s", out)
	}
}

// TestNilRegistryEntryIsNotDeliverable reaches the guard the Mounts contract
// forbids anyone from tripping (engine.go: Get must answer (nil, false), never
// (nil, true)) — through the only thing that can trip it, a fake. The
// interface permits the pair, so `!ok || !target.MayUseDoor(…)` dereferences
// nil unless the predicate is nil-safe, and a panic on this path takes the
// whole ingest down rather than just the metric.
//
// The claim is two things at once: no panic, and no count — "no identity"
// answers "not deliverable over this bus" the same way an unknown identity
// does.
func TestNilRegistryEntryIsNotDeliverable(t *testing.T) {
	ids := testIDs()
	ids.entries["m1"] = nil // (nil, true): the pair the contract forbids
	e, m, _ := newUndeliveredTestEngineWithIDs(t, false /* no subscriber */, ids)

	topic := "colca/v1/_CmdParam/m1/temp/set"
	if _, err := e.IngestAdmin(topic, cmdPayload("corr-nil")); err != nil {
		t.Fatalf("IngestAdmin: %v", err)
	}
	if got := metricstest.Value(t, m, undeliveredCounter); got != 0 {
		t.Fatalf("%s = %v, want 0 (a nil entry is no identity, so nothing here is deliverable)",
			undeliveredCounter, got)
	}
}
