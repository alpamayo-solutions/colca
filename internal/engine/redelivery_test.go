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

// newUndeliveredTestEngine builds an engine with real metrics, a HasSubscriberFor
// stub that always answers subscribed, and a captured log.
func newUndeliveredTestEngine(t *testing.T, subscribed bool) (*Engine, *metrics.Metrics, *bytes.Buffer) {
	t.Helper()
	return newUndeliveredTestEngineWithIDs(t, subscribed, testIDs())
}

// newUndeliveredTestEngineWithIDs is the same with a registry of the caller's
// choice.
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
	e.SetSubscriberCheck(func(string, string) bool { return subscribed })
	placeTestElements(t, e)
	return e, m, logBuf
}

// expiredCmdPayload is cmdPayload with an expires_at in the past.
func expiredCmdPayload(t *testing.T, corr string) []byte {
	t.Helper()
	b, err := json.Marshal(map[string]any{"correlation_id": corr, "expires_at": 1000})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// A live command for an enrolled machine that reaches no subscriber counts
// colca_command_undelivered_total.
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

// The same command with a subscriber connected does not count, which shows the
// counter is conditional.
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

// A command that arrives already expired is not a delivery failure.
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

// A command for a machine enrolled deeper in the tree passes through every
// ancestor with no subscriber there. That is the healthy path and must not count.
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

// A command for a child node enrolled here is fed over replication, never this
// bus, so it does not count; being in the registry is not enough.
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

// Commands addressed to this node execute in-process and never have a subscriber,
// so they do not count.
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

// The warning carries the topic and correlation id, which an operator needs to
// find a stuck command.
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

// A fake returning (nil, true) from Get, which the Mounts contract forbids,
// neither panics nor counts.
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
