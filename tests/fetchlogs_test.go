// fetchLogs on the three-level tree: the hub asks an edge for a window of its
// own logs, and the page comes back up in the command's _Ack.
package tests

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/alpamayo-solutions/colca/internal/node"
	"github.com/alpamayo-solutions/colca/internal/store"
)

const (
	fetchLogsCmd = "colca/v1/_CmdAdmin/n-edge1/site1/edge1/fetchLogs"
	fetchLogsAck = "colca/v1/_Ack/n-edge1/site1/edge1/fetchLogs"
)

// awaitAckPayload waits for the ack with corr at n and returns its payload.
func awaitAckPayload(t *testing.T, n *node.Node, ackTopic, corr string) map[string]any {
	t.Helper()
	var found map[string]any
	waitFor(t, fmt.Sprintf("ack %s (corr %s)", ackTopic, corr), 20*time.Second, func() bool {
		for _, r := range fetchRecords(t, n, "commands", unique("fl-ack"), "", 1000) {
			rec := r.(map[string]any)
			if rec["topic"] != ackTopic {
				continue
			}
			if payload, ok := rec["payload"].(map[string]any); ok && payload["correlation_id"] == corr {
				found = payload
				return true
			}
		}
		return false
	})
	return found
}

// publishLog appends one _Log record stored at ts to n's logs stream. The tests run on the
// built-in contract floor, which has no _Log schema, so the record goes straight
// into the store the way the node's log door would put it there.
func publishLog(t *testing.T, n *node.Node, ts int64, service, level, message string) {
	t.Helper()
	payload, err := json.Marshal(map[string]any{"level": level, "message": message, "logger_name": service})
	if err != nil {
		t.Fatal(err)
	}
	rec := store.Record{
		Topic:   fmt.Sprintf("colca/v1/_Log/%s/%s/%s", n.Cfg.ULID, service, level),
		Payload: payload, TS: ts,
	}
	if _, _, err := n.Engine.Store().Append("logs", []store.Record{rec}); err != nil {
		t.Fatal(err)
	}
}

// pageMessages returns the messages of a page's records and its next/complete.
func pageMessages(t *testing.T, ack map[string]any) (msgs []string, next any, complete bool) {
	t.Helper()
	if ack["result_code"].(float64) != 200 {
		t.Fatalf("fetchLogs ack = %v, want 200", ack)
	}
	result, ok := ack["result"].(map[string]any)
	if !ok {
		t.Fatalf("fetchLogs ack carries no result: %v", ack)
	}
	for _, r := range result["records"].([]any) {
		payload := r.(map[string]any)["payload"].(map[string]any)
		msgs = append(msgs, payload["message"].(string))
	}
	if result["gap"] != false || result["lwm"] == nil {
		t.Fatalf("page without lwm/gap: %v", result)
	}
	return msgs, result["next"], result["complete"] == true
}

func TestFetchLogsFromTheHub(t *testing.T) {
	tp := startTopo(t)
	// A window that ended more than the skew margin ago, so the head completes it.
	from := time.Now().Add(-3 * time.Hour).UnixMilli()
	publishLog(t, tp.edge1, from+1, "fetchsvc", "INFO", "one")
	publishLog(t, tp.edge1, from+2, "fetchsvc", "WARNING", "two")
	publishLog(t, tp.edge1, from+3, "fetchsvc", "INFO", "three")
	to := from + 10_000

	// Page one: two records, a resume point.
	corr := cmdAdmin(t, tp.global, fetchLogsCmd, map[string]any{
		"from": from, "to": to, "service": "fetchsvc", "limit": 2,
	})
	msgs, next, complete := pageMessages(t, awaitAckPayload(t, tp.global, fetchLogsAck, corr))
	if strings.Join(msgs, ",") != "one,two" || complete || next == nil {
		t.Fatalf("page 1 = %v next=%v complete=%v", msgs, next, complete)
	}

	// Page two resumes after next and completes the window.
	corr = cmdAdmin(t, tp.global, fetchLogsCmd, map[string]any{
		"from": from, "to": to, "service": "fetchsvc", "limit": 2, "after": next,
	})
	msgs, next, complete = pageMessages(t, awaitAckPayload(t, tp.global, fetchLogsAck, corr))
	if strings.Join(msgs, ",") != "three" || !complete || next != nil {
		t.Fatalf("page 2 = %v next=%v complete=%v", msgs, next, complete)
	}

	// min_level keeps WARNING and above.
	corr = cmdAdmin(t, tp.global, fetchLogsCmd, map[string]any{
		"from": from, "to": to, "service": "fetchsvc", "min_level": "WARNING",
	})
	msgs, _, complete = pageMessages(t, awaitAckPayload(t, tp.global, fetchLogsAck, corr))
	if strings.Join(msgs, ",") != "two" || !complete {
		t.Fatalf("WARNING page = %v complete=%v", msgs, complete)
	}

	// Bad input: 422 with the reason, no result.
	corr = cmdAdmin(t, tp.global, fetchLogsCmd, map[string]any{"from": to, "to": from})
	ack := awaitAckPayload(t, tp.global, fetchLogsAck, corr)
	if ack["result_code"].(float64) != 422 || !strings.Contains(ack["message"].(string), "must be before") || ack["result"] != nil {
		t.Fatalf("bad-input ack = %v", ack)
	}
}

// A fetchLogs issued while the edge is down waits in the commands stream and is
// answered from the edge's store on catch-up.
func TestFetchLogsExecutesAfterOfflineCatchup(t *testing.T) {
	tp := startTopo(t)
	from := time.Now().Add(-3 * time.Hour).UnixMilli()
	publishLog(t, tp.edge1, from+1, "fetchsvc", "INFO", "before-outage")
	to := from + 10_000

	tp.edge1.Stop()
	time.Sleep(300 * time.Millisecond)
	corr := cmdAdmin(t, tp.global, fetchLogsCmd, map[string]any{"from": from, "to": to, "service": "fetchsvc"})

	e1, err := node.Start(tp.cfgs["n-edge1"])
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { e1.Stop() })
	msgs, _, complete := pageMessages(t, awaitAckPayload(t, tp.global, fetchLogsAck, corr))
	if strings.Join(msgs, ",") != "before-outage" || !complete {
		t.Fatalf("catch-up page = %v complete=%v", msgs, complete)
	}
}
