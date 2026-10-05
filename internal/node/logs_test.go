package node

import (
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"testing"

	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/contracts/contractstest"
	"github.com/alpamayo-solutions/colca/internal/engine"
	"github.com/alpamayo-solutions/colca/internal/store"
)

func nodeLogLine(level, message string) map[string]any {
	return map[string]any{
		"timestamp": "2026-10-05T12:00:00Z", "level": level, "message": message,
		"logger_name": "driver", "module": "driver", "function": "connect", "line_no": 7,
	}
}

// A running node collapses an error loop, caps a flooding service with one
// drop notice, leaves a quiet service alone, and writes what is pending when it
// stops. The window is the default minute, so every summary and notice below
// is written by Stop.
func TestTheNodeGatesItsServicesLogsAndFlushesOnStop(t *testing.T) {
	base := t.TempDir()
	keyFile := filepath.Join(base, "n1.key")
	genKey(t, keyFile)
	dataDir := filepath.Join(base, "data")
	n := mustStart(t, &config.Config{
		ULID: "n1", DataDir: dataDir, KeyFile: keyFile,
		API:       config.API{Addr: "127.0.0.1:0", Token: tok},
		Contracts: config.Contracts{Bundle: contractstest.GeneratedBundlePath(t)},
		Logs:      config.Logs{MaxPerService: new(50)},
	})
	// The first records go over HTTP, as a service's log publisher sends them:
	// a stored record is answered 200, a collapsed one 202. The rest go
	// straight to the same admin door, past the HTTP rate limiter.
	const loop = "colca/v1/_Log/n1/line1/press/ERROR"
	for i := range 5 {
		code, body := apiCall(t, n, http.MethodPost, "/publish", map[string]any{
			"topic": loop, "payload": nodeLogLine("ERROR", "connection refused"), "written_by": "press",
		})
		switch {
		case i == 0 && code != http.StatusOK:
			t.Fatalf("first record: %d %v", code, body)
		case i > 0 && (code != http.StatusAccepted || body["withheld"] != "collapsed"):
			t.Fatalf("repeat %d: %d %v, want 202 collapsed", i, code, body)
		}
	}
	publish := func(topic string, payload map[string]any) engine.Result {
		t.Helper()
		raw, _ := json.Marshal(payload)
		res, err := n.Engine.IngestAdminAttributed(topic, raw, engine.Attribution{
			WrittenBy: "press", ActorID: "press", ActorLabel: "press", ActorKind: "system",
		})
		if err != nil {
			t.Fatalf("%s: %v", topic, err)
		}
		return res
	}
	for range 995 {
		if res := publish(loop, nodeLogLine("ERROR", "connection refused")); res.Withheld != "collapsed" {
			t.Fatalf("repeat: %+v, want collapsed", res)
		}
	}
	for i := range 200 {
		publish("colca/v1/_Log/n1/line1/flood/INFO", nodeLogLine("INFO", fmt.Sprintf("debug line %d", i)))
	}
	for i := range 10 {
		if res := publish("colca/v1/_Log/n1/line1/oven/INFO", nodeLogLine("INFO", fmt.Sprintf("oven %d", i))); !res.Persisted {
			t.Fatalf("quiet service record %d: %+v", i, res)
		}
	}

	n.Stop()

	st, err := store.Open(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	recs, _, err := st.Read("logs", 1, 100000, nil)
	if err != nil {
		t.Fatal(err)
	}
	by := map[string][]map[string]any{}
	writers := map[string][]string{}
	for _, r := range recs {
		var body map[string]any
		if err := json.Unmarshal(r.Payload, &body); err != nil {
			t.Fatal(err)
		}
		service := strings.TrimPrefix(r.Topic, "colca/v1/_Log/n1/line1/")
		by[service] = append(by[service], body)
		writers[service] = append(writers[service], r.WrittenBy)
	}

	press := by["press/ERROR"]
	if len(press) != 2 {
		t.Fatalf("error loop: %d records, want the first and one summary", len(press))
	}
	if press[1]["message"] != "connection refused (×999 in 60 s)" ||
		press[1]["extra"].(map[string]any)["repeated"] != float64(999) || writers["press/ERROR"][1] != "press" {
		t.Fatalf("summary %v by %s", press[1], writers["press/ERROR"][1])
	}

	if got := len(by["flood/INFO"]); got != 50 {
		t.Fatalf("flood: %d records stored, want the budget of 50", got)
	}
	notices := by["flood/WARNING"]
	if len(notices) != 1 || notices[0]["message"] != "150 log record(s) dropped: flood exceeded 50 records in 60 s" {
		t.Fatalf("drop notices %v", notices)
	}
	if writers["flood/WARNING"][0] != "colca" {
		t.Fatalf("drop notice written by %s, want the node", writers["flood/WARNING"][0])
	}

	if got := len(by["oven/INFO"]); got != 10 {
		t.Fatalf("quiet service: %d records, want all 10", got)
	}
}
