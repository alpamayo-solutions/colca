// Log forwarding on a three-level tree: every node keeps all its logs, and
// each hop forwards only the levels its own parent.logs allows.
package tests

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/alpamayo-solutions/colca/internal/authtest"
	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/identity"
	"github.com/alpamayo-solutions/colca/internal/metrics/metricstest"
	"github.com/alpamayo-solutions/colca/internal/node"
)

// startLogTree starts hub <- site <- edge. The edge forwards INFO for the
// service "commissioning" and WARNING+ otherwise; the site has the default.
func startLogTree(t *testing.T) (hub, site, edge *node.Node) {
	t.Helper()
	base := t.TempDir()
	keys := map[string]*identity.Identity{}
	for _, n := range []string{"n-hub", "n-site", "n-edge"} {
		id, err := identity.Generate(filepath.Join(base, n+".key"))
		if err != nil {
			t.Fatal(err)
		}
		keys[n] = id
	}
	// The fixture bundle plus _Log, which it does not carry.
	bundle := bundleFixture(t, "logs", map[string]any{
		"_Log": map[string]any{"class": "log", "tombstone": false,
			"schema": map[string]any{"type": "object", "required": []any{"level", "message"}}},
	})
	mk := func(ulid string, parent *config.Parent) *config.Config {
		return &config.Config{
			Contracts: config.Contracts{Bundle: bundle},
			ULID:      ulid, DataDir: filepath.Join(base, ulid+"-data"),
			KeyFile: filepath.Join(base, ulid+".key"),
			API:     config.API{Addr: "127.0.0.1:0", Token: tok},
			MQTT:    config.Endpoint{Addr: "127.0.0.1:0"},
			Repl:    config.Endpoint{Addr: "127.0.0.1:0"},
			Parent:  parent,
		}
	}
	start := func(cfg *config.Config) *node.Node {
		if err := cfg.Validate(); err != nil {
			t.Fatal(err)
		}
		n, err := node.Start(cfg)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(n.Stop)
		return n
	}
	hub = start(mk("n-hub", nil))
	authtest.EnrollNodeAt(t, hub.Registry, hub.Engine, "n-site", keys["n-site"].PublicHex(), "site")
	site = start(mk("n-site", &config.Parent{URL: "https://" + hub.ReplAddr, Pubkey: keys["n-hub"].PublicHex()}))
	waitForPrefix(t, "n-site", site)
	authtest.EnrollNodeAt(t, site.Registry, site.Engine, "n-edge", keys["n-edge"].PublicHex(), "edge")
	edge = start(mk("n-edge", &config.Parent{
		URL: "https://" + site.ReplAddr, Pubkey: keys["n-site"].PublicHex(),
		Logs: config.ParentLogs{Services: map[string]string{"commissioning": "info"}},
	}))
	waitForPrefix(t, "n-edge", edge)
	return hub, site, edge
}

// publishLog appends one _Log record at n for service at level, carrying msg.
func publishLog(t *testing.T, n *node.Node, service, level, msg string) {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"timestamp": time.Now().UTC().Format(time.RFC3339Nano), "level": level, "message": msg,
		"logger_name": service, "module": "test", "function": "publishLog", "line_no": 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	topic := fmt.Sprintf("colca/v1/_Log/%s/%s/%s", n.Cfg.ULID, service, level)
	if _, err := n.Engine.IngestAdmin(topic, body); err != nil {
		t.Fatalf("publish %s: %v", topic, err)
	}
}

// logMessages is the set of messages in n's logs stream.
func logMessages(t *testing.T, n *node.Node) map[string]bool {
	t.Helper()
	out := map[string]bool{}
	cursor := unique("logs")
	for {
		recs := fetchRecords(t, n, "logs", cursor, "", 1000)
		if len(recs) == 0 {
			return out
		}
		for _, r := range recs {
			if msg, ok := r.(map[string]any)["payload"].(map[string]any)["message"].(string); ok {
				out[msg] = true
			}
		}
		if len(recs) < 1000 {
			return out
		}
	}
}

func TestLogsForwardFromMinimumLevelHopByHop(t *testing.T) {
	hub, site, edge := startLogTree(t)
	run := unique("run")
	msg := func(s string) string { return run + "-" + s }

	publishLog(t, edge, "connector", "INFO", msg("connector-info"))
	publishLog(t, edge, "commissioning", "INFO", msg("commissioning-info"))
	publishLog(t, edge, "commissioning", "DEBUG", msg("commissioning-debug"))
	// More withheld records than one uplink page, so a lane made only of
	// withheld records must still advance.
	for i := range 450 {
		publishLog(t, edge, "connector", "DEBUG", msg(fmt.Sprintf("bulk-%d", i)))
	}
	publishLog(t, edge, "connector", "WARNING", msg("connector-warning"))

	// The WARNING is the last record, and a stream keeps its order, so once it
	// is at a node everything forwarded before it is there too.
	waitFor(t, "the edge's WARNING at the hub", 20*time.Second, func() bool {
		return logMessages(t, hub)[msg("connector-warning")]
	})
	waitFor(t, "the edge's WARNING at the site", 5*time.Second, func() bool {
		return logMessages(t, site)[msg("connector-warning")]
	})

	edgeLogs, siteLogs, hubLogs := logMessages(t, edge), logMessages(t, site), logMessages(t, hub)
	for _, m := range []string{"connector-info", "commissioning-info", "commissioning-debug", "bulk-0", "bulk-449", "connector-warning"} {
		if !edgeLogs[msg(m)] {
			t.Errorf("the edge must keep %s in its own logs stream", m)
		}
	}
	// The edge's own policy: WARNING+, INFO for commissioning.
	for m, want := range map[string]bool{
		"connector-info": false, "commissioning-info": true, "commissioning-debug": false,
		"bulk-0": false, "bulk-449": false, "connector-warning": true,
	} {
		if siteLogs[msg(m)] != want {
			t.Errorf("%s at the site: %v, want %v", m, siteLogs[msg(m)], want)
		}
	}
	// The site applies its own default to what it relays: the commissioning
	// INFO the edge forwarded stops there.
	if hubLogs[msg("commissioning-info")] {
		t.Error("the site must not relay INFO to the hub under its default policy")
	}
	if hubLogs[msg("connector-info")] || hubLogs[msg("commissioning-debug")] || hubLogs[msg("bulk-0")] {
		t.Error("a record the edge withheld reached the hub")
	}

	withheld := metricstest.Value(t, edge.Metrics, `colca_uplink_logs_withheld_total{level="DEBUG"}`)
	if withheld < 451 {
		t.Errorf("edge withheld DEBUG counter %v, want at least 451", withheld)
	}
	if v := metricstest.Value(t, site.Metrics, `colca_uplink_logs_withheld_total{level="INFO"}`); v < 1 {
		t.Errorf("site withheld INFO counter %v, want at least 1 (the relayed commissioning INFO)", v)
	}

	// The parents expect holes in a child's logs offsets: withheld records are
	// not data loss.
	for name, n := range map[string]*node.Node{"site": site, "hub": hub} {
		if v := metricstest.Value(t, n.Metrics, "colca_replication_integrity_failures_total"); v != 0 {
			t.Errorf("%s counted %v integrity failures for withheld log records", name, v)
		}
	}

	// A later WARNING still arrives: the withheld records hold nothing up.
	publishLog(t, edge, "connector", "ERROR", msg("connector-error"))
	waitFor(t, "a later ERROR at the hub", 20*time.Second, func() bool {
		return logMessages(t, hub)[msg("connector-error")]
	})
}
