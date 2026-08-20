// Data-model binding through a real node (data-model binding design §5/§6):
// a connector publishes its catalogue at the child, an operator issues
// signal/autobind at the PARENT, the child executes it and the resulting
// _Signal records replicate back up. Everything a person, a preprovisioned
// model file and the lifecycle trigger would do arrives here as this one verb.
package tests

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/alpamayo-solutions/colca/internal/authtest"
	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/identity"
	"github.com/alpamayo-solutions/colca/internal/node"
)

// bindingBundle is the skew fixture plus the two contracts this flow needs.
func bindingBundle(t *testing.T) string {
	t.Helper()
	str := map[string]any{"type": "string", "minLength": 1}
	return bundleFixture(t, "binding", map[string]any{
		// The real shape, not the skew fixture's placeholder: id + name are what
		// a signal must carry, and data_tag is the binding — the tag's own
		// identity, not a connector (design §6).
		"_Signal": map[string]any{"class": "entity", "tombstone": true,
			"schema": map[string]any{"type": "object",
				"properties": map[string]any{
					"id": str, "name": str, "data_tag": str,
					"is_published": map[string]any{"type": "boolean"},
				},
				"required": []any{"id", "name"}}},
		"_Constant": map[string]any{"class": "entity", "tombstone": true,
			"schema": map[string]any{"type": "object",
				"properties": map[string]any{
					"id": str, "name": str,
					"data_type": map[string]any{"type": "string", "enum": []any{
						"float64", "int64", "boolean", "string", "datetime", "json",
					}},
					"value": map[string]any{},
				},
				"required": []any{"id", "name", "data_type", "value"}}},
		"_DataTags": map[string]any{"class": "entity", "tombstone": true,
			"schema": map[string]any{"type": "object",
				"properties": map[string]any{"data_tags": map[string]any{"type": "array"}}}},
		"_CmdConfigure": map[string]any{"class": "cmd", "tombstone": false,
			"schema": map[string]any{"type": "object",
				"properties": map[string]any{"correlation_id": str, "expires_at": map[string]any{"type": "number"}},
				"required":   []any{"correlation_id", "expires_at"}}},
	})
}

// enrollMachineAt places an element at mount and enrolls a machine key there,
// named ulid (autobind's tests reuse the ulid as a human-readable name — real
// deployments mint a ULID and name separately), with an explicit write: grant
// over its own zone — a machine gets no implicit write (auth §5), so a
// connector that will publish its catalogue needs one just as a real
// deployment's would.
func enrollMachineAt(t *testing.T, n *node.Node, ulid, pubkey, mount string) {
	t.Helper()
	element := authtest.Place(t, n.Engine, mount)
	b, err := json.Marshal(map[string]any{"ulid": ulid, "pubkey": pubkey, "kind": "machine",
		"name": ulid, "element": element, "grants": []string{"write:" + element + "/#"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := n.Registry.Enroll(b); err != nil {
		t.Fatalf("enroll machine %s: %v", ulid, err)
	}
}

// signalsAt returns the _Signal records a node currently holds, by path.
func signalsAt(t *testing.T, n *node.Node, contract string) map[string]map[string]any {
	t.Helper()
	out := map[string]map[string]any{}
	for _, r := range fetchRecords(t, n, "entities", unique("sig"), "", 500) {
		rec := r.(map[string]any)
		topic, _ := rec["topic"].(string)
		if !strings.HasPrefix(topic, "colca/v1/"+contract+"/") {
			continue
		}
		var payload map[string]any
		if json.Unmarshal([]byte(mustJSON(rec["payload"])), &payload) != nil {
			continue
		}
		out[topic] = payload
	}
	return out
}

func TestAutobindIssuedAtTheParentBindsAtTheChild(t *testing.T) {
	base := t.TempDir()
	keys := map[string]*identity.Identity{}
	for _, n := range []string{"n-parent", "n-child"} {
		id, err := identity.Generate(filepath.Join(base, n+".key"))
		if err != nil {
			t.Fatal(err)
		}
		keys[n] = id
	}
	bundle := bindingBundle(t)

	mk := func(ulid string, parent *config.Parent) *config.Config {
		return &config.Config{
			ULID: ulid, DataDir: filepath.Join(base, ulid+"-data"), LogLevel: "debug",
			KeyFile:   filepath.Join(base, ulid+".key"),
			API:       config.API{Addr: "127.0.0.1:0", Token: tok},
			MQTT:      config.Endpoint{Addr: "127.0.0.1:0"},
			Repl:      config.Endpoint{Addr: "127.0.0.1:0"},
			Parent:    parent,
			Contracts: config.Contracts{Bundle: bundle},
		}
	}

	parent, err := node.Start(mk("n-parent", nil))
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Stop()
	authtestEnrollNode(t, parent, "n-child", keys["n-child"].PublicHex(), "child1")

	child, err := node.Start(mk("n-child", &config.Parent{
		URL: "https://" + parent.ReplAddr, Pubkey: keys["n-parent"].PublicHex(),
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer child.Stop()

	// A connector enrolls at the child and publishes its catalogue — one
	// record, the whole discovery result, at the topic its own entry computes
	// to: node/mount/name (design §6), not a path it gets to choose.
	conn := authtest.NewMachine(t, "opcua-1")
	enrollMachineAt(t, child, "opcua-1", conn.Pubkey, "opcua-1")
	c := machine(t, child.MQTTAddr, conn)
	catalogue := `{"data_tags":[` +
		`{"id":"01JTAG-TEMP","name":"Temp","data_type":"float"},` +
		`{"id":"01JTAG-SPEED","name":"Line 1/Speed","data_type":"float"}]}`
	if tk := c.Publish("colca/v1/_DataTags/n-child/opcua-1/opcua-1", 1, true, catalogue); !tk.WaitTimeout(5 * time.Second) {
		t.Fatal("catalogue publish: no PUBACK")
	}
	waitFor(t, "catalogue stored at the child", 10*time.Second, func() bool {
		return len(signalsAt(t, child, "_DataTags")) == 1
	})

	// The operator issues autobind at the PARENT, naming the child.
	corr := cmdAdmin(t, parent, "colca/v1/_CmdConfigure/n-child/child1/signal/autobind",
		map[string]any{"connector": "opcua-1", "under": "opcua-1"})
	awaitAdminAck(t, parent, "colca/v1/_Ack/n-child/child1/signal/autobind", corr, 200)

	// The child wrote one signal per tag, under its own identity, and the
	// browse name that carried a separator became one addressable segment.
	signals := signalsAt(t, child, "_Signal")
	if len(signals) != 2 {
		t.Fatalf("signals at the child = %v, want 2", signals)
	}
	for topic, payload := range signals {
		if !strings.HasPrefix(topic, "colca/v1/_Signal/n-child/") {
			t.Errorf("%s: signals are authored by the node, not by the connector", topic)
		}
		if payload["data_tag"] == "" {
			t.Errorf("%s: binding not recorded: %v", topic, payload)
		}
		// Minted through the same port every other identity in this system
		// uses — a ULID, 26 Crockford base32 characters.
		if id, _ := payload["id"].(string); len(id) != 26 {
			t.Errorf("%s: signal id = %q, want a 26-character ULID", topic, id)
		}
		leaf := topic[strings.LastIndex(topic, "/")+1:]
		if strings.ContainsAny(leaf, "+# ") {
			t.Errorf("%s: leaf %q is not one addressable segment", topic, leaf)
		}
	}

	// And they replicate up like any other entity record.
	waitFor(t, "signals replicated to the parent", 20*time.Second, func() bool {
		return len(signalsAt(t, parent, "_Signal")) == 2
	})

	// Re-issuing changes nothing: the second run finds every tag already bound.
	corr2 := cmdAdmin(t, parent, "colca/v1/_CmdConfigure/n-child/child1/signal/autobind",
		map[string]any{"connector": "opcua-1", "under": "opcua-1"})
	awaitAdminAck(t, parent, "colca/v1/_Ack/n-child/child1/signal/autobind", corr2, 200)
	if again := signalsAt(t, child, "_Signal"); len(again) != 2 {
		t.Fatalf("second autobind changed the model: %d signals", len(again))
	}
}

// A connector that has published nothing yet is a conflict with the current
// state, not a bad request — the same command succeeds once it has.
func TestAutobindWithoutACatalogueAcksConflict(t *testing.T) {
	base := t.TempDir()
	if _, err := identity.Generate(filepath.Join(base, "n-solo.key")); err != nil {
		t.Fatal(err)
	}
	n, err := node.Start(&config.Config{
		ULID: "n-solo", DataDir: filepath.Join(base, "data"), LogLevel: "debug",
		KeyFile:   filepath.Join(base, "n-solo.key"),
		API:       config.API{Addr: "127.0.0.1:0", Token: tok},
		Repl:      config.Endpoint{Addr: "127.0.0.1:0"},
		Contracts: config.Contracts{Bundle: bindingBundle(t)},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer n.Stop()

	// The connector is enrolled — this node knows who it is, by name — but has
	// published nothing yet: a retry after it does will succeed, so this is a
	// conflict with the current state, not an unknown identity (that case is
	// TestAutobindRefusesAConnectorThisNodeDoesNotHold at the unit level).
	b, err := json.Marshal(map[string]any{"ulid": "never-seen", "kind": "local", "name": "never-seen"})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := n.Registry.Enroll(b); err != nil {
		t.Fatalf("enroll local service: %v", err)
	}

	corr := cmdAdmin(t, n, "colca/v1/_CmdConfigure/n-solo/signal/autobind",
		map[string]any{"connector": "never-seen"})
	awaitAdminAck(t, n, "colca/v1/_Ack/n-solo/signal/autobind", corr, 409)
}

func TestConfigureConstantUpsertAndDeleteAreRetainedState(t *testing.T) {
	base := t.TempDir()
	if _, err := identity.Generate(filepath.Join(base, "n-constant.key")); err != nil {
		t.Fatal(err)
	}
	n, err := node.Start(&config.Config{
		ULID: "n-constant", DataDir: filepath.Join(base, "data"), LogLevel: "debug",
		KeyFile:   filepath.Join(base, "n-constant.key"),
		API:       config.API{Addr: "127.0.0.1:0", Token: tok},
		Repl:      config.Endpoint{Addr: "127.0.0.1:0"},
		Contracts: config.Contracts{Bundle: bindingBundle(t)},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer n.Stop()

	path := "line1/m6/target-speed"
	corr := cmdAdmin(t, n, "colca/v1/_CmdConfigure/n-constant/constant/upsert", map[string]any{
		"constants": []any{map[string]any{
			"path": path,
			"constant": map[string]any{
				"id": "01HCONSTANT", "name": "Target speed",
				"data_type": "int64", "value": 18000,
			},
		}},
	})
	awaitAdminAck(t, n, "colca/v1/_Ack/n-constant/constant/upsert", corr, 200)
	topic := "colca/v1/_Constant/n-constant/" + path
	current := map[string]any{}
	for _, raw := range kvAt(t, n, path) {
		entry := raw.(map[string]any)
		if entry["topic"] == topic {
			current = entry["payload"].(map[string]any)
		}
	}
	if current["value"] != float64(18000) {
		t.Fatalf("constant not retained at %s: %v", topic, current)
	}

	corr = cmdAdmin(t, n, "colca/v1/_CmdConfigure/n-constant/constant/delete", map[string]any{
		"paths": []string{path},
	})
	awaitAdminAck(t, n, "colca/v1/_Ack/n-constant/constant/delete", corr, 200)
	for _, raw := range kvAt(t, n, path) {
		if raw.(map[string]any)["topic"] == topic {
			t.Fatalf("constant survived its tombstone: %v", raw)
		}
	}
}
