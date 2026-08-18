// Rollout skew (schema-bundle design §9.4/§12): two nodes of one tree may
// briefly run different bundles. A child pinned to bundle N+1 (one added
// contract) accepts and persists the new contract; replication applies it
// upstream UN-revalidated (§10.4) even though the ancestors' bundle N has
// never heard of it; the same publish directly at an N node is rejected.
package tests

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/identity"
	"github.com/alpamayo-solutions/colca/internal/node"
)

// bundleFixture writes a loadable bundle carrying the floor's core contracts
// plus extras; returns its path.
func bundleFixture(t *testing.T, version string, extra map[string]any) string {
	t.Helper()
	numeric := map[string]any{"type": "number"}
	str := map[string]any{"type": "string", "minLength": 1}
	obj := func(class string, tomb bool, req []string, props map[string]any) map[string]any {
		schema := map[string]any{"type": "object", "properties": props}
		if len(req) > 0 {
			schema["required"] = req
		}
		return map[string]any{"class": class, "tombstone": tomb, "schema": schema}
	}
	contractsMap := map[string]any{
		"_Metric":        obj("data", true, []string{"v"}, map[string]any{"v": numeric}),
		"_SystemElement": obj("entity", true, []string{"ulid"}, map[string]any{"ulid": str}),
		"_Signal":        obj("entity", true, []string{"ulid"}, map[string]any{"ulid": str}),
		"_Ack":           obj("ack", false, []string{"correlation_id", "result_code"}, map[string]any{"correlation_id": str, "result_code": numeric}),
		"_CmdParam":      obj("cmd", false, []string{"correlation_id", "expires_at"}, map[string]any{"correlation_id": str, "expires_at": numeric}),
		"_CmdAdmin":      obj("cmd", false, []string{"correlation_id", "expires_at"}, map[string]any{"correlation_id": str, "expires_at": numeric}),
	}
	for k, v := range extra {
		contractsMap[k] = v
	}
	body := map[string]any{
		"bundle_version": version,
		"source":         map[string]any{"package": "colca-data-contracts", "git_sha": "skew-fixture"},
		"contracts":      contractsMap,
	}
	canon, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(canon)
	full := map[string]any{"digest": hex.EncodeToString(sum[:]), "generated_at": "2026-08-17T00:00:00Z"}
	for k, v := range body {
		full[k] = v
	}
	raw, _ := json.Marshal(full)
	p := filepath.Join(t.TempDir(), "bundle-"+version+".json")
	if err := os.WriteFile(p, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

// TestBundleRolloutSkewAcrossTheTree: a 2-level tree — parent on bundle N,
// child on N+1 with contract _Telemetry added.
func TestBundleRolloutSkewAcrossTheTree(t *testing.T) {
	base := t.TempDir()
	keys := map[string]*identity.Identity{}
	for _, n := range []string{"n-parent", "n-child"} {
		id, err := identity.Generate(filepath.Join(base, n+".key"))
		if err != nil {
			t.Fatal(err)
		}
		keys[n] = id
	}

	bundleN := bundleFixture(t, "N", nil)
	bundleN1 := bundleFixture(t, "N-plus-1", map[string]any{
		"_Telemetry": map[string]any{"class": "data", "tombstone": true,
			"schema": map[string]any{"type": "object",
				"properties": map[string]any{"reading": map[string]any{"type": "number"}},
				"required":   []any{"reading"}}},
	})

	mk := func(ulid, bundlePath string, parent *config.Parent) *config.Config {
		return &config.Config{
			ULID: ulid, DataDir: filepath.Join(base, ulid+"-data"), LogLevel: "debug",
			KeyFile:   filepath.Join(base, ulid+".key"),
			API:       config.API{Addr: "127.0.0.1:0", Token: tok},
			Repl:      config.Endpoint{Addr: "127.0.0.1:0"},
			Parent:    parent,
			Contracts: config.Contracts{Bundle: bundlePath},
		}
	}

	parent, err := node.Start(mk("n-parent", bundleN, nil))
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Stop()
	authtestEnrollNode(t, parent, "n-child", keys["n-child"].PublicHex(), "child1")

	child, err := node.Start(mk("n-child", bundleN1,
		&config.Parent{URL: "https://" + parent.ReplAddr, Pubkey: keys["n-parent"].PublicHex()}))
	if err != nil {
		t.Fatal(err)
	}
	defer child.Stop()

	// The N+1-only contract lands at the child (admin door, node-local
	// coordinates)...
	out := api(t, child, "POST", "/publish", map[string]any{
		"topic": "colca/v1/_Telemetry/n-child/skew/reading", "payload": map[string]any{"reading": 42.5}})
	if out["stream"] != "metrics" {
		t.Fatalf("child must route the N+1 contract by its manifest class, got %v", out)
	}

	// ...and replicates upstream UN-revalidated: the parent's bundle N has
	// never heard of _Telemetry, yet the record appears in its stream.
	waitFor(t, "the N+1 contract's record to replicate to the N parent", 15*time.Second, func() bool {
		for _, r := range fetchRecords(t, parent, "metrics", unique("skew"), "", 200) {
			rec := r.(map[string]any)
			if strings.Contains(rec["topic"].(string), "_Telemetry") &&
				strings.Contains(rec["topic"].(string), "child1/skew/reading") {
				return true
			}
		}
		return false
	})

	// The same publish directly at the N parent is rejected — its authority
	// does not know the contract.
	body, _ := json.Marshal(map[string]any{
		"topic": "colca/v1/_Telemetry/n-parent/skew/reading", "payload": map[string]any{"reading": 1.0}})
	req, err := newRequest("POST", "https://"+parent.APIAddr+"/publish", tok, string(body))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := httpsClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 422 {
		t.Fatalf("the N-pinned parent must reject the unknown contract, got %d", resp.StatusCode)
	}
}

// authtestEnrollNode enrolls a child node key at the parent (kind node).
func authtestEnrollNode(t *testing.T, parent *node.Node, ulid, pubkey, mount string) {
	t.Helper()
	b, err := json.Marshal(map[string]any{"ulid": ulid, "pubkey": pubkey, "kind": "node", "mount": mount})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := parent.Registry.Enroll(b); err != nil {
		t.Fatalf("enroll node %s: %v", ulid, err)
	}
}

var _ = fmt.Sprintf // keep fmt for future debug additions
