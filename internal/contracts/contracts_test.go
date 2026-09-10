package contracts

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/alpamayo-solutions/colca/internal/contracts/contractstest"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// fixture writes a bundle assembled from parts, computing a correct digest
// unless overrideDigest is set.
func fixture(t *testing.T, contracts map[string]any, overrideDigest string) string {
	t.Helper()
	body := map[string]any{
		"bundle_version": "9.9.9-test",
		"source":         map[string]any{"package": "colca-data-contracts", "git_sha": "fixture"},
		"contracts":      contracts,
	}
	canon, err := json.Marshal(sorted(body))
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(canon)
	digest := hex.EncodeToString(sum[:])
	if overrideDigest != "" {
		digest = overrideDigest
	}
	full := map[string]any{}
	for k, v := range body {
		full[k] = v
	}
	full["digest"] = digest
	full["generated_at"] = "2026-08-17T00:00:00Z"
	raw, err := json.Marshal(full)
	if err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(t.TempDir(), "bundle.json")
	if err := os.WriteFile(p, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

// sorted forces deterministic marshalling of nested any-maps (Go sorts map
// keys on Marshal already; this exists to normalize via a round-trip).
func sorted(v any) any {
	b, _ := json.Marshal(v)
	var out any
	_ = json.Unmarshal(b, &out)
	return out
}

func metricEntry() map[string]any {
	return map[string]any{
		"class": "data", "tombstone": true,
		"schema": map[string]any{
			"type": "object",
			"properties": map[string]any{
				"value":     map[string]any{},
				"signal_id": map[string]any{"type": "string", "minLength": 1},
			},
			"required": []any{"signal_id", "value"},
		},
	}
}

func TestLoadValidBundle(t *testing.T) {
	p := fixture(t, map[string]any{
		"_Metric": metricEntry(),
		"_CmdWrite": map[string]any{"class": "cmd", "tombstone": false,
			"schema": map[string]any{"type": "object", "required": []any{"correlation_id"},
				"properties": map[string]any{"correlation_id": map[string]any{"type": "string", "minLength": 1}}}},
	}, "")
	tbl, err := Load(p, "")
	if err != nil {
		t.Fatal(err)
	}
	version, digest, n := tbl.Info()
	if version != "9.9.9-test" || n != 2 || len(digest) != 64 {
		t.Fatalf("Info() = %q %q %d", version, digest, n)
	}
	r, ok := tbl.Lookup("_Metric")
	if !ok || r.Class != uns.ClassData || !r.Tombstone {
		t.Fatalf("Lookup(_Metric) = %+v %v", r, ok)
	}
	if err := r.Validate([]byte(`{"value": 3, "signal_id": "s1"}`)); err != nil {
		t.Fatalf("valid payload rejected: %v", err)
	}
	if err := r.Validate([]byte(`{"value": 3}`)); err == nil {
		t.Fatal("missing signal_id must be rejected")
	}
	if err := r.Validate([]byte(`{"value": 3, "signal_id": ""}`)); err == nil {
		t.Fatal("empty signal_id must be rejected (minLength)")
	}
	if _, ok := tbl.Lookup("_Unknown"); ok {
		t.Fatal("unknown contract must not resolve")
	}
	cmd, _ := tbl.Lookup("_CmdWrite")
	if cmd.Class != uns.ClassCmd || cmd.Tombstone {
		t.Fatalf("cmd entry: %+v", cmd)
	}
}

// The generator's real output loads, and the Go loader computes the same digest
// Python did.
func TestLoadRealGeneratedBundle(t *testing.T) {
	path := contractstest.GeneratedBundlePath(t)
	tbl, err := Load(path, "")
	if err != nil {
		t.Fatalf("real generated bundle failed to load: %v", err)
	}
	raw, _ := os.ReadFile(path)
	var f struct {
		Digest string `json:"digest"`
	}
	_ = json.Unmarshal(raw, &f)
	_, digest, n := tbl.Info()
	if digest != f.Digest {
		t.Fatalf("Go-recomputed digest %s != generator digest %s (canonicalization drift)", digest, f.Digest)
	}
	if n < 20 {
		t.Fatalf("expected the full registry (>=20 contracts), got %d", n)
	}
	if _, ok := tbl.Lookup("_CmdAdmin"); !ok {
		t.Fatal("generated bundle must carry _CmdAdmin")
	}
}

// pattern and maxLength are in the subset: a bundle using them loads, the
// pattern is compiled once, and a value outside it is refused with a message
// naming the pattern.
func TestPatternAndMaxLengthAreInTheSubset(t *testing.T) {
	e := metricEntry()
	e["schema"].(map[string]any)["properties"].(map[string]any)["signal_id"] = map[string]any{
		"type": "string", "pattern": "^[0-9A-HJKMNP-TV-Z]{26}$", "maxLength": 26,
	}
	tbl, err := Load(fixture(t, map[string]any{"_Metric": e}, ""), "")
	if err != nil {
		t.Fatalf("a bundle using pattern/maxLength must load: %v", err)
	}
	rule, _ := tbl.Lookup("_Metric")
	if err := rule.Validate([]byte(`{"value": 1, "signal_id": "01ARZ3NDEKTSV4RRFFQ69G5FAV"}`)); err != nil {
		t.Fatalf("a ULID must pass the pattern: %v", err)
	}
	err = rule.Validate([]byte(`{"value": 1, "signal_id": "01ARZ3NDEKTSV4RRFFQ69G5FAVEXTRA"}`))
	if err == nil || !strings.Contains(err.Error(), "^[0-9A-HJKMNP-TV-Z]{26}$") {
		t.Fatalf("a 31-character id must be refused naming the pattern, got: %v", err)
	}
}

func TestFailStartConditions(t *testing.T) {
	valid := map[string]any{"_Metric": metricEntry()}

	cases := []struct {
		name string
		path func(t *testing.T) string
		want string
	}{
		{"missing file", func(t *testing.T) string { return filepath.Join(t.TempDir(), "absent.json") }, "unreadable"},
		{"bad json", func(t *testing.T) string {
			p := filepath.Join(t.TempDir(), "b.json")
			_ = os.WriteFile(p, []byte("{nope"), 0o600)
			return p
		}, "not valid JSON"},
		{"empty contracts", func(t *testing.T) string { return fixture(t, map[string]any{}, "") }, "no contracts"},
		{"out-of-subset keyword", func(t *testing.T) string {
			e := metricEntry()
			e["schema"].(map[string]any)["patternProperties"] = map[string]any{}
			return fixture(t, map[string]any{"_Metric": e}, "")
		}, "outside the §4.1 subset"},
		{"unknown class", func(t *testing.T) string {
			e := metricEntry()
			e["class"] = "telemetry"
			return fixture(t, map[string]any{"_Metric": e}, "")
		}, "unknown class"},
		{"builtin redeclared", func(t *testing.T) string {
			return fixture(t, map[string]any{"_Metric": metricEntry(), "_StreamGap": metricEntry()}, "")
		}, "builtin-only"},
		{"internal digest mismatch", func(t *testing.T) string {
			return fixture(t, valid, strings.Repeat("ab", 32))
		}, "does not match content"},
		{"malformed pattern", func(t *testing.T) string {
			// pattern is compiled at load, so an expression Go's regexp refuses fails
			// startup.
			e := metricEntry()
			e["schema"].(map[string]any)["properties"].(map[string]any)["signal_id"] = map[string]any{"type": "string", "pattern": "^[0-9A-Z{26}$"}
			return fixture(t, map[string]any{"_Metric": e}, "")
		}, "does not compile"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := Load(c.path(t), "")
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("Load must fail naming %q, got: %v", c.want, err)
			}
		})
	}

	// Pin mismatch (config sha256): correct file, wrong pin.
	p := fixture(t, valid, "")
	if _, err := Load(p, strings.Repeat("cd", 32)); err == nil || !strings.Contains(err.Error(), "pinned sha256") {
		t.Fatalf("pin mismatch must fail start, got: %v", err)
	}
	// Correct pin loads.
	tbl, err := Load(p, "")
	if err != nil {
		t.Fatal(err)
	}
	_, digest, _ := tbl.Info()
	if _, err := Load(p, digest); err != nil {
		t.Fatalf("matching pin must load: %v", err)
	}
}
