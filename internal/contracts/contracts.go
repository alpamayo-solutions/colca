// Package contracts loads the generated schema bundle (schema-bundle design
// §4/§7): one canonical-JSON artifact carrying, per contract, the routing
// class, the tombstone capability and a restricted JSON-Schema. Loading is
// start-time only and all-or-nothing — a bad bundle refuses to start, and
// with no bundle configured the engine falls back to the builtin floor
// (plugins/uns), never to a mix of the two.
//
// The jsonschema dependency lives HERE, not in plugins/uns — the plugin stays
// stdlib-only (arch-tested); the engine consults this table through a narrow
// Rule surface.
package contracts

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"

	jsonschema "github.com/santhosh-tekuri/jsonschema/v6"

	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// builtinOnly are contracts produced and validated by the colca binary
// itself (design §10.2); a bundle declaring one is a load error.
var builtinOnly = map[string]bool{"_StreamGap": true, "_EnrolledIdentity": true, "_TimeSync": true}

// allowedKeywords is the §4.1 schema subset, re-enforced at load so a bundle
// can never pull capabilities the broker did not sign up for.
var allowedKeywords = map[string]bool{
	"type": true, "properties": true, "required": true, "enum": true,
	"items": true, "minLength": true, "minimum": true, "maximum": true,
	"minItems": true, "additionalProperties": true,
}

// Rule is everything the engine needs to judge one contract.
type Rule struct {
	Class     uns.Class
	Tombstone bool
	schema    *jsonschema.Schema
}

// Validate applies the compiled schema to a non-empty payload.
func (r Rule) Validate(payload []byte) error {
	inst, err := jsonschema.UnmarshalJSON(bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("payload is not valid JSON: %w", err)
	}
	if err := r.schema.Validate(inst); err != nil {
		return fmt.Errorf("schema: %w", err)
	}
	return nil
}

// Table is the immutable per-process contract authority built from one bundle.
type Table struct {
	rules   map[string]Rule
	version string
	digest  string
}

// Lookup resolves a contract; ok=false means unknown (reject — validated
// namespace, design §7.1).
func (t *Table) Lookup(contract string) (Rule, bool) {
	r, ok := t.rules[contract]
	return r, ok
}

// Info reports the loaded bundle's identity for colca_contracts_bundle_info.
func (t *Table) Info() (version, digest string, contracts int) {
	return t.version, t.digest, len(t.rules)
}

type bundleFile struct {
	BundleVersion string                     `json:"bundle_version"`
	Digest        string                     `json:"digest"`
	GeneratedAt   string                     `json:"generated_at"`
	Source        json.RawMessage            `json:"source"`
	Contracts     map[string]json.RawMessage `json:"contracts"`
}

type contractEntry struct {
	Class     string          `json:"class"`
	Tombstone bool            `json:"tombstone"`
	Schema    json.RawMessage `json:"schema"`
}

// Load reads, verifies and compiles a bundle. wantSHA (optional, from the
// node config) is the revision pin: a mismatch refuses to start (design
// §6.1). Every failure names its §7.1 condition.
func Load(path, wantSHA string) (*Table, error) {
	raw, err := os.ReadFile(path) // #nosec G304 -- operator-configured path
	if err != nil {
		return nil, fmt.Errorf("contracts bundle: configured but unreadable: %w", err)
	}
	var f bundleFile
	if err := json.Unmarshal(raw, &f); err != nil {
		return nil, fmt.Errorf("contracts bundle %s: not valid JSON: %w", path, err)
	}
	if len(f.Contracts) == 0 {
		return nil, fmt.Errorf("contracts bundle %s: no contracts — an empty bundle can only reject", path)
	}

	digest, err := computeDigest(f)
	if err != nil {
		return nil, fmt.Errorf("contracts bundle %s: %w", path, err)
	}
	if f.Digest != "" && f.Digest != digest {
		return nil, fmt.Errorf("contracts bundle %s: internal digest %s does not match content %s — artifact edited after generation", path, short(f.Digest), short(digest))
	}
	if wantSHA != "" && wantSHA != digest {
		return nil, fmt.Errorf("contracts bundle %s: digest %s does not match the pinned sha256 %s (design §6.1: a drifted file must not silently validate with the wrong rules)", path, short(digest), short(wantSHA))
	}

	rules := make(map[string]Rule, len(f.Contracts))
	for name, rawEntry := range f.Contracts {
		if builtinOnly[name] {
			return nil, fmt.Errorf("contracts bundle %s: %s is builtin-only (design §10.2) — its producer and validator are this binary", path, name)
		}
		var e contractEntry
		if err := json.Unmarshal(rawEntry, &e); err != nil {
			return nil, fmt.Errorf("contracts bundle %s: contract %s unparseable: %w", path, name, err)
		}
		class, ok := uns.ClassFromManifest(e.Class)
		if !ok {
			return nil, fmt.Errorf("contracts bundle %s: contract %s has unknown class %q (want data|entity|definition|cmd|ack)", path, name, e.Class)
		}
		if err := lintSubset(e.Schema, name); err != nil {
			return nil, fmt.Errorf("contracts bundle %s: %w", path, err)
		}
		schema, err := compile(name, e.Schema)
		if err != nil {
			return nil, fmt.Errorf("contracts bundle %s: contract %s schema does not compile: %w", path, name, err)
		}
		rules[name] = Rule{Class: class, Tombstone: e.Tombstone, schema: schema}
	}
	return &Table{rules: rules, version: f.BundleVersion, digest: digest}, nil
}

// computeDigest reproduces the generator's rule: sha256 over the canonical
// JSON of {bundle_version, source, contracts} — generated_at and the digest
// field itself sit outside the hashed region (design §5.1).
func computeDigest(f bundleFile) (string, error) {
	contracts := make(map[string]json.RawMessage, len(f.Contracts))
	for k, v := range f.Contracts {
		canon, err := canonicalize(v)
		if err != nil {
			return "", fmt.Errorf("contract %s: %w", k, err)
		}
		contracts[k] = canon
	}
	source, err := canonicalize(f.Source)
	if err != nil {
		return "", fmt.Errorf("source: %w", err)
	}
	body := map[string]json.RawMessage{
		"bundle_version": mustJSON(f.BundleVersion),
		"source":         source,
		"contracts":      mustJSON(contracts),
	}
	canon, err := canonicalize(mustJSON(body))
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(canon)
	return hex.EncodeToString(sum[:]), nil
}

// canonicalize re-marshals JSON with sorted keys and no insignificant
// whitespace — Go maps marshal with sorted keys, matching the generator's
// json.dumps(sort_keys=True, separators=(",", ":")).
func canonicalize(raw json.RawMessage) (json.RawMessage, error) {
	var v any
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber() // keep numbers byte-exact
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	return json.Marshal(v)
}

func mustJSON(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err) // marshalling maps/strings of RawMessage cannot fail
	}
	return b
}

// lintSubset walks a schema and rejects any keyword outside §4.1.
func lintSubset(raw json.RawMessage, path string) error {
	var node map[string]json.RawMessage
	if err := json.Unmarshal(raw, &node); err != nil {
		return fmt.Errorf("contract %s: schema is not an object: %w", path, err)
	}
	for k, v := range node {
		if !allowedKeywords[k] {
			return fmt.Errorf("contract %s: schema keyword %q is outside the §4.1 subset", path, k)
		}
		switch k {
		case "properties":
			var props map[string]json.RawMessage
			if err := json.Unmarshal(v, &props); err != nil {
				return fmt.Errorf("contract %s: properties not an object: %w", path, err)
			}
			for name, sub := range props {
				if err := lintSubset(sub, path+"."+name); err != nil {
					return err
				}
			}
		case "items":
			if err := lintSubset(v, path+".items"); err != nil {
				return err
			}
		}
	}
	return nil
}

func compile(name string, raw json.RawMessage) (*jsonschema.Schema, error) {
	doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	c := jsonschema.NewCompiler()
	c.DefaultDraft(jsonschema.Draft2020)
	url := "bundle:///" + name + ".json"
	if err := c.AddResource(url, doc); err != nil {
		return nil, err
	}
	return c.Compile(url)
}

func short(s string) string {
	if len(s) > 12 {
		return s[:12] + "…"
	}
	return s
}
