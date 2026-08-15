// Package config loads and validates a Colca node's YAML configuration.
//
// The most important rule enforced here is the static mount-collision check:
// children and clients share ONE mount namespace, so two entries claiming the
// same mount is a config error. That is what guarantees a node can never end up
// able to steal another node's subtree at runtime.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type Endpoint struct {
	Addr string `yaml:"addr"`
}

type Parent struct {
	URL    string `yaml:"url"`
	Pubkey string `yaml:"pubkey"` // pinned parent key
}

type API struct {
	Addr  string `yaml:"addr"`
	Token string `yaml:"token"`
}

// Config is a node's full configuration. Only ULID, DataDir and KeyFile are
// required: a global node has no Parent and no MQTT listener. Machines and
// child nodes are NOT config: they are runtime registry state, enrolled
// through the admin API (auth design §2, §4).
type Config struct {
	ULID     string   `yaml:"ulid"`
	DataDir  string   `yaml:"data_dir"`
	LogLevel string   `yaml:"log_level"`
	KeyFile  string   `yaml:"key_file"`
	API      API      `yaml:"api"`
	MQTT     Endpoint `yaml:"mqtt"`
	Repl     Endpoint `yaml:"repl"`
	Parent   *Parent  `yaml:"parent"`

	// Retention configures the background pruner (design §3). Absent entirely
	// = every default in the §3.1 table applies (pruning ON by default).
	Retention Retention `yaml:"retention"`
}

// Duration is a time.Duration that unmarshals from Go duration syntax
// ("336h", "5m" — design §3.1: "Go duration syntax; no \"d\" unit") or the
// bare integer 0, which spec §3.1/§5.2 use as the "never"/"disabled"
// sentinel (e.g. ignore_cursors_after: 0). Any other bare number is
// rejected: allowing arbitrary bare integers would make the unit ambiguous
// (nanoseconds? seconds?) and silently accept exactly the malformed input
// ("14d") the spec comment warns against.
type Duration time.Duration

func (d *Duration) UnmarshalYAML(node *yaml.Node) error {
	if node.Tag == "!!int" {
		var n int64
		if err := node.Decode(&n); err != nil {
			return fmt.Errorf("config: invalid duration %q: %w", node.Value, err)
		}
		if n != 0 {
			return fmt.Errorf("config: invalid duration %q: bare integers other than 0 are not allowed, use Go duration syntax (e.g. \"336h\")", node.Value)
		}
		*d = 0
		return nil
	}
	var s string
	if err := node.Decode(&s); err != nil {
		return fmt.Errorf("config: invalid duration %q: %w", node.Value, err)
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("config: invalid duration %q: %w", s, err)
	}
	*d = Duration(v)
	return nil
}

// ByteSize is a uint64 byte count that unmarshals from a plain integer (raw
// byte count) or a binary-unit-suffixed string ("4GiB" — design §3.1/§3.3:
// "logical bytes (topic+payload)"). Units are IEC binary (1024-based),
// matching the spec's own examples (KiB, MiB, GiB, TiB).
type ByteSize uint64

var byteUnits = []struct {
	suffix string
	mult   uint64
}{
	{"TiB", 1 << 40},
	{"GiB", 1 << 30},
	{"MiB", 1 << 20},
	{"KiB", 1 << 10},
	{"B", 1},
}

func parseByteSize(s string) (uint64, error) {
	s = strings.TrimSpace(s)
	for _, u := range byteUnits {
		if rest, ok := strings.CutSuffix(s, u.suffix); ok && rest != "" {
			n, err := strconv.ParseFloat(rest, 64)
			if err != nil || n < 0 {
				return 0, fmt.Errorf("invalid byte size %q", s)
			}
			return uint64(n * float64(u.mult)), nil
		}
	}
	n, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid byte size %q", s)
	}
	return n, nil
}

func (b *ByteSize) UnmarshalYAML(node *yaml.Node) error {
	if node.Tag == "!!int" {
		var n uint64
		if err := node.Decode(&n); err != nil {
			return fmt.Errorf("config: invalid byte size %q: %w", node.Value, err)
		}
		*b = ByteSize(n)
		return nil
	}
	var s string
	if err := node.Decode(&s); err != nil {
		return fmt.Errorf("config: invalid byte size %q: %w", node.Value, err)
	}
	v, err := parseByteSize(s)
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}
	*b = ByteSize(v)
	return nil
}

// StreamRetention is one stream's entry under retention.streams (design §3.1).
// Both limits are optional; either one is sufficient to enable pruning for
// that stream. Zero-value MaxAge defaults per the §3.1 table (see
// EffectiveStream); zero-value MaxBytes and IgnoreCursorsAfter already ARE
// their own defaults (unset / never).
type StreamRetention struct {
	MaxAge             Duration `yaml:"max_age"`
	MaxBytes           ByteSize `yaml:"max_bytes"`
	IgnoreCursorsAfter Duration `yaml:"ignore_cursors_after"`
}

// Retention is the retention: block (design §3.1). Streams is keyed by
// stream name (metrics/entities/commands — the only names uns.StreamFor ever
// produces); a missing entry gets the full default row.
//
// Interval is a pointer because absent and explicit-0 are NOT the same
// state, despite the §3.1 YAML example's inline comment reading "0 or absent
// = pruner disabled": the prose two paragraphs below it is authoritative —
// "`interval` default: 5 minutes. Absent `retention:` block entirely →
// defaults above apply (pruning ON by default...)" — so absent must default
// to 5m (pruning ON), while an operator who writes `interval: 0` explicitly
// is turning the pruner off. A bare Duration cannot distinguish "the key was
// never written" from "the key was written as 0"; the pointer can.
type Retention struct {
	Interval *Duration                  `yaml:"interval"`
	Streams  map[string]StreamRetention `yaml:"streams"`
}

// defaultRetentionInterval is the pruner cadence when retention.interval is
// absent (design §3.1).
const defaultRetentionInterval = 5 * time.Minute

// minCommandsMaxAge is the build-time floor from design §3.4: "Config
// validation rejects commands.max_age below a build-time floor (proposed: 7
// days)". §11.1 leaves a future max_command_ttl config knob as an open
// question; until that knob exists, this fixed floor is
// the only enforceable form of the "commands retention must exceed the
// longest command TTL" rule.
const minCommandsMaxAge = 7 * 24 * time.Hour

// defaultStreamMaxAge is the §3.1 defaults table, one row per canonical
// stream. max_bytes has no per-stream default: it is always "unset" (0) as
// the table states, so it needs no entry here.
var defaultStreamMaxAge = map[string]time.Duration{
	"metrics":  336 * time.Hour,  // 14 days
	"entities": 8760 * time.Hour, // 365 days
	"commands": 2160 * time.Hour, // 90 days
}

// knownStreams are the only stream names the system ever produces
// (uns.StreamFor's fixed output set). A retention.streams key outside this
// set can never match a real stream, so it is rejected as a config error
// rather than silently doing nothing (design §3.2: per-stream, not per-path,
// retention over a closed set of streams).
var knownStreams = map[string]bool{"metrics": true, "entities": true, "commands": true}

// EffectiveInterval returns the pruner cadence: the §3.1 default (5m) when
// Interval is absent (nil), or the configured value — including an explicit
// 0, which is the operator's "pruner disabled" (§3.1). Callers (the
// pruner goroutine) MUST treat a returned 0 as "never run", not as "use the
// default" — that translation already happened here.
func (r Retention) EffectiveInterval() time.Duration {
	if r.Interval == nil {
		return defaultRetentionInterval
	}
	return time.Duration(*r.Interval)
}

// EffectiveStream returns stream's effective retention policy, applying the
// §3.1 default MaxAge when the config left it at the zero value (whether
// because the whole retention: block, the stream's entry, or just this field
// was absent — the brief is explicit that "zero-value = defaults table").
// MaxBytes and IgnoreCursorsAfter need no defaulting: their zero value already
// is their documented default (unset / never).
func (r Retention) EffectiveStream(stream string) StreamRetention {
	s := r.Streams[stream]
	if time.Duration(s.MaxAge) <= 0 {
		if d, ok := defaultStreamMaxAge[stream]; ok {
			s.MaxAge = Duration(d)
		}
	}
	return s
}

// Load reads a YAML config from path and validates it.
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var c Config
	if err := yaml.Unmarshal(raw, &c); err != nil {
		return nil, fmt.Errorf("config: parse %s: %w", path, err)
	}
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return &c, nil
}

// Validate checks required fields. Identity and mount rules moved to
// enrollment validation (registry manager + uns.Entry.Validate) — machines
// and children are runtime registry state, not config.
func (c *Config) Validate() error {
	if c.ULID == "" || c.DataDir == "" || c.KeyFile == "" {
		return fmt.Errorf("config: ulid, data_dir, key_file are required")
	}
	return c.Retention.validate()
}

// validate checks the retention: block (design §3.1/§3.4): non-negative
// durations, known stream names, and the commands.max_age floor.
func (r Retention) validate() error {
	if r.Interval != nil && time.Duration(*r.Interval) < 0 {
		return fmt.Errorf("config: retention.interval must not be negative, got %s", time.Duration(*r.Interval))
	}
	for name, s := range r.Streams {
		if !knownStreams[name] {
			return fmt.Errorf("config: retention.streams: unknown stream %q, want one of metrics, entities, commands", name)
		}
		if time.Duration(s.MaxAge) < 0 {
			return fmt.Errorf("config: retention.streams.%s.max_age must not be negative, got %s", name, time.Duration(s.MaxAge))
		}
		if time.Duration(s.IgnoreCursorsAfter) < 0 {
			return fmt.Errorf("config: retention.streams.%s.ignore_cursors_after must not be negative, got %s", name, time.Duration(s.IgnoreCursorsAfter))
		}
	}
	commands := r.EffectiveStream("commands")
	if time.Duration(commands.MaxAge) < minCommandsMaxAge {
		return fmt.Errorf("config: retention.streams.commands.max_age must be >= %s (build-time command-TTL floor, design §3.4), got %s",
			minCommandsMaxAge, time.Duration(commands.MaxAge))
	}
	return nil
}
