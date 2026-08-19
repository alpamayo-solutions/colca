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

// Auth is the human-identity issuer block (human-authz design §4): the OIDC
// coordinates colca validates human JWTs against — offline, via the JWKS.
// Absent = the human world does not exist on this node.
type Auth struct {
	Issuer      string   `yaml:"issuer"`
	Audience    string   `yaml:"audience"`
	JWKSURL     string   `yaml:"jwks_url"`
	JWKSRefresh Duration `yaml:"jwks_refresh"` // 0 → 1h (EffectiveRefresh)
}

// EffectiveRefresh is the JWKS refresh cadence with the §4 default applied.
func (a *Auth) EffectiveRefresh() time.Duration {
	if a.JWKSRefresh == 0 {
		return time.Hour
	}
	return time.Duration(a.JWKSRefresh)
}

// MQTTHuman are the token-authenticated MQTT listeners (§4): raw MQTT over
// TLS and MQTT over WebSocket over TLS. Either may be empty.
type MQTTHuman struct {
	TCPAddr string `yaml:"tcp_addr"`
	WSAddr  string `yaml:"ws_addr"`
}

// TLS is an OPTIONAL certificate for the doors whose trust is not pinning.
//
// colcad self-signs by default (the certificate is a container for the node's
// ed25519 key, and trust comes from pinning that key), which is right for a
// machine or an enrolled child and unacceptable to a browser. A node that
// ordinary clients reach names a certificate here instead.
//
// It applies to the human MQTT/WebSocket doors and the HTTP API, and NOT to
// replication or the machine door — see identity.ServerCert for why that
// boundary is forced rather than chosen. Where the certificate comes from
// (ACME, a private PKI, a customer file) is deliberately outside colcad.
type TLS struct {
	CertFile string `yaml:"cert_file"`
	KeyFile  string `yaml:"key_file"`
}

type Endpoint struct {
	Addr string `yaml:"addr"`
}

type Parent struct {
	URL    string `yaml:"url"`
	Pubkey string `yaml:"pubkey"` // pinned parent key
}

type API struct {
	// Addr is the mTLS/token-authenticated API door. Conventionally ":443" —
	// HTTPS, so an operator's tooling reaches this node as https://node with
	// no port suffix (local-service-trust design §4).
	Addr  string `yaml:"addr"`
	Token string `yaml:"token"`
	// LocalAddr is the unpublished, plaintext local HTTP door (local-service-
	// trust design §4): reachability from inside the deployment's own
	// network IS the credential, so this listener carries no TLSConfig and
	// serves no admin routes. Conventionally ":80" — plaintext HTTP, so a
	// local service's configuration becomes http://colca with no port.
	// Absent = no local API door on this node.
	LocalAddr string `yaml:"local_addr"`
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
	TLS      TLS      `yaml:"tls"`
	API      API      `yaml:"api"`
	MQTT     Endpoint `yaml:"mqtt"`
	Repl     Endpoint `yaml:"repl"`
	Parent   *Parent  `yaml:"parent"`

	// MQTTLocal is the unpublished, plaintext local door (local-service-trust
	// design §4): reachability from inside the deployment's own network IS
	// the credential, so this listener carries no TLSConfig. Conventionally
	// ":1883" — the conventional plaintext MQTT port, used deliberately
	// because the "1883 announces plaintext" rule was written for a port a
	// scanner can reach, and this listener is never published. Absent = no
	// local door on this node.
	MQTTLocal Endpoint `yaml:"mqtt_local"`

	// Human world (human-authz design §4).
	Auth      *Auth     `yaml:"auth"`
	MQTTHuman MQTTHuman `yaml:"mqtt_human"`

	// Retention configures the background pruner (design §3). Absent entirely
	// = every default in the §3.1 table applies (pruning ON by default).
	Retention Retention `yaml:"retention"`

	// TimeSync configures the authoritative-time protocol (time-sync design
	// §2.5). Absent entirely = every default below applies (default-on).
	TimeSync TimeSync `yaml:"time_sync"`

	// Contracts configures the generated schema bundle (schema-bundle design
	// §6/§7). Absent = the baked default path if that file exists, else the
	// builtin floor. SHA256, when set, is the deployment revision's pin: a
	// bundle whose content digest mismatches refuses to start.
	Contracts Contracts `yaml:"contracts"`

	// Plugin is an opaque settings bag handed to the domain plugin. The core
	// never reads a key from it: what these mean is the plugin's business, and
	// keeping them out of the typed config above is what stops domain
	// vocabulary from leaking into the broker's own configuration surface.
	Plugin map[string]string `yaml:"plugin"`
}

// Contracts is the schema-bundle block (schema-bundle design §6.1).
type Contracts struct {
	Bundle string `yaml:"bundle"`
	SHA256 string `yaml:"sha256"`
}

// BakedBundlePath is where the image build copies the bundle generated from
// the same commit (design §6.1 channel 1). Used only when the config does
// not name a bundle explicitly and the file exists.
const BakedBundlePath = "/etc/colca/contracts-bundle.json"

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

// TimeSync configures the authoritative-time protocol (time-sync design
// §2.1/§2.5): telescoping clock offsets learned from /downlink and
// /replicate responses, plus the MQTT time beacon and the machine hold
// window a later task consumes. Default-on with zero config — an absent
// time_sync: block gets every default below (design §2.5 [delta]).
type TimeSync struct {
	// BeaconInterval is how often the node re-publishes colca/v1/_TimeSync
	// while machines are attached (design §2.2, default 30s). Parsed and
	// validated here; not yet consumed — a later task builds the beacon.
	BeaconInterval Duration `yaml:"beacon_interval"`
	// HoldMS is how long a machine buffers expiry decisions after
	// (re)connect before proceeding on its last-known offset (design §2.3
	// rule 2). A pointer because absent and explicit-0 are NOT the same
	// state (same rationale as Retention.Interval): absent means "apply the
	// §2.5 default (10000)"; an operator who writes hold_ms: 0 means "no
	// hold" (proceed immediately on last-known offset, skip the buffering
	// window entirely) — a later task builds the machine-side hold that
	// consumes this distinction.
	HoldMS *int64 `yaml:"hold_ms"`
	// DriftWarnMS is the |offset_ms| threshold past which colcad logs a
	// warning (design §2.4). A pointer for the same reason as HoldMS: absent
	// means "apply the §2.5 default (5000)"; an operator who writes
	// drift_warn_ms: 0 means "warn on any nonzero offset" — a real,
	// consumable setting (maximally sensitive drift alerting), not "use the
	// default". Consumed by this task (engine.ApplyClockSample).
	DriftWarnMS *int64 `yaml:"drift_warn_ms"`
}

const (
	defaultBeaconInterval = 30 * time.Second
	defaultHoldMS         = int64(10000)
	defaultDriftWarnMS    = int64(5000)
)

// EffectiveBeaconInterval applies the §2.5 default (30s) when
// BeaconInterval is unset (zero value). BeaconInterval stays a bare Duration
// (not a pointer) because, unlike HoldMS/DriftWarnMS, an explicit
// beacon_interval: 0 has no distinct meaning the design defines — "never
// beacon" is not a documented setting — so zero-means-default is
// unambiguous here.
func (t TimeSync) EffectiveBeaconInterval() time.Duration {
	if time.Duration(t.BeaconInterval) <= 0 {
		return defaultBeaconInterval
	}
	return time.Duration(t.BeaconInterval)
}

// EffectiveHoldMS returns the §2.5 default (10000) when HoldMS is absent
// (nil), or the configured value — including an explicit 0, which is the
// operator's "no hold" (design §2.3 rule 2). Callers (task 2's machine hold
// logic) MUST treat a returned 0 as "do not buffer at all", not as "use the
// default" — that translation already happened here.
func (t TimeSync) EffectiveHoldMS() int64 {
	if t.HoldMS == nil {
		return defaultHoldMS
	}
	return *t.HoldMS
}

// EffectiveDriftWarnMS returns the §2.5 default (5000) when DriftWarnMS is
// absent (nil), or the configured value — including an explicit 0, which is
// the operator's "warn on any nonzero offset" (design §2.4). Callers
// (engine.ApplyClockSample) MUST treat a returned 0 as that literal
// threshold, not as "use the default" — that translation already happened
// here.
func (t TimeSync) EffectiveDriftWarnMS() int64 {
	if t.DriftWarnMS == nil {
		return defaultDriftWarnMS
	}
	return *t.DriftWarnMS
}

// validate checks the time_sync: block (design §2.5): non-negative values
// only — an absent field or an explicit 0 (HoldMS/DriftWarnMS) are both
// meaningful, documented settings, not errors; only negative is rejected.
func (t TimeSync) validate() error {
	if time.Duration(t.BeaconInterval) < 0 {
		return fmt.Errorf("config: time_sync.beacon_interval must not be negative, got %s", time.Duration(t.BeaconInterval))
	}
	if t.HoldMS != nil && *t.HoldMS < 0 {
		return fmt.Errorf("config: time_sync.hold_ms must not be negative, got %d", *t.HoldMS)
	}
	if t.DriftWarnMS != nil && *t.DriftWarnMS < 0 {
		return fmt.Errorf("config: time_sync.drift_warn_ms must not be negative, got %d", *t.DriftWarnMS)
	}
	return nil
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
	if (c.MQTTHuman.TCPAddr != "" || c.MQTTHuman.WSAddr != "") && c.Auth == nil {
		// Fail at startup, not at the first CONNECT: a token door with no
		// verifier authenticates nobody (design §4).
		return fmt.Errorf("config: mqtt_human requires an auth: block — a token door with no verifier authenticates nobody")
	}
	if c.Auth != nil {
		if c.Auth.Issuer == "" || c.Auth.Audience == "" || c.Auth.JWKSURL == "" {
			return fmt.Errorf("config: auth needs issuer, audience and jwks_url")
		}
		if c.Auth.JWKSRefresh < 0 {
			return fmt.Errorf("config: auth.jwks_refresh must not be negative")
		}
	}
	if err := c.Retention.validate(); err != nil {
		return err
	}
	return c.TimeSync.validate()
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
