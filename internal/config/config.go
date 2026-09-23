// Package config loads and validates a Colca node's YAML configuration.
package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// Auth lists the OIDC issuers that people's tokens are checked against, offline
// through their JWKS. Without it the node has no token doors.
//
// One identity provider reached under several host names (a site with two
// networks) is several issuers sharing one JWKS: the provider puts the host the
// browser used into `iss`. Issuers without their own jwks_url use the shared
// one.
type Auth struct {
	Issuers     []AuthIssuer `yaml:"issuers"`
	Audience    string       `yaml:"audience"`
	JWKSURL     string       `yaml:"jwks_url"`     // shared by every issuer without its own
	JWKSRefresh Duration     `yaml:"jwks_refresh"` // 0 → 1h (EffectiveRefresh)
}

// AuthIssuer is one accepted `iss` value and, optionally, where its signing
// keys come from when they differ from the shared auth.jwks_url.
type AuthIssuer struct {
	URL     string `yaml:"url"`
	JWKSURL string `yaml:"jwks_url"`
}

// UnmarshalYAML rejects the single-issuer form, which would otherwise be
// ignored silently and leave the node without any accepted issuer.
func (a *Auth) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind == yaml.MappingNode {
		for i := 0; i+1 < len(node.Content); i += 2 {
			if node.Content[i].Value == "issuer" {
				return fmt.Errorf("config: auth.issuer was replaced by auth.issuers, a list: "+
					"write `issuers: [{ url: %s }]`", node.Content[i+1].Value)
			}
		}
	}
	type plain Auth
	return node.Decode((*plain)(a))
}

// EffectiveIssuers returns the issuers with their JWKS URL resolved: their own,
// else the shared one.
func (a *Auth) EffectiveIssuers() []AuthIssuer {
	out := make([]AuthIssuer, len(a.Issuers))
	for i, is := range a.Issuers {
		out[i] = is
		if out[i].JWKSURL == "" {
			out[i].JWKSURL = a.JWKSURL
		}
	}
	return out
}

// EffectiveRefresh is the JWKS refresh interval, one hour by default.
func (a *Auth) EffectiveRefresh() time.Duration {
	if a.JWKSRefresh == 0 {
		return time.Hour
	}
	return time.Duration(a.JWKSRefresh)
}

// MQTTHuman are the token-authenticated MQTT listeners: MQTT over TLS and MQTT
// over WebSocket. Either may be empty.
type MQTTHuman struct {
	TCPAddr string `yaml:"tcp_addr"`
	WSAddr  string `yaml:"ws_addr"`
}

// TLS is an optional certificate for the doors people reach: the token MQTT
// doors and the HTTP API. By default colcad self-signs and trust comes from
// pinning the node's key, which browsers do not accept. Replication and the
// machine door always use the pinned key (see identity.ServerCert).
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
	// Addr is the API door with mTLS or tokens, conventionally ":443".
	Addr  string `yaml:"addr"`
	Token string `yaml:"token"`
	// LocalAddr is the plaintext local HTTP door, conventionally ":80". Being
	// inside the deployment's network is the credential, so it serves no admin
	// routes. Empty means no local door.
	LocalAddr string `yaml:"local_addr"`
}

// Config is a node's configuration. Only ULID, DataDir and KeyFile are
// required. Machines and child nodes are not configured here; they are
// enrolled at runtime through the admin API.
type Config struct {
	ULID string `yaml:"ulid"`
	// Name is what the node calls itself in its own _Node record: a deployment's
	// name, never its position. Empty means the ULID.
	Name string `yaml:"name"`
	// TopicRoot is the first segment of every topic in this node's tree,
	// "colca" when empty. COLCA_TOPIC_ROOT overrides it. All nodes of one tree
	// must use the same root; nodes do not translate between roots.
	TopicRoot string `yaml:"topic_root"`
	DataDir   string `yaml:"data_dir"`
	// AddrFile, when set, receives the resolved door addresses as JSON
	// ({"api","api_local","mqtt","mqtt_local","repl"}) once every listener is up.
	// A supervisor that configures the doors as ":0" reads the real ports from it;
	// picking free ports up front can hand one port to two doors on Linux. The
	// file is written atomically.
	AddrFile string `yaml:"addr_file"`
	// SecretsDir is a separate node-local Pebble database containing only
	// service-sealed ciphertext. It is deliberately outside DataDir so stream
	// reset/restore and replication lifecycle can never include it by accident.
	// Empty disables the secret store for compositions that do not expose it.
	SecretsDir string   `yaml:"secrets_dir"`
	LogLevel   string   `yaml:"log_level"`
	KeyFile    string   `yaml:"key_file"`
	TLS        TLS      `yaml:"tls"`
	API        API      `yaml:"api"`
	MQTT       Endpoint `yaml:"mqtt"`
	Repl       Endpoint `yaml:"repl"`
	Parent     *Parent  `yaml:"parent"`
	// Standalone permanently retires fleet trust in this data directory.
	Standalone      bool            `yaml:"standalone"`
	StandaloneSince int64           `yaml:"-"` // persisted activation, populated before serving
	RetiredPATs     map[string]bool `yaml:"-"`

	// MQTTLocal is the plaintext local MQTT door, conventionally ":1883". Being
	// inside the deployment's network is the credential, and the door is never
	// published. Empty means no local door.
	MQTTLocal Endpoint `yaml:"mqtt_local"`

	// Token doors for people.
	Auth      *Auth     `yaml:"auth"`
	MQTTHuman MQTTHuman `yaml:"mqtt_human"`

	// MQTTLimits bounds broker-owned memory and connection state. The defaults
	// are deliberately generous for recovery bursts and large installations,
	// but unlike mochi's defaults none of the long-lived dimensions is
	// effectively unlimited.
	MQTTLimits MQTTLimits `yaml:"mqtt_limits"`

	// Retention configures the background pruner. Without the block the defaults
	// apply and pruning is on.
	Retention Retention `yaml:"retention"`

	// Limits caps the size of a single record or blob.
	Limits Limits `yaml:"limits"`

	// BlobGC configures the blob sweeper. Without the block it runs every 15m with
	// a one-hour grace period.
	BlobGC BlobGC `yaml:"blob_gc"`

	// TimeSync configures authoritative time. It is on by default.
	TimeSync TimeSync `yaml:"time_sync"`

	// Contracts names the schema bundle. Without it the bundle baked into the
	// image is used if present, else the built-in rules. SHA256 pins the bundle:
	// a mismatching digest refuses to start.
	Contracts Contracts `yaml:"contracts"`

	// Plugin holds settings for the domain plugin. The core never reads them,
	// which keeps domain vocabulary out of the broker's configuration.
	Plugin map[string]string `yaml:"plugin"`
}

// Contracts is the contracts: block.
type Contracts struct {
	Bundle string `yaml:"bundle"`
	SHA256 string `yaml:"sha256"`
}

// BakedBundlePath is where the image puts the bundle built from the same
// commit. It is used when the config names no bundle and the file exists.
const BakedBundlePath = "/etc/colca/contracts-bundle.json"

// Duration unmarshals from Go duration syntax ("336h", "5m") or a bare 0,
// which means never or disabled. Other bare numbers are rejected because their
// unit would be ambiguous.
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

// ByteSize unmarshals from a byte count or a string with a binary unit such as
// "4GiB" (1024-based).
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

// StreamRetention is one stream's retention entry. Either limit enables
// pruning. A zero MaxAge takes the stream's default (see EffectiveStream);
// zero MaxBytes and IgnoreCursorsAfter mean unset and never.
type StreamRetention struct {
	MaxAge             Duration `yaml:"max_age"`
	MaxBytes           ByteSize `yaml:"max_bytes"`
	IgnoreCursorsAfter Duration `yaml:"ignore_cursors_after"`
	KeepForever        bool     `yaml:"keep_forever"`
}

// Retention is the retention: block, keyed by stream name. A stream without an
// entry gets the defaults.
//
// Interval is a pointer because absent and 0 differ: absent means every five
// minutes, an explicit 0 turns the pruner off.
type Retention struct {
	Interval *Duration                  `yaml:"interval"`
	Streams  map[string]StreamRetention `yaml:"streams"`
}

// Limits is the limits: block. Zero values mean the defaults.
type Limits struct {
	MaxRecordBytes ByteSize `yaml:"max_record_bytes"`
	MaxBlobBytes   ByteSize `yaml:"max_blob_bytes"`
}

// MQTTLimits is the mqtt_limits: block. Zero values select the defaults below;
// operators can lower or raise a ceiling without rebuilding Colca.
type MQTTLimits struct {
	MaxClients                int64    `yaml:"max_clients"`
	MaxSubscriptionsPerClient int      `yaml:"max_subscriptions_per_client"`
	ReceiveMaximum            uint16   `yaml:"receive_maximum"`
	MaximumInflight           uint16   `yaml:"maximum_inflight"`
	MaxPendingWritesPerClient int32    `yaml:"max_pending_writes_per_client"`
	MaxTopicAliasesPerClient  uint16   `yaml:"max_topic_aliases_per_client"`
	MaxSessionExpiry          Duration `yaml:"max_session_expiry"`
}

const (
	defaultMQTTMaxClients                int64  = 4096
	defaultMQTTMaxSubscriptionsPerClient int    = 1024
	defaultMQTTReceiveMaximum            uint16 = 1024
	defaultMQTTMaximumInflight           uint16 = 65535
	// mochi drops a publish when this queue is full, and a new subscriber's
	// retained replay fills it in one burst. 8192 is mochi's own default and where
	// TestRetainedReplayDeliversAllMessages stops losing messages;
	// colca_mqtt_publish_dropped_total shows whether a deployment hits it. Memory
	// is bounded by MaximumPacketSize times this, per client.
	defaultMQTTMaxPendingWritesPerClient int32  = 8192
	defaultMQTTMaxTopicAliasesPerClient  uint16 = 256
	defaultMQTTMaxSessionExpiry                 = 7 * 24 * time.Hour
	maxMQTTSessionExpiry                        = time.Duration(^uint32(0)) * time.Second
)

func (l MQTTLimits) EffectiveMaxClients() int64 {
	if l.MaxClients == 0 {
		return defaultMQTTMaxClients
	}
	return l.MaxClients
}

func (l MQTTLimits) EffectiveMaxSubscriptionsPerClient() int {
	if l.MaxSubscriptionsPerClient == 0 {
		return defaultMQTTMaxSubscriptionsPerClient
	}
	return l.MaxSubscriptionsPerClient
}

func (l MQTTLimits) EffectiveReceiveMaximum() uint16 {
	if l.ReceiveMaximum == 0 {
		return defaultMQTTReceiveMaximum
	}
	return l.ReceiveMaximum
}

func (l MQTTLimits) EffectiveMaximumInflight() uint16 {
	if l.MaximumInflight == 0 {
		return defaultMQTTMaximumInflight
	}
	return l.MaximumInflight
}

func (l MQTTLimits) EffectiveMaxPendingWritesPerClient() int32 {
	if l.MaxPendingWritesPerClient == 0 {
		return defaultMQTTMaxPendingWritesPerClient
	}
	return l.MaxPendingWritesPerClient
}

func (l MQTTLimits) EffectiveMaxTopicAliasesPerClient() uint16 {
	if l.MaxTopicAliasesPerClient == 0 {
		return defaultMQTTMaxTopicAliasesPerClient
	}
	return l.MaxTopicAliasesPerClient
}

func (l MQTTLimits) EffectiveMaxSessionExpiry() time.Duration {
	if l.MaxSessionExpiry == 0 {
		return defaultMQTTMaxSessionExpiry
	}
	return time.Duration(l.MaxSessionExpiry)
}

func (l MQTTLimits) validate() error {
	if l.MaxClients < 0 {
		return fmt.Errorf("config: mqtt_limits.max_clients must not be negative, got %d", l.MaxClients)
	}
	if l.MaxSubscriptionsPerClient < 0 {
		return fmt.Errorf("config: mqtt_limits.max_subscriptions_per_client must not be negative, got %d", l.MaxSubscriptionsPerClient)
	}
	if l.MaxPendingWritesPerClient < 0 {
		return fmt.Errorf("config: mqtt_limits.max_pending_writes_per_client must not be negative, got %d", l.MaxPendingWritesPerClient)
	}
	if time.Duration(l.MaxSessionExpiry) < 0 {
		return fmt.Errorf("config: mqtt_limits.max_session_expiry must not be negative, got %s", time.Duration(l.MaxSessionExpiry))
	}
	if l.MaxSessionExpiry != 0 && time.Duration(l.MaxSessionExpiry) < time.Second {
		return fmt.Errorf("config: mqtt_limits.max_session_expiry must be at least 1s, got %s", time.Duration(l.MaxSessionExpiry))
	}
	if time.Duration(l.MaxSessionExpiry) > maxMQTTSessionExpiry {
		return fmt.Errorf("config: mqtt_limits.max_session_expiry exceeds the MQTT maximum of %s, got %s",
			maxMQTTSessionExpiry, time.Duration(l.MaxSessionExpiry))
	}
	return nil
}

// defaultMaxRecordBytes leaves headroom for the fattest known record (a
// connector's full _DataTags catalogue) while making a file smuggled into a
// record impossible by an order of magnitude.
const defaultMaxRecordBytes = 4 << 20

// defaultMaxBlobBytes is the conservative resource-file ceiling; whole-file
// transfer with retry is enough at this size, which is why no chunking
// protocol exists.
const defaultMaxBlobBytes = 32 << 20

// maxMaxRecordBytes bounds max_record_bytes. The MQTT door adds 64KiB of
// headroom and narrows the result to a uint32, which would overflow near 4GiB
// and quietly cap packets below the record limit. 1GiB is far above any real
// record.
const maxMaxRecordBytes = 1 << 30

// maxMaxBlobBytes bounds max_blob_bytes so the blob doors can hold it in an
// int64. 1 TiB is far beyond any resource file.
const maxMaxBlobBytes = 1 << 40

func (l Limits) EffectiveMaxRecordBytes() uint64 {
	if l.MaxRecordBytes == 0 {
		return defaultMaxRecordBytes
	}
	return uint64(l.MaxRecordBytes)
}

func (l Limits) EffectiveMaxBlobBytes() uint64 {
	if l.MaxBlobBytes == 0 {
		return defaultMaxBlobBytes
	}
	return uint64(l.MaxBlobBytes)
}

// validate refuses a blob cap below the record cap, which is almost always a
// typo, and caps above maxMaxRecordBytes or maxMaxBlobBytes.
func (l Limits) validate() error {
	if l.EffectiveMaxRecordBytes() > maxMaxRecordBytes {
		return fmt.Errorf("limits: max_record_bytes (%d) exceeds the maximum of %d",
			l.EffectiveMaxRecordBytes(), uint64(maxMaxRecordBytes))
	}
	if l.EffectiveMaxBlobBytes() > maxMaxBlobBytes {
		return fmt.Errorf("limits: max_blob_bytes (%d) exceeds the maximum of %d",
			l.EffectiveMaxBlobBytes(), uint64(maxMaxBlobBytes))
	}
	if l.EffectiveMaxBlobBytes() < l.EffectiveMaxRecordBytes() {
		return fmt.Errorf("limits: max_blob_bytes (%d) is below max_record_bytes (%d)",
			l.EffectiveMaxBlobBytes(), l.EffectiveMaxRecordBytes())
	}
	return nil
}

// BlobGC is the blob_gc: block for the sweeper that reclaims blobs no live
// _Resource references.
//
// Both fields are pointers because absent and 0 differ, and not in the same
// way: interval: 0 turns the sweeper off, while grace: 0 means no grace and
// sweeps unreferenced blobs immediately. Absent means 15m and 1h.
type BlobGC struct {
	Interval *Duration `yaml:"interval"`
	Grace    *Duration `yaml:"grace"`
}

// defaultBlobGCInterval is the sweeper interval when blob_gc.interval is absent.
const defaultBlobGCInterval = 15 * time.Minute

// defaultBlobGCGrace covers the windows in which a blob exists before the
// record that references it: upload before upsert, blob before entity at an
// ancestor, and pull before execute. The record arrives well within the hour.
const defaultBlobGCGrace = time.Hour

// EffectiveInterval returns the sweeper interval: 15m when absent, otherwise
// the configured value. An explicit 0 means the sweeper is off.
func (b BlobGC) EffectiveInterval() time.Duration {
	if b.Interval == nil {
		return defaultBlobGCInterval
	}
	return time.Duration(*b.Interval)
}

// EffectiveGrace returns the grace period: 1h when absent, otherwise the
// configured value. An explicit 0 means sweep immediately.
func (b BlobGC) EffectiveGrace() time.Duration {
	if b.Grace == nil {
		return defaultBlobGCGrace
	}
	return time.Duration(*b.Grace)
}

// validate rejects negative durations; absent and 0 are both valid.
func (b BlobGC) validate() error {
	if b.Interval != nil && time.Duration(*b.Interval) < 0 {
		return fmt.Errorf("config: blob_gc.interval must not be negative, got %s", time.Duration(*b.Interval))
	}
	if b.Grace != nil && time.Duration(*b.Grace) < 0 {
		return fmt.Errorf("config: blob_gc.grace must not be negative, got %s", time.Duration(*b.Grace))
	}
	return nil
}

// defaultRetentionInterval is the pruner interval when retention.interval is
// absent.
const defaultRetentionInterval = 5 * time.Minute

// minCommandsMaxAge is the lowest allowed commands.max_age. Commands must be
// kept longer than the longest command TTL, and a fixed floor is how that is
// enforced for now.
const minCommandsMaxAge = 7 * 24 * time.Hour

// defaultStreamMaxAge is the default max_age per stream. max_bytes has no
// default.
var defaultStreamMaxAge = map[string]time.Duration{
	"metrics":  336 * time.Hour,  // 14 days
	"entities": 8760 * time.Hour, // 365 days
	"commands": 2160 * time.Hour, // 90 days
	"audit":    8760 * time.Hour, // 365 days
	// Alarms change rarely and record what fired and who was told.
	"alarms": 8760 * time.Hour, // 365 days
	// An annotation is a record of what was observed, not a sample of it, so
	// rebuild_projection replays this stream, so a year is also how far back a
	// rebuild can restore annotations. The projected table itself is never pruned.
	"annotations": 8760 * time.Hour, // 365 days
	// Logs are high-volume and rarely needed after a fortnight; matching the
	// metrics window keeps both answerable for the same period.
	"logs": 336 * time.Hour, // 14 days
}

// knownStreams are the stream names the system produces. A retention key
// outside this set would never match, so it is a config error.
var knownStreams = map[string]bool{
	"metrics": true, "entities": true, "commands": true, "audit": true, "alarms": true,
	"annotations": true, "logs": true,
}

// EffectiveInterval returns the pruner interval: 5m when absent, otherwise the
// configured value. An explicit 0 means the pruner is off.
func (r Retention) EffectiveInterval() time.Duration {
	if r.Interval == nil {
		return defaultRetentionInterval
	}
	return time.Duration(*r.Interval)
}

// EffectiveStream returns stream's retention policy, with the default MaxAge
// applied when none is set.
func (r Retention) EffectiveStream(stream string) StreamRetention {
	s := r.Streams[stream]
	if !s.KeepForever && time.Duration(s.MaxAge) <= 0 {
		if d, ok := defaultStreamMaxAge[stream]; ok {
			s.MaxAge = Duration(d)
		}
	}
	return s
}

// TimeSync configures authoritative time: clock offsets learned over
// replication, the MQTT time beacon and the machines' hold window. It is on by
// default.
type TimeSync struct {
	// BeaconInterval is how often the node publishes _TimeSync while machines are
	// attached, 30s by default.
	BeaconInterval Duration `yaml:"beacon_interval"`
	// HoldMS is how long a machine holds expiry decisions after connecting before
	// it trusts its last known offset, 10000 by default. A pointer because an
	// explicit 0 means no hold.
	HoldMS *int64 `yaml:"hold_ms"`
	// DriftWarnMS is the offset beyond which colcad logs a warning, 5000 by
	// default. A pointer because an explicit 0 means warn on any offset.
	DriftWarnMS *int64 `yaml:"drift_warn_ms"`
}

const (
	defaultBeaconInterval = 30 * time.Second
	defaultHoldMS         = int64(10000)
	defaultDriftWarnMS    = int64(5000)
)

// EffectiveBeaconInterval returns BeaconInterval, or 30s when unset.
func (t TimeSync) EffectiveBeaconInterval() time.Duration {
	if time.Duration(t.BeaconInterval) <= 0 {
		return defaultBeaconInterval
	}
	return time.Duration(t.BeaconInterval)
}

// EffectiveHoldMS returns HoldMS, or 10000 when absent. An explicit 0 means no
// hold.
func (t TimeSync) EffectiveHoldMS() int64 {
	if t.HoldMS == nil {
		return defaultHoldMS
	}
	return *t.HoldMS
}

// EffectiveDriftWarnMS returns DriftWarnMS, or 5000 when absent. An explicit 0
// means warn on any non-zero offset.
func (t TimeSync) EffectiveDriftWarnMS() int64 {
	if t.DriftWarnMS == nil {
		return defaultDriftWarnMS
	}
	return *t.DriftWarnMS
}

// validate rejects negative values; absent and 0 are both valid.
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
	raw, err := os.ReadFile(path) //nolint:gosec // the config file the operator named
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

// NodeName is how this node names itself: its name, or its ULID when it has
// none.
func (c *Config) NodeName() string {
	if c.Name != "" {
		return c.Name
	}
	return c.ULID
}

// EffectiveTopicRoot is the topic root this node runs with: COLCA_TOPIC_ROOT
// when set, else topic_root, else the default.
func (c *Config) EffectiveTopicRoot() string {
	if r := os.Getenv(uns.RootEnv); r != "" {
		return r
	}
	if c.TopicRoot != "" {
		return c.TopicRoot
	}
	return uns.DefaultRoot
}

// Validate checks the required fields and every block.
func (c *Config) Validate() error {
	if c.Standalone && c.Parent != nil {
		return fmt.Errorf("config: standalone cannot have a parent")
	}
	if c.ULID == "" || c.DataDir == "" || c.KeyFile == "" {
		return fmt.Errorf("config: ulid, data_dir, key_file are required")
	}
	if err := uns.ValidRoot(c.EffectiveTopicRoot()); err != nil {
		return fmt.Errorf("config: %w", err)
	}
	if c.SecretsDir != "" && filepath.Clean(c.SecretsDir) == filepath.Clean(c.DataDir) {
		return fmt.Errorf("config: secrets_dir must be separate from data_dir")
	}
	if (c.MQTTHuman.TCPAddr != "" || c.MQTTHuman.WSAddr != "") && c.Auth == nil {
		// A token door without a verifier authenticates nobody.
		return fmt.Errorf("config: mqtt_human requires an auth: block — a token door with no verifier authenticates nobody")
	}
	if c.Auth != nil {
		if err := c.Auth.validate(); err != nil {
			return err
		}
	}
	if err := c.Retention.validate(); err != nil {
		return err
	}
	if err := c.Limits.validate(); err != nil {
		return err
	}
	if err := c.MQTTLimits.validate(); err != nil {
		return err
	}
	if err := c.BlobGC.validate(); err != nil {
		return err
	}
	return c.TimeSync.validate()
}

// validate checks the auth: block: an audience, at least one issuer, each named
// once and each with a JWKS URL of its own or the shared one.
func (a *Auth) validate() error {
	if a.Audience == "" {
		return fmt.Errorf("config: auth needs an audience")
	}
	if len(a.Issuers) == 0 {
		return fmt.Errorf("config: auth needs at least one entry in issuers")
	}
	seen := make(map[string]bool, len(a.Issuers))
	for i, is := range a.EffectiveIssuers() {
		if is.URL == "" {
			return fmt.Errorf("config: auth.issuers[%d] needs a url", i)
		}
		if seen[is.URL] {
			return fmt.Errorf("config: auth.issuers lists %q twice", is.URL)
		}
		seen[is.URL] = true
		if is.JWKSURL == "" {
			return fmt.Errorf("config: auth.issuers[%d] (%s) has no jwks_url and auth.jwks_url is empty", i, is.URL)
		}
	}
	if a.JWKSRefresh < 0 {
		return fmt.Errorf("config: auth.jwks_refresh must not be negative")
	}
	return nil
}

// validate checks the retention: block: non-negative durations, known streams
// and the commands.max_age floor.
func (r Retention) validate() error {
	if r.Interval != nil && time.Duration(*r.Interval) < 0 {
		return fmt.Errorf("config: retention.interval must not be negative, got %s", time.Duration(*r.Interval))
	}
	for name, s := range r.Streams {
		if !knownStreams[name] {
			return fmt.Errorf("config: retention.streams: unknown stream %q, want one of metrics, entities, commands, audit, alarms, annotations, logs", name)
		}
		if time.Duration(s.MaxAge) < 0 {
			return fmt.Errorf("config: retention.streams.%s.max_age must not be negative, got %s", name, time.Duration(s.MaxAge))
		}
		if s.KeepForever && time.Duration(s.MaxAge) > 0 {
			return fmt.Errorf(
				"config: retention.streams.%s cannot set both keep_forever and max_age", name,
			)
		}
		if time.Duration(s.IgnoreCursorsAfter) < 0 {
			return fmt.Errorf("config: retention.streams.%s.ignore_cursors_after must not be negative, got %s", name, time.Duration(s.IgnoreCursorsAfter))
		}
	}
	commands := r.EffectiveStream("commands")
	if time.Duration(commands.MaxAge) < minCommandsMaxAge {
		return fmt.Errorf("config: retention.streams.commands.max_age must be >= %s (the command TTL floor), got %s",
			minCommandsMaxAge, time.Duration(commands.MaxAge))
	}
	return nil
}
