// Package config loads and validates a Colca node's YAML configuration.
package config

import (
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unicode"

	"gopkg.in/yaml.v3"

	"github.com/alpamayo-solutions/colca/internal/identity"
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
	// Mount is where this node asks to be placed at its parent. It only fills
	// in the approval dialog: the person approving decides.
	Mount string `yaml:"mount"`
	// Logs decides which of this node's log records the uplink forwards. Every
	// record stays in the local logs stream either way.
	Logs ParentLogs `yaml:"logs"`
}

// LogLevels are the _Log level segments, lowest first. They match the Python
// contracts' LOG_LEVELS and the levels door/logpublisher writes.
var LogLevels = []string{"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}

// DefaultParentLogMinLevel is the lowest level forwarded to the parent when
// parent.logs.min_level is absent.
const DefaultParentLogMinLevel = "WARNING"

// LogLevelRank is the position of an upper-case level in LogLevels, and false
// for anything else.
func LogLevelRank(level string) (int, bool) {
	for i, l := range LogLevels {
		if l == level {
			return i, true
		}
	}
	return 0, false
}

// ParentLogs is the parent.logs block: the lowest level forwarded to the
// parent, with optional per-service overrides keyed by the service name (the
// topic segment before the level). Level names are case-insensitive.
type ParentLogs struct {
	MinLevel string            `yaml:"min_level"`
	Services map[string]string `yaml:"services"`
}

// EffectiveMinLevel is min_level in upper case, or the default when absent.
func (l ParentLogs) EffectiveMinLevel() string {
	if l.MinLevel == "" {
		return DefaultParentLogMinLevel
	}
	return strings.ToUpper(l.MinLevel)
}

// EffectiveServices is the per-service overrides with upper-case levels.
func (l ParentLogs) EffectiveServices() map[string]string {
	out := make(map[string]string, len(l.Services))
	for svc, level := range l.Services {
		out[svc] = strings.ToUpper(level)
	}
	return out
}

func (l ParentLogs) validate() error {
	if l.MinLevel != "" {
		if _, ok := LogLevelRank(l.EffectiveMinLevel()); !ok {
			return fmt.Errorf("config: parent.logs.min_level %q, want one of %s", l.MinLevel, strings.Join(LogLevels, ", "))
		}
	}
	for svc, level := range l.Services {
		if svc == "" || strings.Contains(svc, "/") {
			return fmt.Errorf("config: parent.logs.services: %q is not a service name (one topic segment)", svc)
		}
		if _, ok := LogLevelRank(strings.ToUpper(level)); !ok {
			return fmt.Errorf("config: parent.logs.services.%s %q, want one of %s", svc, level, strings.Join(LogLevels, ", "))
		}
	}
	return nil
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

// Access declares how people reach this node: a hostname and the URL of its
// UI. The node reports it in its own _Node record; nothing else owns it.
type Access struct {
	Hostname string `yaml:"hostname"`
	UIURL    string `yaml:"ui_url"`
}

func (a *Access) validate() error {
	if a.Hostname == "" || len(a.Hostname) > 253 || strings.Contains(a.Hostname, "/") || strings.IndexFunc(a.Hostname, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) >= 0 {
		return fmt.Errorf("config: access.hostname %q must be 1-253 characters with no \"/\" or whitespace", a.Hostname)
	}
	u, err := url.Parse(a.UIURL)
	if err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return fmt.Errorf("config: access.ui_url %q must be an http(s) URL with a host, no credentials, and no query or fragment", a.UIURL)
	}
	return nil
}

// Identity is the identity: block: where the node key lives. Only key_file is
// required, here or as the top-level key_file.
type Identity struct {
	// KeyStore is auto (default), tpm or file; see identity.Open.
	KeyStore string `yaml:"key_store"`
	// KeyFile holds the file key, or the TPM key blob when the key is in a TPM.
	KeyFile string `yaml:"key_file"`
	// TPMDevice is the TPM to use, /dev/tpmrm0 when empty.
	TPMDevice string `yaml:"tpm_device"`
}

// Enrollment levels for Enrollment.Require.
const (
	EnrollmentRequireAny         = "any"
	EnrollmentRequireTPM         = "tpm"
	EnrollmentRequireTPMAttested = "tpm-attested"
)

// Key change policies for Enrollment.KeyChange.
const (
	KeyChangeAuto    = "auto"
	KeyChangeApprove = "approve"
)

// Enrollment is the enrollment: block, this node's policy for the children
// that ask to join it.
type Enrollment struct {
	// Require is the lowest key store a request must prove before it can be
	// approved: any (default), tpm or tpm-attested.
	Require string `yaml:"require"`
	// KeyChange decides a key change an enrolled child asks for with its
	// current key: auto (default) accepts a move into a TPM at once, approve
	// leaves every key change to a person.
	KeyChange string `yaml:"key_change"`
	// RequireIssuedCert refuses children that still present a self-signed
	// certificate (cert_state none, from before issued certificates).
	RequireIssuedCert bool `yaml:"require_issued_cert"`
	// TPMRoots is a PEM file of additional TPM manufacturer certificates
	// (roots and intermediates) endorsement key certificates may chain to,
	// beside the ones colcad ships.
	TPMRoots string `yaml:"tpm_roots"`
}

// EffectiveRequire is Require with its default.
func (e Enrollment) EffectiveRequire() string {
	if e.Require == "" {
		return EnrollmentRequireAny
	}
	return e.Require
}

// EffectiveKeyChange is KeyChange with its default.
func (e Enrollment) EffectiveKeyChange() string {
	if e.KeyChange == "" {
		return KeyChangeAuto
	}
	return e.KeyChange
}

func (e Enrollment) validate() error {
	switch e.EffectiveRequire() {
	case EnrollmentRequireAny, EnrollmentRequireTPM, EnrollmentRequireTPMAttested:
	default:
		return fmt.Errorf("config: enrollment.require %q, want any, tpm or tpm-attested", e.Require)
	}
	switch e.EffectiveKeyChange() {
	case KeyChangeAuto, KeyChangeApprove:
	default:
		return fmt.Errorf("config: enrollment.key_change %q, want auto or approve", e.KeyChange)
	}
	return nil
}

// Config is a node's configuration. Only ULID, DataDir and the key file
// (identity.key_file or key_file) are required. Machines and child nodes are not configured here; they are
// enrolled at runtime through the admin API.
type Config struct {
	ULID string `yaml:"ulid"`
	// Name is what the node calls itself in its own _Node record: a deployment's
	// name, never its position. Empty means the ULID.
	Name string `yaml:"name"`
	// Access, when set, is how people reach this node. It is copied into the
	// node's _Node record and withdrawn from it when removed.
	Access *Access `yaml:"access"`
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
	SecretsDir string `yaml:"secrets_dir"`
	LogLevel   string `yaml:"log_level"`
	// KeyFile is the top-level spelling of identity.key_file, kept for configs
	// written before the identity block. Use EffectiveKeyFile.
	KeyFile  string   `yaml:"key_file"`
	Identity Identity `yaml:"identity"`
	TLS      TLS      `yaml:"tls"`
	API      API      `yaml:"api"`
	MQTT     Endpoint `yaml:"mqtt"`
	Repl     Endpoint `yaml:"repl"`
	Parent   *Parent  `yaml:"parent"`
	// Enrollment is the policy for children asking to join this node.
	Enrollment Enrollment `yaml:"enrollment"`
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

	// Bus decides what the local MQTT bus keeps as retained messages.
	Bus Bus `yaml:"bus"`

	// Retention configures the background pruner. Without the block the defaults
	// apply and pruning is on.
	Retention Retention `yaml:"retention"`

	// Storage tunes the stream store on disk.
	Storage Storage `yaml:"storage"`

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

	// Commands sets how the node answers commands no service executes.
	Commands Commands `yaml:"commands"`

	// Cursors configures the cursor watchdog: how long a record a consumer
	// reads may wait unread before the node writes a cursor_lag finding.
	Cursors Cursors `yaml:"cursors"`

	// Logs bounds the _Log records written on this node: repeats are collapsed
	// into one record with a count, and each service has a record budget per
	// window. Without the block the defaults apply.
	Logs Logs `yaml:"logs"`

	// Plugin holds settings for the domain plugin. The core never reads them,
	// which keeps domain vocabulary out of the broker's configuration.
	Plugin map[string]string `yaml:"plugin"`
}

// Commands is the commands: block.
//
// Without Strict, a command at an element where no service announced any
// command passes on to its executor, which may not announce (a machine, or a
// service built before announcements). With Strict, every command to this node
// that no service announced is answered 404, for a deployment whose executors
// all announce.
type Commands struct {
	Strict bool `yaml:"strict"`
}

// Cursors is the cursors: block.
type Cursors struct {
	// LagAlarmAfter is how old the oldest unread record a consumer reads may
	// get before the node writes a cursor_lag finding about its service. 60s
	// when absent; 0 keeps the colca_cursor_unread_age_seconds gauge and writes
	// no finding.
	LagAlarmAfter *Duration `yaml:"lag_alarm_after"`
	// StaleAfter is how long a cursor may stand still while records wait past
	// it before the node's stale_cursors finding names it, whether or not
	// anyone still reads it. 24h when absent; 0 writes no finding.
	StaleAfter *Duration `yaml:"stale_after"`
}

// defaultStaleAfter is cursors.stale_after when absent.
const defaultStaleAfter = 24 * time.Hour

// EffectiveStaleAfter returns the threshold: 24h when absent.
func (c Cursors) EffectiveStaleAfter() time.Duration {
	if c.StaleAfter == nil {
		return defaultStaleAfter
	}
	return time.Duration(*c.StaleAfter)
}

// defaultLagAlarmAfter is cursors.lag_alarm_after when absent.
const defaultLagAlarmAfter = time.Minute

// EffectiveLagAlarmAfter returns the threshold: 60s when absent.
func (c Cursors) EffectiveLagAlarmAfter() time.Duration {
	if c.LagAlarmAfter == nil {
		return defaultLagAlarmAfter
	}
	return time.Duration(*c.LagAlarmAfter)
}

func (c Cursors) validate() error {
	if c.LagAlarmAfter != nil && time.Duration(*c.LagAlarmAfter) < 0 {
		return fmt.Errorf("config: cursors.lag_alarm_after must not be negative, got %s", time.Duration(*c.LagAlarmAfter))
	}
	if c.StaleAfter != nil && time.Duration(*c.StaleAfter) < 0 {
		return fmt.Errorf("config: cursors.stale_after must not be negative, got %s", time.Duration(*c.StaleAfter))
	}
	return nil
}

// Logs is the logs: block. It gates the _Log records a node admits from its own
// services and from itself; records replicated from a child were gated there.
type Logs struct {
	// Window is the repeat-collapse and rate-cap window. 60s when absent; 0
	// turns both off.
	Window *Duration `yaml:"window"`
	// MaxPerService is how many records one service (one _Log position) may
	// write per window; collapse summaries do not count. 600 when absent; 0
	// means no cap.
	MaxPerService *int `yaml:"max_per_service"`
	// MaxTracked bounds the distinct repeat keys, and separately the services,
	// held in memory. A record whose key finds no room is stored without
	// collapsing. 4096 when absent.
	MaxTracked *int `yaml:"max_tracked"`
}

// Defaults and upper bounds of the logs: block.
const (
	defaultLogsWindow        = time.Minute
	defaultLogsMaxPerService = 600
	defaultLogsMaxTracked    = 4096
	maxLogsWindow            = 24 * time.Hour
	maxLogsMaxPerService     = 10_000_000
	maxLogsMaxTracked        = 65_536
)

// EffectiveWindow returns the window: 60s when absent, 0 when turned off.
func (l Logs) EffectiveWindow() time.Duration {
	if l.Window == nil {
		return defaultLogsWindow
	}
	return time.Duration(*l.Window)
}

// EffectiveMaxPerService returns the per-service cap: 600 when absent, 0 for
// no cap.
func (l Logs) EffectiveMaxPerService() int {
	if l.MaxPerService == nil {
		return defaultLogsMaxPerService
	}
	return *l.MaxPerService
}

// EffectiveMaxTracked returns the bound on tracked keys: 4096 when absent.
func (l Logs) EffectiveMaxTracked() int {
	if l.MaxTracked == nil {
		return defaultLogsMaxTracked
	}
	return *l.MaxTracked
}

func (l Logs) validate() error {
	if w := l.EffectiveWindow(); w < 0 || w > maxLogsWindow {
		return fmt.Errorf("config: logs.window must be between 0 and %s, got %s", maxLogsWindow, w)
	}
	if n := l.EffectiveMaxPerService(); n < 0 || n > maxLogsMaxPerService {
		return fmt.Errorf("config: logs.max_per_service must be between 0 and %d, got %d", maxLogsMaxPerService, n)
	}
	if n := l.EffectiveMaxTracked(); n < 1 || n > maxLogsMaxTracked {
		return fmt.Errorf("config: logs.max_tracked must be between 1 and %d, got %d", maxLogsMaxTracked, n)
	}
	return nil
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
	// Signals are per-signal windows inside the stream's own (metrics only).
	// The first rule that matches a signal sets its window; a signal no rule
	// matches keeps the stream's policy. A rule can only shorten: the stream
	// policy still cuts every signal at its own max_age.
	Signals []SignalRetention `yaml:"signals"`
}

// SignalRetention is one per-signal rule. A signal matches when its id is in
// SignalIDs or its _Metric topic matches one of Topics (MQTT filters, "+" for
// one level, "#" for the rest). The newest record older than MaxAge is kept,
// so the value in force when the window opens stays readable.
type SignalRetention struct {
	Topics    []string `yaml:"topics"`
	SignalIDs []string `yaml:"signal_ids"`
	MaxAge    Duration `yaml:"max_age"`
}

// matches reports whether the rule selects the signal with this id and topic.
func (r SignalRetention) matches(signalID, topic string) bool {
	for _, id := range r.SignalIDs {
		if id == signalID {
			return true
		}
	}
	for _, f := range r.Topics {
		if uns.MatchFilter(f, topic) {
			return true
		}
	}
	return false
}

// SignalMaxAge returns the window of the first rule that selects the signal;
// ok=false when none does.
func (s StreamRetention) SignalMaxAge(signalID, topic string) (time.Duration, bool) {
	for _, r := range s.Signals {
		if r.matches(signalID, topic) {
			return time.Duration(r.MaxAge), true
		}
	}
	return 0, false
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

// Storage is the storage: block.
type Storage struct {
	// Compression is the block compression of the stream store: "snappy" (the
	// default) or "zstd", smaller at more CPU per flush and compaction. It
	// applies to data written from then on; older files are rewritten in the
	// new format as compaction reaches them. Either setting reads both.
	Compression string `yaml:"compression"`
}

// knownCompressions are the storage.compression values.
var knownCompressions = map[string]bool{"": true, "snappy": true, "zstd": true}

// Limits is the limits: block. Zero values mean the defaults.
type Limits struct {
	MaxRecordBytes ByteSize `yaml:"max_record_bytes"`
	MaxBlobBytes   ByteSize `yaml:"max_blob_bytes"`
}

// Bus is the bus: block.
type Bus struct {
	// RetainChildMetrics keeps the _Metric records that children replicate up as
	// retained messages on this node's bus, as every state record is. Off by
	// default: a node with children receives every sample of its subtree, and
	// retaining each one cost a parent with 100 children and 140,000 signals at
	// 28 k records/s ~30 % of its CPU and ~450 MB (fleet scale benchmark, 2026-10). The records
	// are still published live, and /kv holds every current value. A node's own
	// _Metric records, and every other state contract, stay retained.
	RetainChildMetrics bool `yaml:"retain_child_metrics"`
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

// EffectiveKeyFile is identity.key_file, else the top-level key_file.
func (c *Config) EffectiveKeyFile() string {
	if c.Identity.KeyFile != "" {
		return c.Identity.KeyFile
	}
	return c.KeyFile
}

// IdentityOptions are the identity block as identity.Open takes them.
func (c *Config) IdentityOptions() identity.Options {
	return identity.Options{
		KeyStore:  c.Identity.KeyStore,
		KeyFile:   c.EffectiveKeyFile(),
		TPMDevice: c.Identity.TPMDevice,
	}
}

// Validate checks the required fields and every block.
func (c *Config) Validate() error {
	if c.Standalone && c.Parent != nil {
		return fmt.Errorf("config: standalone cannot have a parent")
	}
	if c.ULID == "" || c.DataDir == "" || c.EffectiveKeyFile() == "" {
		return fmt.Errorf("config: ulid, data_dir, key_file are required")
	}
	if c.KeyFile != "" && c.Identity.KeyFile != "" && c.KeyFile != c.Identity.KeyFile {
		return fmt.Errorf("config: key_file %q and identity.key_file %q differ; set one of them", c.KeyFile, c.Identity.KeyFile)
	}
	if !identity.ValidKeyStore(c.Identity.KeyStore) {
		return fmt.Errorf("config: identity.key_store %q, want auto, tpm or file", c.Identity.KeyStore)
	}
	if err := c.Enrollment.validate(); err != nil {
		return err
	}
	if err := uns.ValidRoot(c.EffectiveTopicRoot()); err != nil {
		return fmt.Errorf("config: %w", err)
	}
	if c.Access != nil {
		if err := c.Access.validate(); err != nil {
			return err
		}
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
	if c.Parent != nil {
		if err := c.Parent.Logs.validate(); err != nil {
			return err
		}
	}
	if !knownCompressions[c.Storage.Compression] {
		return fmt.Errorf("config: storage.compression %q, want snappy or zstd", c.Storage.Compression)
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
	if err := c.Cursors.validate(); err != nil {
		return err
	}
	if err := c.Logs.validate(); err != nil {
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

// validateSignalRules checks retention.streams.<name>.signals: metrics only,
// a selector and a positive max_age per rule, valid topic filters, and a window
// shorter than the stream's own, which would otherwise cut it anyway.
func validateSignalRules(name string, s, eff StreamRetention) error {
	if len(s.Signals) == 0 {
		return nil
	}
	if name != "metrics" {
		return fmt.Errorf("config: retention.streams.%s.signals: per-signal retention applies to the metrics stream only", name)
	}
	if s.KeepForever {
		return fmt.Errorf("config: retention.streams.%s cannot set both keep_forever and signals: keep_forever keeps every record, signals would remove some", name)
	}
	for i, rule := range s.Signals {
		at := fmt.Sprintf("config: retention.streams.%s.signals[%d]", name, i)
		if len(rule.Topics) == 0 && len(rule.SignalIDs) == 0 {
			return fmt.Errorf("%s: needs topics or signal_ids", at)
		}
		for _, f := range rule.Topics {
			if !uns.ValidFilter(f) {
				return fmt.Errorf("%s: invalid topic filter %q", at, f)
			}
		}
		for _, id := range rule.SignalIDs {
			if id == "" {
				return fmt.Errorf("%s: empty signal id", at)
			}
		}
		maxAge := time.Duration(rule.MaxAge)
		if maxAge <= 0 {
			return fmt.Errorf("%s: max_age must be positive, got %s", at, maxAge)
		}
		if streamAge := time.Duration(eff.MaxAge); streamAge > 0 && maxAge >= streamAge {
			return fmt.Errorf("%s: max_age %s must be shorter than the stream's max_age %s", at, maxAge, streamAge)
		}
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
		if err := validateSignalRules(name, s, r.EffectiveStream(name)); err != nil {
			return err
		}
	}
	commands := r.EffectiveStream("commands")
	if time.Duration(commands.MaxAge) < minCommandsMaxAge {
		return fmt.Errorf("config: retention.streams.commands.max_age must be >= %s (the command TTL floor), got %s",
			minCommandsMaxAge, time.Duration(commands.MaxAge))
	}
	return nil
}
