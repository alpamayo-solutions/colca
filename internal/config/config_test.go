package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

const sample = `
ulid: n-edge1
data_dir: /tmp/colca-test
log_level: debug
key_file: /keys/edge1.key
api:
  addr: "127.0.0.1:8081"
  token: secret-admin
mqtt:
  addr: "127.0.0.1:1884"
repl:
  addr: "127.0.0.1:9444"
parent:
  url: https://127.0.0.1:9443
  pubkey: aabbcc
`

func TestLoad(t *testing.T) {
	p := filepath.Join(t.TempDir(), "c.yaml")
	if err := os.WriteFile(p, []byte(sample), 0o600); err != nil {
		t.Fatal(err)
	}
	c, err := Load(p)
	if err != nil {
		t.Fatal(err)
	}
	if c.ULID != "n-edge1" || c.Parent == nil || c.Parent.Pubkey != "aabbcc" {
		t.Fatalf("%+v", c)
	}
	if _, err := Load("/nonexistent.yaml"); err == nil {
		t.Fatal("want error")
	}

	// The remaining fields round-trip too.
	if c.DataDir != "/tmp/colca-test" || c.LogLevel != "debug" || c.KeyFile != "/keys/edge1.key" {
		t.Fatalf("scalars: %+v", c)
	}
	if c.API.Addr != "127.0.0.1:8081" || c.API.Token != "secret-admin" {
		t.Fatalf("api: %+v", c.API)
	}
	if c.MQTT.Addr != "127.0.0.1:1884" || c.Repl.Addr != "127.0.0.1:9444" {
		t.Fatalf("endpoints: mqtt=%+v repl=%+v", c.MQTT, c.Repl)
	}
	if c.Parent.URL != "https://127.0.0.1:9443" {
		t.Fatalf("parent url: %+v", c.Parent)
	}
}

// The auth block and the token MQTT listeners.
func TestAuthAndHumanMQTTConfig(t *testing.T) {
	doc := `
ulid: n-edge1
data_dir: /tmp/colca-test
key_file: /keys/edge1.key
auth:
  issuer: https://kc.example/realms/colca
  audience: colca
  jwks_url: https://kc.example/realms/colca/protocol/openid-connect/certs
  jwks_refresh: 30m
mqtt_human:
  tcp_addr: ":8884"
  ws_addr: ":8885"
`
	p := filepath.Join(t.TempDir(), "c.yaml")
	if err := os.WriteFile(p, []byte(doc), 0o600); err != nil {
		t.Fatal(err)
	}
	c, err := Load(p)
	if err != nil {
		t.Fatal(err)
	}
	if c.Auth == nil || c.Auth.Issuer != "https://kc.example/realms/colca" ||
		c.Auth.Audience != "colca" || time.Duration(c.Auth.JWKSRefresh) != 30*time.Minute {
		t.Fatalf("auth block: %+v", c.Auth)
	}
	if c.MQTTHuman.TCPAddr != ":8884" || c.MQTTHuman.WSAddr != ":8885" {
		t.Fatalf("mqtt_human block: %+v", c.MQTTHuman)
	}

	// refresh absent → 1h effective default
	base := &Config{ULID: "x", DataDir: "/tmp", KeyFile: "/k",
		Auth: &Auth{Issuer: "i", Audience: "a", JWKSURL: "j"}}
	if err := base.Validate(); err != nil {
		t.Fatalf("auth without mqtt_human must validate: %v", err)
	}
	if got := base.Auth.EffectiveRefresh(); got != time.Hour {
		t.Fatalf("EffectiveRefresh default: %v", got)
	}

	for name, c := range map[string]*Config{
		"mqtt_human without auth": {ULID: "x", DataDir: "/tmp", KeyFile: "/k",
			MQTTHuman: MQTTHuman{TCPAddr: ":8884"}},
		"ws without auth": {ULID: "x", DataDir: "/tmp", KeyFile: "/k",
			MQTTHuman: MQTTHuman{WSAddr: ":8885"}},
		"auth missing issuer": {ULID: "x", DataDir: "/tmp", KeyFile: "/k",
			Auth: &Auth{Audience: "a", JWKSURL: "j"}},
		"auth missing audience": {ULID: "x", DataDir: "/tmp", KeyFile: "/k",
			Auth: &Auth{Issuer: "i", JWKSURL: "j"}},
		"auth missing jwks_url": {ULID: "x", DataDir: "/tmp", KeyFile: "/k",
			Auth: &Auth{Issuer: "i", Audience: "a"}},
		"negative refresh": {ULID: "x", DataDir: "/tmp", KeyFile: "/k",
			Auth: &Auth{Issuer: "i", Audience: "a", JWKSURL: "j", JWKSRefresh: Duration(-time.Hour)}},
	} {
		if err := c.Validate(); err == nil {
			t.Errorf("%s: want validation error", name)
		}
	}
}

func TestValidateRequiredFields(t *testing.T) {
	base := func() *Config { return &Config{ULID: "x", DataDir: "/tmp", KeyFile: "/k"} }

	// A global node has no parent and no MQTT. Machines and children are not
	// config.
	if err := base().Validate(); err != nil {
		t.Fatalf("minimal config must be valid: %v", err)
	}

	for name, mutate := range map[string]func(*Config){
		"no ulid":     func(c *Config) { c.ULID = "" },
		"no data_dir": func(c *Config) { c.DataDir = "" },
		"no key_file": func(c *Config) { c.KeyFile = "" },
	} {
		c := base()
		mutate(c)
		if err := c.Validate(); err == nil {
			t.Errorf("%s: want error", name)
		}
	}
}

func TestSecretStoreMustHaveASeparateLifecycleDirectory(t *testing.T) {
	cfg := &Config{ULID: "n1", DataDir: "/var/lib/colca", SecretsDir: "/var/lib/colca", KeyFile: "/keys/node.key"}
	if err := cfg.Validate(); err == nil {
		t.Fatal("secrets_dir equal to data_dir was accepted")
	}
	cfg.SecretsDir = "/var/lib/colca-secrets"
	if err := cfg.Validate(); err != nil {
		t.Fatalf("separate secrets_dir rejected: %v", err)
	}
}

// retentionSample is a full retention block on top of the minimal config.
const retentionSample = `
ulid: n-edge1
data_dir: /tmp/colca-test
key_file: /keys/edge1.key
retention:
  interval: 5m
  streams:
    metrics:
      max_age: 336h
      max_bytes: 4GiB
      ignore_cursors_after: 0
    entities:
      max_age: 8760h
    commands:
      max_age: 2160h
`

func TestRetentionRoundTrip(t *testing.T) {
	p := filepath.Join(t.TempDir(), "c.yaml")
	if err := os.WriteFile(p, []byte(retentionSample), 0o600); err != nil {
		t.Fatal(err)
	}
	c, err := Load(p)
	if err != nil {
		t.Fatal(err)
	}

	if c.Retention.Interval == nil {
		t.Fatal("interval: explicitly set in YAML, must not be nil")
	}
	if got, want := time.Duration(*c.Retention.Interval), 5*time.Minute; got != want {
		t.Fatalf("interval: got %s want %s", got, want)
	}

	metrics, ok := c.Retention.Streams["metrics"]
	if !ok {
		t.Fatal("streams.metrics missing")
	}
	if got, want := time.Duration(metrics.MaxAge), 336*time.Hour; got != want {
		t.Fatalf("metrics.max_age: got %s want %s", got, want)
	}
	if got, want := uint64(metrics.MaxBytes), uint64(4)<<30; got != want {
		t.Fatalf("metrics.max_bytes: got %d want %d (4GiB)", got, want)
	}
	if got := time.Duration(metrics.IgnoreCursorsAfter); got != 0 {
		t.Fatalf("metrics.ignore_cursors_after: got %s want 0 (never)", got)
	}

	entities, ok := c.Retention.Streams["entities"]
	if !ok {
		t.Fatal("streams.entities missing")
	}
	if got, want := time.Duration(entities.MaxAge), 8760*time.Hour; got != want {
		t.Fatalf("entities.max_age: got %s want %s", got, want)
	}
	if got := uint64(entities.MaxBytes); got != 0 {
		t.Fatalf("entities.max_bytes: left unset in YAML, got %d want 0", got)
	}

	commands, ok := c.Retention.Streams["commands"]
	if !ok {
		t.Fatal("streams.commands missing")
	}
	if got, want := time.Duration(commands.MaxAge), 2160*time.Hour; got != want {
		t.Fatalf("commands.max_age: got %s want %s", got, want)
	}
}

// TestRetentionDefaultsWhenAbsent: without a retention block pruning is on,
// with the default max_age per stream, no max_bytes and no cursor override.
func TestRetentionDefaultsWhenAbsent(t *testing.T) {
	c := &Config{ULID: "x", DataDir: "/tmp", KeyFile: "/k"}
	if err := c.Validate(); err != nil {
		t.Fatalf("config with no retention: block must validate: %v", err)
	}

	if got, want := c.Retention.EffectiveInterval(), defaultRetentionInterval; got != want {
		t.Fatalf("interval default: got %s want %s", got, want)
	}
	cases := map[string]time.Duration{
		"metrics":     336 * time.Hour,
		"entities":    8760 * time.Hour,
		"commands":    2160 * time.Hour,
		"audit":       8760 * time.Hour,
		"alarms":      8760 * time.Hour,
		"annotations": 8760 * time.Hour,
		// Logs match the metrics window.
		"logs": 336 * time.Hour,
	}
	// Fail when defaultStreamMaxAge gains a stream this table does not pin.
	for stream := range defaultStreamMaxAge {
		if _, ok := cases[stream]; !ok {
			t.Fatalf("defaultStreamMaxAge has %q but this test does not pin its default; add it to cases", stream)
		}
	}
	for stream, wantAge := range cases {
		eff := c.Retention.EffectiveStream(stream)
		if got := time.Duration(eff.MaxAge); got != wantAge {
			t.Fatalf("%s default max_age: got %s want %s", stream, got, wantAge)
		}
		if got := uint64(eff.MaxBytes); got != 0 {
			t.Fatalf("%s default max_bytes: got %d want 0 (unset)", stream, got)
		}
		if got := time.Duration(eff.IgnoreCursorsAfter); got != 0 {
			t.Fatalf("%s default ignore_cursors_after: got %s want 0 (never)", stream, got)
		}
	}
}

func TestRetentionCanExplicitlyKeepAStreamForever(t *testing.T) {
	c, err := loadRetention(t, "  streams:\n    logs:\n      keep_forever: true\n")
	if err != nil {
		t.Fatalf("loading keep-forever policy: %v", err)
	}
	logs := c.Retention.EffectiveStream("logs")
	if !logs.KeepForever || time.Duration(logs.MaxAge) != 0 {
		t.Fatalf("logs policy = %#v, want keep_forever with no age bound", logs)
	}
}

func TestRetentionRejectsKeepForeverWithAnAgeBound(t *testing.T) {
	_, err := loadRetention(t, "  streams:\n    logs:\n      keep_forever: true\n      max_age: 24h\n")
	if err == nil || !strings.Contains(err.Error(), "both keep_forever and max_age") {
		t.Fatalf("error = %v, want conflicting-policy error", err)
	}
}

// A stream entry that sets only max_bytes still gets the default max_age.
func TestRetentionDefaultsApplyPerFieldNotOnlyWhenStreamEntryAbsent(t *testing.T) {
	c := &Config{ULID: "x", DataDir: "/tmp", KeyFile: "/k",
		Retention: Retention{Streams: map[string]StreamRetention{
			"metrics": {MaxBytes: ByteSize(2 << 30)},
		}},
	}
	if err := c.Validate(); err != nil {
		t.Fatalf("must validate: %v", err)
	}
	eff := c.Retention.EffectiveStream("metrics")
	if got, want := time.Duration(eff.MaxAge), 336*time.Hour; got != want {
		t.Fatalf("max_age should still default: got %s want %s", got, want)
	}
	if got, want := uint64(eff.MaxBytes), uint64(2)<<30; got != want {
		t.Fatalf("explicit max_bytes must survive: got %d want %d", got, want)
	}
}

// durPtr returns a *Duration, as YAML produces for a written key.
func durPtr(d time.Duration) *Duration {
	v := Duration(d)
	return &v
}

func TestRetentionValidationRejectsNegativeDurations(t *testing.T) {
	for name, r := range map[string]Retention{
		"negative interval": {Interval: durPtr(-time.Minute)},
		"negative max_age": {Streams: map[string]StreamRetention{
			"metrics": {MaxAge: Duration(-time.Hour)},
		}},
		"negative ignore_cursors_after": {Streams: map[string]StreamRetention{
			"metrics": {IgnoreCursorsAfter: Duration(-time.Hour)},
		}},
	} {
		c := &Config{ULID: "x", DataDir: "/tmp", KeyFile: "/k", Retention: r}
		if err := c.Validate(); err == nil {
			t.Errorf("%s: want validation error", name)
		} else if !strings.Contains(err.Error(), "config:") {
			t.Errorf("%s: error must use the config: prefix, got %q", name, err)
		}
	}
}

// loadRetention loads a minimal config with the given retention body, or none
// when body is empty. Only Load can show a key that was never written.
func loadRetention(t *testing.T, body string) (*Config, error) {
	t.Helper()
	doc := "ulid: n-edge1\ndata_dir: /tmp/colca-test\nkey_file: /keys/edge1.key\n"
	if body != "" {
		doc += "retention:\n" + body
	}
	p := filepath.Join(t.TempDir(), "c.yaml")
	if err := os.WriteFile(p, []byte(doc), 0o600); err != nil {
		t.Fatal(err)
	}
	return Load(p)
}

// retention.interval has three states through Load: absent means 5m, an
// explicit 0 disables the pruner, anything else is used as given.
func TestRetentionIntervalThreeStatesThroughLoad(t *testing.T) {
	t.Run("absent defaults to 5m", func(t *testing.T) {
		c, err := loadRetention(t, "  streams:\n    metrics:\n      max_age: 336h\n")
		if err != nil {
			t.Fatal(err)
		}
		if c.Retention.Interval != nil {
			t.Fatalf("interval must be nil (absent), got %v", *c.Retention.Interval)
		}
		if got, want := c.Retention.EffectiveInterval(), 5*time.Minute; got != want {
			t.Fatalf("got %s want %s", got, want)
		}
	})

	t.Run("no retention block at all defaults to 5m", func(t *testing.T) {
		c, err := loadRetention(t, "")
		if err != nil {
			t.Fatal(err)
		}
		if got, want := c.Retention.EffectiveInterval(), 5*time.Minute; got != want {
			t.Fatalf("got %s want %s", got, want)
		}
	})

	t.Run("explicit 0 disables the pruner", func(t *testing.T) {
		c, err := loadRetention(t, "  interval: 0\n")
		if err != nil {
			t.Fatal(err)
		}
		if c.Retention.Interval == nil {
			t.Fatal("interval must be non-nil — the key was explicitly written")
		}
		if got := c.Retention.EffectiveInterval(); got != 0 {
			t.Fatalf("explicit interval:0 must disable the pruner (EffectiveInterval()==0), got %s", got)
		}
	})

	t.Run("explicit 2m", func(t *testing.T) {
		c, err := loadRetention(t, "  interval: 2m\n")
		if err != nil {
			t.Fatal(err)
		}
		if got, want := c.Retention.EffectiveInterval(), 2*time.Minute; got != want {
			t.Fatalf("got %s want %s", got, want)
		}
	})
}

// A negative interval parses as a duration but is still rejected by Load.
func TestRetentionIntervalNegativeThroughLoad(t *testing.T) {
	_, err := loadRetention(t, "  interval: -5m\n")
	if err == nil {
		t.Fatal("negative interval must be rejected by Load")
	}
	if !strings.Contains(err.Error(), "retention.interval") {
		t.Fatalf("error must name retention.interval: %q", err)
	}
}

func TestRetentionValidationRejectsUnknownStream(t *testing.T) {
	c := &Config{ULID: "x", DataDir: "/tmp", KeyFile: "/k",
		Retention: Retention{Streams: map[string]StreamRetention{
			"bogus": {MaxAge: Duration(30 * 24 * time.Hour)},
		}},
	}
	err := c.Validate()
	if err == nil {
		t.Fatal("unknown stream name must be a config error")
	}
	if !strings.Contains(err.Error(), "bogus") {
		t.Fatalf("error must name the offending stream: %q", err)
	}
}

// commands.max_age below the floor is rejected, so a valid command's audit
// trail cannot age out under it.
func TestRetentionValidationRejectsCommandsBelowFloor(t *testing.T) {
	c := &Config{ULID: "x", DataDir: "/tmp", KeyFile: "/k",
		Retention: Retention{Streams: map[string]StreamRetention{
			"commands": {MaxAge: Duration(24 * time.Hour)}, // 1 day < 7-day floor
		}},
	}
	err := c.Validate()
	if err == nil {
		t.Fatal("commands.max_age below the TTL floor must be a config error")
	}
	for _, want := range []string{"commands", "max_age"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q must name %q", err, want)
		}
	}

	// At the floor and above passes, even below the stream's default.
	for _, age := range []time.Duration{minCommandsMaxAge, 30 * 24 * time.Hour} {
		ok := &Config{ULID: "x", DataDir: "/tmp", KeyFile: "/k",
			Retention: Retention{Streams: map[string]StreamRetention{
				"commands": {MaxAge: Duration(age)},
			}},
		}
		if err := ok.Validate(); err != nil {
			t.Errorf("commands.max_age=%s must validate: %v", age, err)
		}
	}
}

func TestRetentionValidationRejectsGarbageDurationsAndByteSizes(t *testing.T) {
	base := `
ulid: n-edge1
data_dir: /tmp/colca-test
key_file: /keys/edge1.key
retention:
`
	cases := map[string]string{
		"non-Go duration unit (14d)": base + "  streams:\n    metrics:\n      max_age: 14d\n",
		"bare nonzero integer":       base + "  interval: 5\n",
		"garbage byte size":          base + "  streams:\n    metrics:\n      max_bytes: 5XB\n",
		"not a duration at all":      base + "  streams:\n    metrics:\n      max_age: banana\n",
	}
	for name, yamlDoc := range cases {
		dir := t.TempDir()
		p := filepath.Join(dir, "c.yaml")
		if err := os.WriteFile(p, []byte(yamlDoc), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := Load(p); err == nil {
			t.Errorf("%s: want load error", name)
		}
	}
}

func TestLoadRejectsInvalidYAMLAndInvalidConfig(t *testing.T) {
	dir := t.TempDir()

	bad := filepath.Join(dir, "bad.yaml")
	if err := os.WriteFile(bad, []byte("ulid: [unclosed\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(bad); err == nil {
		t.Fatal("malformed yaml must error")
	}

	// Load must run Validate, not just unmarshal.
	incomplete := filepath.Join(dir, "incomplete.yaml")
	if err := os.WriteFile(incomplete, []byte("ulid: only-this\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(incomplete); err == nil {
		t.Fatal("config missing data_dir/key_file must error")
	}
}

// TestTimeSyncDefaultsWhenAbsent: without a time_sync block every value has its
// default.
func TestTimeSyncDefaultsWhenAbsent(t *testing.T) {
	c := &Config{ULID: "x", DataDir: "/tmp", KeyFile: "/k"}
	if err := c.Validate(); err != nil {
		t.Fatalf("config with no time_sync: block must validate: %v", err)
	}
	if got, want := c.TimeSync.EffectiveBeaconInterval(), 30*time.Second; got != want {
		t.Fatalf("beacon_interval default: got %s want %s", got, want)
	}
	if got, want := c.TimeSync.EffectiveHoldMS(), int64(10000); got != want {
		t.Fatalf("hold_ms default: got %d want %d", got, want)
	}
	if got, want := c.TimeSync.EffectiveDriftWarnMS(), int64(5000); got != want {
		t.Fatalf("drift_warn_ms default: got %d want %d", got, want)
	}
}

// TestTimeSyncRoundTrip: explicit values override every default.
func TestTimeSyncRoundTrip(t *testing.T) {
	yamlDoc := `
ulid: n-edge1
data_dir: /tmp/colca-test
key_file: /keys/edge1.key
time_sync:
  beacon_interval: 45s
  hold_ms: 20000
  drift_warn_ms: 2500
`
	p := filepath.Join(t.TempDir(), "c.yaml")
	if err := os.WriteFile(p, []byte(yamlDoc), 0o600); err != nil {
		t.Fatal(err)
	}
	c, err := Load(p)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := c.TimeSync.EffectiveBeaconInterval(), 45*time.Second; got != want {
		t.Fatalf("beacon_interval: got %s want %s", got, want)
	}
	if got, want := c.TimeSync.EffectiveHoldMS(), int64(20000); got != want {
		t.Fatalf("hold_ms: got %d want %d", got, want)
	}
	if got, want := c.TimeSync.EffectiveDriftWarnMS(), int64(2500); got != want {
		t.Fatalf("drift_warn_ms: got %d want %d", got, want)
	}
}

// msPtr returns a *int64, as YAML produces for a written key.
func msPtr(v int64) *int64 { return &v }

// Negative values are rejected for all three time_sync fields.
func TestTimeSyncValidationRejectsNegativeValues(t *testing.T) {
	for name, ts := range map[string]TimeSync{
		"negative beacon_interval": {BeaconInterval: Duration(-time.Second)},
		"negative hold_ms":         {HoldMS: msPtr(-1)},
		"negative drift_warn_ms":   {DriftWarnMS: msPtr(-1)},
	} {
		c := &Config{ULID: "x", DataDir: "/tmp", KeyFile: "/k", TimeSync: ts}
		if err := c.Validate(); err == nil {
			t.Fatalf("%s: want validation error", name)
		}
	}
}

// For HoldMS and DriftWarnMS, absent (nil) means the default while an explicit
// 0 is a real setting.
func TestTimeSyncHoldMSAbsentVsExplicitZeroDiverge(t *testing.T) {
	absent := TimeSync{}
	if got, want := absent.EffectiveHoldMS(), int64(10000); got != want {
		t.Fatalf("absent hold_ms: got %d want default %d", got, want)
	}
	explicitZero := TimeSync{HoldMS: msPtr(0)}
	if got, want := explicitZero.EffectiveHoldMS(), int64(0); got != want {
		t.Fatalf("explicit hold_ms: 0 must mean \"no hold\" (0), not silently apply the default: got %d want %d", got, want)
	}
}

func TestTimeSyncDriftWarnMSAbsentVsExplicitZeroDiverge(t *testing.T) {
	absent := TimeSync{}
	if got, want := absent.EffectiveDriftWarnMS(), int64(5000); got != want {
		t.Fatalf("absent drift_warn_ms: got %d want default %d", got, want)
	}
	explicitZero := TimeSync{DriftWarnMS: msPtr(0)}
	if got, want := explicitZero.EffectiveDriftWarnMS(), int64(0); got != want {
		t.Fatalf("explicit drift_warn_ms: 0 must mean \"warn on any nonzero offset\" (0), not silently apply the default: got %d want %d", got, want)
	}
}

// loadTimeSync loads a minimal config with the given time_sync body.
func loadTimeSync(t *testing.T, body string) (*Config, error) {
	t.Helper()
	doc := "ulid: n-edge1\ndata_dir: /tmp/colca-test\nkey_file: /keys/edge1.key\n"
	if body != "" {
		doc += "time_sync:\n" + body
	}
	p := filepath.Join(t.TempDir(), "c.yaml")
	if err := os.WriteFile(p, []byte(doc), 0o600); err != nil {
		t.Fatal(err)
	}
	return Load(p)
}

// The same absent-versus-zero distinction, through the real Load path.
func TestTimeSyncHoldMSAndDriftWarnMSThreeStatesThroughLoad(t *testing.T) {
	t.Run("hold_ms absent defaults to 10000", func(t *testing.T) {
		c, err := loadTimeSync(t, "  beacon_interval: 30s\n")
		if err != nil {
			t.Fatal(err)
		}
		if c.TimeSync.HoldMS != nil {
			t.Fatalf("hold_ms must be nil (absent), got %v", *c.TimeSync.HoldMS)
		}
		if got, want := c.TimeSync.EffectiveHoldMS(), int64(10000); got != want {
			t.Fatalf("got %d want %d", got, want)
		}
	})

	t.Run("no time_sync block at all defaults hold_ms to 10000", func(t *testing.T) {
		c, err := loadTimeSync(t, "")
		if err != nil {
			t.Fatal(err)
		}
		if got, want := c.TimeSync.EffectiveHoldMS(), int64(10000); got != want {
			t.Fatalf("got %d want %d", got, want)
		}
	})

	t.Run("explicit hold_ms: 0 means no hold, not the default", func(t *testing.T) {
		c, err := loadTimeSync(t, "  hold_ms: 0\n")
		if err != nil {
			t.Fatal(err)
		}
		if c.TimeSync.HoldMS == nil {
			t.Fatal("hold_ms must be non-nil — the key was explicitly written")
		}
		if got := c.TimeSync.EffectiveHoldMS(); got != 0 {
			t.Fatalf("explicit hold_ms:0 must mean no hold (EffectiveHoldMS()==0), got %d", got)
		}
	})

	t.Run("explicit drift_warn_ms: 0 means warn on any offset, not the default", func(t *testing.T) {
		c, err := loadTimeSync(t, "  drift_warn_ms: 0\n")
		if err != nil {
			t.Fatal(err)
		}
		if c.TimeSync.DriftWarnMS == nil {
			t.Fatal("drift_warn_ms must be non-nil — the key was explicitly written")
		}
		if got := c.TimeSync.EffectiveDriftWarnMS(); got != 0 {
			t.Fatalf("explicit drift_warn_ms:0 must mean warn on any offset (EffectiveDriftWarnMS()==0), got %d", got)
		}
	})

	t.Run("explicit hold_ms: 20000", func(t *testing.T) {
		c, err := loadTimeSync(t, "  hold_ms: 20000\n")
		if err != nil {
			t.Fatal(err)
		}
		if got, want := c.TimeSync.EffectiveHoldMS(), int64(20000); got != want {
			t.Fatalf("got %d want %d", got, want)
		}
	})
}

func loadDoc(t *testing.T, doc string) *Config {
	t.Helper()
	p := filepath.Join(t.TempDir(), "node.yaml")
	if err := os.WriteFile(p, []byte(doc), 0o600); err != nil {
		t.Fatal(err)
	}
	c, err := Load(p)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestTLSBlockIsOptionalAndParsed(t *testing.T) {
	c := loadDoc(t, `
ulid: n-a
data_dir: /data
key_file: /keys/n.key
tls:
  cert_file: /certs/node.crt
  key_file: /certs/node.key
`)
	if c.TLS.CertFile != "/certs/node.crt" || c.TLS.KeyFile != "/certs/node.key" {
		t.Fatalf("tls parsed as %+v", c.TLS)
	}

	bare := loadDoc(t, `
ulid: n-a
data_dir: /data
key_file: /keys/n.key
`)
	if bare.TLS.CertFile != "" || bare.TLS.KeyFile != "" {
		t.Fatalf("a config with no tls block got %+v", bare.TLS)
	}
}

// Alarm history is kept for a year, like entities and audit.
func TestAlarmsRetentionDefaultsTo365Days(t *testing.T) {
	var r Retention
	got := time.Duration(r.EffectiveStream("alarms").MaxAge)
	if want := 8760 * time.Hour; got != want {
		t.Fatalf("alarms default max_age = %s, want %s", got, want)
	}
}

func TestAlarmsIsAConfigurableStream(t *testing.T) {
	r := Retention{Streams: map[string]StreamRetention{
		"alarms": {MaxAge: Duration(time.Hour)},
	}}
	if err := r.validate(); err != nil {
		t.Fatalf("retention.streams.alarms rejected: %v", err)
	}
}

func TestLimitsDefaultWhenAbsent(t *testing.T) {
	var l Limits
	if got := l.EffectiveMaxRecordBytes(); got != 4<<20 {
		t.Fatalf("record default = %d, want %d", got, 4<<20)
	}
	if got := l.EffectiveMaxBlobBytes(); got != 32<<20 {
		t.Fatalf("blob default = %d, want %d", got, 32<<20)
	}
}

func TestMQTTLimitsDefaultsAreGenerousAndFinite(t *testing.T) {
	var l MQTTLimits
	if got, want := l.EffectiveMaxClients(), int64(4096); got != want {
		t.Fatalf("max clients = %d, want %d", got, want)
	}
	if got, want := l.EffectiveMaxSubscriptionsPerClient(), 1024; got != want {
		t.Fatalf("subscriptions/client = %d, want %d", got, want)
	}
	if got, want := l.EffectiveReceiveMaximum(), uint16(1024); got != want {
		t.Fatalf("receive maximum = %d, want %d", got, want)
	}
	if got, want := l.EffectiveMaximumInflight(), uint16(65535); got != want {
		t.Fatalf("maximum inflight = %d, want %d", got, want)
	}
	if got, want := l.EffectiveMaxPendingWritesPerClient(), int32(8192); got != want {
		t.Fatalf("pending writes/client = %d, want %d", got, want)
	}
	if got, want := l.EffectiveMaxTopicAliasesPerClient(), uint16(256); got != want {
		t.Fatalf("topic aliases/client = %d, want %d", got, want)
	}
	if got, want := l.EffectiveMaxSessionExpiry(), 7*24*time.Hour; got != want {
		t.Fatalf("session expiry = %s, want %s", got, want)
	}
}

func TestMQTTLimitsParseOverrides(t *testing.T) {
	var c Config
	if err := yaml.Unmarshal([]byte(`mqtt_limits:
  max_clients: 12
  max_subscriptions_per_client: 34
  receive_maximum: 56
  maximum_inflight: 78
  max_pending_writes_per_client: 90
  max_topic_aliases_per_client: 123
  max_session_expiry: 48h
`), &c); err != nil {
		t.Fatal(err)
	}
	l := c.MQTTLimits
	if l.EffectiveMaxClients() != 12 || l.EffectiveMaxSubscriptionsPerClient() != 34 ||
		l.EffectiveReceiveMaximum() != 56 || l.EffectiveMaximumInflight() != 78 ||
		l.EffectiveMaxPendingWritesPerClient() != 90 || l.EffectiveMaxTopicAliasesPerClient() != 123 ||
		l.EffectiveMaxSessionExpiry() != 48*time.Hour {
		t.Fatalf("parsed MQTT limits = %+v", l)
	}
}

func TestMQTTLimitsRejectInvalidSignedValues(t *testing.T) {
	base := Config{ULID: "n1", DataDir: "/tmp/x", KeyFile: "/tmp/x.key"}
	cases := []MQTTLimits{
		{MaxClients: -1},
		{MaxSubscriptionsPerClient: -1},
		{MaxPendingWritesPerClient: -1},
		{MaxSessionExpiry: Duration(-time.Second)},
		{MaxSessionExpiry: Duration(time.Millisecond)},
	}
	for _, limits := range cases {
		cfg := base
		cfg.MQTTLimits = limits
		if err := cfg.Validate(); err == nil {
			t.Fatalf("invalid MQTT limits accepted: %+v", limits)
		}
	}
}

func TestLimitsParseHumanSizes(t *testing.T) {
	var c Config
	if err := yaml.Unmarshal([]byte("limits:\n  max_record_bytes: 8MiB\n  max_blob_bytes: 1GiB\n"), &c); err != nil {
		t.Fatal(err)
	}
	if got := c.Limits.EffectiveMaxRecordBytes(); got != 8<<20 {
		t.Fatalf("record = %d, want %d", got, 8<<20)
	}
	if got := c.Limits.EffectiveMaxBlobBytes(); got != 1<<30 {
		t.Fatalf("blob = %d, want %d", got, 1<<30)
	}
}

func TestLimitsRejectABlobCapBelowTheRecordCap(t *testing.T) {
	c := Config{ULID: "n1", DataDir: "/tmp/x", KeyFile: "/tmp/x.key"}
	c.Limits = Limits{MaxRecordBytes: ByteSize(8 << 20), MaxBlobBytes: ByteSize(1 << 20)}
	if err := c.Validate(); err == nil {
		t.Fatal("want an error when the blob cap is below the record cap")
	}
}

// mqttsrv.New adds 64KiB to max_record_bytes and narrows it to a uint32, so a
// record cap above maxMaxRecordBytes must fail validation.
func TestLimitsRejectsARecordCapAboveTheUint32SafetyBound(t *testing.T) {
	// At the bound it is accepted, with a blob cap large enough not to trip the
	// inversion check.
	at := Config{ULID: "n1", DataDir: "/tmp/x", KeyFile: "/tmp/x.key"}
	at.Limits = Limits{MaxRecordBytes: ByteSize(maxMaxRecordBytes), MaxBlobBytes: ByteSize(maxMaxRecordBytes)}
	if err := at.Validate(); err != nil {
		t.Fatalf("record cap at the bound (%d) rejected: %v", maxMaxRecordBytes, err)
	}

	over := Config{ULID: "n1", DataDir: "/tmp/x", KeyFile: "/tmp/x.key"}
	over.Limits = Limits{MaxRecordBytes: ByteSize(maxMaxRecordBytes + 1), MaxBlobBytes: ByteSize(maxMaxRecordBytes + 1)}
	if err := over.Validate(); err == nil {
		t.Fatal("want an error when max_record_bytes exceeds the uint32 safety bound")
	}
}

func TestBlobGCDefaultsWhenAbsent(t *testing.T) {
	var b BlobGC
	if got, want := b.EffectiveInterval(), 15*time.Minute; got != want {
		t.Fatalf("interval default: got %s want %s", got, want)
	}
	if got, want := b.EffectiveGrace(), time.Hour; got != want {
		t.Fatalf("grace default: got %s want %s", got, want)
	}
}

// blob_gc.interval: absent means the default, an explicit 0 disables the
// sweeper.
func TestBlobGCIntervalThreeStatesThroughLoad(t *testing.T) {
	t.Run("absent defaults to 15m", func(t *testing.T) {
		c := loadDoc(t, "ulid: n1\ndata_dir: /tmp/x\nkey_file: /tmp/x.key\n")
		if c.BlobGC.Interval != nil {
			t.Fatalf("interval must be nil (absent), got %v", *c.BlobGC.Interval)
		}
		if got, want := c.BlobGC.EffectiveInterval(), 15*time.Minute; got != want {
			t.Fatalf("got %s want %s", got, want)
		}
	})

	t.Run("explicit interval: 0 disables the sweeper, not the default", func(t *testing.T) {
		c := loadDoc(t, "ulid: n1\ndata_dir: /tmp/x\nkey_file: /tmp/x.key\nblob_gc:\n  interval: 0\n")
		if c.BlobGC.Interval == nil {
			t.Fatal("interval must be non-nil — the key was explicitly written")
		}
		if got := c.BlobGC.EffectiveInterval(); got != 0 {
			t.Fatalf("explicit interval:0 must mean disabled (EffectiveInterval()==0), got %s", got)
		}
	})

	t.Run("explicit interval: 30m", func(t *testing.T) {
		c := loadDoc(t, "ulid: n1\ndata_dir: /tmp/x\nkey_file: /tmp/x.key\nblob_gc:\n  interval: 30m\n")
		if got, want := c.BlobGC.EffectiveInterval(), 30*time.Minute; got != want {
			t.Fatalf("got %s want %s", got, want)
		}
	})
}

// blob_gc.grace: absent means one hour, an explicit 0 means no grace at all.
func TestBlobGCGraceThreeStatesThroughLoad(t *testing.T) {
	t.Run("absent defaults to 1h", func(t *testing.T) {
		c := loadDoc(t, "ulid: n1\ndata_dir: /tmp/x\nkey_file: /tmp/x.key\n")
		if c.BlobGC.Grace != nil {
			t.Fatalf("grace must be nil (absent), got %v", *c.BlobGC.Grace)
		}
		if got, want := c.BlobGC.EffectiveGrace(), time.Hour; got != want {
			t.Fatalf("got %s want %s", got, want)
		}
	})

	t.Run("explicit grace: 0 means no grace, not the default", func(t *testing.T) {
		c := loadDoc(t, "ulid: n1\ndata_dir: /tmp/x\nkey_file: /tmp/x.key\nblob_gc:\n  grace: 0\n")
		if c.BlobGC.Grace == nil {
			t.Fatal("grace must be non-nil — the key was explicitly written")
		}
		if got := c.BlobGC.EffectiveGrace(); got != 0 {
			t.Fatalf("explicit grace:0 must mean no grace (EffectiveGrace()==0), got %s", got)
		}
	})

	t.Run("explicit grace: 10m", func(t *testing.T) {
		c := loadDoc(t, "ulid: n1\ndata_dir: /tmp/x\nkey_file: /tmp/x.key\nblob_gc:\n  grace: 10m\n")
		if got, want := c.BlobGC.EffectiveGrace(), 10*time.Minute; got != want {
			t.Fatalf("got %s want %s", got, want)
		}
	})
}

func TestBlobGCValidationRejectsNegativeDurations(t *testing.T) {
	for name, b := range map[string]BlobGC{
		"negative interval": {Interval: durPtr(-time.Minute)},
		"negative grace":    {Grace: durPtr(-time.Minute)},
	} {
		c := Config{ULID: "n1", DataDir: "/tmp/x", KeyFile: "/tmp/x.key", BlobGC: b}
		if err := c.Validate(); err == nil {
			t.Fatalf("%s: want an error", name)
		}
	}
}
