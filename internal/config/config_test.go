package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
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
children: []
clients:
  - ulid: m1
    token: machine-secret
    mount: m1
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
	if c.Clients[0].Mount != "m1" {
		t.Fatal("client mount")
	}
	if _, err := Load("/nonexistent.yaml"); err == nil {
		t.Fatal("want error")
	}

	// Remaining fields of the sample must round-trip too — later tasks read
	// these directly off the struct (addresses, admin token, parent URL).
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
	if len(c.Children) != 0 {
		t.Fatalf("children: %+v", c.Children)
	}
	if len(c.Clients) != 1 || c.Clients[0].ULID != "m1" || c.Clients[0].Token != "machine-secret" {
		t.Fatalf("clients: %+v", c.Clients)
	}
}

func TestValidateRejectsDuplicateMounts(t *testing.T) {
	c := &Config{ULID: "x", DataDir: "/tmp", KeyFile: "/k",
		Children: []Child{{ULID: "a", Pubkey: "p1", Mount: "same"}, {ULID: "b", Pubkey: "p2", Mount: "same"}}}
	if err := c.Validate(); err == nil {
		t.Fatal("duplicate mounts must be a config error (static collision check)")
	}
}

// The mount namespace is shared between children and clients: a client may not
// claim a mount a child already owns, or it could steal that subtree.
func TestValidateRejectsChildClientMountCollision(t *testing.T) {
	c := &Config{ULID: "x", DataDir: "/tmp", KeyFile: "/k",
		Children: []Child{{ULID: "childA", Pubkey: "p1", Mount: "same"}},
		Clients:  []Client{{ULID: "clientB", Token: "t", Mount: "same"}}}
	err := c.Validate()
	if err == nil {
		t.Fatal("child/client mount collision must be a config error")
	}
	msg := err.Error()
	for _, want := range []string{"same", "childA", "clientB"} {
		if !strings.Contains(msg, want) {
			t.Fatalf("error %q must name %q", msg, want)
		}
	}
}

func TestValidateRequiredFields(t *testing.T) {
	base := func() *Config { return &Config{ULID: "x", DataDir: "/tmp", KeyFile: "/k"} }

	// A global node has no parent, no mqtt, no children and no clients.
	if err := base().Validate(); err != nil {
		t.Fatalf("minimal config must be valid: %v", err)
	}

	for name, mutate := range map[string]func(*Config){
		"no ulid":     func(c *Config) { c.ULID = "" },
		"no data_dir": func(c *Config) { c.DataDir = "" },
		"no key_file": func(c *Config) { c.KeyFile = "" },
		"child without pubkey": func(c *Config) {
			c.Children = []Child{{ULID: "a", Mount: "m"}}
		},
		"child without mount": func(c *Config) {
			c.Children = []Child{{ULID: "a", Pubkey: "p"}}
		},
		"child without ulid": func(c *Config) {
			c.Children = []Child{{Pubkey: "p", Mount: "m"}}
		},
		"client without token": func(c *Config) {
			c.Clients = []Client{{ULID: "a", Mount: "m"}}
		},
		"client without mount": func(c *Config) {
			c.Clients = []Client{{ULID: "a", Token: "t"}}
		},
		"client without ulid": func(c *Config) {
			c.Clients = []Client{{Token: "t", Mount: "m"}}
		},
	} {
		c := base()
		mutate(c)
		if err := c.Validate(); err == nil {
			t.Errorf("%s: want error", name)
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
