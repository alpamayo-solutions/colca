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

	"gopkg.in/yaml.v3"
)

type Child struct {
	ULID   string `yaml:"ulid"`
	Pubkey string `yaml:"pubkey"` // hex ed25519, pinned on connect
	Mount  string `yaml:"mount"`
}

type Client struct {
	ULID  string `yaml:"ulid"`
	Token string `yaml:"token"` // pre-shared secret (MVP stand-in for machine keys)
	Mount string `yaml:"mount"`
}

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
// required: a global node has no Parent and no MQTT listener, and a leaf edge
// node has clients but no children.
type Config struct {
	ULID     string   `yaml:"ulid"`
	DataDir  string   `yaml:"data_dir"`
	LogLevel string   `yaml:"log_level"`
	KeyFile  string   `yaml:"key_file"`
	API      API      `yaml:"api"`
	MQTT     Endpoint `yaml:"mqtt"`
	Repl     Endpoint `yaml:"repl"`
	Parent   *Parent  `yaml:"parent"`
	Children []Child  `yaml:"children"`
	Clients  []Client `yaml:"clients"`
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

// Validate checks required fields and enforces the shared mount namespace.
func (c *Config) Validate() error {
	if c.ULID == "" || c.DataDir == "" || c.KeyFile == "" {
		return fmt.Errorf("config: ulid, data_dir, key_file are required")
	}
	mounts := map[string]string{}
	for _, ch := range c.Children {
		if ch.ULID == "" || ch.Pubkey == "" || ch.Mount == "" {
			return fmt.Errorf("config: child needs ulid, pubkey, mount: %+v", ch)
		}
		if prev, dup := mounts[ch.Mount]; dup {
			return fmt.Errorf("config: mount collision %q between %s and %s", ch.Mount, prev, ch.ULID)
		}
		mounts[ch.Mount] = ch.ULID
	}
	for _, cl := range c.Clients {
		if cl.ULID == "" || cl.Token == "" || cl.Mount == "" {
			return fmt.Errorf("config: client needs ulid, token, mount: %+v", cl)
		}
		if prev, dup := mounts[cl.Mount]; dup {
			return fmt.Errorf("config: mount collision %q between %s and %s", cl.Mount, prev, cl.ULID)
		}
		mounts[cl.Mount] = cl.ULID
	}
	return nil
}
