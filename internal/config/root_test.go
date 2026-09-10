package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestTopicRootComesFromTheEnvironmentThenTheFileThenTheDefault(t *testing.T) {
	t.Setenv("COLCA_TOPIC_ROOT", "")
	c := &Config{}
	if got := c.EffectiveTopicRoot(); got != "colca" {
		t.Errorf("no root configured: got %q, want colca", got)
	}

	c.TopicRoot = "acme"
	if got := c.EffectiveTopicRoot(); got != "acme" {
		t.Errorf("topic_root: acme: got %q, want acme", got)
	}

	t.Setenv("COLCA_TOPIC_ROOT", "plant")
	if got := c.EffectiveTopicRoot(); got != "plant" {
		t.Errorf("COLCA_TOPIC_ROOT=plant with topic_root: acme: got %q, want plant", got)
	}
}

func TestTopicRootIsReadFromTheConfigFile(t *testing.T) {
	t.Setenv("COLCA_TOPIC_ROOT", "")
	path := filepath.Join(t.TempDir(), "node.yaml")
	if err := os.WriteFile(path, []byte("ulid: n1\ndata_dir: /data\nkey_file: /keys/n1.key\ntopic_root: acme\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	c, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.EffectiveTopicRoot() != "acme" {
		t.Fatalf("topic root %q, want acme", c.EffectiveTopicRoot())
	}
}

func TestAnInvalidTopicRootFailsValidation(t *testing.T) {
	for name, set := range map[string]func(*Config){
		"in the file":        func(c *Config) { c.TopicRoot = "a/b" },
		"in the environment": func(*Config) { t.Setenv("COLCA_TOPIC_ROOT", "#") },
	} {
		t.Run(name, func(t *testing.T) {
			t.Setenv("COLCA_TOPIC_ROOT", "")
			c := &Config{ULID: "n1", DataDir: "/data", KeyFile: "/keys/n1.key"}
			set(c)
			err := c.Validate()
			if err == nil || !strings.Contains(err.Error(), "topic root") {
				t.Fatalf("Validate() = %v, want a topic root error", err)
			}
		})
	}
}
