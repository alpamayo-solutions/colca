package main

import (
	"strings"
	"testing"
	"time"
)

func env(pairs map[string]string) func(string) string {
	return func(key string) string { return pairs[key] }
}

func complete() map[string]string {
	return map[string]string{
		"COLCA_URL":        "http://global:8080",
		"COLCA_SERVICE":    "grantsync",
		"KC_URL":           "http://keycloak:8080",
		"KC_CLIENT_SECRET": "secret",
		"OWNER":            "dev-hub",
	}
}

func TestConfigNamesEveryMissingVariableAtOnce(t *testing.T) {
	// A service that starts with half its configuration and dies on the first
	// cycle is harder to diagnose than one that refuses to start and says why —
	// and naming them one per restart is worse still.
	_, err := loadConfig(env(map[string]string{"COLCA_URL": "http://global:8080"}))
	if err == nil {
		t.Fatal("started with nothing configured")
	}
	for _, want := range []string{"KC_URL", "KC_CLIENT_SECRET", "OWNER"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error does not name %s: %v", want, err)
		}
	}
	if strings.Contains(err.Error(), "COLCA_URL") {
		t.Errorf("error names a variable that WAS set: %v", err)
	}
}

func TestConfigDefaults(t *testing.T) {
	vars := complete()
	delete(vars, "COLCA_URL")
	delete(vars, "COLCA_SERVICE")
	cfg, err := loadConfig(env(vars))
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if cfg.kcRealm != "colca" {
		t.Errorf("realm default is %q", cfg.kcRealm)
	}
	if cfg.kcAuthzClient != "colca-authz" || cfg.kcClientID != "colca-authz" {
		t.Errorf("client defaults are %q / %q", cfg.kcClientID, cfg.kcAuthzClient)
	}
	if cfg.interval != 30*time.Second {
		t.Errorf("interval default is %v", cfg.interval)
	}
	if cfg.httpAddr != ":9090" {
		t.Errorf("http addr default is %q", cfg.httpAddr)
	}
	if cfg.colcaURL != "http://colca" || cfg.colcaService != "grantsync" {
		t.Errorf("local Colca defaults are %q / %q", cfg.colcaURL, cfg.colcaService)
	}
	if cfg.once || cfg.dryRun {
		t.Errorf("once/dry-run should default off: %+v", cfg)
	}
}

func TestTrailingSlashesAreTrimmedFromBothURLs(t *testing.T) {
	// Every path is built by concatenation; a trailing slash would produce
	// //kv and //realms, which some proxies answer with a redirect and a 404.
	vars := complete()
	vars["COLCA_URL"] = "http://global:8080/"
	vars["KC_URL"] = "http://keycloak:8080/auth/"
	cfg, err := loadConfig(env(vars))
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if strings.HasSuffix(cfg.colcaURL, "/") || strings.HasSuffix(cfg.kcURL, "/") {
		t.Fatalf("urls not trimmed: %q %q", cfg.colcaURL, cfg.kcURL)
	}
}

func TestAnUnusableIntervalRefusesToStart(t *testing.T) {
	for _, raw := range []string{"soon", "0", "-5"} {
		vars := complete()
		vars["SYNC_INTERVAL_MS"] = raw
		if _, err := loadConfig(env(vars)); err == nil {
			t.Errorf("SYNC_INTERVAL_MS=%q was accepted", raw)
		}
	}
}

func TestBooleanFlagsAcceptTheUsualSpellings(t *testing.T) {
	for _, raw := range []string{"1", "true", "TRUE", "yes", "on"} {
		vars := complete()
		vars["SYNC_ONCE"] = raw
		cfg, err := loadConfig(env(vars))
		if err != nil || !cfg.once {
			t.Errorf("SYNC_ONCE=%q did not enable one-shot mode (%v)", raw, err)
		}
	}
	for _, raw := range []string{"", "0", "false", "no"} {
		vars := complete()
		vars["SYNC_ONCE"] = raw
		cfg, err := loadConfig(env(vars))
		if err != nil || cfg.once {
			t.Errorf("SYNC_ONCE=%q enabled one-shot mode (%v)", raw, err)
		}
	}
}
