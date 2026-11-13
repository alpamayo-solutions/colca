package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
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
	// All missing variables are named at once, not one per restart.
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
	if cfg.interval != 5*time.Minute {
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

// Keycloak pushes nothing: POST /sync is how an edit there asks for a cycle
// instead of waiting out the safety interval.
func TestPostSyncWakesTheSyncer(t *testing.T) {
	woken := 0
	srv := httptest.NewServer(mux(prometheus.NewRegistry(), func() { woken++ }))
	defer srv.Close()
	resp, err := http.Post(srv.URL+"/sync", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted || woken != 1 {
		t.Fatalf("POST /sync = %d, woke %d time(s); want 202 and one wake", resp.StatusCode, woken)
	}
	if resp, err = http.Get(srv.URL + "/sync"); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed || woken != 1 {
		t.Fatalf("GET /sync = %d, woke %d; want 405 and no wake", resp.StatusCode, woken)
	}
}
