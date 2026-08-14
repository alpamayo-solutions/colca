package bench

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeFile(t *testing.T, dir, name, content string) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestCheckPassFailAndNull(t *testing.T) {
	dir := t.TempDir()
	results := writeFile(t, dir, "results.json", `[
		{"scenario":"ingest","started_at":"2026-08-14T00:00:00Z",
		 "host":{"os":"linux","arch":"arm64","num_cpu":4,"hostname":"x","go_version":"go1.25","storage":"emmc"},
		 "params":{},"metrics":{"ingest_msgs_per_sec": 500, "write_amplification": 9}}
	]`)

	pass := writeFile(t, dir, "pass.json", `{"ingest":{"ingest_msgs_per_sec":{"min":100}}}`)
	if err := Check(results, pass, os.Stderr); err != nil {
		t.Fatalf("expected pass, got: %v", err)
	}

	fail := writeFile(t, dir, "fail.json",
		`{"ingest":{"ingest_msgs_per_sec":{"min":1000},"write_amplification":{"max":5}}}`)
	err := Check(results, fail, os.Stderr)
	if err == nil {
		t.Fatal("expected gate failure")
	}
	msg := err.Error()
	if !strings.Contains(msg, "ingest_msgs_per_sec") || !strings.Contains(msg, "write_amplification") {
		t.Fatalf("error must name every violation, got: %v", msg)
	}

	nullGate := writeFile(t, dir, "null.json", `{"ingest":{"ingest_msgs_per_sec":{"min":null}}}`)
	if err := Check(results, nullGate, os.Stderr); err != nil {
		t.Fatalf("null bound must be report-only, got: %v", err)
	}

	if err := Check(results, filepath.Join(dir, "missing.json"), os.Stderr); err == nil {
		t.Fatal("missing thresholds file must be an error, not a silent pass")
	}
}

func TestCheckAbsentScenario(t *testing.T) {
	dir := t.TempDir()
	empty := writeFile(t, dir, "empty.json", `[]`)

	active := writeFile(t, dir, "active.json", `{"ingest":{"ingest_msgs_per_sec":{"min":100}}}`)
	err := Check(empty, active, os.Stderr)
	if err == nil {
		t.Fatal("expected violation: scenario with active bounds is absent from results")
	}
	if !strings.Contains(err.Error(), "ingest") {
		t.Fatalf("error must name the missing scenario, got: %v", err)
	}

	allNull := writeFile(t, dir, "all-null.json", `{"ingest":{"ingest_msgs_per_sec":{"min":null}}}`)
	if err := Check(empty, allNull, os.Stderr); err != nil {
		t.Fatalf("all-null bounds must stay skip-silent when scenario absent, got: %v", err)
	}
}
