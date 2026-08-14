package bench

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAppendRecordsReadRecordsRoundtrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "results", "myhost.jsonl")

	first := []*Report{NewReport("ingest", "nvme", map[string]any{"machines": 4})}
	if err := AppendRecords(path, first); err != nil {
		t.Fatalf("first AppendRecords: %v", err)
	}
	more := []*Report{
		NewReport("live", "nvme", map[string]any{"rate_hz": 10}),
		NewReport("catchup", "nvme", map[string]any{"records": 20000}),
	}
	if err := AppendRecords(path, more); err != nil {
		t.Fatalf("second AppendRecords: %v", err)
	}

	records, err := ReadRecords(path)
	if err != nil {
		t.Fatalf("ReadRecords: %v", err)
	}
	if len(records) != 3 {
		t.Fatalf("got %d records, want 3", len(records))
	}
	wantOrder := []string{"ingest", "live", "catchup"}
	for i, want := range wantOrder {
		if records[i].Scenario != want {
			t.Errorf("record[%d].Scenario = %q, want %q (order not preserved)", i, records[i].Scenario, want)
		}
	}
}

func TestAppendRecordsCreatesParentDir(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "nested", "results", "myhost.jsonl")
	if err := AppendRecords(path, []*Report{NewReport("ingest", "nvme", nil)}); err != nil {
		t.Fatalf("AppendRecords: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected file to exist: %v", err)
	}
}

func TestStampFieldsPopulated(t *testing.T) {
	r := NewReport("ingest", "nvme", nil)
	if r.Service != "colca" {
		t.Errorf("Service = %q, want %q", r.Service, "colca")
	}
	if r.GitCommit == "" {
		t.Error("GitCommit is empty, want a resolved value or \"unknown\"")
	}
}

func TestReadRecordsUnparseableLineNamesLineNumber(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.jsonl")
	content := `{"scenario":"ingest","started_at":"2026-08-14T00:00:00Z","host":{},"params":{},"metrics":{}}
not valid json
{"scenario":"live","started_at":"2026-08-14T00:00:00Z","host":{},"params":{},"metrics":{}}
`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := ReadRecords(path)
	if err == nil {
		t.Fatal("expected error for unparseable line")
	}
	if !strings.Contains(err.Error(), "line 2") {
		t.Fatalf("error must name the line number, got: %v", err)
	}
}

func TestReadRecordsSkipsBlankLines(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "blanks.jsonl")
	content := "{\"scenario\":\"ingest\",\"started_at\":\"2026-08-14T00:00:00Z\",\"host\":{},\"params\":{},\"metrics\":{}}\n\n\n{\"scenario\":\"live\",\"started_at\":\"2026-08-14T00:00:00Z\",\"host\":{},\"params\":{},\"metrics\":{}}\n"
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	records, err := ReadRecords(path)
	if err != nil {
		t.Fatalf("ReadRecords: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("got %d records, want 2 (blank lines skipped)", len(records))
	}
}

func TestReadRecordsMissingFileIsError(t *testing.T) {
	if _, err := ReadRecords(filepath.Join(t.TempDir(), "missing.jsonl")); err == nil {
		t.Fatal("expected error for missing file")
	}
}
