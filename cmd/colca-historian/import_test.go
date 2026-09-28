package main

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestImportValidatesWholeFileBeforeDatabaseAccess(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	t.Setenv("COLCA_TOPIC_ROOT", "colca")
	valid := `{"topic":"colca/v1/_Metric/01J0000000000000000000000A/m/temp","payload":{"signal_id":"01J0000000000000000000000B","timestamp":100,"value":1}}` + "\n"
	path := filepath.Join(t.TempDir(), "history.jsonl")
	for _, input := range []string{valid, valid + "{broken\n"} {
		if err := os.WriteFile(path, []byte(input), 0600); err != nil {
			t.Fatal(err)
		}
		digest := fmt.Sprintf("%x", sha256.Sum256([]byte(input)))
		args := []string{"--file", path, "--sha256", digest, "--before", "2020-01-01T00:00:00Z"}
		err := importFile(append(args, "--dry-run"))
		if input == valid && err != nil {
			t.Fatal(err)
		}
		if input != valid && (err == nil || !strings.Contains(err.Error(), "line 2")) {
			t.Fatalf("expected whole-file validation: %v", err)
		}
		if err := importFile([]string{"--file", path, "--sha256", strings.Repeat("0", 64), "--before", "2020-01-01T00:00:00Z", "--dry-run"}); err == nil {
			t.Fatal("wrong digest accepted")
		}
	}
}
