package bench

// Always-on run records. This is the reference implementation of a
// repo-wide pattern: every performance-test execution leaves a durable,
// commit-stamped record on disk, one compact JSON line per report
// (bench/results/<host>.jsonl), committed to git. READMEs and results
// tables are written FROM these records after the fact, never the other
// way around.

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime/debug"
	"strings"
)

// Stamp fills in the Service/GitCommit/GitDirty identity fields on r. It
// never fails the run: on any resolution error it falls through silently,
// leaving GitCommit as "unknown" and GitDirty false.
func Stamp(r *Report) {
	r.Service = "colca"
	if commit, dirty, ok := vcsFromBuildInfo(); ok {
		r.GitCommit, r.GitDirty = commit, dirty
		return
	}
	if commit, dirty, ok := vcsFromGitCLI(); ok {
		r.GitCommit, r.GitDirty = commit, dirty
		return
	}
	r.GitCommit, r.GitDirty = "unknown", false
}

// vcsFromBuildInfo reads the commit embedded by the Go toolchain (present
// when running a built binary; typically absent under `go test`/`go run`).
func vcsFromBuildInfo() (commit string, dirty bool, ok bool) {
	info, available := debug.ReadBuildInfo()
	if !available {
		return "", false, false
	}
	var revision string
	for _, s := range info.Settings {
		switch s.Key {
		case "vcs.revision":
			revision = s.Value
		case "vcs.modified":
			dirty = s.Value == "true"
		}
	}
	if revision == "" {
		return "", false, false
	}
	if len(revision) > 9 {
		revision = revision[:9]
	}
	return revision, dirty, true
}

// vcsFromGitCLI shells out to git as a fallback for `go test`/`go run`,
// where build info carries no VCS stamp.
func vcsFromGitCLI() (commit string, dirty bool, ok bool) {
	out, err := exec.CommandContext(context.Background(), "git", "rev-parse", "--short", "HEAD").Output()
	if err != nil {
		return "", false, false
	}
	commit = strings.TrimSpace(string(out))
	if commit == "" {
		return "", false, false
	}
	if statusOut, err := exec.CommandContext(context.Background(), "git", "status", "--porcelain").Output(); err == nil {
		dirty = strings.TrimSpace(string(statusOut)) != ""
	}
	return commit, dirty, true
}

// AppendRecords appends each report to path as one compact JSON line,
// creating the parent directory if needed. It never reads or rewrites
// existing content — one os.Write call per line, so concurrent appenders
// (and interrupted runs) never corrupt earlier records.
func AppendRecords(path string, reports []*Report) (err error) {
	if dir := filepath.Dir(path); dir != "." {
		if err := os.MkdirAll(dir, 0o750); err != nil {
			return fmt.Errorf("create records dir: %w", err)
		}
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("open records file: %w", err)
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil && err == nil {
			err = fmt.Errorf("close records file: %w", closeErr)
		}
	}()
	for _, r := range reports {
		line, err := json.Marshal(r)
		if err != nil {
			return fmt.Errorf("marshal report %q: %w", r.Scenario, err)
		}
		line = append(line, '\n')
		if _, err := f.Write(line); err != nil {
			return fmt.Errorf("write record: %w", err)
		}
	}
	return nil
}

// ReadRecords reads a JSONL run-record file, returning one Report per
// non-blank line in file order. An unparseable line is an error naming the
// 1-indexed line number.
func ReadRecords(path string) ([]Report, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open records file: %w", err)
	}
	defer func() { _ = f.Close() }()

	var records []Report
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 1<<20)
	lineNo := 0
	for scanner.Scan() {
		lineNo++
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var r Report
		if err := json.Unmarshal([]byte(line), &r); err != nil {
			return nil, fmt.Errorf("%s: line %d: %w", path, lineNo, err)
		}
		records = append(records, r)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read records file: %w", err)
	}
	return records, nil
}
