package memlimit

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCeilingFromTheCgroupFiles(t *testing.T) {
	dir := t.TempDir()
	write := func(name, content string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	missing := filepath.Join(dir, "absent")
	for _, tc := range []struct {
		name  string
		files []string
		want  int64
	}{
		{"cgroup v2 limit", []string{write("v2", "2147483648\n")}, 2 << 30},
		{"cgroup v2 unlimited", []string{write("v2max", "max\n")}, 0},
		{"cgroup v1 after a missing v2 file", []string{missing, write("v1", "536870912\n")}, 512 << 20},
		{"cgroup v1 unlimited sentinel", []string{write("v1max", "9223372036854771712\n")}, 0},
		{"no cgroup", []string{missing}, 0},
		{"garbage", []string{write("bad", "lots\n")}, 0},
	} {
		if got := ceilingFrom(tc.files); got != tc.want {
			t.Errorf("%s: %d, want %d", tc.name, got, tc.want)
		}
	}
}

func TestAnExplicitGOMEMLIMITWins(t *testing.T) {
	t.Setenv("GOMEMLIMIT", "1GiB")
	if got := Apply(); got != 0 {
		t.Fatalf("Apply set %d over an explicit GOMEMLIMIT", got)
	}
}
