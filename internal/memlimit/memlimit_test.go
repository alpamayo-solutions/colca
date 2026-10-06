package memlimit

import (
	"os"
	"path/filepath"
	"testing"
)

func write(t *testing.T, root, rel, content string) {
	t.Helper()
	p := filepath.Join(root, rel)
	if err := os.MkdirAll(filepath.Dir(p), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestTheCeilingIsTheProcesssOwnCgroups(t *testing.T) {
	for _, tc := range []struct {
		name  string
		files map[string]string
		want  int64
	}{
		{"v2 in a cgroup namespace (the container's cgroup is the root)", map[string]string{
			"proc": "0::/\n", "cg/memory.max": "2147483648\n"}, 2 << 30},
		{"v2 without a namespace: the process's own cgroup, not the root", map[string]string{
			"proc": "0::/system.slice/docker-abc.scope\n", "cg/memory.max": "max\n",
			"cg/system.slice/memory.max": "max\n", "cg/system.slice/docker-abc.scope/memory.max": "536870912\n"}, 512 << 20},
		{"v2: a smaller limit on an ancestor wins", map[string]string{
			"proc": "0::/kubepods/pod1/c1\n", "cg/kubepods/pod1/memory.max": "1073741824\n",
			"cg/kubepods/pod1/c1/memory.max": "2147483648\n"}, 1 << 30},
		{"v2 unlimited everywhere", map[string]string{"proc": "0::/a\n", "cg/memory.max": "max\n", "cg/a/memory.max": "max\n"}, 0},
		{"v1 memory controller", map[string]string{
			"proc": "12:cpu,cpuacct:/docker/x\n9:memory:/docker/x\n", "cg/memory/docker/x/memory.limit_in_bytes": "536870912\n"}, 512 << 20},
		{"v1 unlimited sentinel", map[string]string{
			"proc": "9:memory:/\n", "cg/memory/memory.limit_in_bytes": "9223372036854771712\n"}, 0},
		{"no membership file: the mount's root", map[string]string{"cg/memory.max": "268435456\n"}, 256 << 20},
		{"nothing at all", map[string]string{}, 0},
		{"garbage", map[string]string{"proc": "0::/\n", "cg/memory.max": "lots\n"}, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			for rel, content := range tc.files {
				write(t, dir, rel, content)
			}
			if got := ceiling(filepath.Join(dir, "cg"), filepath.Join(dir, "proc")); got != tc.want {
				t.Fatalf("ceiling %d, want %d", got, tc.want)
			}
		})
	}
}

func TestAnExplicitGOMEMLIMITWins(t *testing.T) {
	t.Setenv("GOMEMLIMIT", "1GiB")
	if got := Apply(); got != 0 {
		t.Fatalf("Apply set %d over an explicit GOMEMLIMIT", got)
	}
}
