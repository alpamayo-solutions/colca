// Package memlimit reads the memory ceiling of the container a process runs
// in, and gives the Go runtime a soft memory limit below it.
//
// Without one the garbage collector lets the heap grow to twice the live data
// (GOGC=100) whatever the ceiling, and a node under load is killed by the
// kernel while half its memory is garbage: a parent ingesting more than it
// could store reached its 2 GiB ceiling with ~0.7 GiB live (fleet scale
// benchmark, 2026-10). With a limit the collector works harder as the heap
// nears it instead.
package memlimit

import (
	"bufio"
	"os"
	"path"
	"runtime/debug"
	"strconv"
	"strings"
)

// Fraction of the container ceiling the Go heap may use. The rest is for
// memory the runtime does not manage (Pebble's memtables and block cache,
// thread stacks) and the kernel's view of page cache charged to the cgroup.
const Fraction = 0.75

// The cgroup file system and this process's membership, variables so tests
// can point them at a fake tree.
var (
	cgroupRoot = "/sys/fs/cgroup"
	selfCgroup = "/proc/self/cgroup"
)

// Ceiling returns the memory ceiling of this process's cgroup in bytes, or 0
// when none is visible (no cgroup, or no limit on it or any ancestor). With
// cgroup v2 the smallest limit on the way from the process's cgroup to the
// root applies; with v1, the memory controller's limit.
func Ceiling() int64 {
	return ceiling(cgroupRoot, selfCgroup)
}

// Apply sets the runtime's soft memory limit to Fraction of Ceiling and
// returns it. It returns 0 and sets nothing when GOMEMLIMIT is set (the
// runtime already honours it) or no ceiling is visible.
func Apply() int64 {
	if os.Getenv("GOMEMLIMIT") != "" {
		return 0
	}
	c := Ceiling()
	if c <= 0 {
		return 0
	}
	limit := int64(float64(c) * Fraction)
	debug.SetMemoryLimit(limit)
	return limit
}

// Report describes what Apply did, for the caller's log: the message and
// whether it is a warning (no ceiling found, so nothing protects the process
// from the kernel's OOM killer but the default collector pacing).
func Report(limit int64) (msg string, warn bool) {
	switch {
	case limit > 0:
		return "go memory limit set below the container's memory ceiling", false
	case os.Getenv("GOMEMLIMIT") != "":
		return "go memory limit taken from GOMEMLIMIT", false
	default:
		return "no container memory ceiling found (cgroup v1 or v2); the go memory limit is not set", true
	}
}

func ceiling(root, self string) int64 {
	v2, v1 := membership(self)
	if v2 != "" {
		if c := smallestV2(root, v2); c > 0 {
			return c
		}
	}
	if v1 != "" {
		if c := readLimit(path.Join(root, "memory", v1, "memory.limit_in_bytes")); c > 0 {
			return c
		}
	}
	// No membership file, or a cgroup namespace whose own path does not exist
	// under the mount: the mount's root is the container's cgroup.
	if c := readLimit(path.Join(root, "memory.max")); c > 0 {
		return c
	}
	return readLimit(path.Join(root, "memory", "memory.limit_in_bytes"))
}

// membership reads /proc/self/cgroup: the unified (v2) path and the v1 memory
// controller's path, each "" when absent.
func membership(file string) (v2, v1 string) {
	f, err := os.Open(file) //nolint:gosec // a fixed kernel interface path, or a test's
	if err != nil {
		return "", ""
	}
	defer func() { _ = f.Close() }()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		parts := strings.SplitN(sc.Text(), ":", 3)
		if len(parts) != 3 {
			continue
		}
		switch {
		case parts[0] == "0" && parts[1] == "":
			v2 = parts[2]
		case containsController(parts[1], "memory"):
			v1 = parts[2]
		}
	}
	return v2, v1
}

func containsController(list, name string) bool {
	for _, c := range strings.Split(list, ",") {
		if c == name {
			return true
		}
	}
	return false
}

// smallestV2 is the smallest memory.max from the cgroup at rel up to the root.
func smallestV2(root, rel string) int64 {
	var smallest int64
	for dir := path.Clean("/" + rel); ; dir = path.Dir(dir) {
		if c := readLimit(path.Join(root, dir, "memory.max")); c > 0 && (smallest == 0 || c < smallest) {
			smallest = c
		}
		if dir == "/" {
			return smallest
		}
	}
}

// readLimit reads a byte count. "max" (no limit), a missing file and the huge
// sentinel cgroup v1 reports for "unlimited" all mean 0.
func readLimit(file string) int64 {
	raw, err := os.ReadFile(file) //nolint:gosec // cgroup interface paths, or a test's
	if err != nil {
		return 0
	}
	s := strings.TrimSpace(string(raw))
	if s == "max" {
		return 0
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n <= 0 || n >= 1<<60 {
		return 0
	}
	return n
}
