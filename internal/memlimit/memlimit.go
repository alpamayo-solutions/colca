// Package memlimit gives the Go runtime a soft memory limit below the
// container's memory ceiling.
//
// Without one the garbage collector lets the heap grow to twice the live data
// (GOGC=100) whatever the ceiling, and a node under load is killed by the
// kernel while half its memory is garbage: a parent ingesting more than it
// could store reached its 2 GiB ceiling with ~0.7 GiB live (fleet scale
// benchmark, 2026-10). With a limit the collector works harder as the heap
// nears it instead.
package memlimit

import (
	"os"
	"runtime/debug"
	"strconv"
	"strings"
)

// Fraction of the container ceiling the Go heap may use. The rest is for
// memory the runtime does not manage (Pebble's memtables and block cache,
// thread stacks) and the kernel's view of page cache charged to the cgroup.
const Fraction = 0.75

// cgroupFiles are where cgroup v2 and v1 publish the memory ceiling.
var cgroupFiles = []string{"/sys/fs/cgroup/memory.max", "/sys/fs/cgroup/memory/memory.limit_in_bytes"}

// Apply sets the runtime's soft memory limit to Fraction of the container's
// memory ceiling and returns it, or 0 when it set nothing: GOMEMLIMIT is set
// (the runtime already honours it), or no ceiling is visible.
func Apply() int64 {
	if os.Getenv("GOMEMLIMIT") != "" {
		return 0
	}
	ceiling := ceilingFrom(cgroupFiles)
	if ceiling <= 0 {
		return 0
	}
	limit := int64(float64(ceiling) * Fraction)
	debug.SetMemoryLimit(limit)
	return limit
}

// ceilingFrom reads the first file that holds a byte count. "max" (no limit),
// a missing file and the huge sentinel cgroup v1 reports for "unlimited" all
// mean no ceiling.
func ceilingFrom(files []string) int64 {
	for _, f := range files {
		raw, err := os.ReadFile(f) //nolint:gosec // fixed kernel interface paths, or a test's
		if err != nil {
			continue
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
	return 0
}
