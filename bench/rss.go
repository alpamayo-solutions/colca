package bench

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
)

// RSSBytes returns the resident set size of pid. Linux reads
// /proc/<pid>/status (target hardware); darwin shells out to ps (dev laptops).
func RSSBytes(pid int) (uint64, error) {
	if runtime.GOOS == "linux" {
		raw, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid))
		if err != nil {
			return 0, err
		}
		for _, line := range strings.Split(string(raw), "\n") {
			if !strings.HasPrefix(line, "VmRSS:") {
				continue
			}
			f := strings.Fields(line) // "VmRSS:  12345 kB"
			if len(f) < 2 {
				break
			}
			kb, err := strconv.ParseUint(f[1], 10, 64)
			if err != nil {
				return 0, err
			}
			return kb * 1024, nil
		}
		return 0, fmt.Errorf("no VmRSS line in /proc/%d/status", pid)
	}
	out, err := exec.CommandContext(context.Background(), "ps", "-o", "rss=", "-p", strconv.Itoa(pid)).Output()
	if err != nil {
		return 0, err
	}
	kb, err := strconv.ParseUint(strings.TrimSpace(string(out)), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse ps rss output %q: %w", out, err)
	}
	return kb * 1024, nil
}
