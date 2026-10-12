//go:build linux

package hostclock

import "testing"

// A read-only adjtimex needs no privilege, so on Linux Read reports unless a
// sandbox refuses the call. A refusal is skipped rather than failed: it is the
// documented "unreported" case, not a defect.
func TestReadReportsOnLinux(t *testing.T) {
	got := Read()
	if !got.Reported {
		t.Skip("adjtimex is refused in this environment; Read reports nothing, as documented")
	}
	if got.EstimatedError < 0 || got.MaxError < 0 {
		t.Fatalf("Read = %+v: the kernel's error bounds are never negative", got)
	}
}
