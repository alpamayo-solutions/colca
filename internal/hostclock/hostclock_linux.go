//go:build linux

package hostclock

import "golang.org/x/sys/unix"

// Read asks the kernel for the clock's discipline with a read-only adjtimex.
// A refused call (a seccomp profile that blocks it, for one) is unreported.
func Read() Status {
	var tx unix.Timex // Modes == 0: read, change nothing
	state, err := unix.Adjtimex(&tx)
	if err != nil {
		return Status{}
	}
	// The error fields are a C long: int64 here, int32 on a 32-bit platform.
	return interpret(state, tx.Status, int64(tx.Esterror), int64(tx.Maxerror)) //nolint:unconvert // needed where long is 32 bits
}
