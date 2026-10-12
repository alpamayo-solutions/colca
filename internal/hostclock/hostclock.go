// Package hostclock reads what the kernel knows about the host's clock. colca
// does not set or correct time: the host's NTP daemon (chrony, ntpd,
// systemd-timesyncd) disciplines the clock and reports its verdict to the
// kernel, and a container shares the host's kernel. A read-only adjtimex
// (modes == 0) returns that verdict without a privilege, a mount or a
// daemon-specific socket.
//
// Anything that is not Linux, or a kernel or sandbox that refuses the call, is
// unreported, never guessed. node-exporter's node_timex_* metrics carry the
// same reading for such a host.
package hostclock

import "time"

const (
	// timeError is the adjtimex return value for a clock that is not
	// synchronized (TIME_ERROR in <sys/timex.h>).
	timeError = 5
	// staUnsync is the timex.status bit set while no daemon vouches for the
	// clock (STA_UNSYNC).
	staUnsync = 0x0040
)

// Status is one reading of the host clock's discipline.
type Status struct {
	// Reported is false when the kernel's verdict could not be read; the other
	// fields then mean nothing.
	Reported bool
	// Synchronized is the kernel's verdict: a daemon vouches for the clock.
	Synchronized bool
	// EstimatedError is the kernel's estimate of the clock's error.
	EstimatedError time.Duration
	// MaxError is the kernel's bound on the clock's error. It grows while no
	// daemon updates the clock.
	MaxError time.Duration
}

// interpret turns one adjtimex result into a Status. state is the call's return
// value, status the timex.status flags; esterror and maxerror are in
// microseconds whatever the clock's resolution.
func interpret(state int, status int32, esterrorUS, maxerrorUS int64) Status {
	return Status{
		Reported:       true,
		Synchronized:   state != timeError && status&staUnsync == 0,
		EstimatedError: time.Duration(esterrorUS) * time.Microsecond,
		MaxError:       time.Duration(maxerrorUS) * time.Microsecond,
	}
}
