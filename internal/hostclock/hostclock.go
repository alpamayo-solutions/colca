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

// PollAllowance is what the kernel's error bound gains between two updates of
// a healthy NTP service: the bound grows by 500 µs every second (the kernel's
// assumed worst drift, 500 ppm), and the services in use poll at most every
// 2048 s (systemd-timesyncd's default maximum; chrony's and ntpd's is 1024 s).
const PollAllowance = 2048 * 500 * time.Microsecond

// Status is one reading of the host clock's discipline.
type Status struct {
	// Reported is false when the kernel's verdict could not be read; the other
	// fields then mean nothing.
	Reported bool
	// Synchronized is the kernel's verdict: a daemon vouches for the clock.
	Synchronized bool
	// EstimateReported says the NTP service maintains the kernel's error
	// estimate. chrony and ntpd do. systemd-timesyncd writes 0 at every
	// update, so on such a host the estimate is not a measurement and reads
	// as not reported, never as an error of zero.
	EstimateReported bool
	// EstimatedError is the service's estimate of the clock's error; zero
	// without EstimateReported.
	EstimatedError time.Duration
	// MaxError is the kernel's bound on the clock's error. Every service sets
	// it when it updates the clock and the kernel then adds 500 µs a second,
	// so it also records how long ago the last update was.
	MaxError time.Duration
}

// StaleBound is the part of the kernel's error bound that a healthy service's
// poll interval does not explain: zero while the clock is updated, then 500 µs
// for every second since an update came due. It is the one host signal every
// NTP service maintains, so it is what an alarm on the host's clock reads: 0.5 s
// is reached 51 minutes after the last update, 5 s after 3 h 21 min. The kernel
// declares the clock unsynchronized at a bound of 16 s.
func (s Status) StaleBound() time.Duration {
	return max(0, s.MaxError-PollAllowance)
}

// interpret turns one adjtimex result into a Status. state is the call's return
// value, status the timex.status flags; esterror and maxerror are in
// microseconds whatever the clock's resolution.
func interpret(state int, status int32, esterrorUS, maxerrorUS int64) Status {
	return Status{
		Reported:         true,
		Synchronized:     state != timeError && status&staUnsync == 0,
		EstimateReported: esterrorUS > 0,
		EstimatedError:   time.Duration(max(0, esterrorUS)) * time.Microsecond,
		MaxError:         time.Duration(maxerrorUS) * time.Microsecond,
	}
}
