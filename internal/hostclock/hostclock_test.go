package hostclock

import (
	"testing"
	"time"
)

// The kernel's verdict has two parts and either one means "not synchronized":
// the adjtimex return value TIME_ERROR and the STA_UNSYNC status bit. The error
// fields are microseconds.
func TestInterpretReadsTheKernelsVerdict(t *testing.T) {
	const staPLL, staNano = 0x0001, 0x2000
	for name, tc := range map[string]struct {
		state              int
		status             int32
		esterror, maxerror int64
		want               Status
	}{
		"synchronized": {
			state: 0, status: staPLL | staNano, esterror: 1500, maxerror: 250_000,
			want: Status{Reported: true, Synchronized: true, EstimateReported: true, EstimatedError: 1500 * time.Microsecond, MaxError: 250 * time.Millisecond},
		},
		"STA_UNSYNC set, state still OK": {
			state: 0, status: staPLL | staUnsync, esterror: 16_000_000, maxerror: 16_000_000,
			want: Status{Reported: true, Synchronized: false, EstimateReported: true, EstimatedError: 16 * time.Second, MaxError: 16 * time.Second},
		},
		"TIME_ERROR without STA_UNSYNC": {
			state: timeError, status: staPLL, esterror: 0, maxerror: 16_000_000,
			want: Status{Reported: true, Synchronized: false, MaxError: 16 * time.Second},
		},
		"a leap second announced is still synchronized": {
			state: 1, status: staPLL, esterror: 10, maxerror: 20,
			want: Status{Reported: true, Synchronized: true, EstimateReported: true, EstimatedError: 10 * time.Microsecond, MaxError: 20 * time.Microsecond},
		},
		// What adjtimex returned on a host synchronized by systemd-timesyncd
		// 28 minutes after its last update: the service writes esterror 0.
		"systemd-timesyncd keeps no estimate": {
			state: 0, status: staPLL | staNano, esterror: 0, maxerror: 848_500,
			want: Status{Reported: true, Synchronized: true, MaxError: 848_500 * time.Microsecond},
		},
	} {
		t.Run(name, func(t *testing.T) {
			if got := interpret(tc.state, tc.status, tc.esterror, tc.maxerror); got != tc.want {
				t.Fatalf("interpret = %+v, want %+v", got, tc.want)
			}
		})
	}
}

// The host signal that works under every NTP service is the kernel's error
// bound beyond one poll interval. timesyncd resets the bound to 0 at each
// update and chrony to its root distance; the kernel adds 500 µs a second.
func TestStaleBoundIsZeroWhileTheClockIsUpdatedAndGrowsOnceItIsNot(t *testing.T) {
	const staPLL = 0x0001
	sinceUpdate := func(d time.Duration, atUpdateUS int64) Status {
		return interpret(0, staPLL, 0, atUpdateUS+int64(d/time.Second)*500)
	}
	for name, tc := range map[string]struct {
		status Status
		want   time.Duration
	}{
		"timesyncd, just updated":                    {sinceUpdate(0, 0), 0},
		"timesyncd, at its longest poll (2048 s)":    {sinceUpdate(2048*time.Second, 0), 0},
		"chrony, 1024 s poll, 30 ms root distance":   {sinceUpdate(1024*time.Second, 30_000), 0},
		"no update for 51 minutes":                   {sinceUpdate(3060*time.Second, 0), 506 * time.Millisecond},
		"no update for 4 hours":                      {sinceUpdate(4*time.Hour, 0), 6176 * time.Millisecond},
		"the kernel's ceiling, where it sets UNSYNC": {interpret(timeError, staPLL|staUnsync, 0, 16_000_000), 16*time.Second - PollAllowance},
	} {
		t.Run(name, func(t *testing.T) {
			if got := tc.status.StaleBound(); got != tc.want {
				t.Fatalf("StaleBound = %s, want %s (max error %s)", got, tc.want, tc.status.MaxError)
			}
		})
	}
	// The thresholds the alarms use, in time without an update.
	if warn := sinceUpdate(51*time.Minute, 0).StaleBound(); warn < 500*time.Millisecond {
		t.Fatalf("51 minutes without an update give %s, want at least the 0.5 s warning", warn)
	}
	if ok := sinceUpdate(50*time.Minute, 0).StaleBound(); ok >= 500*time.Millisecond {
		t.Fatalf("50 minutes without an update give %s, want it below the 0.5 s warning", ok)
	}
}
