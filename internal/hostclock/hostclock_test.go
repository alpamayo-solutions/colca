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
			want: Status{Reported: true, Synchronized: true, EstimatedError: 1500 * time.Microsecond, MaxError: 250 * time.Millisecond},
		},
		"STA_UNSYNC set, state still OK": {
			state: 0, status: staPLL | staUnsync, esterror: 16_000_000, maxerror: 16_000_000,
			want: Status{Reported: true, Synchronized: false, EstimatedError: 16 * time.Second, MaxError: 16 * time.Second},
		},
		"TIME_ERROR without STA_UNSYNC": {
			state: timeError, status: staPLL, esterror: 0, maxerror: 16_000_000,
			want: Status{Reported: true, Synchronized: false, MaxError: 16 * time.Second},
		},
		"a leap second announced is still synchronized": {
			state: 1, status: staPLL, esterror: 10, maxerror: 20,
			want: Status{Reported: true, Synchronized: true, EstimatedError: 10 * time.Microsecond, MaxError: 20 * time.Microsecond},
		},
	} {
		t.Run(name, func(t *testing.T) {
			if got := interpret(tc.state, tc.status, tc.esterror, tc.maxerror); got != tc.want {
				t.Fatalf("interpret = %+v, want %+v", got, tc.want)
			}
		})
	}
}
