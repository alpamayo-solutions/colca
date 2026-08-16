package main

import (
	"context"
	"sync"
	"testing"
	"time"
)

// fakeClock is a manually-advanced wall clock (no real time.Sleep anywhere in
// this file — testing.md's "poll with deadlines / fake clocks preferred"):
// every decision below is driven by explicit Advance calls, not wall-clock
// waits.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock(start time.Time) *fakeClock { return &fakeClock{now: start} }

func (f *fakeClock) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

func (f *fakeClock) Advance(d time.Duration) {
	f.mu.Lock()
	f.now = f.now.Add(d)
	f.mu.Unlock()
}

var epoch = time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC)

// Design §2.3 rule 3: a command with no usable expires_at always executes,
// regardless of the clock.
func TestResultNoUsableExpiryAlwaysExecutes(t *testing.T) {
	cases := []map[string]any{
		{},
		{"expires_at": nil},
		{"expires_at": "not-a-number"},
		{"expires_at": map[string]any{}},
	}
	for _, cmd := range cases {
		code, msg := result(cmd, epoch.UnixMilli())
		if code != codeOK {
			t.Errorf("cmd %+v: code = %d, want %d (%s)", cmd, code, codeOK, msg)
		}
	}
}

func TestResultExpiryBoundary(t *testing.T) {
	now := epoch.UnixMilli()
	if code, _ := result(map[string]any{"expires_at": float64(now)}, now); code != codeOK {
		t.Errorf("exp == now must not be expired, got %d", code)
	}
	if code, _ := result(map[string]any{"expires_at": float64(now - 1)}, now); code != codeExpired {
		t.Errorf("exp < now must be expired, got %d", code)
	}
	if code, _ := result(map[string]any{"expires_at": float64(now + 1)}, now); code != codeOK {
		t.Errorf("exp > now must not be expired, got %d", code)
	}
}

// Design §2.3 rule 2: immediately after Connect, with a nonzero hold, an
// expiry decision must be held — never allowed to proceed — until a beacon
// arrives. This is the pure decide() state machine: no goroutines, no real
// time, fully deterministic.
func TestHoldReleasesOnBeacon(t *testing.T) {
	fc := newFakeClock(epoch)
	ts := newTimeSync(fc.Now, 10000) // 10s hold
	ts.Connect()

	if ready, _, _ := decide(ts.Snapshot(), fc.Now()); ready {
		t.Fatal("must be holding immediately after Connect, before any beacon")
	}

	fc.Advance(2 * time.Second) // well before the 10s deadline
	if ready, _, _ := decide(ts.Snapshot(), fc.Now()); ready {
		t.Fatal("must still be holding before the beacon and before the deadline")
	}

	ts.Beacon(fc.Now().UnixMilli())
	ready, _, deadlineHit := decide(ts.Snapshot(), fc.Now())
	if !ready {
		t.Fatal("a beacon must release the hold immediately, without waiting for the deadline")
	}
	if deadlineHit {
		t.Fatal("release via beacon must not report deadlineHit — that flag is for the fail-open path only")
	}
}

// Design §2.3 rule 2: on deadline with no beacon, the decision proceeds
// (fail open) on the last-known offset (0 here, since none was ever
// learned) and reports deadlineHit so the caller logs the warning.
func TestHoldDeadlineProceedsWithWarning(t *testing.T) {
	fc := newFakeClock(epoch)
	ts := newTimeSync(fc.Now, 10000)
	ts.Connect()

	fc.Advance(9999 * time.Millisecond)
	if ready, _, _ := decide(ts.Snapshot(), fc.Now()); ready {
		t.Fatal("must still be holding 1ms before the deadline")
	}

	fc.Advance(1 * time.Millisecond) // now exactly at the deadline
	ready, syncedNow, deadlineHit := decide(ts.Snapshot(), fc.Now())
	if !ready {
		t.Fatal("the deadline must fail open, not keep holding forever")
	}
	if !deadlineHit {
		t.Fatal("proceeding via the deadline must report deadlineHit=true")
	}
	if !syncedNow.Equal(fc.Now()) {
		t.Fatalf("synced_now with a never-learned offset (0) must equal wall_now: got %v, want %v", syncedNow, fc.Now())
	}
}

// hold_ms == 0 is the operator's explicit "no hold" (mirrors
// config.TimeSync.EffectiveHoldMS's own contract): Connect must not open a
// hold at all, so a decision proceeds immediately even with no beacon ever
// received.
func TestZeroHoldMeansNoHold(t *testing.T) {
	fc := newFakeClock(epoch)
	ts := newTimeSync(fc.Now, 0)
	ts.Connect()
	ready, _, deadlineHit := decide(ts.Snapshot(), fc.Now())
	if !ready || deadlineHit {
		t.Fatalf("hold_ms=0 must proceed immediately with deadlineHit=false, got ready=%v deadlineHit=%v", ready, deadlineHit)
	}
}

// Design §2.3 rule 1 + §2.1: offset = beacon.now_ms - wall_receipt, and
// synced_now = wall_now + offset corrects a machine's skewed wall clock back
// to the authority's time — proven in BOTH directions (design §5's two
// skewed-machine chaos cases), with an inline mutation check: deciding on the
// RAW (uncorrected) skewed clock must give the WRONG answer for each case,
// or this test would not actually be pinning synced_now as load-bearing.
func TestSkewedClockCorrectsBothDirections(t *testing.T) {
	const fiveMinMS = 5 * 60 * 1000
	tests := []struct {
		name          string
		skewMS        int64 // machine's wall clock relative to authoritative time
		expiresOffset int64 // expires_at relative to authoritative "now", ms
		wantCode      int
	}{
		{
			name:          "ahead skew: raw clock would falsely reject a live command",
			skewMS:        10 * 60 * 1000, // machine thinks it's 10 min ahead
			expiresOffset: fiveMinMS,      // 5-minute TTL from authoritative now: still live
			wantCode:      codeOK,
		},
		{
			name:          "behind skew: raw clock would wrongly execute an expired command",
			skewMS:        -10 * 60 * 1000, // machine thinks it's 10 min behind
			expiresOffset: -1 * 60 * 1000,  // expired 1 minute ago, authoritative time
			wantCode:      codeExpired,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			skewedNow := func() time.Time { return epoch.Add(time.Duration(tc.skewMS) * time.Millisecond) }
			ts := newTimeSync(skewedNow, 0) // no hold: isolate the offset-correction math
			ts.Connect()
			// The beacon reports the AUTHORITATIVE (unskewed) now — exactly
			// what the node's real beacon does (engine.AuthoritativeNow).
			ts.Beacon(epoch.UnixMilli())

			ready, syncedNow, _ := decide(ts.Snapshot(), skewedNow())
			if !ready {
				t.Fatal("no hold configured — must be ready immediately")
			}
			if !syncedNow.Equal(epoch) {
				t.Fatalf("synced_now = %v, want the corrected authoritative time %v", syncedNow, epoch)
			}

			expiresAtMS := epoch.UnixMilli() + tc.expiresOffset
			cmd := map[string]any{"expires_at": float64(expiresAtMS)}

			code, msg := result(cmd, syncedNow.UnixMilli())
			if code != tc.wantCode {
				t.Fatalf("deciding on synced_now: code = %d (%s), want %d", code, msg, tc.wantCode)
			}

			// Mutation check: deciding on the RAW skewed wall clock (as if
			// result() were wired to time.Now() directly, ignoring the
			// learned offset) must give the OPPOSITE, wrong answer here —
			// otherwise this test would not catch a regression back to a
			// bare wall-clock read.
			rawCode, _ := result(cmd, skewedNow().UnixMilli())
			if rawCode == tc.wantCode {
				t.Fatalf("mutation check failed: deciding on the raw skewed wall clock also produced %d — "+
					"this test case cannot distinguish synced_now from a bare time.Now(), strengthen it", rawCode)
			}
		})
	}
}

// Await is the blocking production wrapper around decide(): a beacon
// arriving mid-wait must release it immediately, without waiting anywhere
// near the (long) configured hold deadline. Real goroutine, real (but
// generous) timeout in the test's own select — a legitimate poll-with-
// deadline, not a synchronization sleep: Beacon is called with no sleep in
// between, and correctness does not depend on scheduling order (see the
// Await/Connect/Beacon doc comments).
func TestAwaitReleasesOnBeaconWithoutWaitingForDeadline(t *testing.T) {
	ts := newTimeSync(time.Now, 5000) // 5s hold — must NOT be what we wait out
	ts.Connect()

	type result struct {
		syncedNow   time.Time
		deadlineHit bool
	}
	done := make(chan result, 1)
	start := time.Now()
	go func() {
		syncedNow, _, deadlineHit := ts.Await(context.Background())
		done <- result{syncedNow, deadlineHit}
	}()

	ts.Beacon(time.Now().UnixMilli())

	select {
	case r := <-done:
		if r.deadlineHit {
			t.Fatal("release via beacon must not report deadlineHit")
		}
		if elapsed := time.Since(start); elapsed > 2*time.Second {
			t.Fatalf("Await took %v to release on a beacon, want near-immediate (well under the 5s hold)", elapsed)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("Await never released after Beacon — it appears to be waiting out the full hold deadline instead")
	}
}

// Await's deadline path: with NO beacon at all, it must still return
// (fail open) once the (short) configured hold elapses, with
// deadlineHit=true.
func TestAwaitDeadlineFailsOpen(t *testing.T) {
	ts := newTimeSync(time.Now, 100) // 100ms hold — short but real wall-clock time
	ts.Connect()

	done := make(chan bool, 1)
	go func() {
		_, _, deadlineHit := ts.Await(context.Background())
		done <- deadlineHit
	}()

	select {
	case deadlineHit := <-done:
		if !deadlineHit {
			t.Fatal("Await returned without a beacon but deadlineHit=false")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Await never returned — the deadline path did not fail open")
	}
}

// Await must not hang forever past shutdown: ctx cancellation returns it
// even with an open hold and no beacon.
func TestAwaitReturnsOnContextCancel(t *testing.T) {
	ts := newTimeSync(time.Now, 60_000) // 60s hold — long enough that only cancellation explains a quick return
	ts.Connect()
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		ts.Await(ctx)
		close(done)
	}()
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Await did not return after ctx cancellation")
	}
}

// newSimClock's offset is constant for the process lifetime: two readings
// taken apart in real time must differ by (approximately) that real elapsed
// time, not by the elapsed time plus/minus the skew — pinning the doc
// comment's claim that a constant additive skew cancels out of a subtraction
// between two readings of the SAME clock (load-bearing for Await's
// deadline.Sub(wallNow) timer-duration math).
func TestSimClockConstantSkewCancelsInSubtraction(t *testing.T) {
	clk := newSimClock(10 * 60 * 1000) // +10 minutes
	a := clk()
	time.Sleep(5 * time.Millisecond)
	b := clk()
	if d := b.Sub(a); d < 0 || d > 200*time.Millisecond {
		t.Fatalf("b.Sub(a) = %v, want a small positive duration close to the real elapsed time (skew must cancel)", d)
	}
	// The skew itself must still be present in the absolute reading.
	if b.Sub(time.Now()) < 9*time.Minute {
		t.Fatalf("clock does not appear skewed by ~10 minutes: b=%v now=%v", b, time.Now())
	}
}

func TestIsTimeSyncTopic(t *testing.T) {
	cases := map[string]bool{
		"colca/v1/_TimeSync/n-edge1":       true,
		"colca/v1/_TimeSync/n-edge1/extra": false, // beacon topics never have a 5th segment
		"colca/v1/_Metric/n-edge1":         false,
		"colca/v1/_CmdParam/m1/m1/set":     false,
		"other/topic":                    false,
	}
	for topic, want := range cases {
		if got := isTimeSyncTopic(topic); got != want {
			t.Errorf("isTimeSyncTopic(%q) = %v, want %v", topic, got, want)
		}
	}
}
