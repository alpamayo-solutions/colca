package main

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	pahomqtt "github.com/eclipse/paho.mqtt.golang"
)

// fakeBeaconMessage is a minimal pahomqtt.Message for driving handleBeacon
// directly in tests — the paho Message interface is small enough that a real
// broker round trip is unnecessary to pin the Duplicate() handling.
type fakeBeaconMessage struct {
	nowMS     int64
	duplicate bool
}

func (m fakeBeaconMessage) Duplicate() bool   { return m.duplicate }
func (m fakeBeaconMessage) Qos() byte         { return 0 }
func (m fakeBeaconMessage) Retained() bool    { return false }
func (m fakeBeaconMessage) Topic() string     { return "colca/v1/_TimeSync/n1" }
func (m fakeBeaconMessage) MessageID() uint16 { return 0 }
func (m fakeBeaconMessage) Payload() []byte {
	b, _ := json.Marshal(struct {
		NowMS int64 `json:"now_ms"`
	}{m.nowMS})
	return b
}
func (m fakeBeaconMessage) Ack() {}

var _ pahomqtt.Message = fakeBeaconMessage{}

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

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

// A broker-flagged duplicate/resent beacon — the
// exact shape of a stale beacon a persistent MQTT session could otherwise
// redeliver after an outage spanning >= 1 beacon_interval — must be dropped
// outright by handleBeacon: it must not release the post-connect hold, and
// it must not corrupt the offset. This pins the traced scenario
// through mochi-mqtt/paho: on reconnect, ts.Connect() opens a fresh hold,
// then a stale (Duplicate()==true) beacon carrying an OLD now_ms could
// otherwise satisfy wasHolding and compute
// offsetMS = old_now_ms - current_wall_receipt, understating authoritative
// time by roughly the outage length — the "clock behind -> expired commands
// execute" failure design §1.1 exists to prevent. A genuine (non-duplicate)
// beacon that arrives afterwards must still work normally.
func TestHandleBeaconIgnoresDuplicateFlaggedMessage(t *testing.T) {
	fc := newFakeClock(epoch)
	ts := newTimeSync(fc.Now, 10000) // 10s hold
	ts.Connect()

	// A stale beacon: Duplicate()=true, carrying a now_ms from long before
	// the outage (simulates the redelivered pre-outage sample).
	staleNowMS := epoch.Add(-10 * time.Minute).UnixMilli()
	handleBeacon(discardLogger(), ts, fakeBeaconMessage{nowMS: staleNowMS, duplicate: true})

	if ready, _, _ := decide(ts.Snapshot(), fc.Now()); ready {
		t.Fatal("a duplicate-flagged beacon must not release the hold")
	}
	if got := ts.Snapshot().offsetMS; got != 0 {
		t.Fatalf("a duplicate-flagged beacon must not update the offset, got offsetMS=%d (a real sample would show ~ -10 minutes = %d)",
			got, epoch.UnixMilli()-staleNowMS)
	}

	// A genuine, fresh (non-duplicate) beacon must still work normally.
	handleBeacon(discardLogger(), ts, fakeBeaconMessage{nowMS: fc.Now().UnixMilli(), duplicate: false})
	ready, syncedNow, deadlineHit := decide(ts.Snapshot(), fc.Now())
	if !ready || deadlineHit {
		t.Fatalf("a genuine beacon must release the hold via the beacon path, got ready=%v deadlineHit=%v", ready, deadlineHit)
	}
	if !syncedNow.Equal(fc.Now()) {
		t.Fatalf("synced_now after the genuine beacon = %v, want %v", syncedNow, fc.Now())
	}
}

// Same scenario as above, but proving the concrete failure mode end to end
// through result() — the mutation-evidence companion to
// TestHandleBeaconIgnoresDuplicateFlaggedMessage. Per design §1.1, the
// dangerous direction is a clock reading BEHIND true time: "genuinely
// expired commands execute." A stale beacon's negative offset makes
// synced_now UNDERSTATE true time, so a command that is ALREADY EXPIRED
// relative to the real reconnect instant can look not-yet-expired relative
// to the corrupted synced_now — a false 200 on a command that should 498.
func TestStaleDuplicateBeaconWouldCorruptExpiryWithoutTheGuard(t *testing.T) {
	// The reconnect happens at `epoch`. The command expired 1 minute BEFORE
	// that, in real/authoritative time — a genuinely stale command that must
	// 498 no matter what.
	cmd := map[string]any{"expires_at": float64(epoch.Add(-1 * time.Minute).UnixMilli())}
	// The stale, redelivered beacon reports authoritative time from 10
	// minutes before the outage even started — an old now_ms a persistent
	// session could requeue and redeliver on reconnect.
	staleNowMS := epoch.Add(-10 * time.Minute).UnixMilli()

	t.Run("guarded: the stale duplicate is ignored, decision stays correct", func(t *testing.T) {
		fc := newFakeClock(epoch)
		ts := newTimeSync(fc.Now, 10000)
		ts.Connect()

		handleBeacon(discardLogger(), ts, fakeBeaconMessage{nowMS: staleNowMS, duplicate: true})
		if ready, _, _ := decide(ts.Snapshot(), fc.Now()); ready {
			t.Fatal("precondition: must still be holding — the duplicate must not have released it")
		}
		// No genuine beacon arrives before the deadline: fail open on the
		// last-known offset, which the duplicate never touched (still 0).
		fc.Advance(10 * time.Second)
		ready, syncedNow, deadlineHit := decide(ts.Snapshot(), fc.Now())
		if !ready || !deadlineHit {
			t.Fatalf("expected the deadline fail-open path, got ready=%v deadlineHit=%v", ready, deadlineHit)
		}
		code, msg := result(cmd, syncedNow.UnixMilli())
		if code != codeExpired {
			t.Fatalf("guarded path: code=%d (%s), want %d (codeExpired) — a genuinely stale command must still 498", code, msg, codeExpired)
		}
	})

	t.Run("mutation check: skipping the Duplicate() guard reproduces the false-200 bug", func(t *testing.T) {
		// Simulates main.go with handleBeacon's `if msg.Duplicate() { return }`
		// removed: the stale sample is fed straight into ts.Beacon.
		fc := newFakeClock(epoch)
		ts := newTimeSync(fc.Now, 10000)
		ts.Connect()
		ts.Beacon(staleNowMS) // no Duplicate() check — the pre-fix behavior

		ready, syncedNow, _ := decide(ts.Snapshot(), fc.Now())
		if !ready {
			t.Fatal("a beacon (even a stale one) releases the hold immediately")
		}
		code, msg := result(cmd, syncedNow.UnixMilli())
		if code != codeOK {
			t.Fatalf("mutation check failed: without the Duplicate() guard, got code=%d (%s), want codeOK (%d) — "+
				"the bug did not reproduce, so this test cannot distinguish the guard from a no-op", code, msg, codeOK)
		}
	})
}
