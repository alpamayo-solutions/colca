package clock

import (
	"testing"
	"time"

	"github.com/alpamayo-solutions/colca/internal/hostclock"
)

type fakeNow struct{ t time.Time }

func (f *fakeNow) now() time.Time { return f.t }

func unreported() hostclock.Status { return hostclock.Status{} }

// instant is a sample whose request and response took no time at at: the
// parent's now_ms is then exactly comparable with this node's clock.
func instant(nowMS int64, at time.Time) ParentSample {
	return ParentSample{NowMS: nowMS, Sent: at, Received: at}
}

// With no round trip to account for, the skew is the parent's now_ms minus this
// node's clock at receipt, signed, and the node's own clock is not moved by it.
func TestObserveParentMeasuresTheSkewAndCorrectsNothing(t *testing.T) {
	wall := &fakeNow{t: time.UnixMilli(1_700_000_000_000)}
	c := NewWithHost(false, wall.now, unreported)

	if _, ok := c.ParentSkew(); ok {
		t.Fatal("a node that never heard from its parent has no skew to report")
	}
	if _, ok := c.ParentSampleAge(wall.t); ok {
		t.Fatal("a node that never heard from its parent has no sample age to report")
	}

	skew, _, _ := c.ObserveParent(instant(wall.t.UnixMilli()+1500, wall.t))
	if skew != 1500*time.Millisecond {
		t.Fatalf("skew = %v, want 1.5s (parent ahead)", skew)
	}
	if got := c.Now(); !got.Equal(wall.t) {
		t.Fatalf("Now = %v after a parent sample, want the unchanged wall clock %v", got, wall.t)
	}

	c.ObserveParent(instant(wall.t.UnixMilli()-250, wall.t))
	if got, ok := c.ParentSkew(); !ok || got != -250*time.Millisecond {
		t.Fatalf("ParentSkew = %v, %v, want -250ms (parent behind): the latest sample wins", got, ok)
	}

	wall.t = wall.t.Add(42 * time.Second)
	if age, ok := c.ParentSampleAge(wall.t); !ok || age != 42*time.Second {
		t.Fatalf("ParentSampleAge = %v, %v, want 42s", age, ok)
	}
}

// Only a move across SkewAlarm is a transition, in either direction and for
// either sign of the skew.
func TestObserveParentReportsOnlyTheCrossing(t *testing.T) {
	wall := &fakeNow{t: time.UnixMilli(1_700_000_000_000)}
	c := NewWithHost(false, wall.now, unreported)
	at := func(skew time.Duration) Transition {
		_, tr, _ := c.ObserveParent(instant(wall.t.Add(skew).UnixMilli(), wall.t))
		return tr
	}
	for i, step := range []struct {
		skew time.Duration
		want Transition
	}{
		{100 * time.Millisecond, Unchanged},
		{SkewAlarm, Unchanged}, // exactly at the threshold is still within it
		{SkewAlarm + time.Millisecond, Exceeded},
		{time.Minute, Unchanged},
		{-time.Minute, Unchanged}, // still beyond, on the other side
		{-SkewAlarm, Recovered},
		{0, Unchanged},
		{-SkewAlarm - time.Millisecond, Exceeded},
	} {
		if got := at(step.skew); got != step.want {
			t.Fatalf("step %d (skew %v): transition = %v, want %v", i, step.skew, got, step.want)
		}
	}
}

// The root has no parent: a sample is ignored. A now_ms that is not a time (a
// response without the field decodes to 0) is no sample either, on any node.
func TestObserveParentIgnoresTheRootAndAMissingNowMS(t *testing.T) {
	wall := &fakeNow{t: time.UnixMilli(1_700_000_000_000)}
	root := NewWithHost(true, wall.now, unreported)
	if _, tr, used := root.ObserveParent(instant(wall.t.UnixMilli()+60_000, wall.t)); tr != Unchanged || used {
		t.Fatalf("root transition = %v, want Unchanged", tr)
	}
	if _, ok := root.ParentSkew(); ok {
		t.Fatal("the root must never report a parent skew")
	}

	child := NewWithHost(false, wall.now, unreported)
	if _, tr, used := child.ObserveParent(instant(0, wall.t)); tr != Unchanged || used {
		t.Fatalf("transition for now_ms 0 = %v, want Unchanged: it is not a 56-year skew", tr)
	}
	if _, ok := child.ParentSkew(); ok {
		t.Fatal("now_ms 0 must not be recorded as a sample")
	}
}

func TestHostReturnsTheInjectedReading(t *testing.T) {
	want := hostclock.Status{Reported: true, Synchronized: true, EstimatedError: time.Millisecond}
	c := NewWithHost(true, time.Now, func() hostclock.Status { return want })
	if got := c.Host(); got != want {
		t.Fatalf("Host = %+v, want %+v", got, want)
	}
}

// The parent stamped now_ms somewhere inside the round trip, so the sample
// takes its middle: a request that left at T and whose response was read at
// T+400ms, from a parent with the same clock that answered at once, measures a
// skew within half the round trip of zero wherever in it the stamp was taken.
func TestObserveParentTakesTheMiddleOfTheRoundTrip(t *testing.T) {
	sent := time.UnixMilli(1_700_000_000_000)
	received := sent.Add(400 * time.Millisecond)
	for _, stampedAfter := range []time.Duration{0, 150 * time.Millisecond, 400 * time.Millisecond} {
		c := NewWithHost(false, time.Now, unreported)
		skew, _, used := c.ObserveParent(ParentSample{NowMS: sent.Add(stampedAfter).UnixMilli(), Sent: sent, Received: received})
		if !used || skew.Abs() > 200*time.Millisecond {
			t.Fatalf("stamp %v into a 400ms round trip: skew = %v, used = %v, want within 200ms of zero", stampedAfter, skew, used)
		}
	}

	// A parent 2s ahead, stamped in the middle: exactly 2s.
	c := NewWithHost(false, time.Now, unreported)
	skew, _, _ := c.ObserveParent(ParentSample{NowMS: sent.Add(2200 * time.Millisecond).UnixMilli(), Sent: sent, Received: received})
	if skew != 2*time.Second {
		t.Fatalf("skew = %v, want 2s: the parent's stamp against the middle of the round trip", skew)
	}
}

// The time the parent held the request (a long poll's wait) is not round trip:
// a poll answered after 20s over a fast link is as good a sample as one
// answered at once, and the parent's stamp is compared with the end of the
// hold.
func TestObserveParentSubtractsTheParentsHold(t *testing.T) {
	sent := time.UnixMilli(1_700_000_000_000)
	const hold, link = 20 * time.Second, 10 * time.Millisecond
	received := sent.Add(hold + link)
	c := NewWithHost(false, time.Now, unreported)
	// Same clock on both nodes: the parent stamped 5ms after the request left
	// plus its hold.
	skew, _, used := c.ObserveParent(ParentSample{
		NowMS: sent.Add(hold + link/2).UnixMilli(), Held: hold, Sent: sent, Received: received,
	})
	if !used || skew != 0 {
		t.Fatalf("skew = %v, used = %v, want a used sample of 0: the hold is the parent's, not the link's", skew, used)
	}
}

// A response that waited on a stalled link says nothing about the clocks. Both
// hosts are on one clock; the uplink stalls for 6s while the response is on
// its way. Read as skew that is 6s behind and an alarm; the sample is
// discarded instead and the previous measurement stands.
func TestAStalledResponseIsNotSkew(t *testing.T) {
	wall := &fakeNow{t: time.UnixMilli(1_700_000_000_000)}
	c := NewWithHost(false, wall.now, unreported)
	c.ObserveParent(instant(wall.t.UnixMilli()+12, wall.t)) // the measurement before the stall

	sent := wall.t.Add(30 * time.Second)
	const hold, stall = 20 * time.Second, 6 * time.Second
	stalled := ParentSample{NowMS: sent.Add(hold).UnixMilli(), Held: hold, Sent: sent, Received: sent.Add(hold + stall)}
	skew, tr, used := c.ObserveParent(stalled)
	if used || tr != Unchanged || skew != 0 {
		t.Fatalf("stalled response: skew = %v, transition = %v, used = %v, want it discarded", skew, tr, used)
	}
	if got, ok := c.ParentSkew(); !ok || got != 12*time.Millisecond {
		t.Fatalf("ParentSkew = %v, %v after a discarded sample, want the previous 12ms", got, ok)
	}
	// The age is of the measurement, so a link that only yields stalled
	// responses shows as a measurement growing old.
	if age, ok := c.ParentSampleAge(stalled.Received); !ok || age != 30*time.Second+hold+stall {
		t.Fatalf("ParentSampleAge = %v, %v, want the age of the last used sample", age, ok)
	}

	// The same stall with no measurement before it leaves the node without one.
	fresh := NewWithHost(false, wall.now, unreported)
	fresh.ObserveParent(stalled)
	if _, ok := fresh.ParentSkew(); ok {
		t.Fatal("a discarded first sample must not make a skew appear")
	}
}

// The bound is MaxRoundTrip, the warning threshold: a sample is off by at most
// half its round trip, so one at the bound is still used and one past it is
// not. A parent that does not report its hold has the whole exchange counted.
func TestObserveParentDiscardsARoundTripBeyondTheWarningThreshold(t *testing.T) {
	if MaxRoundTrip != SkewWarn {
		t.Fatalf("MaxRoundTrip = %v, want SkewWarn (%v): the bound follows the smallest alert threshold", MaxRoundTrip, SkewWarn)
	}
	sent := time.UnixMilli(1_700_000_000_000)
	for _, tc := range []struct {
		name     string
		exchange time.Duration
		held     time.Duration
		wantUsed bool
	}{
		{"at the bound", MaxRoundTrip, 0, true},
		{"past the bound", MaxRoundTrip + time.Millisecond, 0, false},
		{"long poll, short link", 20*time.Second + MaxRoundTrip, 20 * time.Second, true},
		{"long poll, hold not reported", 20 * time.Second, 0, false},
		// Two hosts' clocks measured the two: a hold a little over the exchange
		// is a round trip too short to measure.
		{"hold just over the exchange", 20 * time.Second, 20*time.Second + 2*time.Millisecond, true},
		{"negative hold counts as none", MaxRoundTrip + time.Millisecond, -time.Second, false},
	} {
		c := NewWithHost(false, time.Now, unreported)
		received := sent.Add(tc.exchange)
		_, _, used := c.ObserveParent(ParentSample{NowMS: received.UnixMilli(), Held: tc.held, Sent: sent, Received: received})
		if used != tc.wantUsed {
			t.Errorf("%s: used = %v, want %v", tc.name, used, tc.wantUsed)
		}
	}
}

// A real skew over a short round trip is reported, alarm included: 6s ahead
// over 40ms is 6s, not a slow link.
func TestARealSkewWithAShortRoundTripIsReported(t *testing.T) {
	sent := time.UnixMilli(1_700_000_000_000)
	received := sent.Add(40 * time.Millisecond)
	c := NewWithHost(false, time.Now, unreported)
	skew, tr, used := c.ObserveParent(ParentSample{NowMS: sent.Add(6*time.Second + 20*time.Millisecond).UnixMilli(), Sent: sent, Received: received})
	if !used || skew != 6*time.Second || tr != Exceeded {
		t.Fatalf("skew = %v, transition = %v, used = %v, want a used 6s sample that crosses the alarm", skew, tr, used)
	}
}

// A hold that comes out longer than the exchange leaves no round trip to take
// the middle of: the parent's stamp is compared with the time of receipt.
func TestAHoldLongerThanTheExchangeLeavesNoRoundTrip(t *testing.T) {
	sent := time.UnixMilli(1_700_000_000_000)
	received := sent.Add(20 * time.Second)
	c := NewWithHost(false, time.Now, unreported)
	skew, _, used := c.ObserveParent(ParentSample{
		NowMS: received.Add(30 * time.Millisecond).UnixMilli(), Held: 20*time.Second + 2*time.Millisecond,
		Sent: sent, Received: received,
	})
	if !used || skew != 30*time.Millisecond {
		t.Fatalf("skew = %v, used = %v, want a used sample of 30ms", skew, used)
	}
}
