package clock

import (
	"testing"
	"time"

	"github.com/alpamayo-solutions/colca/internal/hostclock"
)

type fakeNow struct{ t time.Time }

func (f *fakeNow) now() time.Time { return f.t }

func unreported() hostclock.Status { return hostclock.Status{} }

// The skew is the parent's now_ms minus this node's clock at receipt, signed,
// and the node's own clock is not moved by it.
func TestObserveParentMeasuresTheSkewAndCorrectsNothing(t *testing.T) {
	wall := &fakeNow{t: time.UnixMilli(1_700_000_000_000)}
	c := NewWithHost(false, wall.now, unreported)

	if _, ok := c.ParentSkew(); ok {
		t.Fatal("a node that never heard from its parent has no skew to report")
	}
	if _, ok := c.ParentSampleAge(wall.t); ok {
		t.Fatal("a node that never heard from its parent has no sample age to report")
	}

	skew, _ := c.ObserveParent(wall.t.UnixMilli() + 1500)
	if skew != 1500*time.Millisecond {
		t.Fatalf("skew = %v, want 1.5s (parent ahead)", skew)
	}
	if got := c.Now(); !got.Equal(wall.t) {
		t.Fatalf("Now = %v after a parent sample, want the unchanged wall clock %v", got, wall.t)
	}

	c.ObserveParent(wall.t.UnixMilli() - 250)
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
		_, tr := c.ObserveParent(wall.t.Add(skew).UnixMilli())
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
	if _, tr := root.ObserveParent(wall.t.UnixMilli() + 60_000); tr != Unchanged {
		t.Fatalf("root transition = %v, want Unchanged", tr)
	}
	if _, ok := root.ParentSkew(); ok {
		t.Fatal("the root must never report a parent skew")
	}

	child := NewWithHost(false, wall.now, unreported)
	if _, tr := child.ObserveParent(0); tr != Unchanged {
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
