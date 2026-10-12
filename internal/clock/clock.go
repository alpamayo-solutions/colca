// Package clock is what a node knows about time. A node has one time, the
// host's wall clock, which the host's NTP daemon disciplines; colca never
// corrects it. This package reads that clock for every decision that needs the
// time, and holds two measurements for monitoring: the kernel's verdict on the
// host clock (internal/hostclock) and the skew to the parent, taken from the
// now_ms on every /downlink and /replicate response and bounded by that
// request's round trip. It is its own package so metrics can read it without
// importing engine.
package clock

import (
	"sync"
	"time"

	"github.com/alpamayo-solutions/colca/internal/hostclock"
)

// SkewAlarm is the parent skew beyond which a node logs once, and once more
// when the skew is back under it. It is the scale at which a command's
// expires_at is judged differently on the two nodes.
const SkewAlarm = 5 * time.Second

// SkewWarn is the smallest parent skew anything alerts on: the warning
// threshold of the node's clock health metric and of the documented alert.
const SkewWarn = 500 * time.Millisecond

// MaxRoundTrip is the longest round trip a parent sample may have had. The
// parent stamps now_ms at one unknown moment of the round trip, so a sample is
// off by at most half of it. One whose round trip exceeds SkewWarn could be off
// by more than half the warning threshold: it cannot tell a skew of that size
// from a slow or stalled link, and is discarded.
const MaxRoundTrip = SkewWarn

// ParentSample is one reading of the parent's clock, with what bounds it.
type ParentSample struct {
	// NowMS is the parent's wall clock when it wrote the response.
	NowMS int64
	// Held is how long the parent had the request before it stamped NowMS (a
	// long poll's wait, an ingest), on the parent's monotonic clock. Zero from
	// a parent that does not say; the whole exchange then counts as round trip.
	Held time.Duration
	// Sent and Received are this node's clock before the request left and
	// after the response was read.
	Sent, Received time.Time
}

// roundTrip is the time the sample spent on the link: the exchange minus the
// parent's hold. ok is false when the readings contradict each other (the
// response before the request, a hold longer than the exchange).
func (s ParentSample) roundTrip() (rt time.Duration, ok bool) {
	rt = s.Received.Sub(s.Sent) - s.Held
	return rt, s.Held >= 0 && rt >= 0
}

// Transition says whether a parent sample moved the skew across SkewAlarm.
type Transition int

// The transitions of ObserveParent.
const (
	Unchanged Transition = iota
	Exceeded             // the skew was within SkewAlarm and now is not
	Recovered            // the skew was beyond SkewAlarm and now is within it
)

// Clock is one node's time. It is safe for concurrent use and takes an
// injectable wall clock; decision code never calls time.Now directly.
type Clock struct {
	now    func() time.Time
	host   func() hostclock.Status
	isRoot bool

	mu         sync.Mutex
	skew       time.Duration
	sampled    bool
	lastSample time.Time
	exceeded   bool
}

// New builds a Clock. isRoot marks a node without a parent, which has no skew
// to measure. now is the wall clock: time.Now in production, a fake in tests.
func New(isRoot bool, now func() time.Time) *Clock {
	return NewWithHost(isRoot, now, hostclock.Read)
}

// NewWithHost is New with the host-clock reading injected, for tests.
func NewWithHost(isRoot bool, now func() time.Time, host func() hostclock.Status) *Clock {
	return &Clock{now: now, host: host, isRoot: isRoot}
}

// IsRoot reports whether this node has no parent.
func (c *Clock) IsRoot() bool { return c.isRoot }

// Now returns the node's wall clock.
func (c *Clock) Now() time.Time { return c.now() }

// Host returns the kernel's current verdict on the host clock.
func (c *Clock) Host() hostclock.Status { return c.host() }

// ObserveParent records the skew to the parent from one of its responses. The
// parent stamped now_ms somewhere between this node sending the request and
// reading the response, less the time the parent held it; the sample assumes
// the middle of that round trip, as NTP does, so it is off by at most half the
// round trip. A positive skew means the parent's clock is ahead. It is a
// measurement only: nothing is corrected by it.
//
// used is false when nothing was recorded and the previous measurement stands:
// on the root, for a now_ms that is not a time (0, from a response without the
// field), and for a sample whose round trip exceeds MaxRoundTrip or cannot be
// determined. A response that waited on a stalled link is such a sample: the
// stall is not skew.
func (c *Clock) ObserveParent(s ParentSample) (skew time.Duration, t Transition, used bool) {
	if c.isRoot || s.NowMS <= 0 {
		return 0, Unchanged, false
	}
	rt, ok := s.roundTrip()
	if !ok || rt > MaxRoundTrip {
		return 0, Unchanged, false
	}
	skew = time.UnixMilli(s.NowMS).Sub(s.Received) + rt/2
	exceeded := skew.Abs() > SkewAlarm
	c.mu.Lock()
	was := c.exceeded
	c.skew, c.sampled, c.lastSample, c.exceeded = skew, true, s.Received, exceeded
	c.mu.Unlock()
	switch {
	case exceeded && !was:
		t = Exceeded
	case !exceeded && was:
		t = Recovered
	}
	return skew, t, true
}

// ParentSkew returns the latest skew to the parent. ok is false on the root
// and before the first usable sample.
func (c *Clock) ParentSkew() (skew time.Duration, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.skew, c.sampled
}

// ParentSampleAge returns the time since the latest parent sample, measured
// against now so a scrape can use its own reading. ok is false on the root and
// before the first sample.
func (c *Clock) ParentSampleAge(now time.Time) (age time.Duration, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.sampled {
		return 0, false
	}
	return now.Sub(c.lastSample), true
}
