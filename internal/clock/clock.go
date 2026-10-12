// Package clock is what a node knows about time. A node has one time, the
// host's wall clock, which the host's NTP daemon disciplines; colca never
// corrects it. This package reads that clock for every decision that needs the
// time, and holds two measurements for monitoring: the kernel's verdict on the
// host clock (internal/hostclock) and the skew to the parent, taken from the
// now_ms on every /downlink and /replicate response. It is its own package so
// metrics can read it without importing engine.
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

// ObserveParent records the skew to the parent from the now_ms of one of its
// responses: now_ms minus the wall time at receipt, so a positive skew means
// the parent's clock is ahead. The sample is off by the response's network
// latency. It is a measurement only: nothing is corrected by it. A now_ms that
// is not a time (0, from a response without the field) and any call on the root
// record nothing.
func (c *Clock) ObserveParent(nowMS int64) (skew time.Duration, t Transition) {
	if c.isRoot || nowMS <= 0 {
		return 0, Unchanged
	}
	receipt := c.now()
	skew = time.Duration(nowMS-receipt.UnixMilli()) * time.Millisecond
	exceeded := skew.Abs() > SkewAlarm
	c.mu.Lock()
	was := c.exceeded
	c.skew, c.sampled, c.lastSample, c.exceeded = skew, true, receipt, exceeded
	c.mu.Unlock()
	switch {
	case exceeded && !was:
		t = Exceeded
	case !exceeded && was:
		t = Recovered
	}
	return skew, t
}

// ParentSkew returns the latest skew to the parent. ok is false on the root
// and before the first sample.
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
