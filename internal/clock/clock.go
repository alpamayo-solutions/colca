// Package clock keeps a node's offset to the authoritative time. The root node,
// with no parent, is the authority; every other node keeps offset_ms, the
// difference between the authority's clock and its own, taken from the latest
// /downlink or /replicate response without smoothing, so corrections carry down
// the tree. It is its own package so metrics can read it without importing
// engine.
package clock

import (
	"math"
	"sync"
	"time"
)

// Clock is one node's authoritative-time state. It is safe for concurrent use
// and takes an injectable wall clock; decision code never calls time.Now
// directly.
type Clock struct {
	now    func() time.Time
	isRoot bool

	mu           sync.Mutex
	offsetMS     int64
	synced       bool
	lastSyncWall time.Time
}

// New builds a Clock. isRoot marks the time authority, which never learns an
// offset. now is the wall clock: time.Now in production, a fake in tests.
func New(isRoot bool, now func() time.Time) *Clock {
	return &Clock{now: now, isRoot: isRoot}
}

// IsRoot reports whether this is the time authority.
func (c *Clock) IsRoot() bool { return c.isRoot }

// Now returns the node's raw wall clock, the reading samples are measured
// against.
func (c *Clock) Now() time.Time { return c.now() }

// AuthoritativeNow is this node's best estimate of the authority's clock, wall
// time plus offset_ms. On the root, or before the first sample, that is the raw
// wall clock.
func (c *Clock) AuthoritativeNow() time.Time {
	c.mu.Lock()
	offset := c.offsetMS
	c.mu.Unlock()
	return c.now().Add(time.Duration(offset) * time.Millisecond)
}

// ApplySample records the offset learned from a parent's now_ms, which is now_ms
// minus the wall time at receipt. The last sample wins with no smoothing: sample
// noise is network latency, far below the drift this corrects. On the root it
// does nothing.
func (c *Clock) ApplySample(nowMS int64) (offsetMS int64) {
	if c.isRoot {
		return 0
	}
	wallReceipt := c.now()
	offsetMS = nowMS - wallReceipt.UnixMilli()
	c.mu.Lock()
	c.offsetMS = offsetMS
	c.synced = true
	c.lastSyncWall = wallReceipt
	c.mu.Unlock()
	return offsetMS
}

// OffsetMS returns the current offset estimate: 0 on the root and on a
// non-root node that has never synced.
func (c *Clock) OffsetMS() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.offsetMS
}

// SyncAgeSeconds returns the time since the last accepted sample, measured
// against now so a metrics scrape can use its own reading. The root reports 0;
// a node that never synced reports +Inf.
func (c *Clock) SyncAgeSeconds(now time.Time) float64 {
	if c.isRoot {
		return 0
	}
	c.mu.Lock()
	synced, last := c.synced, c.lastSyncWall
	c.mu.Unlock()
	if !synced {
		return math.Inf(1)
	}
	return now.Sub(last).Seconds()
}
