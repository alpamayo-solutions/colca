// Package clock implements the node-local authoritative-time offset state
// described in the time sync move drain design
// §2.1: the root node (no configured parent) is the time authority; every
// other node maintains offset_ms, the signed difference between the
// authority's clock and its own, learned from the most recent /downlink or
// /replicate response and applied with no smoothing (last sample wins), so
// corrections telescope down the tree.
//
// This is a separate package rather than fields on *engine.Engine so the
// metrics package — which cannot import engine (engine already imports
// metrics) — can read the exact same live state a scrape-time gauge needs,
// without introducing an import cycle.
package clock

import (
	"math"
	"sync"
	"time"
)

// Clock is one node's authoritative-time state: thread-safe, with an
// injectable wall clock (mandatory for every new decision path here — no
// bare time.Now() calls).
type Clock struct {
	now    func() time.Time
	isRoot bool

	mu           sync.Mutex
	offsetMS     int64
	synced       bool
	lastSyncWall time.Time
}

// New builds a Clock. isRoot marks the time authority (design §2.1: "a node
// with no parent configured is the root/authority") — it never learns an
// offset, so AuthoritativeNow always returns its own raw wall clock. now is
// the injectable clock; production callers pass time.Now, tests pass a
// fake.
func New(isRoot bool, now func() time.Time) *Clock {
	return &Clock{now: now, isRoot: isRoot}
}

// IsRoot reports whether this is the time authority.
func (c *Clock) IsRoot() bool { return c.isRoot }

// Now returns the node's raw, uncorrected wall clock (the injected clock
// itself) — the reading offset samples are measured AGAINST, never the
// corrected estimate (design §2.1: "offset_ms = now_ms − wall_receipt_time").
func (c *Clock) Now() time.Time { return c.now() }

// AuthoritativeNow is this node's current best estimate of the authority's
// clock: wall_now + offset_ms (design §2.1). The root's offset is always 0
// (it never learns one), so this is its raw wall clock; a non-root node
// that has never synced also returns its raw wall clock (offset 0) — "best
// effort" per §2.1.
func (c *Clock) AuthoritativeNow() time.Time {
	c.mu.Lock()
	offset := c.offsetMS
	c.mu.Unlock()
	return c.now().Add(time.Duration(offset) * time.Millisecond)
}

// ApplySample records one offset sample learned from a parent's now_ms
// (design §2.1/§2.3 rule 4): offset = now_ms − wall_receipt, last sample
// wins, no smoothing — sample noise is one-way network latency
// (milliseconds), irrelevant at the drift scale this rule targets. A no-op
// on the root: design §2.1 — the authority never learns an offset from
// anyone (defensive; production call sites never invoke this on a root
// node, since a root has no parent client to receive a response from).
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

// SyncAgeSeconds returns time since the last accepted sample, evaluated
// against now (an explicit parameter, not c.now(), so a metrics scrape can
// use its own wall reading independent of the injected decision clock). The
// root exports 0 by definition (design §2.4); a non-root node that has
// never synced exports +Inf ("never synced").
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
