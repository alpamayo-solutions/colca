package clock

import (
	"math"
	"testing"
	"time"
)

func fixed(t time.Time) func() time.Time {
	return func() time.Time { return t }
}

// TestRootNeverLearnsAnOffset: the root's offset stays 0 whatever it is fed, and
// its sync age is 0.
func TestRootNeverLearnsAnOffset(t *testing.T) {
	base := time.UnixMilli(1_000_000)
	c := New(true, fixed(base))

	if got := c.AuthoritativeNow(); !got.Equal(base) {
		t.Fatalf("root AuthoritativeNow = %v, want %v", got, base)
	}
	// A sample far from its own clock must be ignored.
	if off := c.ApplySample(base.UnixMilli() + 999_999); off != 0 {
		t.Fatalf("root ApplySample returned offset %d, want 0", off)
	}
	if got := c.OffsetMS(); got != 0 {
		t.Fatalf("root OffsetMS = %d, want 0", got)
	}
	if got := c.AuthoritativeNow(); !got.Equal(base) {
		t.Fatalf("root AuthoritativeNow after ApplySample = %v, want unchanged %v", got, base)
	}
	if age := c.SyncAgeSeconds(base.Add(time.Hour)); age != 0 {
		t.Fatalf("root SyncAgeSeconds = %v, want 0", age)
	}
}

// TestNeverSyncedNonRootServesOwnWallClock: a non-root node without a sample has
// offset 0, so AuthoritativeNow is its raw wall clock, and its sync age is +Inf.
func TestNeverSyncedNonRootServesOwnWallClock(t *testing.T) {
	base := time.UnixMilli(5_000_000)
	c := New(false, fixed(base))

	if got := c.AuthoritativeNow(); !got.Equal(base) {
		t.Fatalf("never-synced AuthoritativeNow = %v, want raw wall clock %v", got, base)
	}
	if age := c.SyncAgeSeconds(base); !math.IsInf(age, 1) {
		t.Fatalf("never-synced SyncAgeSeconds = %v, want +Inf", age)
	}
}

// TestApplySampleComputesOffsetAndCorrectsAuthoritativeNow: the offset is now_ms
// minus the wall time at receipt, and AuthoritativeNow adds it to wall time
// afterwards.
func TestApplySampleComputesOffsetAndCorrectsAuthoritativeNow(t *testing.T) {
	wall := time.UnixMilli(10_000)              // this node's raw clock at receipt
	authorityNowMS := wall.UnixMilli() + 30_000 // authority is 30s ahead
	cur := wall
	c := New(false, func() time.Time { return cur })

	off := c.ApplySample(authorityNowMS)
	if off != 30_000 {
		t.Fatalf("offset = %d, want 30000", off)
	}
	if got := c.OffsetMS(); got != 30_000 {
		t.Fatalf("OffsetMS = %d, want 30000", got)
	}
	// Advance the raw clock by 5s; AuthoritativeNow must track wall+offset.
	cur = wall.Add(5 * time.Second)
	want := cur.Add(30 * time.Second)
	if got := c.AuthoritativeNow(); !got.Equal(want) {
		t.Fatalf("AuthoritativeNow = %v, want %v", got, want)
	}
}

// TestApplySampleLastWriteWinsNoSmoothing: a second, very different sample
// replaces the first instead of averaging with it.
func TestApplySampleLastWriteWinsNoSmoothing(t *testing.T) {
	wall := time.UnixMilli(0)
	c := New(false, fixed(wall))

	c.ApplySample(100_000) // offset 100000
	if got := c.OffsetMS(); got != 100_000 {
		t.Fatalf("first sample offset = %d, want 100000", got)
	}
	c.ApplySample(-50_000) // offset -50000 (clock behind)
	if got := c.OffsetMS(); got != -50_000 {
		t.Fatalf("second sample must fully replace the first (no smoothing): offset = %d, want -50000", got)
	}
}

// TestSyncAgeSecondsTracksElapsedTimeSinceLastSample: the gauge is wall time
// since the last accepted sample, measured against an explicit now rather than
// the injected clock.
func TestSyncAgeSecondsTracksElapsedTimeSinceLastSample(t *testing.T) {
	receiptWall := time.UnixMilli(1_000_000)
	c := New(false, fixed(receiptWall))
	c.ApplySample(receiptWall.UnixMilli()) // offset 0, synced at receiptWall

	later := receiptWall.Add(42 * time.Second)
	if age := c.SyncAgeSeconds(later); age != 42 {
		t.Fatalf("SyncAgeSeconds = %v, want 42", age)
	}
}
