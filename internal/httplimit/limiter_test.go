package httplimit

import (
	"testing"
	"time"
)

func TestTokenBucketRefills(t *testing.T) {
	now := time.Unix(1_000, 0)
	l := newWithClock(10, time.Minute, func() time.Time { return now })
	policy := Policy{RatePerSecond: 2, Burst: 2, PerCallerConcurrent: 10, GlobalConcurrent: 10}

	for i := 0; i < 2; i++ {
		release, _, ok := l.Acquire("write", "m1", policy)
		if !ok {
			t.Fatalf("burst request %d rejected", i)
		}
		release()
	}
	if _, retry, ok := l.Acquire("write", "m1", policy); ok || retry != 500*time.Millisecond {
		t.Fatalf("empty bucket = ok %v retry %s, want false and 500ms", ok, retry)
	}
	now = now.Add(500 * time.Millisecond)
	if release, _, ok := l.Acquire("write", "m1", policy); !ok {
		t.Fatal("one replenished token was rejected")
	} else {
		release()
	}
}

func TestConcurrencyIsBoundPerCallerAndGlobally(t *testing.T) {
	now := time.Unix(1_000, 0)
	l := newWithClock(10, time.Minute, func() time.Time { return now })
	policy := Policy{Burst: 100, PerCallerConcurrent: 1, GlobalConcurrent: 2}

	release1, _, ok := l.Acquire("transfer", "m1", policy)
	if !ok {
		t.Fatal("first transfer rejected")
	}
	if _, _, ok := l.Acquire("transfer", "m1", policy); ok {
		t.Fatal("second transfer for one caller exceeded its concurrency limit")
	}
	release2, _, ok := l.Acquire("transfer", "m2", policy)
	if !ok {
		t.Fatal("second caller should fit the global limit")
	}
	if _, _, ok := l.Acquire("transfer", "m3", policy); ok {
		t.Fatal("third caller exceeded the global concurrency limit")
	}
	release1()
	if release3, _, ok := l.Acquire("transfer", "m3", policy); !ok {
		t.Fatal("released global slot was not reusable")
	} else {
		release3()
	}
	release2()
}

func TestCallerStateIsBoundedAndIdleEntriesAreReplaced(t *testing.T) {
	now := time.Unix(1_000, 0)
	l := newWithClock(2, time.Minute, func() time.Time { return now })
	policy := Policy{RatePerSecond: 1, Burst: 1, PerCallerConcurrent: 1, GlobalConcurrent: 10}
	for _, caller := range []string{"m1", "m2"} {
		release, _, ok := l.Acquire("read", caller, policy)
		if !ok {
			t.Fatalf("%s rejected", caller)
		}
		release()
	}
	now = now.Add(2 * time.Minute)
	if release, _, ok := l.Acquire("read", "m3", policy); !ok {
		t.Fatal("new caller was rejected instead of evicting idle state")
	} else {
		release()
	}
	if len(l.buckets) > 2 {
		t.Fatalf("bucket map grew to %d, want at most 2", len(l.buckets))
	}
}
