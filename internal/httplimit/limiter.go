// Package httplimit provides a small in-process request limiter for Colca's
// HTTP doors. It deliberately has no external state: each Colca node is its
// own availability boundary, so a local limiter remains effective during the
// network and control-plane failures the node is supposed to survive.
package httplimit

import (
	"math"
	"sync"
	"time"
)

// Policy bounds one route class. Rate and Burst form a per-caller token
// bucket; PerCallerConcurrent and GlobalConcurrent bound in-flight work. A
// zero field disables only that dimension.
type Policy struct {
	RatePerSecond       float64
	Burst               int
	PerCallerConcurrent int
	GlobalConcurrent    int
}

const (
	defaultMaxCallers = 16_384
	defaultIdleTTL    = 10 * time.Minute
)

type bucket struct {
	tokens   float64
	last     time.Time
	lastSeen time.Time
	active   int
}

// Limiter bounds its own caller map as well as the requests passing through
// it. The map key is route class + authenticated identity (or source address
// on an unauthenticated/local door).
type Limiter struct {
	mu         sync.Mutex
	buckets    map[string]*bucket
	global     map[string]int
	maxCallers int
	idleTTL    time.Duration
	now        func() time.Time
	lastSweep  time.Time
}

// New returns a limiter with bounded caller state and idle eviction.
func New() *Limiter {
	return newWithClock(defaultMaxCallers, defaultIdleTTL, time.Now)
}

func newWithClock(maxCallers int, idleTTL time.Duration, now func() time.Time) *Limiter {
	return &Limiter{
		buckets: make(map[string]*bucket), global: make(map[string]int),
		maxCallers: maxCallers, idleTTL: idleTTL, now: now,
	}
}

// Reason says which bound refused a request. Only a rate refusal clears by
// itself after a known delay; a concurrency refusal clears when the caller (or,
// for the global bound, anyone) finishes a request it holds open, which for a
// long-lived /watch can be never.
type Reason string

const (
	// ReasonRate: the caller's token bucket is empty. Refusal.RetryAfter says
	// when the next token is there.
	ReasonRate Reason = "rate_limit"
	// ReasonCallerConcurrency: the caller already holds Policy.PerCallerConcurrent
	// requests of the class open.
	ReasonCallerConcurrency Reason = "concurrency_limit"
	// ReasonGlobalConcurrency: all callers together hold Policy.GlobalConcurrent
	// requests of the class open.
	ReasonGlobalConcurrency Reason = "global_concurrency_limit"
	// ReasonCallers: the limiter tracks as many callers as it may and none of
	// them is idle.
	ReasonCallers Reason = "caller_limit"
)

// Refusal describes a refused request. RetryAfter is set only for ReasonRate:
// for the other reasons no delay is known. Limit is the bound that was reached.
type Refusal struct {
	Reason     Reason
	RetryAfter time.Duration
	Limit      int
}

// Acquire admits one request or says why it did not. release must be called
// exactly once for an admitted request; it is idempotent so a defensive double
// defer cannot corrupt the concurrency count.
func (l *Limiter) Acquire(class, caller string, policy Policy) (release func(), refusal Refusal, ok bool) {
	now := l.now()
	key := class + "\x00" + caller

	l.mu.Lock()
	defer l.mu.Unlock()
	l.sweep(now)

	b := l.buckets[key]
	if b == nil {
		if len(l.buckets) >= l.maxCallers {
			l.evictOldestIdle()
		}
		if len(l.buckets) >= l.maxCallers {
			return nil, Refusal{Reason: ReasonCallers, Limit: l.maxCallers}, false
		}
		b = &bucket{tokens: float64(policy.Burst), last: now, lastSeen: now}
		l.buckets[key] = b
	}
	b.lastSeen = now
	l.refill(b, now, policy)

	if policy.PerCallerConcurrent > 0 && b.active >= policy.PerCallerConcurrent {
		return nil, Refusal{Reason: ReasonCallerConcurrency, Limit: policy.PerCallerConcurrent}, false
	}
	if policy.GlobalConcurrent > 0 && l.global[class] >= policy.GlobalConcurrent {
		return nil, Refusal{Reason: ReasonGlobalConcurrency, Limit: policy.GlobalConcurrent}, false
	}
	if policy.RatePerSecond > 0 && policy.Burst > 0 && b.tokens < 1 {
		missing := 1 - b.tokens
		delay := time.Duration(math.Ceil(missing / policy.RatePerSecond * float64(time.Second)))
		if delay < time.Millisecond {
			delay = time.Millisecond
		}
		return nil, Refusal{Reason: ReasonRate, RetryAfter: delay, Limit: policy.Burst}, false
	}

	if policy.RatePerSecond > 0 && policy.Burst > 0 {
		b.tokens--
	}
	b.active++
	l.global[class]++

	var once sync.Once
	return func() {
		once.Do(func() {
			l.mu.Lock()
			defer l.mu.Unlock()
			if current := l.buckets[key]; current != nil && current.active > 0 {
				current.active--
				current.lastSeen = l.now()
			}
			if l.global[class] > 0 {
				l.global[class]--
			}
		})
	}, Refusal{}, true
}

func (l *Limiter) refill(b *bucket, now time.Time, policy Policy) {
	if policy.RatePerSecond <= 0 || policy.Burst <= 0 {
		b.last = now
		return
	}
	if elapsed := now.Sub(b.last); elapsed > 0 {
		b.tokens = math.Min(float64(policy.Burst), b.tokens+elapsed.Seconds()*policy.RatePerSecond)
		b.last = now
	}
}

func (l *Limiter) sweep(now time.Time) {
	if !l.lastSweep.IsZero() && now.Sub(l.lastSweep) < time.Minute {
		return
	}
	for key, b := range l.buckets {
		if b.active == 0 && now.Sub(b.lastSeen) > l.idleTTL {
			delete(l.buckets, key)
		}
	}
	l.lastSweep = now
}

func (l *Limiter) evictOldestIdle() {
	var oldestKey string
	var oldest time.Time
	for key, b := range l.buckets {
		if b.active != 0 || (!oldest.IsZero() && !b.lastSeen.Before(oldest)) {
			continue
		}
		oldestKey, oldest = key, b.lastSeen
	}
	if oldestKey != "" {
		delete(l.buckets, oldestKey)
	}
}
