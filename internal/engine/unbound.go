package engine

import (
	"sync"
	"time"

	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// unboundMetricReminder is how long a path may keep arriving with no _Signal
// before it is logged again — same rationale and interval as
// internal/repl/linkstate.go's linkReminderInterval: an expected startup
// window (a connector publishing before its own catalogue has bound) must
// not turn into a log line per sample.
const unboundMetricReminder = 5 * time.Minute

// unboundMetricLogCap bounds the tracker's memory the same way every other
// per-lane log tracker in this codebase is bounded ("Every
// publisher is asynchronous, bounded..."): an unbounded number of distinct
// never-bound paths must cost bounded memory. Past the cap, a path that is
// not already tracked is logged every time instead of being silently
// dropped from tracking — visibility degrades to "no rate limit for the
// overflow", never to silence.
const unboundMetricLogCap = 4096

// unboundMetricLog tracks, per path, when a "_Metric with no _Signal" line
// was last logged, so a connector publishing ahead of its own catalogue
// binding does not flood the log once per sample.
type unboundMetricLog struct {
	mu   sync.Mutex
	seen map[string]time.Time
}

func newUnboundMetricLog() *unboundMetricLog {
	return &unboundMetricLog{seen: map[string]time.Time{}}
}

// shouldLog reports whether path has not been logged within the reminder
// window, and records now against it when it has not.
func (u *unboundMetricLog) shouldLog(path string, now time.Time) bool {
	u.mu.Lock()
	defer u.mu.Unlock()
	if last, ok := u.seen[path]; ok {
		if now.Sub(last) < unboundMetricReminder {
			return false
		}
		u.seen[path] = now
		return true
	}
	if len(u.seen) >= unboundMetricLogCap {
		// At the cap: do not start tracking this path (nothing already
		// tracked is evicted mid-window), but still report it this once.
		return true
	}
	u.seen[path] = now
	return true
}

// checkMetricBinding is SDK design §7 gap 6: a _Metric accepted on a path
// with no _Signal there is a valid, authorized write that will never surface
// as a signal in the editor or replicate as a governed entity — today a
// silent, invisible write. Called after a _Metric is durably persisted, so a
// miss here never blocks or delays a write; it only misses telling someone
// about one.
func (e *Engine) checkMetricBinding(p uns.Parsed) {
	if !uns.IsMetric(p.Contract) {
		return
	}
	if _, ok := e.EntityStore().KVGet(uns.SignalTopicForMetric(p)); ok {
		return
	}
	e.metrics.MetricUnbound()
	if e.unboundLog.shouldLog(p.Path, e.clk.Now()) {
		e.log.Warn("_Metric accepted on a path with no _Signal — it will not appear as a signal until something binds it",
			"path", p.Path)
	}
}
