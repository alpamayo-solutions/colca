package engine

import (
	"sync"
	"time"

	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// unboundMetricReminder is how long before an unbound path is logged again, the
// same interval as linkReminderInterval: a connector publishing before its
// catalogue is bound should not log once per sample.
const unboundMetricReminder = 5 * time.Minute

// unboundMetricLogCap bounds the tracker's memory. Past the cap an untracked path
// is logged every time instead of dropped, so visibility degrades to no rate
// limit, never to silence.
const unboundMetricLogCap = 4096

// unboundMetricLog remembers per path when an unbound _Metric was last logged.
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

// checkMetricBinding counts and logs a _Metric accepted on a path with no _Signal,
// which would never show up as a signal. It runs after the write is persisted, so
// it never delays one.
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
