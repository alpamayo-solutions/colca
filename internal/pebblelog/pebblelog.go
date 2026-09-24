// Package pebblelog routes a Pebble database's logging into slog and tracks
// whether its background work (flushes, compactions) is failing.
//
// Pebble retries a failed flush straight away, with no backoff, so a full disk
// raises the same background error dozens of times a second. The retry loop
// runs inside Pebble with the database lock held, so it cannot be slowed from
// here; the monitor only keeps the log readable and the state visible.
package pebblelog

import (
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/cockroachdb/pebble/v2"
)

// Window is how long repeated background errors stay suppressed before the next
// one is logged, carrying the count of the ones in between.
const Window = 30 * time.Second

// State is the condition of a database's background work.
type State string

const (
	// StateOK: no background error since the last successful flush.
	StateOK State = "ok"
	// StateFailing: a flush or compaction failed and no flush has succeeded since.
	StateFailing State = "failing"
)

// Status is a snapshot of a Monitor, safe to read at any time.
type Status struct {
	State State `json:"state"`
	// Since is when the database started failing, zero while it is ok.
	Since time.Time `json:"since,omitzero"`
	// Error is the latest background error, empty while ok.
	Error string `json:"error,omitempty"`
}

// Monitor is one database's Pebble logger and event listener.
type Monitor struct {
	log *slog.Logger
	now func() time.Time

	mu         sync.Mutex
	status     Status
	lastLogged time.Time
	suppressed int
}

// New returns a monitor whose log lines carry db as the database name.
func New(db string) *Monitor {
	return &Monitor{
		log:    slog.Default().With("comp", "pebble", "db", db),
		now:    time.Now,
		status: Status{State: StateOK},
	}
}

// Options returns Pebble options that log through m and report to it.
func (m *Monitor) Options() *pebble.Options {
	return &pebble.Options{
		Logger: logger{m.log},
		EventListener: &pebble.EventListener{
			BackgroundError: m.backgroundError,
			FlushEnd: func(info pebble.FlushInfo) {
				if info.Err == nil {
					m.recovered()
				}
			},
		},
	}
}

// Status returns the current condition of the database's background work.
func (m *Monitor) Status() Status {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.status
}

func (m *Monitor) backgroundError(err error) {
	now := m.now()
	m.mu.Lock()
	if m.status.State != StateFailing {
		m.status = Status{State: StateFailing, Since: now.UTC()}
	}
	m.status.Error = err.Error()
	if !m.lastLogged.IsZero() && now.Sub(m.lastLogged) < Window {
		m.suppressed++
		m.mu.Unlock()
		return
	}
	suppressed := m.suppressed
	m.lastLogged, m.suppressed = now, 0
	m.mu.Unlock()

	attrs := []any{"err", err}
	if suppressed > 0 {
		attrs = append(attrs, "repeats_suppressed", suppressed)
	}
	m.log.Error("storage background error", attrs...)
}

func (m *Monitor) recovered() {
	m.mu.Lock()
	if m.status.State == StateOK {
		m.mu.Unlock()
		return
	}
	since, suppressed := m.status.Since, m.suppressed
	// lastLogged stays, so a flush that succeeds between two failures does not
	// let the next failure past the window.
	m.status = Status{State: StateOK}
	m.suppressed = 0
	m.mu.Unlock()

	attrs := []any{"failing_since", since}
	if suppressed > 0 {
		attrs = append(attrs, "repeats_suppressed", suppressed)
	}
	m.log.Info("storage background work recovered", attrs...)
}

type logger struct{ log *slog.Logger }

func (l logger) Infof(format string, args ...any) { l.log.Info(fmt.Sprintf(format, args...)) }

func (l logger) Errorf(format string, args ...any) { l.log.Error(fmt.Sprintf(format, args...)) }

// Fatalf exits like Pebble's default logger: Pebble calls it on invariant
// violations and does not expect it to return.
func (l logger) Fatalf(format string, args ...any) {
	l.log.Error(fmt.Sprintf(format, args...))
	os.Exit(1)
}
