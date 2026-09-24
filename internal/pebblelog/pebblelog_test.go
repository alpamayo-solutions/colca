package pebblelog

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/cockroachdb/pebble/v2/vfs/errorfs"
)

type captured struct {
	level slog.Level
	msg   string
	attrs map[string]slog.Value
}

type sink struct {
	mu      sync.Mutex
	records []captured
}

func (s *sink) Enabled(context.Context, slog.Level) bool { return true }
func (s *sink) WithAttrs([]slog.Attr) slog.Handler       { return s }
func (s *sink) WithGroup(string) slog.Handler            { return s }
func (s *sink) Handle(_ context.Context, r slog.Record) error {
	c := captured{level: r.Level, msg: r.Message, attrs: map[string]slog.Value{}}
	r.Attrs(func(a slog.Attr) bool { c.attrs[a.Key] = a.Value; return true })
	s.mu.Lock()
	s.records = append(s.records, c)
	s.mu.Unlock()
	return nil
}

func (s *sink) snapshot() []captured {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]captured(nil), s.records...)
}

func newTestMonitor(now *time.Time) (*Monitor, *sink) {
	out := &sink{}
	m := New("test")
	m.log = slog.New(out)
	m.now = func() time.Time { return *now }
	return m, out
}

func TestBackgroundErrorLogsOnceAtErrorThenSummarises(t *testing.T) {
	now := time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC)
	m, out := newTestMonitor(&now)
	listener := m.Options().EventListener
	diskFull := errors.New("write 000042.sst: no space left on device")

	for range 3000 {
		listener.BackgroundError(diskFull)
		now = now.Add(time.Millisecond)
	}
	records := out.snapshot()
	if len(records) != 1 || records[0].level != slog.LevelError {
		t.Fatalf("want one ERROR line for a burst inside the window, got %+v", records)
	}
	st := m.Status()
	if st.State != StateFailing || st.Error != diskFull.Error() || st.Since.IsZero() {
		t.Fatalf("want failing with the error, got %+v", st)
	}

	now = now.Add(Window)
	listener.BackgroundError(diskFull)
	records = out.snapshot()
	if len(records) != 2 || records[1].level != slog.LevelError {
		t.Fatalf("want a second ERROR line after the window, got %+v", records)
	}
	if got := records[1].attrs["repeats_suppressed"].Int64(); got != 2999 {
		t.Fatalf("want repeats_suppressed=2999, got %d", got)
	}
}

func TestSuccessfulFlushRecovers(t *testing.T) {
	now := time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC)
	m, out := newTestMonitor(&now)
	listener := m.Options().EventListener

	listener.FlushEnd(pebble.FlushInfo{Done: true})
	if len(out.snapshot()) != 0 {
		t.Fatalf("a flush while healthy must not log: %+v", out.snapshot())
	}
	listener.BackgroundError(errors.New("no space left on device"))
	listener.BackgroundError(errors.New("no space left on device"))
	listener.FlushEnd(pebble.FlushInfo{Done: true, Err: errors.New("still full")})
	if m.Status().State != StateFailing {
		t.Fatal("a failed flush must not recover")
	}
	listener.FlushEnd(pebble.FlushInfo{Done: true})
	if st := m.Status(); st.State != StateOK || st.Error != "" || !st.Since.IsZero() {
		t.Fatalf("want ok after a successful flush, got %+v", st)
	}
	records := out.snapshot()
	last := records[len(records)-1]
	if last.level != slog.LevelInfo || last.attrs["repeats_suppressed"].Int64() != 1 {
		t.Fatalf("want an INFO recovery line carrying the suppressed count, got %+v", last)
	}

	// A failure right after recovery is still inside the window of the last line.
	listener.BackgroundError(errors.New("no space left on device"))
	if n := len(out.snapshot()); n != len(records) {
		t.Fatalf("a failure inside the window must not log again, got %d lines", n)
	}
}

// TestPebbleReportsFullDisk runs a real Pebble database on a filesystem that
// refuses new tables, the way a full disk does.
func TestPebbleReportsFullDisk(t *testing.T) {
	var full atomic.Bool
	fs := errorfs.Wrap(vfs.NewMem(), errorfs.InjectorFunc(func(op errorfs.Op) error {
		if full.Load() && op.Kind == errorfs.OpCreate && strings.HasSuffix(op.Path, ".sst") {
			return syscall.ENOSPC
		}
		return nil
	}))
	now := time.Now()
	m, out := newTestMonitor(&now)
	m.now = time.Now
	opts := m.Options()
	opts.FS = fs
	db, err := pebble.Open("db", opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	full.Store(true)
	if err := db.Set([]byte("k"), []byte("v"), pebble.NoSync); err != nil {
		t.Fatal(err)
	}
	if _, err := db.AsyncFlush(); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return m.Status().State == StateFailing })
	if !strings.Contains(m.Status().Error, "no space left on device") {
		t.Fatalf("want the ENOSPC error in the status, got %+v", m.Status())
	}
	var errorLines int
	for _, r := range out.snapshot() {
		if r.msg == "storage background error" {
			if r.level != slog.LevelError {
				t.Fatalf("want background errors at ERROR, got %+v", r)
			}
			errorLines++
		}
	}
	if errorLines != 1 {
		t.Fatalf("want one ERROR line for Pebble's retry loop, got %d", errorLines)
	}

	full.Store(false)
	waitFor(t, func() bool { return m.Status().State == StateOK })
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not reached within 10s")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
